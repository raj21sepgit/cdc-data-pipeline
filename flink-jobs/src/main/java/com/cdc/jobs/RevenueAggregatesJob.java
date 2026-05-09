package com.cdc.jobs;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Revenue Aggregates Job  (Flink SQL)
 * =====================================
 * Uses Flink SQL to compute rolling 5-minute revenue per product category
 * from the CDC orders stream, sinking to Elasticsearch.
 *
 * This is the "Flink SQL" showcase — demonstrating that the same pipeline
 * can be expressed declaratively, which is how most modern data teams
 * write Flink at scale (SQL is more approachable for analytics engineers).
 *
 * Input  : cdc.ecommerce.orders (Kafka, CDC JSON)
 * Output : revenue-5m (Elasticsearch index)
 * Window : 5-minute tumbling window on event_time
 */
public class RevenueAggregatesJob {

    private static final String KAFKA_BOOTSTRAP = System.getenv()
            .getOrDefault("KAFKA_BOOTSTRAP", "kafka:29092");
    private static final String ES_HOST = System.getenv()
            .getOrDefault("ES_HOST", "elasticsearch");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000);
        env.setParallelism(2);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(
                env, EnvironmentSettings.newInstance().inStreamingMode().build());

        // ── 1. Declare the Kafka source table ──────────────────────────────
        tEnv.executeSql("""
            CREATE TABLE raw_orders (
                id            BIGINT,
                order_ref     STRING,
                product_id    BIGINT,
                quantity      INT,
                total_amount  DOUBLE,
                currency      STRING,
                status        STRING,
                channel       STRING,
                created_at    TIMESTAMP(3),
                __op          STRING,
                WATERMARK FOR created_at AS created_at - INTERVAL '5' SECOND
            ) WITH (
                'connector'                    = 'kafka',
                'topic'                        = 'cdc.ecommerce.orders',
                'properties.bootstrap.servers' = '%s',
                'properties.group.id'          = 'revenue-agg-sql',
                'scan.startup.mode'            = 'earliest-offset',
                'format'                       = 'json',
                'json.ignore-parse-errors'     = 'true'
            )
            """.formatted(KAFKA_BOOTSTRAP));

        // ── 2. Declare the products dimension table (bounded Kafka scan) ───
        tEnv.executeSql("""
            CREATE TABLE dim_products (
                id        BIGINT,
                sku       STRING,
                name      STRING,
                category  STRING,
                price     DOUBLE
            ) WITH (
                'connector'                    = 'kafka',
                'topic'                        = 'cdc.ecommerce.products',
                'properties.bootstrap.servers' = '%s',
                'properties.group.id'          = 'revenue-agg-products',
                'scan.startup.mode'            = 'earliest-offset',
                'format'                       = 'json',
                'json.ignore-parse-errors'     = 'true'
            )
            """.formatted(KAFKA_BOOTSTRAP));

        // ── 3. Declare the Elasticsearch sink ─────────────────────────────
        tEnv.executeSql("""
            CREATE TABLE revenue_5m (
                window_start   TIMESTAMP(3),
                window_end     TIMESTAMP(3),
                category       STRING,
                order_count    BIGINT,
                total_revenue  DOUBLE,
                avg_order_value DOUBLE,
                PRIMARY KEY (window_start, category) NOT ENFORCED
            ) WITH (
                'connector'          = 'elasticsearch-7',
                'hosts'              = 'http://%s:9200',
                'index'              = 'revenue-5m',
                'format'             = 'json',
                'sink.bulk-flush.max-size' = '2mb'
            )
            """.formatted(ES_HOST));

        // ── 4. The aggregation query ───────────────────────────────────────
        // Filter op != 'd' (we don't reverse revenue on order deletion — by design)
        // Tumbling window of 5 minutes on event_time
        TableResult result = tEnv.executeSql("""
            INSERT INTO revenue_5m
            SELECT
                TUMBLE_START(o.created_at, INTERVAL '5' MINUTE)  AS window_start,
                TUMBLE_END(o.created_at,   INTERVAL '5' MINUTE)  AS window_end,
                COALESCE(p.category, 'UNKNOWN')                   AS category,
                COUNT(*)                                          AS order_count,
                SUM(o.total_amount)                               AS total_revenue,
                AVG(o.total_amount)                               AS avg_order_value
            FROM raw_orders AS o
            LEFT JOIN dim_products AS p
                ON o.product_id = p.id
            WHERE o.__op <> 'd'
              AND o.status <> 'CANCELLED'
            GROUP BY
                TUMBLE(o.created_at, INTERVAL '5' MINUTE),
                COALESCE(p.category, 'UNKNOWN')
            """);

        result.await();
    }
}
