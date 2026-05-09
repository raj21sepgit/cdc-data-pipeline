package com.cdc.jobs;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Customer 360 Job
 * =================
 * Builds a continuously-updated "customer 360" view: total spend, order count,
 * last order time, and preferred channel — all materialised into Elasticsearch
 * for instant lookup and segmentation.
 *
 * Uses Flink SQL with an OVER aggregation (running total) pattern rather than
 * windowed aggregations, since we want the *current cumulative* value, not
 * a per-window slice.
 *
 * Input  : cdc.ecommerce.orders, cdc.ecommerce.customers
 * Output : customer-360 (Elasticsearch)
 */
public class Customer360Job {

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

        // ── Orders source ─────────────────────────────────────────────────
        tEnv.executeSql("""
            CREATE TABLE c360_orders (
                id            BIGINT,
                customer_id   BIGINT,
                total_amount  DOUBLE,
                status        STRING,
                channel       STRING,
                created_at    TIMESTAMP(3),
                __op          STRING,
                WATERMARK FOR created_at AS created_at - INTERVAL '5' SECOND
            ) WITH (
                'connector'                    = 'kafka',
                'topic'                        = 'cdc.ecommerce.orders',
                'properties.bootstrap.servers' = '%s',
                'properties.group.id'          = 'customer-360-orders',
                'scan.startup.mode'            = 'earliest-offset',
                'format'                       = 'json',
                'json.ignore-parse-errors'     = 'true'
            )
            """.formatted(KAFKA_BOOTSTRAP));

        // ── Customers source ──────────────────────────────────────────────
        tEnv.executeSql("""
            CREATE TABLE c360_customers (
                id         BIGINT,
                email      STRING,
                first_name STRING,
                last_name  STRING,
                city       STRING,
                tier       STRING,
                PRIMARY KEY (id) NOT ENFORCED
            ) WITH (
                'connector'                    = 'kafka',
                'topic'                        = 'cdc.ecommerce.customers',
                'properties.bootstrap.servers' = '%s',
                'properties.group.id'          = 'customer-360-customers',
                'scan.startup.mode'            = 'earliest-offset',
                'format'                       = 'json',
                'json.ignore-parse-errors'     = 'true'
            )
            """.formatted(KAFKA_BOOTSTRAP));

        // ── Elasticsearch sink ────────────────────────────────────────────
        tEnv.executeSql("""
            CREATE TABLE customer_360 (
                customer_id       BIGINT,
                email             STRING,
                full_name         STRING,
                city              STRING,
                tier              STRING,
                total_spend       DOUBLE,
                order_count       BIGINT,
                avg_order_value   DOUBLE,
                last_order_time   TIMESTAMP(3),
                preferred_channel STRING,
                computed_at       TIMESTAMP(3),
                PRIMARY KEY (customer_id) NOT ENFORCED
            ) WITH (
                'connector' = 'elasticsearch-7',
                'hosts'     = 'http://%s:9200',
                'index'     = 'customer-360',
                'format'    = 'json'
            )
            """.formatted(ES_HOST));

        // ── Customer 360 aggregation ──────────────────────────────────────
        tEnv.executeSql("""
            INSERT INTO customer_360
            SELECT
                o.customer_id,
                c.email,
                CONCAT(c.first_name, ' ', c.last_name)  AS full_name,
                c.city,
                c.tier,
                SUM(o.total_amount)                      AS total_spend,
                COUNT(o.id)                              AS order_count,
                AVG(o.total_amount)                      AS avg_order_value,
                MAX(o.created_at)                        AS last_order_time,
                LAST_VALUE(o.channel)                     AS preferred_channel,
                CURRENT_TIMESTAMP                        AS computed_at
            FROM c360_orders AS o
            JOIN c360_customers FOR SYSTEM_TIME AS OF o.created_at AS c
                ON o.customer_id = c.id
            WHERE o.__op <> 'd'
              AND o.status <> 'CANCELLED'
            GROUP BY o.customer_id, c.email, c.first_name, c.last_name, c.city, c.tier
            """).await();
    }
}
