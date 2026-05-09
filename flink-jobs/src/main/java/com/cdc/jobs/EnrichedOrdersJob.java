package com.cdc.jobs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Enriched Orders Job
 * ===================
 * Joins the CDC `orders` stream with cached customer state to produce
 * an enriched order document in Elasticsearch that includes customer
 * name, city, and tier without needing a database lookup at query time.
 *
 * Pattern: Broadcast-state join (customer state cached per-key in Flink state,
 * orders look up the cached customer on arrival).
 *
 * Input topics  : cdc.ecommerce.orders, cdc.ecommerce.customers
 * Output index  : enriched-orders
 */
public class EnrichedOrdersJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String KAFKA_BOOTSTRAP = System.getenv()
            .getOrDefault("KAFKA_BOOTSTRAP", "kafka:29092");
    private static final String ES_HOST = System.getenv()
            .getOrDefault("ES_HOST", "elasticsearch");

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000);
        env.setParallelism(2);

        // ── Source: customers (CDC) ──────────────────────────────────────
        KafkaSource<String> customerSource = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setTopics("cdc.ecommerce.customers")
                .setGroupId("enriched-orders-customers")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                .build();

        // ── Source: orders (CDC) ─────────────────────────────────────────
        KafkaSource<String> orderSource = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setTopics("cdc.ecommerce.orders")
                .setGroupId("enriched-orders-main")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                .build();

        DataStream<String> customerStream = env.fromSource(
                customerSource, WatermarkStrategy.noWatermarks(), "CustomerCDC");
        DataStream<String> orderStream = env.fromSource(
                orderSource, WatermarkStrategy.noWatermarks(), "OrderCDC");

        // ── Process orders — look up customer from state ─────────────────
        DataStream<Map<String, Object>> enriched = orderStream
                .connect(customerStream)
                .keyBy(
                        o -> extractCustomerId(o),
                        c -> extractId(c)
                )
                .flatMap(new EnrichOrderFn());

        // ── Sink to Elasticsearch ────────────────────────────────────────
        enriched.addSink(new org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink.Builder<Map<String, Object>>(
                java.util.List.of(new HttpHost(ES_HOST, 9200, "http")),
                (element, ctx, indexer) -> {
                    String id = element.get("order_id") != null
                            ? element.get("order_id").toString() : "unknown";
                    IndexRequest req = Requests.indexRequest()
                            .index("enriched-orders")
                            .id(id)
                            .source(element);
                    indexer.add(req);
                }
        ).build());

        env.execute("Enriched Orders Job");
    }

    private static String extractCustomerId(String json) {
        try {
            JsonNode n = MAPPER.readTree(json);
            JsonNode after = n.has("after") ? n.get("after") : n;
            return after.path("customer_id").asText("0");
        } catch (Exception e) { return "0"; }
    }

    private static String extractId(String json) {
        try {
            JsonNode n = MAPPER.readTree(json);
            JsonNode after = n.has("after") ? n.get("after") : n;
            return after.path("id").asText("0");
        } catch (Exception e) { return "0"; }
    }

    // ── Stateful join function ────────────────────────────────────────────
    static class EnrichOrderFn
            extends org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction<String, String, Map<String, Object>> {

        private ValueState<Map<String, Object>> customerState;

        @Override
        public void open(Configuration cfg) {
            customerState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("customer",
                            org.apache.flink.api.common.typeinfo.Types.MAP(
                                    org.apache.flink.api.common.typeinfo.Types.STRING,
                                    org.apache.flink.api.common.typeinfo.Types.GENERIC(Object.class))));
        }

        @Override
        public void flatMap1(String orderJson, Collector<Map<String, Object>> out) throws Exception {
            // Order event
            JsonNode node  = MAPPER.readTree(orderJson);
            String   op    = node.path("__op").asText("c");
            if ("d".equals(op)) return;  // skip deletes

            JsonNode after = node.has("after") ? node.get("after") : node;

            Map<String, Object> doc = new HashMap<>();
            doc.put("order_id",      after.path("id").asLong());
            doc.put("order_ref",     after.path("order_ref").asText());
            doc.put("product_id",    after.path("product_id").asLong());
            doc.put("quantity",      after.path("quantity").asInt());
            doc.put("total_amount",  after.path("total_amount").asDouble());
            doc.put("currency",      after.path("currency").asText("AED"));
            doc.put("status",        after.path("status").asText());
            doc.put("channel",       after.path("channel").asText());
            doc.put("event_time",    Instant.now().toString());

            // Enrich with cached customer
            Map<String, Object> customer = customerState.value();
            if (customer != null) {
                doc.put("customer_email",     customer.get("email"));
                doc.put("customer_name",
                        customer.get("first_name") + " " + customer.get("last_name"));
                doc.put("customer_city",      customer.get("city"));
                doc.put("customer_country",   customer.get("country"));
                doc.put("customer_tier",      customer.get("tier"));
            }
            out.collect(doc);
        }

        @Override
        public void flatMap2(String customerJson, Collector<Map<String, Object>> out) throws Exception {
            // Cache customer state
            JsonNode node  = MAPPER.readTree(customerJson);
            String   op    = node.path("__op").asText("c");
            if ("d".equals(op)) { customerState.clear(); return; }

            JsonNode after = node.has("after") ? node.get("after") : node;
            Map<String, Object> c = new HashMap<>();
            c.put("email",      after.path("email").asText());
            c.put("first_name", after.path("first_name").asText());
            c.put("last_name",  after.path("last_name").asText());
            c.put("city",       after.path("city").asText());
            c.put("country",    after.path("country").asText());
            c.put("tier",       after.path("tier").asText());
            customerState.update(c);
        }
    }
}
