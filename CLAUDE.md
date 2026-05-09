# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

CDC streaming pipeline: **MySQL → Debezium → Kafka → Flink → Elasticsearch/Kibana**

Real-time Change Data Capture from a MySQL `ecommerce` database (customers, products, orders), processed by three independent Flink jobs, materialized into Elasticsearch for Kibana dashboards.

## Commands

### Stack Management
```bash
make up          # Start full stack (~90s for all services to be healthy)
make connectors  # Register Debezium MySQL connector with Kafka Connect
make seed        # Start Python seeder (generates ~5 events/sec continuously)
make jobs        # Submit all 3 Flink jobs to the cluster
make kibana      # Create ES index patterns + import Kibana dashboards
make down        # Stop all containers
make clean       # Stop + remove volumes (full reset)
```

### Flink Jobs
```bash
cd flink-jobs && mvn package -DskipTests       # Build fat JAR → target/cdc-flink-jobs-1.0.0.jar
cd flink-jobs && mvn package                   # Build + run tests
```

### Observability
```bash
curl http://localhost:9200/_cat/indices?v      # Check ES indices and doc counts
curl http://localhost:8083/connectors          # List registered Kafka connectors
curl http://localhost:8083/connectors/mysql-cdc-connector/status  # Connector health
```

## Service Endpoints

| Service | URL |
|---------|-----|
| Flink Dashboard | http://localhost:8081 |
| Kafka UI | http://localhost:8080 |
| Kafka Connect REST | http://localhost:8083 |
| Elasticsearch | http://localhost:9200 |
| Kibana | http://localhost:5601 |
| MySQL | localhost:3306 (user: `debezium`, pass: `debezium`) |

## Architecture

### Data Flow
```
MySQL binlog → Debezium → Kafka topics → Flink jobs → Elasticsearch indices
```

**Kafka topics produced by Debezium:**
- `cdc.ecommerce.orders`
- `cdc.ecommerce.customers`
- `cdc.ecommerce.products`
- `schema-changes.ecommerce` (DDL history)

**Debezium event envelope:** `{ op, before, after, source: { db, table, ts_ms } }`  
`op` values: `c`=insert, `u`=update, `d`=delete, `r`=snapshot read

### Three Flink Jobs (`flink-jobs/src/main/java/com/cdc/jobs/`)

| Job | API | Pattern | Output Index |
|-----|-----|---------|--------------|
| `EnrichedOrdersJob` | DataStream | Broadcast-state join: customers cached in Flink state, orders look up on arrival | `enriched-orders` |
| `RevenueAggregatesJob` | Flink SQL | Tumbling 5-minute windows per product category | `revenue-5m` |
| `Customer360Job` | Flink SQL | OVER aggregations for cumulative per-customer metrics | `customer-360` |

All three jobs write via `Elasticsearch7SinkFunction` in UPSERT mode (idempotent).

### Key Design Decisions
- **No dual-write**: CDC at the binlog level — no application code changes needed
- **Stateful join in EnrichedOrdersJob**: Customer data is broadcast-state cached; orders join in-memory without external DB calls
- **Exactly-once**: Flink checkpointing (30s interval, RocksDB state backend) + ES UPSERT semantics
- **Deletes ignored**: Revenue and customer-360 aggregates do not reverse on order deletion by design
- **Idempotent replay**: Safe to replay from any Kafka offset — ES UPSERT keyed on order ID

### Flink Cluster Config (via docker-compose)
- Parallelism: 2
- State backend: RocksDB
- Checkpoint interval: 30s
- TaskManager slots: 6

### Schema Evolution
Debezium stores DDL history in `schema-changes.ecommerce`. Breaking schema changes require: pause connector → migrate → resume connector.

## Seeder

`seeder/seeder.py` continuously inserts realistic e-commerce events at ~5 events/sec (configurable via `EVENTS_PER_SECOND` env var):
- 65% new orders
- 20% order status updates
- 10% product stock updates

## CI/CD

`.github/workflows/ci.yml` runs on every push:
1. Maven build + JAR artifact upload
2. JSON syntax validation on Debezium connector config
3. `docker-compose config` validation

## Flink Consumer Groups

Used to monitor consumer lag (key SLA metric):
- `enriched-orders-main`
- `revenue-agg-sql`
- `customer-360-orders`
