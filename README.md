# CDC Data Pipeline: MySQL → Kafka → Flink → Elasticsearch

A real-time data pipeline that automatically captures every database change (inserts, updates, deletes) and streams it through a processing chain — no manual exports, no polling, no missed events.

---

## What problem does this solve?

Imagine you run an e-commerce site. Your orders, customers, and products live in a MySQL database. You want a **live dashboard** showing revenue by category, customer spend totals, and enriched order views — updated within seconds of any database change.

The naive approach would be to run SQL queries on your production database every few seconds. That's slow, expensive, and kills your database under load.

This pipeline uses **Change Data Capture (CDC)**: instead of querying the database, it reads MySQL's internal change log (the *binary log* / binlog) — the same log MySQL uses for replication. Every INSERT, UPDATE, and DELETE is captured the moment it happens, with zero impact on the database.

---

## The technology stack — explained simply

### MySQL (the source)
Your regular relational database. Every database write is recorded in MySQL's **binary log (binlog)** — think of it as a running journal of every change ever made to the database. This log already exists; we just tap into it.

### Debezium (the change capture tool)
Debezium is a tool that **reads MySQL's binlog and turns each change into a message**. It pretends to be a MySQL replica (follower) so MySQL sends it every change automatically.

When you insert a row, Debezium produces a message like:
```json
{
  "op": "c",
  "before": null,
  "after": { "id": 1, "customer_id": 42, "total_amount": 149.99, "status": "PENDING" },
  "source": { "table": "orders", "ts_ms": 1714000000000 }
}
```
`op` tells you what happened: `c` = create (insert), `u` = update, `d` = delete, `r` = snapshot read.

Debezium runs inside **Kafka Connect** — a framework for connecting data systems to Kafka.

### Kafka (the message bus)
Kafka is a **high-throughput message queue**. Debezium publishes every database change as a Kafka message. Kafka holds these messages durably so downstream consumers can read them at their own pace, and replay from any point in time if something goes wrong.

Each database table gets its own Kafka topic:
- `cdc.ecommerce.orders`
- `cdc.ecommerce.customers`
- `cdc.ecommerce.products`

### Apache Flink (the stream processor)
Flink reads from Kafka and **processes the stream in real time** — joining tables, computing aggregates, filtering events. It keeps running state across millions of events without ever going to disk for each one.

This project runs three independent Flink jobs:

| Job | What it does | Output |
|-----|-------------|--------|
| `EnrichedOrdersJob` | Joins each order with the customer's name and city | `enriched-orders` index |
| `RevenueAggregatesJob` | Sums revenue per product category in 5-minute windows | `revenue-5m` index |
| `Customer360Job` | Keeps a running total of each customer's spend, order count, and last order | `customer-360` index |

### Elasticsearch + Kibana (the sink and dashboard)
Flink writes results into **Elasticsearch** — a search and analytics database optimised for fast aggregation queries. **Kibana** sits on top and provides live dashboards that auto-refresh as new data arrives.

---

## How data flows

```
MySQL database
    │
    │  (Debezium reads the binlog — MySQL's internal change journal)
    ▼
Kafka topics  ──  cdc.ecommerce.orders / customers / products
    │
    │  (Flink reads from Kafka, joins + aggregates in memory)
    ▼
┌─────────────────────────────────────────┐
│ EnrichedOrdersJob  →  enriched-orders   │
│ RevenueAggregatesJob  →  revenue-5m     │  Elasticsearch
│ Customer360Job  →  customer-360         │
└─────────────────────────────────────────┘
    │
    ▼
Kibana dashboards  (live, auto-refreshing)
```

---

## Running the project

**Prerequisites:** Docker and Docker Compose installed. That's it — everything else runs in containers.

```bash
make up          # Start all services (~90 seconds for everything to be healthy)
make connectors  # Tell Debezium to start watching the MySQL database
make seed        # Start the data generator (~5 e-commerce events per second)
make jobs        # Submit all 3 Flink processing jobs
make kibana      # Open Kibana in your browser (http://localhost:5601)
```

To stop:
```bash
make down        # Stop all containers (keeps data volumes)
make clean       # Stop and delete all data (full reset)
```

### Service URLs

| Service | URL | What you'll see |
|---------|-----|-----------------|
| Kafka UI | http://localhost:8080 | Live message flow through topics |
| Flink Dashboard | http://localhost:8081 | 3 running jobs, throughput metrics |
| Kafka Connect | http://localhost:8083/connectors | Debezium connector status |
| Elasticsearch | http://localhost:9200/_cat/indices?v | Index doc counts growing in real time |
| Kibana | http://localhost:5601 | Live dashboards |

### Verify it's working

```bash
# Watch Elasticsearch index sizes grow as events flow through
curl http://localhost:9200/_cat/indices?v

# Check Debezium connector is healthy
curl http://localhost:8083/connectors/mysql-ecommerce/status
```

---

## Project layout

```
.
├── docker-compose.yml          # All services defined here
├── Makefile                    # Convenience commands
├── mysql/
│   └── init/                   # Database schema (auto-applied on first start)
├── debezium/
│   └── mysql-connector.json    # Tells Debezium which database/tables to watch
├── flink-jobs/
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/main/java/com/cdc/jobs/
│       ├── EnrichedOrdersJob.java      # DataStream API join
│       ├── RevenueAggregatesJob.java   # Flink SQL windowed aggregation
│       └── Customer360Job.java         # Flink SQL running totals
├── seeder/
│   └── seeder.py               # Generates realistic e-commerce events
└── kibana/
    └── dashboards/             # Kibana dashboard export JSON
```

---

## Key design decisions

### Why read the binlog instead of writing events from the application?

The alternative is to have your application code publish a Kafka message every time it writes to the database — called "dual write". The problem: if the database write succeeds but the Kafka publish fails, your two systems are now out of sync. With CDC, the Kafka message is derived directly from the database write, so they're always consistent.

### Why Kafka in the middle?

Kafka acts as a **buffer and replay log**. If a Flink job crashes and restarts, it just re-reads from where it left off in Kafka. No events are lost. You can also add new consumers (more Flink jobs, other systems) without touching the pipeline.

### Why Elasticsearch instead of writing back to MySQL?

Elasticsearch is optimised for the read patterns dashboards need — aggregations across millions of rows in milliseconds. OLTP databases like MySQL are designed for transactional reads/writes, not analytical queries. Using a dedicated analytics store keeps production MySQL fast.

### Exactly-once guarantees

Flink checkpoints its state (which Kafka offset it has processed up to) every 30 seconds to disk. On restart, it resumes from the last checkpoint. Combined with Elasticsearch UPSERT writes (keyed on the database primary key), replaying events produces the same result — no duplicates, no gaps.

---

## Common questions

**What if MySQL goes down?**
Debezium pauses. When MySQL comes back, it resumes from where it left off in the binlog — nothing is lost.

**What if Flink crashes?**
Kafka holds the messages. Flink restarts from its last checkpoint and reprocesses. Elasticsearch UPSERTs ensure reprocessing produces the same result.

**What if I add a new column to MySQL?**
Debezium stores the full schema history in a Kafka topic (`schema-changes.ecommerce`). It can decode old events using the old schema and new events using the new schema. Breaking changes (removing a column) require a brief connector pause during migration.

**Why are deletes ignored in revenue/customer-360?**
A cancelled order shouldn't retroactively remove revenue from past reporting windows. For the `enriched-orders` index, deletes could be handled by sending an Elasticsearch `_delete` action when `op=d` is received.

---

## What's next

- [ ] Schema Registry + Avro serialisation (type-safe schema evolution)
- [ ] Prometheus + Grafana monitoring (connector lag, Flink throughput)
- [ ] Dead-letter topic for malformed/unparseable events
- [ ] Kubernetes manifests (Flink Operator + Kafka Connect Operator)

---

## License
MIT
