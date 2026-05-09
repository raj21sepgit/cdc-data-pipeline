.PHONY: up down clean connectors seed jobs kibana ps logs

up:
	docker compose up -d --build
	@echo ""
	@echo "Stack starting (~90s). URLs:"
	@echo "  Kafka UI    : http://localhost:8080"
	@echo "  Kafka Connect : http://localhost:8083/connectors"
	@echo "  Flink UI    : http://localhost:8081"
	@echo "  Elasticsearch : http://localhost:9200"
	@echo "  Kibana      : http://localhost:5601"
	@echo ""
	@echo "Next: make connectors && make seed && make jobs"

connectors:
	@echo "Registering Debezium MySQL connector..."
	@curl -sf -X POST http://localhost:8083/connectors \
		-H "Content-Type: application/json" \
		-d @debezium/mysql-connector.json | python3 -m json.tool || true
	@echo ""
	@echo "Connector status:"
	@curl -sf http://localhost:8083/connectors/mysql-ecommerce/status | python3 -m json.tool

seed:
	@echo "Starting data seeder (runs continuously)..."
	docker compose exec -d seeder python seeder.py
	@echo "Seeder running. Watch topics at http://localhost:8080"

jobs:
	@echo "Submitting Flink jobs..."
	docker compose exec flink-jobs bash -c "\
		flink run -d -m flink-jobmanager:8081 /app/jobs/enriched-orders-job.jar && \
		flink run -d -m flink-jobmanager:8081 /app/jobs/revenue-aggregates-job.jar && \
		flink run -d -m flink-jobmanager:8081 /app/jobs/customer-360-job.jar \
		|| echo 'Note: compile jobs first with: docker compose exec flink-jobs mvn package'"

kibana:
	@command -v xdg-open >/dev/null && xdg-open http://localhost:5601 || open http://localhost:5601

es-indices:
	@echo "=== Elasticsearch indices ==="
	@curl -sf http://localhost:9200/_cat/indices?v

connector-status:
	@curl -sf http://localhost:8083/connectors/mysql-ecommerce/status | python3 -m json.tool

ps:
	docker compose ps

logs:
	docker compose logs -f kafka-connect flink-jobmanager

down:
	docker compose down

clean:
	docker compose down -v
