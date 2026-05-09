#!/usr/bin/env bash
# Creates Kibana index patterns and imports the dashboard after ES has data.
# Run: bash kibana/setup.sh

KIBANA="http://localhost:5601"
ES="http://localhost:9200"

echo "Waiting for Kibana..."
until curl -sf "$KIBANA/api/status" | grep -q '"available"'; do
  sleep 3
done

echo "Creating index patterns..."

# enriched-orders
curl -sf -X POST "$KIBANA/api/index_patterns/index_pattern" \
  -H "kbn-xsrf: true" -H "Content-Type: application/json" \
  -d '{"index_pattern":{"title":"enriched-orders*","timeFieldName":"event_time"}}' | python3 -m json.tool

# revenue-5m
curl -sf -X POST "$KIBANA/api/index_patterns/index_pattern" \
  -H "kbn-xsrf: true" -H "Content-Type: application/json" \
  -d '{"index_pattern":{"title":"revenue-5m*","timeFieldName":"window_start"}}' | python3 -m json.tool

# customer-360
curl -sf -X POST "$KIBANA/api/index_patterns/index_pattern" \
  -H "kbn-xsrf: true" -H "Content-Type: application/json" \
  -d '{"index_pattern":{"title":"customer-360*"}}' | python3 -m json.tool

echo ""
echo "Done. Open http://localhost:5601 and explore the Discover tab."
echo "Suggested queries:"
echo "  enriched-orders* | filter: customer_tier : GOLD"
echo "  revenue-5m*       | bar chart: total_revenue by category"
echo "  customer-360*     | top 10 by total_spend"
