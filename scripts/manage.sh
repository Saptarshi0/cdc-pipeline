#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════════
# CDC Pipeline – Setup & Management Scripts
# ═══════════════════════════════════════════════════════════════════════════════
set -euo pipefail

CONNECT_URL="http://localhost:8083"
KAFKA_BOOTSTRAP="localhost:29092"
CONNECTOR_NAME="postgres-cdc-connector"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
log()  { echo -e "${GREEN}[CDC]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
err()  { echo -e "${RED}[ERR]${NC} $*"; }

# ── wait_for_service <url> <name> ────────────────────────────────────────────
wait_for_service() {
  local url=$1 name=$2 retries=30
  log "Waiting for $name at $url …"
  for i in $(seq 1 $retries); do
    if curl -sf "$url" >/dev/null 2>&1; then
      log "$name is up"
      return 0
    fi
    echo -n "."
    sleep 5
  done
  err "$name did not start in time"
  return 1
}

# ── create_topics ─────────────────────────────────────────────────────────────
create_topics() {
  log "Creating Kafka topics …"
  local topics=(
    "cdc.public.customers"
    "cdc.public.products"
    "cdc.public.orders"
    "cdc.public.order_items"
    "_cdc_dlq"
    "_cdc_connect_configs"
    "_cdc_connect_offsets"
    "_cdc_connect_statuses"
  )
  for topic in "${topics[@]}"; do
    docker exec cdc-kafka-1 kafka-topics \
      --create \
      --if-not-exists \
      --bootstrap-server kafka-1:9092 \
      --topic "$topic" \
      --partitions 6 \
      --replication-factor 3 \
      --config min.insync.replicas=2 \
      --config retention.ms=604800000 \
      --config compression.type=lz4 \
      2>&1 | grep -v "already exists" || true
    echo "  ✓ $topic"
  done
  log "Topics created"
}

# ── register_connector ────────────────────────────────────────────────────────
register_connector() {
  log "Registering Debezium connector …"
  local response
  response=$(curl -sf -X POST \
    -H "Content-Type: application/json" \
    --data @debezium/connectors/postgres-connector.json \
    "$CONNECT_URL/connectors")
  echo "$response" | python3 -m json.tool
  log "Connector registered"
}

# ── connector_status ──────────────────────────────────────────────────────────
connector_status() {
  echo -e "\n${CYAN}── Connector Status ──────────────────────────────${NC}"
  curl -sf "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" | python3 -m json.tool
}

# ── pause_connector ───────────────────────────────────────────────────────────
pause_connector() {
  warn "Pausing connector $CONNECTOR_NAME …"
  curl -sf -X PUT "$CONNECT_URL/connectors/$CONNECTOR_NAME/pause"
  log "Connector paused"
}

# ── resume_connector ──────────────────────────────────────────────────────────
resume_connector() {
  log "Resuming connector $CONNECTOR_NAME …"
  curl -sf -X PUT "$CONNECT_URL/connectors/$CONNECTOR_NAME/resume"
  log "Connector resumed"
}

# ── restart_connector ─────────────────────────────────────────────────────────
restart_connector() {
  warn "Restarting connector $CONNECTOR_NAME …"
  curl -sf -X POST "$CONNECT_URL/connectors/$CONNECTOR_NAME/restart?includeTasks=true&onlyFailed=false"
  log "Connector restarted"
}

# ── delete_connector ──────────────────────────────────────────────────────────
delete_connector() {
  warn "Deleting connector $CONNECTOR_NAME …"
  curl -sf -X DELETE "$CONNECT_URL/connectors/$CONNECTOR_NAME"
  log "Connector deleted"
}

# ── consume_topic <topic> ─────────────────────────────────────────────────────
consume_topic() {
  local topic="${1:-cdc.public.orders}"
  log "Consuming from $topic (Ctrl-C to stop) …"
  docker exec -it cdc-kafka-1 kafka-console-consumer \
    --bootstrap-server kafka-1:9092 \
    --topic "$topic" \
    --from-beginning \
    --property print.key=true \
    --property key.separator=" → "
}

# ── lag_report ────────────────────────────────────────────────────────────────
lag_report() {
  echo -e "\n${CYAN}── Consumer Lag ──────────────────────────────────${NC}"
  docker exec cdc-kafka-1 kafka-consumer-groups \
    --bootstrap-server kafka-1:9092 \
    --describe \
    --group cdc-consumer-group 2>/dev/null || warn "Consumer group not found"
}

# ── trigger_snapshot <table> ──────────────────────────────────────────────────
trigger_snapshot() {
  local table="${1:-public.orders}"
  log "Triggering incremental snapshot on $table …"
  curl -sf -X POST \
    -H "Content-Type: application/json" \
    -d "{\"data-collections\":[\"$table\"]}" \
    "$CONNECT_URL/connectors/$CONNECTOR_NAME/topics"
  log "Snapshot requested"
}

# ── health ────────────────────────────────────────────────────────────────────
health() {
  echo -e "\n${CYAN}── Service Health ────────────────────────────────${NC}"
  printf "%-25s %s\n" "Service" "Status"
  printf "%-25s %s\n" "───────────────────────" "──────"

  services=(
    "PostgreSQL|http://localhost:5432"
    "Kafka-UI  |http://localhost:8080"
    "Connect   |http://localhost:8083/connectors"
    "Schema Reg|http://localhost:8081/subjects"
    "Prometheus|http://localhost:9090/-/healthy"
    "Grafana   |http://localhost:3000/api/health"
  )
  for svc in "${services[@]}"; do
    IFS='|' read -r name url <<< "$svc"
    if curl -sf "$url" >/dev/null 2>&1; then
      printf "%-25s ${GREEN}✓ UP${NC}\n" "$name"
    else
      printf "%-25s ${RED}✗ DOWN${NC}\n" "$name"
    fi
  done
}

# ── setup (full first-run bootstrap) ──────────────────────────────────────────
setup() {
  log "=== CDC Pipeline Bootstrap ==="
  wait_for_service "http://localhost:8083/connectors" "Kafka Connect"
  wait_for_service "http://localhost:8081/subjects"   "Schema Registry"
  create_topics
  register_connector
  log "Bootstrap complete"
  log "  Kafka UI    → http://localhost:8080"
  log "  Grafana     → http://localhost:3000  (admin / admin)"
  log "  Prometheus  → http://localhost:9090"
  log "  Connect API → http://localhost:8083"
}

# ── dispatch ──────────────────────────────────────────────────────────────────
CMD="${1:-help}"
shift || true
case "$CMD" in
  setup)            setup ;;
  topics)           create_topics ;;
  register)         register_connector ;;
  status)           connector_status ;;
  pause)            pause_connector ;;
  resume)           resume_connector ;;
  restart)          restart_connector ;;
  delete)           delete_connector ;;
  consume)          consume_topic "$@" ;;
  lag)              lag_report ;;
  snapshot)         trigger_snapshot "$@" ;;
  health)           health ;;
  *)
    echo "Usage: $0 {setup|topics|register|status|pause|resume|restart|delete|consume|lag|snapshot|health}"
    exit 1
    ;;
esac
