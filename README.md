# CDC Pipeline — Debezium + Kafka + PostgreSQL

Real-time Change Data Capture pipeline that streams every INSERT / UPDATE / DELETE
from PostgreSQL into Kafka topics via Debezium, then fans out to arbitrary consumers.

## Architecture

```
PostgreSQL (WAL) → Debezium (Kafka Connect) → Kafka (3 brokers + Schema Registry)
                                                    ↓
                       Sink Connector │ Stream Processor │ Custom Consumer │ DWH
```

## Stack

| Component         | Version  | Role                                   |
|-------------------|----------|----------------------------------------|
| PostgreSQL        | 16       | Source DB — logical replication / WAL  |
| Kafka             | 7.6.0    | Message bus (3-broker cluster)         |
| Zookeeper         | 7.6.0    | Kafka coordination                     |
| Debezium          | 2.6      | CDC connector (Kafka Connect)          |
| Schema Registry   | 7.6.0    | Avro schema management                 |
| Kafka UI          | latest   | Web UI for topics / consumers          |
| Prometheus        | 2.50     | Metrics scraping                       |
| Grafana           | 10.3     | Dashboards                             |
| Python consumer   | 3.12     | Example event processor                |

---

## Quickstart

### 1. Prerequisites

```bash
docker >= 24, docker compose >= 2.20
# Increase Docker memory limit to at least 6 GB for 3 brokers
```

### 2. First-time setup

```bash
cp .env.example .env            # edit passwords
docker compose up -d            # start all services (takes ~2 min)
chmod +x scripts/manage.sh
./scripts/manage.sh setup       # wait for connect, create topics, register connector
```

### 3. Verify

```bash
./scripts/manage.sh health      # check all services
./scripts/manage.sh status      # connector state (RUNNING = good)
./scripts/manage.sh consume cdc.public.orders   # watch events live
```

### 4. Generate CDC events

```bash
docker exec -it cdc-postgres psql -U cdc_user -d cdc_db -c "
  INSERT INTO orders (customer_id, status, total_amount)
  VALUES (1, 'confirmed', 99.99);
"

docker exec -it cdc-postgres psql -U cdc_user -d cdc_db -c "
  UPDATE orders SET status = 'shipped' WHERE id = 1;
"
```

---

## Topic Naming

Debezium publishes to: `{prefix}.{schema}.{table}`

| Table              | Topic                        |
|--------------------|------------------------------|
| public.customers   | cdc.public.customers         |
| public.products    | cdc.public.products          |
| public.orders      | cdc.public.orders            |
| public.order_items | cdc.public.order_items       |

---

## CDC Event Schema (after unwrap transform)

```json
{
  "id":          1,
  "customer_id": 1,
  "status":      "shipped",
  "total_amount": 99.99,
  "__op":        "u",
  "__table":     "orders",
  "__source_ts_ms": 1710000000000,
  "__source_lsn":   "0/1234ABC",
  "pipeline":    "cdc-v1"
}
```

`__op` values: `c` = create, `u` = update, `d` = delete, `r` = snapshot read

---

## Operations

```bash
./scripts/manage.sh pause             # pause capture (maintenance window)
./scripts/manage.sh resume            # resume capture
./scripts/manage.sh restart           # restart failed connector
./scripts/manage.sh lag               # consumer group lag report
./scripts/manage.sh snapshot public.orders  # trigger incremental snapshot
./scripts/manage.sh delete            # remove connector (stops capture)
```

---

## Production Checklist

### PostgreSQL

- [x] `wal_level = logical`
- [x] `max_replication_slots >= 5`
- [x] `REPLICA IDENTITY FULL` on critical tables
- [x] Dedicated `replicator` role with least-privilege
- [ ] WAL archiving enabled for PITR
- [ ] `wal_keep_size` tuned to replication lag tolerance

### Kafka

- [x] 3 brokers with `min.insync.replicas=2`
- [x] `acks=all` on producer
- [x] `enable.auto.commit=false` on consumer
- [x] LZ4 compression
- [ ] TLS between brokers (`SASL_SSL`)
- [ ] ACLs per consumer group
- [ ] Tiered storage for long retention topics

### Debezium

- [x] Heartbeat query prevents WAL bloat
- [x] Dead Letter Queue configured
- [x] Avro serialization with Schema Registry
- [x] `snapshot.mode=initial` for cold start
- [ ] Secrets via Vault or AWS Secrets Manager
- [ ] Multiple Connect workers for HA

### Observability

- [x] Prometheus + Grafana stack
- [x] JMX exporter for Kafka broker metrics
- [x] Consumer lag metrics
- [ ] Alert: connector state != RUNNING
- [ ] Alert: consumer lag > 10 000
- [ ] Alert: DLQ message count > 0

---

## Monitoring URLs

| Service    | URL                        | Credentials      |
|------------|----------------------------|------------------|
| Kafka UI   | http://localhost:8080       | —                |
| Grafana    | http://localhost:3000       | admin / admin    |
| Prometheus | http://localhost:9090       | —                |
| Connect    | http://localhost:8083       | —                |
| Schema Reg | http://localhost:8081       | —                |

---

## Troubleshooting

### Connector stuck in FAILED state

```bash
./scripts/manage.sh status         # read the error trace
./scripts/manage.sh restart        # attempt auto-recovery
# If replication slot is stale:
docker exec cdc-postgres psql -U cdc_user -d cdc_db \
  -c "SELECT * FROM pg_replication_slots;"
```

### WAL keeps growing (slot not advancing)

```bash
# Check slot lag
docker exec cdc-postgres psql -U cdc_user -d cdc_db \
  -c "SELECT slot_name, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS lag
      FROM pg_replication_slots;"
# If connector is down and lag is critical, drop and recreate:
docker exec cdc-postgres psql -U cdc_user -d cdc_db \
  -c "SELECT pg_drop_replication_slot('debezium_slot');"
# Then re-register with snapshot.mode=schema_only_recovery
```

### Schema evolution

Debezium + Schema Registry supports `BACKWARD` compatibility (default).
- Adding nullable columns → safe
- Renaming columns → create alias first, migrate consumers, then drop old name
- Removing columns → deprecate with default, then remove after consumers updated
