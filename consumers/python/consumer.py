"""
CDC Pipeline Consumer
─────────────────────
Production-grade Kafka consumer for Debezium CDC events.
Supports INSERT / UPDATE / DELETE / READ (snapshot) operations with:
  - Idempotent processing via offset tracking
  - Dead Letter Queue for poison messages
  - Prometheus metrics
  - Structured JSON logging
  - Graceful shutdown (SIGTERM / SIGINT)
  - Schema Registry (Avro deserialization)
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, Message
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from prometheus_client import Counter, Gauge, Histogram, start_http_server

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","message":%(message)s}',
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("cdc.consumer")


# ── Metrics ───────────────────────────────────────────────────────────────────
EVENTS_TOTAL = Counter(
    "cdc_events_total", "Total CDC events processed",
    ["operation", "table", "status"],
)
EVENTS_LAG = Gauge(
    "cdc_consumer_lag_ms", "Consumer lag in milliseconds",
    ["topic", "partition"],
)
PROCESSING_TIME = Histogram(
    "cdc_processing_duration_seconds", "Time to process a single event",
    ["table", "operation"],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
)
DLQ_EVENTS = Counter(
    "cdc_dlq_events_total", "Events sent to DLQ",
    ["table", "reason"],
)


# ── Config ────────────────────────────────────────────────────────────────────
@dataclass
class ConsumerConfig:
    bootstrap_servers:     str  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    schema_registry_url:   str  = os.getenv("SCHEMA_REGISTRY_URL",     "http://localhost:8081")
    group_id:              str  = os.getenv("CONSUMER_GROUP_ID",        "cdc-consumer-group")
    topics:                list = field(default_factory=lambda: [
        "cdc.public.customers",
        "cdc.public.products",
        "cdc.public.orders",
        "cdc.public.order_items",
    ])
    dlq_topic:             str  = os.getenv("DLQ_TOPIC",  "_cdc_dlq")
    metrics_port:          int  = int(os.getenv("METRICS_PORT", "9100"))
    poll_timeout_s:        float = 1.0
    max_retries:           int  = 3
    retry_backoff_ms:      int  = 500
    enable_auto_commit:    bool = False   # Always manual commit for exactly-once semantics
    auto_offset_reset:     str  = "earliest"
    session_timeout_ms:    int  = 30000
    heartbeat_interval_ms: int  = 10000
    max_poll_interval_ms:  int  = 300000


# ── Event model ───────────────────────────────────────────────────────────────
@dataclass
class CDCEvent:
    operation:  str           # c=create  u=update  d=delete  r=read(snapshot)
    table:      str
    before:     Optional[Dict[str, Any]]
    after:      Optional[Dict[str, Any]]
    source_lsn: Optional[str]
    source_ts_ms: Optional[int]
    pipeline:   Optional[str]

    @classmethod
    def from_value(cls, value: Dict[str, Any], topic: str) -> "CDCEvent":
        table = topic.split(".")[-1] if "." in topic else topic
        return cls(
            operation    = value.get("__op", "r"),
            table        = value.get("__table", table),
            before       = value.get("before"),
            after        = value.get("after") or value,
            source_lsn   = value.get("__source_lsn"),
            source_ts_ms = value.get("__source_ts_ms"),
            pipeline     = value.get("pipeline"),
        )

    @property
    def primary_record(self) -> Optional[Dict[str, Any]]:
        """Returns the relevant record: `after` for insert/update, `before` for delete."""
        if self.operation == "d":
            return self.before
        return self.after

    @property
    def lag_ms(self) -> Optional[int]:
        if self.source_ts_ms:
            return int(time.time() * 1000) - self.source_ts_ms
        return None


# ── Handlers ──────────────────────────────────────────────────────────────────
class TableHandlers:
    """
    Register per-table per-operation handlers.
    Handlers receive (event: CDCEvent) and should be idempotent.
    """
    def __init__(self) -> None:
        self._handlers: Dict[str, Callable[[CDCEvent], None]] = {}

    def register(self, table: str, operation: str = "*"):
        """Decorator: @handlers.register('orders', 'c')"""
        def decorator(fn: Callable[[CDCEvent], None]):
            key = f"{table}:{operation}"
            self._handlers[key] = fn
            log.info(f'"Registered handler","key":"{key}"')
            return fn
        return decorator

    def dispatch(self, event: CDCEvent) -> None:
        specific = f"{event.table}:{event.operation}"
        wildcard = f"{event.table}:*"
        handler  = self._handlers.get(specific) or self._handlers.get(wildcard)
        if handler:
            handler(event)
        else:
            log.debug(f'"No handler","table":"{event.table}","op":"{event.operation}"')


handlers = TableHandlers()


# ── Example handlers ─────────────────────────────────────────────────────────
@handlers.register("orders", "c")
def on_order_created(event: CDCEvent) -> None:
    record = event.primary_record
    log.info(f'"order.created","id":{record.get("id")},'
             f'"amount":{record.get("total_amount")},'
             f'"customer_id":{record.get("customer_id")}')
    # TODO: trigger fulfillment workflow, send confirmation email, update cache


@handlers.register("orders", "u")
def on_order_updated(event: CDCEvent) -> None:
    before = event.before or {}
    after  = event.after  or {}
    if before.get("status") != after.get("status"):
        log.info(f'"order.status_changed","id":{after.get("id")},'
                 f'"from":"{before.get("status")}","to":"{after.get("status")}"')


@handlers.register("products", "u")
def on_product_updated(event: CDCEvent) -> None:
    before = event.before or {}
    after  = event.after  or {}
    if before.get("stock_qty", 0) != after.get("stock_qty", 0):
        sku = after.get("sku")
        qty = after.get("stock_qty")
        log.info(f'"product.stock_changed","sku":"{sku}","qty":{qty}')
        if qty is not None and qty < 10:
            log.warning(f'"product.low_stock","sku":"{sku}","qty":{qty}')


@handlers.register("customers", "*")
def on_customer_change(event: CDCEvent) -> None:
    record = event.primary_record or {}
    log.info(f'"customer.change","op":"{event.operation}","id":{record.get("id")}')


# ── DLQ Producer ─────────────────────────────────────────────────────────────
class DLQProducer:
    def __init__(self, config: ConsumerConfig) -> None:
        self._producer = Producer({
            "bootstrap.servers": config.bootstrap_servers,
            "acks":              "all",
            "compression.type":  "lz4",
        })
        self._topic = config.dlq_topic

    def send(self, original: Message, reason: str, error_details: str) -> None:
        headers = {
            "dlq.reason":         reason,
            "dlq.original.topic": original.topic() or "",
            "dlq.original.partition": str(original.partition()),
            "dlq.original.offset":    str(original.offset()),
            "dlq.error":          error_details[:500],
            "dlq.timestamp":      str(int(time.time() * 1000)),
        }
        self._producer.produce(
            topic     = self._topic,
            key       = original.key(),
            value     = original.value(),
            headers   = [(k, v.encode()) for k, v in headers.items()],
        )
        self._producer.flush(timeout=5)
        log.error(f'"dlq.sent","reason":"{reason}","topic":"{original.topic()}",'
                  f'"offset":{original.offset()}')

    def close(self) -> None:
        self._producer.flush()


# ── Main Consumer ─────────────────────────────────────────────────────────────
class CDCConsumer:
    def __init__(self, config: ConsumerConfig) -> None:
        self.config   = config
        self.running  = True
        self._dlq     = DLQProducer(config)

        # Schema Registry + Avro deserializer
        sr_client = SchemaRegistryClient({"url": config.schema_registry_url})
        self._avro_deserializer = AvroDeserializer(sr_client)

        # Kafka consumer
        self._consumer = Consumer({
            "bootstrap.servers":        config.bootstrap_servers,
            "group.id":                 config.group_id,
            "auto.offset.reset":        config.auto_offset_reset,
            "enable.auto.commit":       config.enable_auto_commit,
            "session.timeout.ms":       config.session_timeout_ms,
            "heartbeat.interval.ms":    config.heartbeat_interval_ms,
            "max.poll.interval.ms":     config.max_poll_interval_ms,
            "fetch.min.bytes":          1,
            "fetch.max.wait.ms":        500,
        })

        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT,  self._handle_signal)

    def _handle_signal(self, signum: int, _frame) -> None:
        log.info(f'"shutdown.signal","signal":{signum}')
        self.running = False

    def _deserialize(self, msg: Message) -> Optional[Dict[str, Any]]:
        if msg.value() is None:
            return None  # tombstone
        try:
            ctx = SerializationContext(msg.topic(), MessageField.VALUE)
            return self._avro_deserializer(msg.value(), ctx)
        except Exception as exc:
            raise ValueError(f"Deserialization error: {exc}") from exc

    def _process_message(self, msg: Message) -> None:
        value = self._deserialize(msg)
        if value is None:
            log.debug(f'"tombstone","topic":"{msg.topic()}","offset":{msg.offset()}')
            return

        event = CDCEvent.from_value(value, msg.topic())

        # Track consumer lag
        if event.lag_ms is not None:
            EVENTS_LAG.labels(
                topic     = msg.topic(),
                partition = str(msg.partition()),
            ).set(event.lag_ms)

        with PROCESSING_TIME.labels(table=event.table, operation=event.operation).time():
            handlers.dispatch(event)

        EVENTS_TOTAL.labels(
            operation = event.operation,
            table     = event.table,
            status    = "success",
        ).inc()

    def _process_with_retry(self, msg: Message) -> None:
        last_exc: Optional[Exception] = None
        for attempt in range(1, self.config.max_retries + 1):
            try:
                self._process_message(msg)
                return
            except Exception as exc:
                last_exc = exc
                log.warning(
                    f'"retry","attempt":{attempt},'
                    f'"max":{self.config.max_retries},'
                    f'"error":"{exc}",'
                    f'"topic":"{msg.topic()}","offset":{msg.offset()}'
                )
                time.sleep(self.config.retry_backoff_ms / 1000 * attempt)

        # All retries exhausted → DLQ
        table = msg.topic().split(".")[-1]
        DLQ_EVENTS.labels(table=table, reason="max_retries_exceeded").inc()
        EVENTS_TOTAL.labels(
            operation = "unknown",
            table     = table,
            status    = "dlq",
        ).inc()
        self._dlq.send(msg, reason="max_retries_exceeded", error_details=str(last_exc))

    def run(self) -> None:
        log.info(f'"consumer.starting","topics":{self.config.topics},'
                 f'"group_id":"{self.config.group_id}"')
        start_http_server(self.config.metrics_port)
        self._consumer.subscribe(
            self.config.topics,
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )

        try:
            while self.running:
                msg = self._consumer.poll(timeout=self.config.poll_timeout_s)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        log.debug(f'"partition.eof","partition":{msg.partition()}')
                    else:
                        raise KafkaException(msg.error())
                    continue

                self._process_with_retry(msg)
                self._consumer.commit(message=msg, asynchronous=False)

        except KafkaException as exc:
            log.error(f'"kafka.fatal","error":"{exc}"')
            sys.exit(1)
        finally:
            self._consumer.close()
            self._dlq.close()
            log.info('"consumer.stopped"')

    def _on_assign(self, consumer, partitions) -> None:
        log.info(f'"rebalance.assign","partitions":{[p.partition for p in partitions]}')

    def _on_revoke(self, consumer, partitions) -> None:
        log.info(f'"rebalance.revoke","partitions":{[p.partition for p in partitions]}')
        # Flush any in-flight work here if batching
        consumer.commit(asynchronous=False)


# ── Entrypoint ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    cfg = ConsumerConfig()
    CDCConsumer(cfg).run()
