FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka-dev gcc curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py .

ENV KAFKA_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092,kafka-3:9092
ENV SCHEMA_REGISTRY_URL=http://schema-registry:8081
ENV CONSUMER_GROUP_ID=cdc-consumer-group
ENV METRICS_PORT=9100

EXPOSE 9100

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD curl -sf http://localhost:9100/metrics || exit 1

CMD ["python", "-u", "consumer.py"]
