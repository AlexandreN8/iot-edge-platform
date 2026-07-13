set -euo pipefail

BOOTSTRAP="kafka:9092"
BIN="/opt/kafka/bin"

echo "Waiting for Kafka to be fully ready..."
until "$BIN/kafka-broker-api-versions.sh" --bootstrap-server "$BOOTSTRAP" > /dev/null 2>&1; do
  sleep 2
done

create_topic() {
  local name="$1"
  local partitions="$2"
  local retention_ms="$3"
  local cleanup_policy="${4:-delete}"

  echo "Creating topic: $name (partitions=$partitions, retention.ms=$retention_ms, cleanup.policy=$cleanup_policy)"

  "$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor 1 \
    --config "retention.ms=$retention_ms" \
    --config "cleanup.policy=$cleanup_policy"
}

# raw: transit topic, not a long-term store (S3 archival handles that later).
create_topic "raw" 3 172800000            # 48h

# enriched: business-cleaned data, consumed by Aggregator/downstream.
create_topic "enriched" 3 604800000       # 7 days

# anomalie: lower volume by nature (only actual anomalies)
create_topic "anomalie" 1 604800000       # 7 days

# dlq: dead-letter queue for messages that failed processing.
create_topic "dlq" 1 1209600000           # 14 days

# ota: compacted, not time-retained
create_topic "ota" 1 -1 "compact"

# ota_status: audit trail of update attempts, one entry per event, retained
# like other event logs 
create_topic "ota_status" 1 604800000      # 7 days

echo "All topics created:"
"$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list