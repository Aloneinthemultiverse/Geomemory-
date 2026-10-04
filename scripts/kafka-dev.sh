#!/usr/bin/env bash
# Start a single-node Kafka (KRaft mode, no ZooKeeper) for development/tests.
#   KAFKA_HOME=/opt/kafka_2.13-3.8.0 scripts/kafka-dev.sh
# Download: https://archive.apache.org/dist/kafka/3.8.0/kafka_2.13-3.8.0.tgz
set -euo pipefail
KAFKA_HOME="${KAFKA_HOME:-/opt/kafka_2.13-3.8.0}"
DATA="${KAFKA_DATA:-/tmp/geomemory-kafka}"
CONF="$DATA/server.properties"
mkdir -p "$DATA"
sed -e "s#^log.dirs=.*#log.dirs=$DATA/logs#" \
    "$KAFKA_HOME/config/kraft/server.properties" > "$CONF"
cat >> "$CONF" <<PROPS
num.partitions=4
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
PROPS
if [ ! -f "$DATA/logs/meta.properties" ]; then
  "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)" -c "$CONF" >/dev/null
fi
export KAFKA_HEAP_OPTS="${KAFKA_HEAP_OPTS:--Xmx512m -Xms256m}"
"$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "$CONF"
for _ in $(seq 60); do
  if "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    echo "Kafka ready on localhost:9092"; exit 0
  fi
  sleep 1
done
echo "Kafka did not start; see $KAFKA_HOME/logs/server.log" >&2; exit 1
