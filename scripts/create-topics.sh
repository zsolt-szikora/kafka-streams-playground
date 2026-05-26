#!/usr/bin/env bash
set -euo pipefail

# remove old topics
#kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic orders-placed
#kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic customer-profiles
#sleep 3

# crete topics
kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic orders-placed \
    --partitions 6 --replication-factor 3
kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic customer-profiles \
    --partitions 6 --replication-factor 3 \
    --config cleanup.policy=compact
kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic orders-per-window \
    --partitions 6 --replication-factor 3

# reset streams app
rm -rf /tmp/kafka-streams/orders-aggregator-v1/
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete --group orders-aggregator-v1 > /dev/null 2>&1 || true