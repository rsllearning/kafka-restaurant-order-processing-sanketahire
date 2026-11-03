#!/bin/bash
# Script to reset offsets for a consumer group using native Kafka CLI tools.
# Note: Consumers in the target group MUST be stopped before running this command.

BOOTSTRAP_SERVER="localhost:9092"
TOPIC_NAME="restaurant.orders.created"
DEFAULT_GROUP="notification-service-group"

TARGET_GROUP="${1:-$DEFAULT_GROUP}"

echo "================================================================="
echo "Resetting Offsets for Consumer Group: $TARGET_GROUP"
echo "Target Topic: $TOPIC_NAME -> Earliest Offset"
echo "================================================================="

docker exec kafka-broker /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --group $TARGET_GROUP \
  --reset-offsets \
  --to-earliest \
  --execute \
  --topic $TOPIC_NAME

echo ""
echo "================================================================="
echo "Verifying Updated Consumer Group Offsets"
echo "================================================================="
docker exec kafka-broker /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --describe \
  --group $TARGET_GROUP
