#!/bin/bash
# Script to create the restaurant.orders.created topic with 2 partitions using Kafka CLI

BOOTSTRAP_SERVER="localhost:9092"
TOPIC_NAME="restaurant.orders.created"
PARTITIONS=2
REPLICATION_FACTOR=1

echo "================================================================="
echo "Creating Kafka Topic: $TOPIC_NAME ($PARTITIONS Partitions)"
echo "================================================================="

# Execute kafka-topics.sh via docker exec against the running kafka-broker container
docker exec kafka-broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --create \
  --if-not-exists \
  --topic $TOPIC_NAME \
  --partitions $PARTITIONS \
  --replication-factor $REPLICATION_FACTOR

echo ""
echo "================================================================="
echo "Verifying Topic Details & Partition Configuration"
echo "================================================================="
docker exec kafka-broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --describe \
  --topic $TOPIC_NAME
