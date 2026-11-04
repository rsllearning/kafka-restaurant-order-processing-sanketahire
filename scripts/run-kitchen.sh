#!/bin/bash
# Launcher script for Kitchen Service Consumer

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JAR_FILE="$DIR/target/restaurant-order-processing-1.0.0.jar"

if [ ! -f "$JAR_FILE" ]; then
    echo "JAR not found. Compiling and packaging project..."
    mvn -f "$DIR/pom.xml" clean package -DskipTests
fi

java -cp "$JAR_FILE" com.rsl.restaurant.consumer.KitchenServiceApp "$@"
