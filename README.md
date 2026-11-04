# Restaurant Franchise Order Event Processing System (Apache Kafka)

[![Java 21](https://img.shields.io/badge/Java-21-orange.svg)](https://openjdk.org/projects/jdk/21/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.9.0-black.svg)](https://kafka.apache.org/)
[![Docker Compose](https://img.shields.io/badge/Docker%20Compose-v2-blue.svg)](https://docs.docker.com/compose/)
[![License](https://img.shields.io/badge/License-RSL%20Confidential-red.svg)]()

An end-to-end, enterprise-grade event-driven restaurant franchise order processing platform built with **Apache Kafka (KRaft mode)**, **Java 21**, and **Docker**.

---

## 1. Executive Summary & Architecture

The system coordinates orders for a restaurant franchise operating two distinct outlets:
* `RESTAURANT-101`
* `RESTAURANT-102`

Order events generated from a CLI-based producer are published to a central topic: `restaurant.orders.created`. Using `restaurant_id` as the message key, Kafka deterministically routes all events for a given outlet to the identical partition via **MurmurHash2 key-hashing**, preserving strict per-outlet chronological order.

Three decoupled downstream microservices consume the stream in parallel under distinct consumer groups:
1. **Kitchen Service (`kitchen-service-group`):** Manages food preparation tickets and station routing.
2. **Inventory Service (`inventory-service-group`):** Automatically reserves and decrements warehouse stock counts.
3. **Customer Notification Service (`notification-service-group`):** Dispatches customer SMS, email receipts, and push notifications.

```
+---------------------------------------------------------------------------------------------------+
|                                     SYSTEM ARCHITECTURE                                           |
|                                                                                                   |
|  [ CLI Order Producer ]                                                                           |
|  - Key: restaurant_id (RESTAURANT-101 / RESTAURANT-102)                                           |
|  - acks=all, enable.idempotence=true, snappy compression                                          |
|            |                                                                                      |
|            v                                                                                      |
|  +---------------------------------------------------------------------------------------------+  |
|  | Kafka Broker (KRaft Mode - Single Node Cluster)                                             |  |
|  | Topic: restaurant.orders.created (2 Partitions, Replication Factor 1)                        |  |
|  |                                                                                             |  |
|  |  Partition 0 [Offset 0, 1, 2, ... 8] <--- RESTAURANT-102 Orders (HashAffinity)              |  |
|  |  Partition 1 [Offset 0, 1, 2, ... 6] <--- RESTAURANT-101 Orders (HashAffinity)              |  |
|  +---------------------------------------------------------------------------------------------+  |
|            |                                   |                                   |              |
|            | (Independent Fan-Out)             | (Independent Fan-Out)             |              |
|            v                                   v                                   v              |
|  +--------------------+               +--------------------+              +--------------------+  |
|  | Kitchen Service    |               | Inventory Service  |              | Notification Svc   |  |
|  | Group:             |               | Group:             |              | Group:             |  |
|  | kitchen-service-grp|               | inventory-service-g|              | notification-serv-g|  |
|  | Commit: commitSync |               | Commit: commitSync |              | Commit: commitSync |  |
|  +--------------------+               +--------------------+              +--------------------+  |
+---------------------------------------------------------------------------------------------------+
```

---

## 2. Quick Start & Execution Runbook

### Prerequisites
* **Java:** JDK 21+ (`java -version`)
* **Maven:** Apache Maven 3.8+ (`mvn -v`)
* **Docker & Docker Compose:** Docker Engine 24+ (`docker compose version`)

---

### Step 1: Start Kafka in KRaft Mode
Start the single-node Kafka KRaft cluster using Docker Compose:
```bash
docker compose up -d
```
Verify container status:
```bash
docker ps
```
*(Reference evidence: `screenshots/01-docker-kafka-and-topic-created.png`)*

---

### Step 2: Create the Multi-Partition Topic via Kafka CLI
Execute the automated CLI topic provisioning script:
```bash
./scripts/create-topic.sh
```
Or run the Kafka CLI command manually:
```bash
docker exec kafka-broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic restaurant.orders.created \
  --partitions 2 \
  --replication-factor 1
```

Verify topic partition layout:
```bash
docker exec kafka-broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic restaurant.orders.created
```
*Expected Output:* Displays **Partition 0** and **Partition 1** with Leader 1 and ISR 1.

---

### Step 3: Build and Package the Java Application
Compile all source code and package the self-contained executable Fat JAR:
```bash
mvn clean package
```

---

### Step 4: Run the Three Downstream Consumers (In 3 Separate Terminals)
Open 3 separate terminal tabs or windows in the project directory:

* **Terminal 1 (Kitchen Service):**
  ```bash
  ./scripts/run-kitchen.sh
  ```
  *Group ID:* `kitchen-service-group`

* **Terminal 2 (Inventory Service):**
  ```bash
  ./scripts/run-inventory.sh
  ```
  *Group ID:* `inventory-service-group`

* **Terminal 3 (Customer Notification Service):**
  ```bash
  ./scripts/run-notification.sh
  ```
  *Group ID:* `notification-service-group`

---

### Step 5: Publish Orders via CLI Producer (In a 4th Terminal)
Open a 4th terminal to emit orders:

#### Option A: Publish Individual Orders via Command-Line Flags
```bash
# Order for RESTAURANT-101 (Routes consistently to Partition 1)
./scripts/run-producer.sh -r RESTAURANT-101 -o ORD-101-01 -c "Alice Johnson" -i "2x Paneer Tikka Burger, 1x Masala Fries"

# Order for RESTAURANT-102 (Routes consistently to Partition 0)
./scripts/run-producer.sh -r RESTAURANT-102 -o ORD-102-01 -c "Bob Smith" -i "1x Cheese Pizza, 2x Coke"
```

#### Option B: Run Automated Partition Affinity Demonstration
```bash
./scripts/run-producer.sh --demo
```
*(Reference evidence: `screenshots/02-producer-partition-affinity.png`)*

---

## 3. Operational Demonstrations & Grading Rubric Verification

### A. Key-Based Partition Affinity (Requirements 4 & 5)
* **Principle:** Kafka calculates $\text{partition} = |\text{MurmurHash2}(\text{key})| \pmod 2$.
* **Observation:**
  * All orders with `restaurant_id = RESTAURANT-101` land on **Partition 1**.
  * All orders with `restaurant_id = RESTAURANT-102` land on **Partition 0**.
* **Evidence:** `screenshots/02-producer-partition-affinity.png`.

---

### B. Multi-Service Consumer Fan-Out (Requirement 6)
* When order `ORD-101-FANOUT` is emitted:
  * **Kitchen Service** prints kitchen prep ticket.
  * **Inventory Service** reserves and decrements item stock.
  * **Customer Notification Service** dispatches SMS confirmation.
* All three microservices process the identical event concurrently without competing for partitions because each maintains an isolated `group.id`.
* **Evidence:** `screenshots/03c-kitchen-service-fanout.png`, `screenshots/04c-inventory-service-fanout.png`, `screenshots/05c-notification-service-fanout.png`, `screenshots/06-producer-fanout-order.png`.

---

### C. Failure Isolation & Offset Recovery (Requirement 7)
Demonstrates system resilience during worker crashes:

1. **Simulate Service Failure:**
   * In Terminal 2 (`Inventory Service`), terminate the worker with `Ctrl + C`.
2. **Publish New Orders During Outage:**
   ```bash
   ./scripts/run-producer.sh -r RESTAURANT-102 -o ORD-102-FAILOVER -c "Peter Parker" -i "1x Pepperoni Pizza, 1x Iced Tea"
   ```
3. **Verify Fault Isolation:**
   * `Kitchen Service` and `Notification Service` immediately ingest and process `ORD-102-FAILOVER` with zero interruption.
4. **Restart Recovering Service:**
   * In Terminal 2, restart `./scripts/run-inventory.sh`.
   * The Inventory Service connects to Kafka, checks `__consumer_offsets`, and **resumes processing exactly from its last committed offset (Offset 8)**, without duplicating old orders!
* **Evidence:** `screenshots/07-producer-failover-order.png`, `screenshots/08-inventory-recovery-offset.png`.

---

### D. Kafka CLI Offset Reset & Historical Event Replay (Requirement 8)
Demonstrates log immutability and the ability to rewind time:

1. **Stop Notification Service:**
   * In Terminal 3 (`Notification Service`), press `Ctrl + C`.
2. **Execute Offset Reset via Kafka CLI:**
   ```bash
   ./scripts/reset-offsets.sh notification-service-group
   ```
   *CLI Output:* Both Partition 0 and Partition 1 `NEW-OFFSET` values are reset to `0`.
3. **Restart Notification Service:**
   ```bash
   ./scripts/run-notification.sh
   ```
4. **Observe Historical Replay:**
   * Notification Service automatically rewinds its offset pointer and reprocesses every order from Offset 0 up to current!
   * Kitchen and Inventory services remain completely unaffected.
* **Evidence:** `screenshots/09-cli-offset-reset-execution.png`, `screenshots/10a-notification-replay-start.png`, `screenshots/10e-notification-replay-complete.png`.

---

## 4. Enterprise Configuration Standards

| Parameter | Configuration | Engineering Rationale |
| :--- | :--- | :--- |
| **Cluster Mode** | KRaft (No ZooKeeper) | Kafka 4.0 production standard; millisecond controller failover. |
| **`acks`** | `all` (`-1`) | Guarantees writes are committed across all in-sync replicas (ISR). |
| **`enable.idempotence`** | `true` | Prevents duplicate record injection caused by network retries. |
| **`compression.type`** | `snappy` | High throughput with optimal CPU-to-compression ratio. |
| **`enable.auto.commit`** | `false` | Enforces explicit manual commits (`commitSync`) after business processing. |
| **`auto.offset.reset`** | `earliest` | Ensures newly provisioned consumer groups process historical logs from offset 0. |

---

*Author: Sanket Ahire (Raja Software Labs)*
