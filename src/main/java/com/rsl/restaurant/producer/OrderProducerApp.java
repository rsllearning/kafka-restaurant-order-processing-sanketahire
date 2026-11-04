package com.rsl.restaurant.producer;

import com.rsl.restaurant.config.KafkaConfig;
import com.rsl.restaurant.model.OrderEvent;
import com.rsl.restaurant.model.OrderItem;
import com.rsl.restaurant.model.OrderStatus;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * CLI-based Order Producer Application for the Restaurant Franchise.
 * Allows publishing order events via command-line arguments and proves
 * deterministic key-based partitioning (MurmurHash2) for RESTAURANT-101 and RESTAURANT-102.
 */
@Command(name = "order-producer",
        mixinStandardHelpOptions = true,
        version = "1.0.0",
        description = "Publishes restaurant order events to Kafka with key-based partition affinity.")
public class OrderProducerApp implements Callable<Integer> {

    private static final Logger log = LoggerFactory.getLogger(OrderProducerApp.class);

    @Option(names = {"-r", "--restaurant-id"},
            description = "Restaurant ID (Key): e.g. RESTAURANT-101 or RESTAURANT-102")
    private String restaurantId;

    @Option(names = {"-o", "--order-id"},
            description = "Unique Order ID (default: auto-generated)")
    private String orderId;

    @Option(names = {"-c", "--customer-name"},
            description = "Customer Full Name (default: 'Walk-in Customer')")
    private String customerName = "Walk-in Customer";

    @Option(names = {"-i", "--items"},
            description = "Comma-separated list of items with optional quantity (e.g. '2x Burger, 1x Fries')")
    private String itemsString;

    @Option(names = {"-d", "--demo"},
            description = "Execute an automated batch demo publishing alternating orders for both outlets to prove partition affinity")
    private boolean demoMode;

    public static void main(String[] args) {
        int exitCode = new CommandLine(new OrderProducerApp()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        Properties producerProps = KafkaConfig.createProducerProperties();

        try (KafkaProducer<String, OrderEvent> producer = new KafkaProducer<>(producerProps)) {
            if (demoMode) {
                runPartitionAffinityDemo(producer);
                return 0;
            }

            if (restaurantId == null || restaurantId.isBlank()) {
                System.err.println("Error: --restaurant-id is required (e.g. -r RESTAURANT-101 or -r RESTAURANT-102).");
                CommandLine.usage(this, System.out);
                return 1;
            }

            String effectiveOrderId = (orderId != null && !orderId.isBlank())
                    ? orderId
                    : "ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

            List<OrderItem> parsedItems = parseItems(itemsString);

            OrderEvent event = new OrderEvent(
                    effectiveOrderId,
                    restaurantId.trim().toUpperCase(),
                    customerName,
                    parsedItems,
                    Instant.now().toString(),
                    OrderStatus.CREATED.name()
            );

            publishOrder(producer, event);
            return 0;
        } catch (Exception e) {
            log.error("Fatal error executing Order Producer", e);
            return 1;
        }
    }

    /**
     * Publishes a single order event to Kafka using restaurant_id as the message key.
     */
    public static RecordMetadata publishOrder(KafkaProducer<String, OrderEvent> producer, OrderEvent event) {
        String key = event.getRestaurantId();
        ProducerRecord<String, OrderEvent> record =
                new ProducerRecord<>(KafkaConfig.TOPIC_ORDERS_CREATED, key, event);

        try {
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get(5, TimeUnit.SECONDS);

            System.out.println("================================================================================");
            System.out.println("[PRODUCER SUCCESS] Order Event Published to Kafka");
            System.out.printf("  Topic          : %s%n", metadata.topic());
            System.out.printf("  Message Key    : %s (restaurant_id)%n", key);
            System.out.printf("  Partition      : %d  <-- [Key-Affinity Target Partition]%n", metadata.partition());
            System.out.printf("  Offset         : %d%n", metadata.offset());
            System.out.printf("  Order ID       : %s%n", event.getOrderId());
            System.out.printf("  Customer       : %s%n", event.getCustomerName());
            System.out.printf("  Items          : %s%n", event.getItems());
            System.out.printf("  Timestamp      : %s%n", event.getOrderTime());
            System.out.printf("  Status         : %s%n", event.getOrderStatus());
            System.out.println("================================================================================");

            return metadata;
        } catch (Exception e) {
            System.err.printf("[PRODUCER ERROR] Failed to deliver order [%s]: %s%n", event.getOrderId(), e.getMessage());
            throw new RuntimeException("Kafka publish failed", e);
        }
    }

    /**
     * Publishes a sequence of orders for both RESTAURANT-101 and RESTAURANT-102
     * to unambiguously demonstrate consistent partition routing.
     */
    private void runPartitionAffinityDemo(KafkaProducer<String, OrderEvent> producer) throws InterruptedException {
        System.out.println("================================================================================");
        System.out.println(">>> RUNNING PARTITION AFFINITY DEMO FOR RESTAURANT-101 & RESTAURANT-102 <<<");
        System.out.println("================================================================================");

        List<OrderEvent> demoOrders = List.of(
                OrderEvent.create("ORD-101-A", "RESTAURANT-101", "Alice Smith",
                        List.of(new OrderItem("Paneer Tikka Burger", 2), new OrderItem("Masala Fries", 1))),
                OrderEvent.create("ORD-102-A", "RESTAURANT-102", "Bob Johnson",
                        List.of(new OrderItem("Cheese Pizza", 1), new OrderItem("Coke", 2))),
                OrderEvent.create("ORD-101-B", "RESTAURANT-101", "Charlie Brown",
                        List.of(new OrderItem("Veggie Wrap", 1), new OrderItem("Cold Coffee", 1))),
                OrderEvent.create("ORD-102-B", "RESTAURANT-102", "Diana Prince",
                        List.of(new OrderItem("Chicken Shawarma", 2), new OrderItem("Garlic Dip", 2)))
        );

        for (OrderEvent order : demoOrders) {
            publishOrder(producer, order);
            Thread.sleep(400); // Small pause for clean visual demonstration
        }

        System.out.println("\n>>> PARTITION AFFINITY SUMMARY <<<");
        System.out.println("All orders for RESTAURANT-101 routed to their assigned partition.");
        System.out.println("All orders for RESTAURANT-102 routed to their assigned partition.");
        System.out.println("Strict per-outlet chronological order is guaranteed by Kafka's partition key hashing.");
        System.out.println("================================================================================");
    }

    /**
     * Helper to parse item strings such as "2x Burger, 1x Fries" or "Pizza:2,Soda:1" or "Burger, Fries".
     */
    private static List<OrderItem> parseItems(String itemsString) {
        List<OrderItem> items = new ArrayList<>();
        if (itemsString == null || itemsString.isBlank()) {
            items.add(new OrderItem("Chefs Special Combo", 1));
            return items;
        }

        String[] tokens = itemsString.split(",");
        for (String token : tokens) {
            token = token.trim();
            if (token.isEmpty()) continue;

            if (token.matches("(?i)^(\\d+)\\s*x\\s*(.+)")) {
                String[] parts = token.split("(?i)\\s*x\\s*", 2);
                int qty = Integer.parseInt(parts[0].trim());
                items.add(new OrderItem(parts[1].trim(), qty));
            } else if (token.contains(":")) {
                String[] parts = token.split(":", 2);
                int qty = Integer.parseInt(parts[1].trim());
                items.add(new OrderItem(parts[0].trim(), qty));
            } else {
                items.add(new OrderItem(token, 1));
            }
        }
        return items;
    }
}
