package com.rsl.restaurant.consumer;

import com.rsl.restaurant.config.KafkaConfig;
import com.rsl.restaurant.model.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Reusable abstract base consumer providing:
 * - Isolated Consumer Group configuration
 * - Manual Offset Commit (commitSync) to prevent message loss during worker crashes
 * - Clean formatted logging showing Topic, Partition, Offset, Key, and Order ID
 * - JVM Shutdown hook handling consumer.wakeup() for clean rebalances
 */
public abstract class BaseOrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(BaseOrderConsumer.class);

    private final String serviceName;
    private final String groupId;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public BaseOrderConsumer(String serviceName, String groupId) {
        this.serviceName = serviceName;
        this.groupId = groupId;
    }

    public void start() {
        Properties props = KafkaConfig.createConsumerProperties(groupId);

        try (KafkaConsumer<String, OrderEvent> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(KafkaConfig.TOPIC_ORDERS_CREATED));

            // Graceful shutdown hook for clean offset commit and group leave
            final Thread mainThread = Thread.currentThread();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.printf("%n[%s] Shutdown signal received. Closing consumer...%n", serviceName);
                running.set(false);
                consumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));

            System.out.println("================================================================================");
            System.out.printf(">>> [%s] ACTIVE & LISTENING <<<%n", serviceName.toUpperCase());
            System.out.printf("  Topic             : %s%n", KafkaConfig.TOPIC_ORDERS_CREATED);
            System.out.printf("  Consumer Group ID : %s%n", groupId);
            System.out.printf("  Commit Mode       : Manual Synchronous Commit (commitSync)%n");
            System.out.printf("  Offset Reset      : earliest%n");
            System.out.println("  Awaiting incoming restaurant orders...");
            System.out.println("================================================================================");

            while (running.get()) {
                ConsumerRecords<String, OrderEvent> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, OrderEvent> record : records) {
                    printBanner(record);
                    processRecord(record);
                    // Manual offset commit ensuring at-least-once processing
                    consumer.commitSync();
                }
            }
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (running.get()) {
                throw e;
            }
        } catch (Exception e) {
            log.error("[{}] Error in consumer execution loop", serviceName, e);
        } finally {
            System.out.printf("[%s] Consumer shutdown complete.%n", serviceName);
        }
    }

    private void printBanner(ConsumerRecord<String, OrderEvent> record) {
        OrderEvent order = record.value();
        System.out.println("--------------------------------------------------------------------------------");
        System.out.printf("[%s RECEIVED ORDER]%n", serviceName.toUpperCase());
        System.out.printf("  Partition    : %d | Offset: %d%n", record.partition(), record.offset());
        System.out.printf("  Message Key  : %s (restaurant_id)%n", record.key());
        System.out.printf("  Order ID     : %s%n", order.getOrderId());
        System.out.printf("  Customer     : %s%n", order.getCustomerName());
        System.out.printf("  Items        : %s%n", order.getItems());
        System.out.printf("  Order Time   : %s%n", order.getOrderTime());
    }

    /**
     * Domain-specific business logic implemented by each microservice.
     */
    protected abstract void processRecord(ConsumerRecord<String, OrderEvent> record);

    public String getServiceName() {
        return serviceName;
    }

    public String getGroupId() {
        return groupId;
    }
}
