package com.rsl.restaurant.consumer;

import com.rsl.restaurant.config.KafkaConfig;
import com.rsl.restaurant.model.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Kitchen Service Microservice:
 * Belongs to 'kitchen-service-group'.
 * Simulates kitchen display ticketing and food preparation workflows.
 */
public class KitchenServiceApp extends BaseOrderConsumer {

    public KitchenServiceApp() {
        super("Kitchen Service", KafkaConfig.KITCHEN_GROUP_ID);
    }

    @Override
    protected void processRecord(ConsumerRecord<String, OrderEvent> record) {
        OrderEvent order = record.value();
        System.out.printf("  Action Taken : [KITCHEN TICKET GENERATED] Sent to station at %s for prep of %s%n",
                order.getRestaurantId(), order.getItems());
        System.out.printf("  Status       : Order [%s] accepted by kitchen. Cooking started.%n", order.getOrderId());
        System.out.println("--------------------------------------------------------------------------------");
    }

    public static void main(String[] args) {
        new KitchenServiceApp().start();
    }
}
