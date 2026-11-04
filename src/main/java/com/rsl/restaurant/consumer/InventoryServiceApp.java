package com.rsl.restaurant.consumer;

import com.rsl.restaurant.config.KafkaConfig;
import com.rsl.restaurant.model.OrderEvent;
import com.rsl.restaurant.model.OrderItem;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Inventory Service Microservice:
 * Belongs to 'inventory-service-group'.
 * Simulates warehouse/kitchen inventory deduction and stock level checks.
 */
public class InventoryServiceApp extends BaseOrderConsumer {

    public InventoryServiceApp() {
        super("Inventory Service", KafkaConfig.INVENTORY_GROUP_ID);
    }

    @Override
    protected void processRecord(ConsumerRecord<String, OrderEvent> record) {
        OrderEvent order = record.value();
        System.out.printf("  Action Taken : [STOCK DEDUCTION] Decrementing warehouse inventory for outlet [%s]:%n", order.getRestaurantId());
        for (OrderItem item : order.getItems()) {
            System.out.printf("                 * Reserved %d unit(s) of '%s'%n", item.getQuantity(), item.getItemName());
        }
        System.out.printf("  Status       : Inventory allocated successfully for Order [%s].%n", order.getOrderId());
        System.out.println("--------------------------------------------------------------------------------");
    }

    public static void main(String[] args) {
        new InventoryServiceApp().start();
    }
}
