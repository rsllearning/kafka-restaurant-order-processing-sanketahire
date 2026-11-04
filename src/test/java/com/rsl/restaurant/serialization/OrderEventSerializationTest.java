package com.rsl.restaurant.serialization;

import com.rsl.restaurant.model.OrderEvent;
import com.rsl.restaurant.model.OrderItem;
import com.rsl.restaurant.model.OrderStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class OrderEventSerializationTest {

    @Test
    @DisplayName("Verify OrderEvent serializes and deserializes correctly with all required fields")
    void testSerializationRoundTrip() {
        OrderEventSerializer serializer = new OrderEventSerializer();
        OrderEventDeserializer deserializer = new OrderEventDeserializer();

        List<OrderItem> items = List.of(
                new OrderItem("Cheese Burger", 2),
                new OrderItem("French Fries", 1)
        );

        OrderEvent originalEvent = OrderEvent.create("ORD-1001", "RESTAURANT-101", "Alice Smith", items);

        byte[] serializedBytes = serializer.serialize("restaurant.orders.created", originalEvent);
        assertNotNull(serializedBytes);
        assertTrue(serializedBytes.length > 0);

        // Verify JSON string contains all required fields
        String jsonString = new String(serializedBytes);
        assertTrue(jsonString.contains("order_id"));
        assertTrue(jsonString.contains("restaurant_id"));
        assertTrue(jsonString.contains("customer_name"));
        assertTrue(jsonString.contains("items"));
        assertTrue(jsonString.contains("order_time"));
        assertTrue(jsonString.contains("order_status"));

        OrderEvent deserializedEvent = deserializer.deserialize("restaurant.orders.created", serializedBytes);
        assertNotNull(deserializedEvent);
        assertEquals("ORD-1001", deserializedEvent.getOrderId());
        assertEquals("RESTAURANT-101", deserializedEvent.getRestaurantId());
        assertEquals("Alice Smith", deserializedEvent.getCustomerName());
        assertEquals(2, deserializedEvent.getItems().size());
        assertEquals(OrderStatus.CREATED.name(), deserializedEvent.getOrderStatus());
        assertNotNull(deserializedEvent.getOrderTime());

        serializer.close();
        deserializer.close();
    }
}
