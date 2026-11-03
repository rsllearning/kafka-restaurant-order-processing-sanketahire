package com.rsl.restaurant.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.rsl.restaurant.model.OrderEvent;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Custom Kafka Deserializer for OrderEvent using Jackson ObjectMapper.
 */
public class OrderEventDeserializer implements Deserializer<OrderEvent> {

    private final ObjectMapper objectMapper;

    public OrderEventDeserializer() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No specific configuration required
    }

    @Override
    public OrderEvent deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }
        try {
            return objectMapper.readValue(data, OrderEvent.class);
        } catch (Exception e) {
            throw new SerializationException("Error deserializing OrderEvent payload from topic [" + topic + "]", e);
        }
    }

    @Override
    public void close() {
        // No resources to close
    }
}
