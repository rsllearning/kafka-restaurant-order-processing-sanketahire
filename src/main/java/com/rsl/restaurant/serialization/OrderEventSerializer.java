package com.rsl.restaurant.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.rsl.restaurant.model.OrderEvent;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Custom Kafka Serializer for OrderEvent using Jackson ObjectMapper.
 */
public class OrderEventSerializer implements Serializer<OrderEvent> {

    private final ObjectMapper objectMapper;

    public OrderEventSerializer() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No specific configuration required
    }

    @Override
    public byte[] serialize(String topic, OrderEvent data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SerializationException("Error serializing OrderEvent payload for topic [" + topic + "]", e);
        }
    }

    @Override
    public void close() {
        // No resources to close
    }
}
