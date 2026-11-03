package com.rsl.restaurant.model;

/**
 * Lifecycle status of a restaurant order.
 */
public enum OrderStatus {
    CREATED,
    PREPARING,
    READY_FOR_PICKUP,
    COMPLETED,
    CANCELLED
}
