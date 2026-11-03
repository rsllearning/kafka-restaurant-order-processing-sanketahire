package com.rsl.restaurant.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Core Domain Event representing an Order Created event in the franchise.
 * Fields strictly comply with Section 3 of the assignment specification:
 * - order_id
 * - restaurant_id (Kafka Message Key)
 * - customer_name
 * - items
 * - order_time
 * - order_status
 */
public class OrderEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("order_id")
    private String orderId;

    @JsonProperty("restaurant_id")
    private String restaurantId;

    @JsonProperty("customer_name")
    private String customerName;

    @JsonProperty("items")
    private List<OrderItem> items = new ArrayList<>();

    @JsonProperty("order_time")
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private String orderTime;

    @JsonProperty("order_status")
    private String orderStatus;

    public OrderEvent() {
    }

    @JsonCreator
    public OrderEvent(
            @JsonProperty("order_id") String orderId,
            @JsonProperty("restaurant_id") String restaurantId,
            @JsonProperty("customer_name") String customerName,
            @JsonProperty("items") List<OrderItem> items,
            @JsonProperty("order_time") String orderTime,
            @JsonProperty("order_status") String orderStatus) {
        this.orderId = orderId;
        this.restaurantId = restaurantId;
        this.customerName = customerName;
        this.items = items != null ? items : new ArrayList<>();
        this.orderTime = orderTime != null ? orderTime : Instant.now().toString();
        this.orderStatus = orderStatus != null ? orderStatus : OrderStatus.CREATED.name();
    }

    // Convenience Builder Factory
    public static OrderEvent create(String orderId, String restaurantId, String customerName, List<OrderItem> items) {
        return new OrderEvent(
                orderId,
                restaurantId,
                customerName,
                items,
                Instant.now().toString(),
                OrderStatus.CREATED.name()
        );
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getRestaurantId() {
        return restaurantId;
    }

    public void setRestaurantId(String restaurantId) {
        this.restaurantId = restaurantId;
    }

    public String getCustomerName() {
        return customerName;
    }

    public void setCustomerName(String customerName) {
        this.customerName = customerName;
    }

    public List<OrderItem> getItems() {
        return items;
    }

    public void setItems(List<OrderItem> items) {
        this.items = items;
    }

    public String getOrderTime() {
        return orderTime;
    }

    public void setOrderTime(String orderTime) {
        this.orderTime = orderTime;
    }

    public String getOrderStatus() {
        return orderStatus;
    }

    public void setOrderStatus(String orderStatus) {
        this.orderStatus = orderStatus;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrderEvent that = (OrderEvent) o;
        return Objects.equals(orderId, that.orderId) &&
               Objects.equals(restaurantId, that.restaurantId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderId, restaurantId);
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "order_id='" + orderId + '\'' +
                ", restaurant_id='" + restaurantId + '\'' +
                ", customer_name='" + customerName + '\'' +
                ", items=" + items +
                ", order_time='" + orderTime + '\'' +
                ", order_status='" + orderStatus + '\'' +
                '}';
    }
}
