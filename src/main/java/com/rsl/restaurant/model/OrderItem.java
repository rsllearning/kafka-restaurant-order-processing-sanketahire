package com.rsl.restaurant.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

/**
 * Represents an individual menu item in a restaurant order.
 */
public class OrderItem implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("item_name")
    private String itemName;

    @JsonProperty("quantity")
    private int quantity;

    public OrderItem() {
    }

    @JsonCreator
    public OrderItem(@JsonProperty("item_name") String itemName,
                     @JsonProperty("quantity") int quantity) {
        this.itemName = itemName;
        this.quantity = quantity;
    }

    public String getItemName() {
        return itemName;
    }

    public void setItemName(String itemName) {
        this.itemName = itemName;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrderItem orderItem = (OrderItem) o;
        return quantity == orderItem.quantity && Objects.equals(itemName, orderItem.itemName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemName, quantity);
    }

    @Override
    public String toString() {
        return quantity + "x " + itemName;
    }
}
