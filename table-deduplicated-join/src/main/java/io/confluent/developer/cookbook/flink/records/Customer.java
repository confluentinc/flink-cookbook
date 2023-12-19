package io.confluent.developer.cookbook.flink.records;

import java.util.Objects;

/** A simple Customer type. */
public class Customer {

    // A Flink POJO must have public fields, or getters and setters
    public long c_id;
    public String c_name;

    // A Flink POJO must have a no-args default constructor
    public Customer() {}

    public Customer(final long id, final String name) {
        this.c_id = id;
        this.c_name = name;
    }

    // Used for printing during development
    @Override
    public String toString() {
        return "Event{" + "id=" + c_id + ", name='" + c_name + '\'' + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Customer customer = (Customer) o;
        return c_id == customer.c_id && c_name.equals(customer.c_name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(c_id, c_name);
    }
}
