package io.confluent.developer.cookbook.flink.events;

import java.util.Objects;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * A simple Event that Flink recognizes as a valid POJO.
 *
 * <p>Because it has valid (deterministic) implementations of equals and hashcode, this type can
 * also be used a key.
 */
@JsonPropertyOrder({"id", "character", "location"})
public class Event {

    /** A Flink POJO must have public fields, or getters and setters */
    public long id;

    public String character;
    public String location;

    /** A Flink POJO must have a no-args default constructor */
    public Event() {}

    public Event(final long id, final String character, final String location) {
        this.id = id;
        this.character = character;
        this.location = location;
    }

    /** Used for printing during development */
    @Override
    public String toString() {
        return "Event{"
                + "id="
                + id
                + ", character='"
                + character
                + '\''
                + ", location='"
                + location
                + '\''
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Event event = (Event) o;
        return id == event.id
                && character.equals(event.character)
                && location.equals(event.location);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, character, location);
    }
}
