package io.confluent.developer.cookbook.flink.events;

import java.time.Instant;

/** A malformed version of an {@link Event}, missing the {@code id} property. */
public class MalformedEvent {

    public String data;
    public Instant timestamp;

    public MalformedEvent(final String data, final Instant timestamp) {
        this.data = data;
        this.timestamp = timestamp;
    }
}
