package io.confluent.developer.cookbook.flink.records;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.types.Row;

public class TestData {
    public static final Customer[] CUSTOMERS =
            new Customer[] {
                new Customer(12L, "Alice"), new Customer(32L, "Bob"), new Customer(7L, "Carol")
            };

    public static final Transaction[] TRANSACTIONS =
            new Transaction[] {
                new Transaction(
                        Instant.parse("2021-10-08T12:33:12.000Z"),
                        1L,
                        12L,
                        new BigDecimal("325.12")),
                new Transaction(
                        Instant.parse("2021-10-10T08:00:00.000Z"), 2L, 7L, new BigDecimal("13.99")),
                new Transaction(
                        Instant.parse("2021-10-10T08:00:00.000Z"), 2L, 7L, new BigDecimal("13.99")),
                new Transaction(
                        Instant.parse("2021-10-14T17:04:00.000Z"),
                        3L,
                        12L,
                        new BigDecimal("52.48")),
                new Transaction(
                        Instant.parse("2021-10-14T17:06:00.000Z"),
                        4L,
                        32L,
                        new BigDecimal("26.11")),
                new Transaction(
                        Instant.parse("2021-10-14T18:23:00.000Z"), 5L, 32L, new BigDecimal("22.03"))
            };

    public static final List<Row> EXPECTED_DEDUPLICATED_JOIN_RESULTS =
            Arrays.asList(
                    Row.of(1L, "Alice", new BigDecimal("325.12")),
                    Row.of(2L, "Carol", new BigDecimal("13.99")),
                    Row.of(3L, "Alice", new BigDecimal("52.48")),
                    Row.of(4L, "Bob", new BigDecimal("26.11")),
                    Row.of(5L, "Bob", new BigDecimal("22.03")));
}
