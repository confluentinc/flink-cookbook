package io.confluent.developer.cookbook.flink.records;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Random;
import java.util.function.Supplier;

/** A supplier that produces Transactions. */
public class TransactionSupplier implements Supplier<Transaction> {
    private static final Random RANDOM = new Random();
    private int id = 0;

    @Override
    public Transaction get() {
        return new Transaction(
                Instant.now().toEpochMilli(),
                this.id++,
                RANDOM.nextInt(ProductSupplier.TOTAL_PRODUCTS),
                new BigDecimal(1000.0 * RANDOM.nextFloat()));
    }
}
