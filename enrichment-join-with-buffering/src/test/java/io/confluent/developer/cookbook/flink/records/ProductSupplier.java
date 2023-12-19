package io.confluent.developer.cookbook.flink.records;

import java.time.Instant;
import java.util.Random;
import java.util.function.Supplier;

/** A supplier that produces Products. */
public class ProductSupplier implements Supplier<Product> {
    public static final int TOTAL_PRODUCTS = 400;
    private static final Random RANDOM = new Random();
    private int id = 0;

    @Override
    public Product get() {
        long timestamp = Instant.now().toEpochMilli();
        String name = "product" + id;
        float rating = RANDOM.nextInt(50) / 10.0F;
        int popularity = RANDOM.nextInt(TOTAL_PRODUCTS);

        return new Product(timestamp, id++, name, rating, popularity);
    }
}
