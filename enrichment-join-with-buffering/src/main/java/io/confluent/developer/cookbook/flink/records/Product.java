package io.confluent.developer.cookbook.flink.records;

/** A POJO class for products. */
public class Product {

    /** A Flink POJO must have public fields, or getters and setters */
    public long p_time;

    public long p_id;
    public String p_name;
    public float p_rating;
    public int p_popularity;

    /** A Flink POJO must have a no-args default constructor */
    public Product() {}

    public Product(
            final long p_time,
            final long p_id,
            final String p_name,
            float p_rating,
            int p_popularity) {
        this.p_time = p_time;
        this.p_id = p_id;
        this.p_name = p_name;
        this.p_rating = p_rating;
        this.p_popularity = p_popularity;
    }

    @Override
    public String toString() {
        return "Product{"
                + "p_time="
                + p_time
                + ", p_id="
                + p_id
                + ", p_name='"
                + p_name
                + '\''
                + ", p_rating="
                + p_rating
                + ", p_popularity="
                + p_popularity
                + '}';
    }
}
