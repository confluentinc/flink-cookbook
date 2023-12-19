package io.confluent.developer.cookbook.flink.records;

import java.math.BigDecimal;
import java.util.Objects;

/** A POJO class for transactions. */
public class Transaction {

    /** A Flink POJO must have public fields, or getters and setters */
    public long t_time;

    public long t_id;
    public long t_product_id;
    public BigDecimal t_amount;
    public Float t_product_rating;
    public Integer t_product_popularity;

    /** A Flink POJO must have a no-args default constructor */
    public Transaction() {}

    public Transaction(
            final long t_time,
            final long t_id,
            final long t_product_id,
            final BigDecimal t_amount) {
        this.t_time = t_time;
        this.t_id = t_id;
        this.t_amount = t_amount;
        this.t_product_id = t_product_id;
    }

    /**
     * Returns a new Transaction. This implementation can be safely used regardless of whether
     * object reuse is enabled or not.
     *
     * @param t the Transaction to be enriched
     * @param p the Product providing the enrichment data
     * @return a new Transaction that is the result of enriching Transaction t with Product p
     */
    public static Transaction enrich(Transaction t, Product p) {
        Transaction enriched = new Transaction(t.t_time, t.t_id, t.t_product_id, t.t_amount);
        enriched.t_product_rating = p.p_rating;
        enriched.t_product_popularity = p.p_popularity;
        return enriched;
    }

    @Override
    public String toString() {
        String result =
                "Transaction{"
                        + "t_time="
                        + t_time
                        + ", t_id="
                        + t_id
                        + ", t_product_id="
                        + t_product_id
                        + ", t_amount="
                        + t_amount;

        if (t_product_rating != null) {
            result += ", t_product_rating=" + t_product_rating;
        }

        if (t_product_popularity != null) {
            result += ", t_product_popularity=" + t_product_popularity;
        }

        result += '}';

        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Transaction)) {
            return false;
        }
        Transaction that = (Transaction) o;
        return t_time == that.t_time
                && t_id == that.t_id
                && t_product_id == that.t_product_id
                && t_amount.equals(that.t_amount)
                && Objects.equals(t_product_rating, that.t_product_rating)
                && Objects.equals(t_product_popularity, that.t_product_popularity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                t_time, t_id, t_product_id, t_amount, t_product_rating, t_product_popularity);
    }
}
