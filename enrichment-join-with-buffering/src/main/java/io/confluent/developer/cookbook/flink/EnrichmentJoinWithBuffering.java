package io.confluent.developer.cookbook.flink;

import io.confluent.developer.cookbook.flink.functions.EnrichmentJoinUsingListState;
import io.confluent.developer.cookbook.flink.functions.EnrichmentJoinUsingMapState;
import io.confluent.developer.cookbook.flink.records.Product;
import io.confluent.developer.cookbook.flink.records.Transaction;
import java.util.function.Consumer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;

public class EnrichmentJoinWithBuffering {

    public static void main(String[] args) throws Exception {

        /* For production use, set up production sources and a sink and call defineWorkflow.
         *
         * In development, use the provided tests.
         */

        throw new RuntimeException(
                "Please set up production sources and a sink, and replace this Exception\n"
                        + "with a call to defineWorkflow.");
    }

    static void defineWorkflow(
            String listOrMapState,
            DataStream<Product> productStream,
            DataStream<Transaction> transactionStream,
            Consumer<DataStream<Transaction>> sinkApplier) {

        DataStream<Transaction> enrichedStream =
                transactionStream
                        .connect(productStream)
                        .keyBy(t -> t.t_product_id, p -> p.p_id)
                        .process(getJoinFunction(listOrMapState));

        sinkApplier.accept(enrichedStream);
    }

    /** Choose between the two implementations; see the recipe description for details. */
    private static KeyedCoProcessFunction<Long, Transaction, Product, Transaction> getJoinFunction(
            String listOrMapState) throws IllegalStateException {
        switch (listOrMapState) {
            case "ListState":
                return new EnrichmentJoinUsingListState();
            case "MapState":
                return new EnrichmentJoinUsingMapState();
            default:
                throw new IllegalStateException("Unknown state type, choose ListState or MapState");
        }
    }
}
