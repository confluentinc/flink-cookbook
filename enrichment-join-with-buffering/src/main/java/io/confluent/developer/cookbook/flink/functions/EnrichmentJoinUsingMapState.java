package io.confluent.developer.cookbook.flink.functions;

import io.confluent.developer.cookbook.flink.records.Product;
import io.confluent.developer.cookbook.flink.records.Transaction;
import java.util.UUID;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class EnrichmentJoinUsingMapState
        extends KeyedCoProcessFunction<Long, Transaction, Product, Transaction> {

    private transient ValueState<Product> savedProductState;
    private transient MapState<String, Transaction> transactionsAwaitingEnrichment;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        savedProductState =
                getRuntimeContext()
                        .getState(new ValueStateDescriptor<>("product-data", Product.class));

        transactionsAwaitingEnrichment =
                getRuntimeContext()
                        .getMapState(
                                new MapStateDescriptor<>(
                                        "list-of-transactions", String.class, Transaction.class));
    }

    @Override
    public void processElement1(
            Transaction transaction, Context context, Collector<Transaction> out) throws Exception {

        Product savedProduct = savedProductState.value();
        if (savedProduct == null) {
            transactionsAwaitingEnrichment.put(UUID.randomUUID().toString(), transaction);
        } else {
            out.collect(Transaction.enrich(transaction, savedProduct));
        }
    }

    @Override
    public void processElement2(Product product, Context context, Collector<Transaction> out)
            throws Exception {

        Product savedProduct = savedProductState.value();
        if (savedProduct == null) {
            transactionsAwaitingEnrichment
                    .iterator()
                    .forEachRemaining(
                            entry -> out.collect(Transaction.enrich(entry.getValue(), product)));
            transactionsAwaitingEnrichment.clear();
            savedProductState.update(product);
        } else {
            // Goal: only store the most recent Product info
            // Side effect: out-of-order updates will be ignored
            if (product.p_time > savedProduct.p_time) {
                savedProductState.update(product);
            }
        }
    }
}
