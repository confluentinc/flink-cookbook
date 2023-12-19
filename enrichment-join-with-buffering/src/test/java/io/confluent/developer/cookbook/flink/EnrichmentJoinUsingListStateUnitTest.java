package io.confluent.developer.cookbook.flink;

import io.confluent.developer.cookbook.flink.functions.EnrichmentJoinUsingListState;
import io.confluent.developer.cookbook.flink.records.Product;
import io.confluent.developer.cookbook.flink.records.Transaction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;

public class EnrichmentJoinUsingListStateUnitTest extends EnrichmentJoinUnitTestBase {

    public KeyedCoProcessFunction<Long, Transaction, Product, Transaction> getFunctionToTest() {
        return new EnrichmentJoinUsingListState();
    }
}
