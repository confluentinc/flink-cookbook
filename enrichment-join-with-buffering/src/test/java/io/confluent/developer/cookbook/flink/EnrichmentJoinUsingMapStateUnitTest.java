package io.confluent.developer.cookbook.flink;

import io.confluent.developer.cookbook.flink.functions.EnrichmentJoinUsingMapState;
import io.confluent.developer.cookbook.flink.records.Product;
import io.confluent.developer.cookbook.flink.records.Transaction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;

public class EnrichmentJoinUsingMapStateUnitTest extends EnrichmentJoinUnitTestBase {

    public KeyedCoProcessFunction<Long, Transaction, Product, Transaction> getFunctionToTest() {
        return new EnrichmentJoinUsingMapState();
    }
}
