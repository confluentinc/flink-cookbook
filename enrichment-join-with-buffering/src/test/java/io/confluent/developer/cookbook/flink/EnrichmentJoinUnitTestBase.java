package io.confluent.developer.cookbook.flink;

import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.developer.cookbook.flink.records.Product;
import io.confluent.developer.cookbook.flink.records.Transaction;
import java.math.BigDecimal;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class EnrichmentJoinUnitTestBase {

    private KeyedTwoInputStreamOperatorTestHarness<Long, Transaction, Product, Transaction> harness;

    abstract KeyedCoProcessFunction<Long, Transaction, Product, Transaction> getFunctionToTest();

    @BeforeEach
    public void initializeTestHarness() throws Exception {

        TwoInputStreamOperator<Transaction, Product, Transaction> operator =
                new KeyedCoProcessOperator<>(getFunctionToTest());

        KeyedTwoInputStreamOperatorTestHarness<Long, Transaction, Product, Transaction>
                testHarness =
                        new KeyedTwoInputStreamOperatorTestHarness<>(
                                operator, t -> t.t_product_id, p -> p.p_id, Types.LONG);

        testHarness.setup();
        testHarness.open();

        this.harness = testHarness;
    }

    private static final Product product = new Product(1, 1, "product1", 5.0F, 1);
    private static final Transaction transaction =
            new Transaction(1, 1, product.p_id, BigDecimal.valueOf(100L));
    private static final Product updatedProduct =
            new Product(10, product.p_id, "updatedProduct1", 4.5F, 10);

    @Test
    public void testProductThenTransaction() throws Exception {
        harness.processElement2(recordFor(product));
        assertThat(harness.numKeyedStateEntries()).isEqualTo(1);
        assertThat(harness.getOutput()).isEmpty();

        harness.processElement1(recordFor(transaction));
        assertThat(harness.numKeyedStateEntries()).isEqualTo(1);
        assertThat(harness.getOutput())
                .containsExactly(recordForExpectedResult(transaction, product));
    }

    @Test
    public void testTransactionThenProduct() throws Exception {
        harness.processElement1(recordFor(transaction));
        assertThat(harness.numKeyedStateEntries()).isEqualTo(1);
        assertThat(harness.getOutput()).isEmpty();

        harness.processElement2(recordFor(product));
        assertThat(harness.numKeyedStateEntries()).isEqualTo(1);
        assertThat(harness.getOutput())
                .containsExactly(recordForExpectedResult(transaction, product));
    }

    @Test
    public void testTransactionThenAnotherProduct() throws Exception {
        harness.processElement1(recordFor(transaction));
        assertThat(harness.numKeyedStateEntries()).isEqualTo(1);
        assertThat(harness.getOutput()).isEmpty();

        harness.processElement2(recordFor(new Product(0, 42, "another-product", 1.0F, 100)));
        assertThat(harness.numKeyedStateEntries()).isEqualTo(2);
        assertThat(harness.getOutput()).isEmpty();
    }

    @Test
    public void testProductUpdateInOrderBeforeTransaction() throws Exception {
        harness.processElement2(recordFor(product));
        harness.processElement2(recordFor(updatedProduct));
        assertThat(harness.numKeyedStateEntries()).isEqualTo(1);

        harness.processElement1(recordFor(transaction));
        assertThat(harness.getOutput())
                .containsExactly(recordForExpectedResult(transaction, updatedProduct));
    }

    @Test
    public void testProductUpdateOutOfOrderBeforeTransaction() throws Exception {
        harness.processElement2(recordFor(updatedProduct));
        harness.processElement2(recordFor(product));
        assertThat(harness.numKeyedStateEntries()).isEqualTo(1);

        harness.processElement1(recordFor(transaction));
        assertThat(harness.getOutput())
                .containsExactly(recordForExpectedResult(transaction, updatedProduct));
    }

    @Test
    public void testProductUpdateInOrderAfterTransaction() throws Exception {
        harness.processElement1(recordFor(transaction));
        harness.processElement2(recordFor(product));
        harness.processElement2(recordFor(updatedProduct));

        StreamRecord<Transaction> expectedResult = recordForExpectedResult(transaction, product);
        assertThat(harness.getOutput()).containsExactly(expectedResult);
    }

    @Test
    public void testProductUpdateOutOfOrderAfterTransaction() throws Exception {
        harness.processElement1(recordFor(transaction));
        harness.processElement2(recordFor(updatedProduct));
        harness.processElement2(recordFor(product));

        StreamRecord<Transaction> expectedResult =
                recordForExpectedResult(transaction, updatedProduct);
        assertThat(harness.getOutput()).containsExactly(expectedResult);
    }

    @Test
    public void testBufferHandling() throws Exception {
        final Transaction t2 = new Transaction(2, 2, product.p_id, BigDecimal.valueOf(1L));

        // state is initially empty
        assertThat(harness.numKeyedStateEntries()).isZero();

        // process 2 transactions for the same missing product
        harness.processElement1(recordFor(transaction));
        harness.processElement1(recordFor(t2));

        // expect only 1 state entry because both transactions are buffered
        // in the same ListState or MapState object
        assertThat(harness.numKeyedStateEntries()).isEqualTo(1);

        // processing the product should clear the buffer, and store the product
        harness.processElement2(recordFor(product));
        assertThat(harness.numKeyedStateEntries()).isEqualTo(1);

        // verify that both transactions were enriched
        StreamRecord<Transaction> expected1 = recordForExpectedResult(transaction, product);
        StreamRecord<Transaction> expected2 = recordForExpectedResult(t2, product);
        assertThat(harness.getOutput()).containsExactlyInAnyOrder(expected1, expected2);
    }

    /**
     * StreamRecord is an internal type used in the Flink runtime (and the test harnesses).
     *
     * <p>It's a thin wrapper around your application-level events.
     */
    private static StreamRecord<Transaction> recordFor(Transaction t) {
        return new StreamRecord<>(t);
    }

    private static StreamRecord<Product> recordFor(Product p) {
        return new StreamRecord<>(p);
    }

    private static StreamRecord<Transaction> recordForExpectedResult(Transaction t, Product p) {
        Transaction expectedResult = Transaction.enrich(t, p);
        return new StreamRecord<>(expectedResult);
    }
}
