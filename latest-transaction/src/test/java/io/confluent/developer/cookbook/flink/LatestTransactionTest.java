package io.confluent.developer.cookbook.flink;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import io.confluent.developer.cookbook.flink.extensions.MiniClusterExtensionFactory;
import io.confluent.developer.cookbook.flink.records.Transaction;
import io.confluent.developer.cookbook.flink.records.TransactionSupplier;
import io.confluent.developer.cookbook.flink.utils.CookbookKafkaCluster;
import java.math.BigDecimal;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class LatestTransactionTest {

    @RegisterExtension
    static final MiniClusterExtension FLINK =
            MiniClusterExtensionFactory.withDefaultConfiguration();

    @Test
    @Disabled("Not running 'testStreamingDataStreamJob()' because it is a manual test.")
    void testStreamingDataStreamJob() throws Exception {
        try (final CookbookKafkaCluster kafka = new CookbookKafkaCluster()) {
            kafka.createTopicAsync("transactions", Stream.generate(new TransactionSupplier()));

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            StreamingDataStreamJob.setupJob(
                    env, "transactions", workflow -> workflow.sinkTo(new PrintSink<>()));
            env.execute();
        }
    }

    @Test
    @Disabled("Not running 'testBatchDataStreamJob()' because it is a manual test.")
    void testBatchDataStreamJob() throws Exception {
        URI testdataURI = LatestTransactionTest.class.getResource("/transactions.json").toURI();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        BatchDataStreamJob.setupJob(
                env, testdataURI, workflow -> workflow.sinkTo(new PrintSink<>()));
        env.execute();
    }

    @Test
    @Disabled("Not running 'testStreamingTableJob()' because it is a manual test.")
    void testStreamingTableJob() {
        try (final CookbookKafkaCluster kafka = new CookbookKafkaCluster()) {
            kafka.createTopicAsync("transactions", Stream.generate(new TransactionSupplier()));

            StreamingTableJob.produceResults("transactions").print();
        }
    }

    @Test
    @Disabled("Not running 'testBatchTableJob()' because it is a manual test.")
    void testBatchTableJob() throws Exception {
        URI testdataURI = LatestTransactionTest.class.getResource("/transactions.json").toURI();

        BatchTableJob.produceResults(testdataURI).print();
    }

    @Test
    void testDataStreamBatchJob() throws Exception {
        final URI testdataURI =
                LatestTransactionTest.class.getResource("/transactions.json").toURI();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStream.Collector<Transaction> testSink = new DataStream.Collector<>();

        BatchDataStreamJob.setupJob(env, testdataURI, workflow -> workflow.collectAsync(testSink));

        env.executeAsync();
        assertThat(testSink.getOutput())
                .toIterable()
                .filteredOn(t -> t.t_customer_id == 0)
                .map(t -> t.t_id)
                .containsExactly(0L, 2L);
    }

    @Test
    void testTableBatchJob() throws Exception {
        final URI testdataURI =
                LatestTransactionTest.class.getResource("/transactions.json").toURI();

        TableResult actualResults = BatchTableJob.produceResults(testdataURI);
        assertThat(getRowsFromTable(actualResults))
                .containsExactlyInAnyOrder(
                        Row.ofKind(
                                RowKind.INSERT,
                                Instant.parse("2022-07-24T12:00:00Z"),
                                2L,
                                0L,
                                BigDecimal.valueOf(500)),
                        Row.ofKind(
                                RowKind.INSERT,
                                Instant.parse("2022-07-24T13:00:00Z"),
                                3L,
                                1L,
                                BigDecimal.valueOf(11)));
    }

    private static List<Row> getRowsFromTable(TableResult resultTable) throws Exception {
        try (CloseableIterator<Row> rowCloseableIterator = resultTable.collect()) {
            List<Row> results = new ArrayList<>();
            rowCloseableIterator.forEachRemaining(results::add);
            return results;
        }
    }
}
