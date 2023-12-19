package io.confluent.developer.cookbook.flink;

import io.confluent.developer.cookbook.flink.extensions.MiniClusterExtensionFactory;
import io.confluent.developer.cookbook.flink.records.JsonToPojoDeserializer;
import io.confluent.developer.cookbook.flink.records.Product;
import io.confluent.developer.cookbook.flink.records.ProductSupplier;
import io.confluent.developer.cookbook.flink.records.Transaction;
import io.confluent.developer.cookbook.flink.records.TransactionSupplier;
import io.confluent.developer.cookbook.flink.utils.CookbookKafkaCluster;
import java.util.stream.Stream;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.PojoTestUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class EnrichmentJoinWithBufferingTest {

    static final String TRANSACTION_TOPIC = "transactions";
    static final String PRODUCT_TOPIC = "products";

    @RegisterExtension
    static final MiniClusterExtension FLINK =
            MiniClusterExtensionFactory.withDefaultConfiguration();

    /**
     * Run the production job against an in-memory Kafka cluster, using the ListState
     * implementation.
     *
     * <p>This is a manual test because this job will never finish.
     */
    @Test
    @Disabled("Not running 'testListStateJob()' because it is a manual test.")
    void testListStateJob() throws Exception {
        try (final CookbookKafkaCluster kafka = new CookbookKafkaCluster()) {
            kafka.createTopicAsync(PRODUCT_TOPIC, Stream.generate(new ProductSupplier()));
            kafka.createTopicAsync(TRANSACTION_TOPIC, Stream.generate(new TransactionSupplier()));

            runTestJob("ListState");
        }
    }

    /**
     * Run the production job against an in-memory Kafka cluster, using the MapState implementation.
     *
     * <p>This is a manual test because this job will never finish.
     */
    @Test
    @Disabled("Not running 'testMapStateJob()' because it is a manual test.")
    void testMapStateJob() throws Exception {
        try (final CookbookKafkaCluster kafka = new CookbookKafkaCluster()) {
            kafka.createTopicAsync(PRODUCT_TOPIC, Stream.generate(new ProductSupplier()));
            kafka.createTopicAsync(TRANSACTION_TOPIC, Stream.generate(new TransactionSupplier()));

            runTestJob("MapState");
        }
    }

    /** Verify Transactions are POJOs that Flink can serialize efficiently. */
    @Test
    void testTransactionsArePojosThatAvoidUsingKyro() {
        PojoTestUtils.assertSerializedAsPojoWithoutKryo(Transaction.class);
    }

    /** Verify Products are POJOs that Flink can serialize efficiently. */
    @Test
    void testProductsArePojosThatAvoidUsingKyro() {
        PojoTestUtils.assertSerializedAsPojoWithoutKryo(Product.class);
    }

    private static void runTestJob(String listOrMapState) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<Product> productSource =
                KafkaSource.<Product>builder()
                        .setBootstrapServers("localhost:9092")
                        .setTopics(PRODUCT_TOPIC)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new JsonToPojoDeserializer(Product.class))
                        .build();

        DataStream<Product> productStream =
                env.fromSource(productSource, WatermarkStrategy.noWatermarks(), "Customers");

        KafkaSource<Transaction> transactionSource =
                KafkaSource.<Transaction>builder()
                        .setBootstrapServers("localhost:9092")
                        .setTopics(TRANSACTION_TOPIC)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new JsonToPojoDeserializer(Transaction.class))
                        .build();

        DataStream<Transaction> transactionStream =
                env.fromSource(transactionSource, WatermarkStrategy.noWatermarks(), "Transactions");

        EnrichmentJoinWithBuffering.defineWorkflow(
                listOrMapState,
                productStream,
                transactionStream,
                workflow -> workflow.sinkTo(new PrintSink<>()));

        env.execute();
    }
}
