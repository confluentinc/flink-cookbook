package io.confluent.developer.cookbook.flink;

import static io.confluent.developer.cookbook.flink.KafkaHeaders.TOPIC;
import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.developer.cookbook.flink.events.EnrichedEvent;
import io.confluent.developer.cookbook.flink.events.EventSupplier;
import io.confluent.developer.cookbook.flink.events.HeaderGenerator;
import io.confluent.developer.cookbook.flink.events.KafkaHeadersEventDeserializationSchema;
import io.confluent.developer.cookbook.flink.extensions.MiniClusterExtensionFactory;
import io.confluent.developer.cookbook.flink.utils.CookbookKafkaCluster;
import java.util.stream.Stream;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.PojoTestUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class KafkaHeadersTest {

    @RegisterExtension
    static final MiniClusterExtension FLINK =
            MiniClusterExtensionFactory.withDefaultConfiguration();

    /**
     * Runs the production job against an in-memory Kafka cluster.
     *
     * <p>This is a manual test because this job will never finish.
     */
    @Test
    @Disabled("Not running 'testProductionJob()' because it is a manual test.")
    void testProductionJob() throws Exception {
        try (final CookbookKafkaCluster kafka = new CookbookKafkaCluster()) {
            kafka.createTopicAsync(
                    TOPIC, Stream.generate(new EventSupplier()), new HeaderGenerator());

            KafkaHeaders.runJob();
        }
    }

    @Test
    void JobProducesAtLeastOneResult() throws Exception {
        try (final CookbookKafkaCluster kafka = new CookbookKafkaCluster()) {
            kafka.createTopicAsync(
                    TOPIC, Stream.generate(new EventSupplier()), new HeaderGenerator());

            KafkaSource<EnrichedEvent> source =
                    KafkaSource.<EnrichedEvent>builder()
                            .setBootstrapServers("localhost:9092")
                            .setTopics(TOPIC)
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            // set an upper bound so that the job (and this test) will end
                            .setBounded(OffsetsInitializer.latest())
                            .setDeserializer(new KafkaHeadersEventDeserializationSchema())
                            .build();

            final DataStream.Collector<EnrichedEvent> testSink = new DataStream.Collector<>();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            KafkaHeaders.defineWorkflow(env, source, workflow -> workflow.collectAsync(testSink));
            env.executeAsync();

            assertThat(testSink.getOutput()).toIterable().isNotEmpty();
        }
    }

    /**
     * Verify that Flink recognizes the EnrichedEvent type as a POJO that it can serialize
     * efficiently.
     */
    @Test
    void EventsAreAPOJOs() {
        PojoTestUtils.assertSerializedAsPojo(EnrichedEvent.class);
    }
}
