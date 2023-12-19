package io.confluent.developer.cookbook.flink;

import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.developer.cookbook.flink.events.Event;
import io.confluent.developer.cookbook.flink.events.EventDeserializationSchema;
import io.confluent.developer.cookbook.flink.events.EventSupplier;
import io.confluent.developer.cookbook.flink.events.UserActivity;
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

class SessionWindowTest {

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
            kafka.createTopicAsync(SessionWindow.TOPIC, Stream.generate(new EventSupplier()));

            SessionWindow.runJob();
        }
    }

    @Test
    void JobProducesAtLeastOneResult() throws Exception {
        try (final CookbookKafkaCluster kafka = new CookbookKafkaCluster()) {
            kafka.createTopicAsync(SessionWindow.TOPIC, Stream.generate(new EventSupplier()));

            KafkaSource<Event> source =
                    KafkaSource.<Event>builder()
                            .setBootstrapServers("localhost:9092")
                            .setTopics(SessionWindow.TOPIC)
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            // set an upper bound so that the job (and this test) will end
                            .setBounded(OffsetsInitializer.latest())
                            .setValueOnlyDeserializer(new EventDeserializationSchema())
                            .build();

            final DataStream.Collector<UserActivity> testSink = new DataStream.Collector<>();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            SessionWindow.defineWorkflow(env, source, workflow -> workflow.collectAsync(testSink));
            env.executeAsync();

            assertThat(testSink.getOutput()).toIterable().isNotEmpty();
        }
    }

    /** Verify that Flink recognizes the Event type as a POJO that it can serialize efficiently. */
    @Test
    void EventsAreAPOJOs() {
        PojoTestUtils.assertSerializedAsPojo(Event.class);
    }
}
