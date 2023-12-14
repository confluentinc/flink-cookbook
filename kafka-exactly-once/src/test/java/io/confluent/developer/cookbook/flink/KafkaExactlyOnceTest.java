package io.confluent.developer.cookbook.flink;

import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.developer.cookbook.flink.events.StringSupplier;
import io.confluent.developer.cookbook.flink.extensions.MiniClusterExtensionFactory;
import io.confluent.developer.cookbook.flink.utils.CookbookKafkaCluster;
import java.util.List;
import java.util.stream.Stream;
import net.mguenther.kafka.junit.TopicConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class KafkaExactlyOnceTest {

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
            kafka.createTopicAsync(KafkaExactlyOnce.TOPIC, Stream.generate(new StringSupplier()));
            kafka.createTopic(TopicConfig.withName(KafkaExactlyOnce.OUTPUT));

            KafkaExactlyOnce.runJob();
        }
    }

    @Test
    void JobProducesExpectedNumberOfResults() throws Exception {
        final int numExpectedRecords = 50;
        try (final CookbookKafkaCluster kafka = new CookbookKafkaCluster()) {
            kafka.createTopic(
                    KafkaExactlyOnce.TOPIC, Stream.generate(new StringSupplier()).limit(numExpectedRecords));

            KafkaSource<String> source =
                    KafkaSource.<String>builder()
                            .setBootstrapServers("localhost:9092")
                            .setTopics(KafkaExactlyOnce.TOPIC)
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            // set an upper bound so that the job (and this test) will end
                            .setBounded(OffsetsInitializer.latest())
                            .setValueOnlyDeserializer(new SimpleStringSchema())
                            .build();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            KafkaExactlyOnce.defineWorkflow(env, source);
            env.execute();

            final List<String> topicRecords = kafka.getTopicRecords(KafkaExactlyOnce.TOPIC, numExpectedRecords + 1);
            assertThat(topicRecords).hasSize(numExpectedRecords);
        }
    }
}
