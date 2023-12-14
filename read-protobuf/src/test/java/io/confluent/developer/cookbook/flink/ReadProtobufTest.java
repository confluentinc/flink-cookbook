package io.confluent.developer.cookbook.flink;

import static io.confluent.developer.cookbook.flink.ReadProtobuf.TRANSACTION_TOPIC;

import com.google.protobuf.AbstractMessageLite;
import io.confluent.developer.cookbook.flink.extensions.MiniClusterExtensionFactory;
import io.confluent.developer.cookbook.flink.records.TransactionSupplier;
import io.confluent.developer.cookbook.flink.utils.CookbookKafkaCluster;
import java.util.stream.Stream;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class ReadProtobufTest {
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
                    TRANSACTION_TOPIC,
                    Stream.generate(new TransactionSupplier())
                            .map(AbstractMessageLite::toByteArray));

            ReadProtobuf.runJob();
        }
    }
}
