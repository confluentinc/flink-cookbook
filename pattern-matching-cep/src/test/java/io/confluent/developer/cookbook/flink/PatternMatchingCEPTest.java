package io.confluent.developer.cookbook.flink;

import static io.confluent.developer.cookbook.flink.PatternMatchingCEP.TOPIC;
import static io.confluent.developer.cookbook.flink.records.SensorReading.HOT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

import io.confluent.developer.cookbook.flink.extensions.MiniClusterExtensionFactory;
import io.confluent.developer.cookbook.flink.patterns.MatcherV1;
import io.confluent.developer.cookbook.flink.patterns.MatcherV2;
import io.confluent.developer.cookbook.flink.patterns.MatcherV3;
import io.confluent.developer.cookbook.flink.patterns.PatternMatcher;
import io.confluent.developer.cookbook.flink.records.OscillatingSensorReadingSupplier;
import io.confluent.developer.cookbook.flink.records.RisingSensorReadingSupplier;
import io.confluent.developer.cookbook.flink.records.SensorReading;
import io.confluent.developer.cookbook.flink.records.SensorReadingDeserializationSchema;
import io.confluent.developer.cookbook.flink.utils.CookbookKafkaCluster;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.PojoTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class PatternMatchingCEPTest {

    @RegisterExtension
    static final MiniClusterExtension FLINK =
            MiniClusterExtensionFactory.withDefaultConfiguration();

    private static final int EVENTS_PER_SECOND = 10;

    /**
     * Verify that Flink recognizes the SensorReading type as a POJO that it can serialize
     * efficiently.
     */
    @Test
    void sensorReadingsAreAPOJOs() {
        PojoTestUtils.assertSerializedAsPojo(SensorReading.class);
    }

    @Test
    void matcherV2FindsMoreThanOneRisingHotStreak() throws Exception {
        Duration limitOfHeatTolerance = Duration.ofSeconds(1);
        long secondsOfData = 3;

        PatternMatcher<SensorReading, SensorReading> matcherV2 = new MatcherV2();
        assertThat(risingHotStreaks(HOT - 1, secondsOfData, matcherV2, limitOfHeatTolerance))
                .hasSizeGreaterThan(1);
    }

    @Test
    void matcherV3FindsOneRisingHotStreak() throws Exception {
        Duration limitOfHeatTolerance = Duration.ofSeconds(1);
        long secondsOfData = 3;

        PatternMatcher<SensorReading, SensorReading> matcherV3 = new MatcherV3();
        assertThat(risingHotStreaks(HOT - 1, secondsOfData, matcherV3, limitOfHeatTolerance))
                .hasSize(1);
    }

    @Test
    void matcherV2FindsAnOscillatingHotStreak() throws Exception {
        long secondsOfHeat = 3;
        Duration limitOfHeatTolerance = Duration.ofSeconds(2);

        PatternMatcher<SensorReading, SensorReading> matcherV2 = new MatcherV2();
        assertThat(oscillatingHotStreaks(secondsOfHeat, matcherV2, limitOfHeatTolerance))
                .isNotEmpty();
    }

    @Test
    void matcherV3FindsAnOscillatingHotStreak() throws Exception {
        long secondsOfHeat = 3;
        Duration limitOfHeatTolerance = Duration.ofSeconds(2);

        PatternMatcher<SensorReading, SensorReading> matcherV3 = new MatcherV3();
        assertThat(oscillatingHotStreaks(secondsOfHeat, matcherV3, limitOfHeatTolerance))
                .isNotEmpty();
    }

    @Test
    void matcherV1ErroneouslyFindsOscillatingHotStreaks() {
        long secondsOfHeat = 1;
        Duration limitOfHeatTolerance = Duration.ofSeconds(2);

        PatternMatcher<SensorReading, SensorReading> matcherV1 = new MatcherV1();

        assertThrows(
                AssertionError.class,
                () ->
                        assertThat(
                                        oscillatingHotStreaks(
                                                secondsOfHeat, matcherV1, limitOfHeatTolerance))
                                .isEmpty());
    }

    @Test
    void matcherV2FindsNoOscillatingHotStreaks() throws Exception {
        long secondsOfHeat = 1;
        Duration limitOfHeatTolerance = Duration.ofSeconds(2);

        PatternMatcher<SensorReading, SensorReading> matcherV2 = new MatcherV2();
        assertThat(oscillatingHotStreaks(secondsOfHeat, matcherV2, limitOfHeatTolerance)).isEmpty();
    }

    @Test
    void matcherV3FindsNoOscillatingHotStreaks() throws Exception {
        long secondsOfHeat = 1;
        Duration limitOfHeatTolerance = Duration.ofSeconds(2);

        PatternMatcher<SensorReading, SensorReading> matcherV3 = new MatcherV3();
        assertThat(oscillatingHotStreaks(secondsOfHeat, matcherV3, limitOfHeatTolerance)).isEmpty();
    }

    private List<SensorReading> risingHotStreaks(
            long initialTemp,
            long secondsOfData,
            PatternMatcher<SensorReading, SensorReading> matcher,
            Duration limitOfHeatTolerance)
            throws Exception {

        Stream<SensorReading> readings =
                Stream.generate(new RisingSensorReadingSupplier(initialTemp))
                        .limit(secondsOfData * EVENTS_PER_SECOND);

        return runTestJob(readings, matcher, limitOfHeatTolerance);
    }

    private List<SensorReading> oscillatingHotStreaks(
            long secondsOfHeat,
            PatternMatcher<SensorReading, SensorReading> matcher,
            Duration limitOfHeatTolerance)
            throws Exception {

        Stream<SensorReading> readings =
                Stream.generate(new OscillatingSensorReadingSupplier(secondsOfHeat))
                        .limit(3 * secondsOfHeat * EVENTS_PER_SECOND);

        return runTestJob(readings, matcher, limitOfHeatTolerance);
    }

    private List<SensorReading> runTestJob(
            Stream<SensorReading> stream,
            PatternMatcher<SensorReading, SensorReading> patternMatcher,
            Duration limitOfHeatTolerance)
            throws Exception {

        try (final CookbookKafkaCluster kafka = new CookbookKafkaCluster(EVENTS_PER_SECOND)) {
            kafka.createTopic(TOPIC, stream);

            KafkaSource<SensorReading> source =
                    KafkaSource.<SensorReading>builder()
                            .setBootstrapServers("localhost:9092")
                            .setTopics(TOPIC)
                            .setStartingOffsets(OffsetsInitializer.earliest())
                            // set an upper bound so that the job (and this test) will end
                            .setBounded(OffsetsInitializer.latest())
                            .setValueOnlyDeserializer(new SensorReadingDeserializationSchema())
                            .build();

            final DataStream.Collector<SensorReading> resultSink = new DataStream.Collector<>();
            final DataStream.Collector<SensorReading> eventSink = new DataStream.Collector<>();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            PatternMatchingCEP.defineWorkflow(
                    env,
                    source,
                    patternMatcher,
                    limitOfHeatTolerance,
                    workflow -> workflow.collectAsync(resultSink),
                    workflow -> workflow.collectAsync(eventSink));

            env.executeAsync();

            List<SensorReading> results = new ArrayList<>();
            resultSink.getOutput().forEachRemaining(results::add);
            return results;
        }
    }
}
