package io.confluent.developer.cookbook.flink.patterns;

import io.confluent.developer.cookbook.flink.conditions.StillHotLater;
import io.confluent.developer.cookbook.flink.records.SensorReading;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.util.Collector;

public class MatcherV2 implements PatternMatcher<SensorReading, SensorReading> {

    public Pattern<SensorReading, ?> pattern(Duration limitOfHeatTolerance) {
        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();

        return Pattern.<SensorReading>begin("first-hot-reading", skipStrategy)
                .where(
                        new SimpleCondition<>() {
                            @Override
                            public boolean filter(SensorReading reading) {
                                return reading.sensorIsHot();
                            }
                        })
                .notFollowedBy("cool-in-between")
                .where(
                        new SimpleCondition<>() {
                            @Override
                            public boolean filter(SensorReading reading) {
                                return reading.sensorIsCool();
                            }
                        })
                .followedBy("still-hot-later")
                .where(new StillHotLater("first-hot-reading", limitOfHeatTolerance));
    }

    public PatternProcessFunction<SensorReading, SensorReading> process() {

        return new PatternProcessFunction<>() {
            @Override
            public void processMatch(
                    Map<String, List<SensorReading>> map,
                    Context context,
                    Collector<SensorReading> out) {

                SensorReading event = map.get("still-hot-later").get(0);
                out.collect(event);
            }
        };
    }
}
