# Capturing late data

## Sending late data to a separate sink

When you are consuming data where the payload contains a timestamp when the event occurred, you can use this timestamp
for so-called [`event-time` processing](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/time/).

Apache Flink measures the progress of event-time with [`watermarks`](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/time/#event-time-and-watermarks).
A watermark declares that the event time has reached a certain time. This means that there should be no more data where
the event-time is older or earlier then the timestamp in that watermark. If the timestamp from the payload is lower than
the current watermark, the data can be considered [late data](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/time/#lateness).

Apache Flink does not always drop late events. This will only happen if you configure `watermarks` and if you use one of
the following specific operations:

* Windows
* Joins
* CEP (Complex Event Processing)
* Process functions (if you enable it)

In this recipe, you are going to consume events from Apache Kafka which have a `timestamp` in the payload. When the
value for `timestamp` is late, we will send this data to a separate sink. Data could arrive late because the mobile 
device of a user has no internet connection and events will only be sent when the device comes back online. 

Sending data to a separate sink allows you to monitor how much data is actually late. It also makes it possible that 
you re-process the late data via an alternative solution, or verify that the late data did not affect the correctness 
of the result.

This recipe for Apache Flink is a self-contained recipe that you can directly copy and run from your favorite editor.
There is no need to download Apache Flink or Apache Kafka.

## The JSON input data

The recipe uses Kafka topic `input`, containing JSON-encoded records.

```json
{"id":1,"data":"Galadriel","timestamp":"2022-07-19T09:59:32.804843Z"}
{"id":2,"data":"Éowyn","timestamp":"2022-07-19T09:59:33.011537Z"}
{"id":3,"data":"Arwen Evenstar","timestamp":"2022-07-19T09:59:33.122527Z"}
{"id":4,"data":"Shelob","timestamp":"2022-07-19T09:59:33.235666Z"}
{"id":5,"data":"Saruman the White","timestamp":"2022-07-19T09:59:33.352016Z"}
```

## Define your watermark

Since the payload contains a `timestamp`, you will configure Flink to use these values for [watermarks](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/event-time/built_in/).
This is done by defining a `WatermarkStrategy` and leveraging Flink's built-in [`forMonotonousTimestamps()`](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/event-time/built_in/)
method. This requires extracting the timestamp from the record and passing this to the `withTimestampAssigner` method.

```java LateDataToSeparateSink.java focus=54:59
                                WatermarkStrategy.<Event>forMonotonousTimestamps()
                                        // extract timestamp from the record
                                        // required so that we can introduce late data
                                        .withTimestampAssigner(
                                                (element, recordTimestamp) ->
                                                        element.timestamp.toEpochMilli()),
```

## Define your side output for late data

You are using Flink's [Side Output](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/side_output/)
to get a stream of data that was considered as late.
To define a side output for late data, you first define an `OutputTag` to identify your side output stream.

```java LateDataToSeparateSink.java focus=49
        final OutputTag<Event> lateDataOutputTag = new OutputTag<>("lateData") {};
```

Next, you use a [`ProcessFunction`](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/process_function/)
that writes all late records to the defined side output.

```java LateDataToSeparateSink.java focus=62,72:93
    private static class LateDataSplittingProcessFunction extends ProcessFunction<Event, Event> {

        private final OutputTag<Event> lateDataOutputTag;

        private LateDataSplittingProcessFunction(OutputTag<Event> lateDataOutputTag) {
            this.lateDataOutputTag = lateDataOutputTag;
        }

        @Override
        public void processElement(
                Event value, ProcessFunction<Event, Event>.Context ctx, Collector<Event> out) {
            final long currentWatermark = ctx.timerService().currentWatermark();

            long eventTimestamp = ctx.timestamp();

            if (eventTimestamp < currentWatermark) {
                ctx.output(lateDataOutputTag, value);
            } else {
                out.collect(value);
            }
        }
    }
```

## The full recipe

This recipe is self-contained. You can run the `LateDataToSeparateSinkTest#testProductionJob` class to see the full recipe
in action. That test uses an embedded Apache Kafka and Apache Flink setup, so you can run it directly via
Maven or in your favorite editor such as IntelliJ IDEA or Visual Studio Code.

See the comments included in the code for more
details.
