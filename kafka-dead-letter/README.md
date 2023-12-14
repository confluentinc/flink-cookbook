# Creating dead letter queues

## Sending events to different topic when failure occurs

When you are processing data, it can happen that something goes wrong during the process. For example, your incoming
data is different from your expected data model.

In this recipe, you are consuming events from Apache Kafka which you transform into your data model. This data model is
defined in a POJO. However, there are some events which are not matching with your POJO. This could happen because 
you add a new IoT-device to your network which introduces new data. 

You are going to send these malformed events to a different topic. This allows you to inspect this incorrect data and 
either consult with your data provider or you can adjust your business logic.

This recipe for Apache Flink is a self-contained recipe that you can directly copy and run from your favorite editor.
There is no need to download Apache Flink or Apache Kafka.

## The JSON input data

The recipe uses Kafka topic `input`, containing JSON-encoded records.

```json
{"id":1,"data":"Éomer","timestamp":"2022-07-23T18:29:12.446820Z"}
{"id":2,"data":"Sauron","timestamp":"2022-07-23T18:29:12.663407Z"}
{"id":3,"data":"Gandalf the Grey","timestamp":"2022-07-23T18:29:12.779154Z"}
{"id":4,"data":"Bilbo Baggins","timestamp":"2022-07-23T18:29:12.894671Z"}
{"id":5,"data":"Éowyn","timestamp":"2022-07-23T18:29:13.010826Z"}
```

## Define your side output

You are going to use Flink's [Side Output](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/side_output/)
to send the malformed events to a different topic.

To use Side Outputs, you first define an `OutputTag` to identify your side output stream.

```java KafkaDeadLetterTopic.java focus=51
final OutputTag<String> deserializationErrors = new OutputTag<>("errors") {};
```

## Try to deserialize your data

You are going to use Flink's [ProcessFunction](https://nightlies.apache.org/flink/flink-docs-stable/api/java/org/apache/flink/streaming/api/functions/ProcessFunction.html)
to try to deserialize your incoming data.

In case you can correctly deserialize the data, it will be processed as expected.

In case the deserialization fails, you will send to the defined `OutputTag` the value of the event that
was not possible to be deserialized.

You can't try to deserialize your data directly in the source. That's because when a source encounters a serialization 
error, they can only either drop the message or fail the job, as they do not have access to side outputs due to Flink 
limitations.

```java KafkaDeadLetterTopic.java focus=56,64,66
    new ProcessFunction<>() {
        @Override
        public void processElement(
                String value,
                ProcessFunction<String, Event>.Context ctx,
                Collector<Event> out) {
            final Event deserialized;
            try {
                deserialized = JsonDeserialization.deserialize(value);
            } catch (IOException e) {
                ctx.output(deserializationErrors, value);
                return;
            }
            out.collect(deserialized);
        }
    }
```

## Apply watermarking afterwards

Since we aren't deserializing events in the source we have to apply the watermark strategy afterwards.
Depending on the source this may not be required if the timestamp can be determined without deserializing the event.

```java KafkaDeadLetterTopic.java focus=80:84
final SingleOutputStreamOperator<EventeventsWithTimestampsAndWatermarks =
        kafka.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                (event, timestamp) -> eventimestamp.toEpochMilli()));
```

## The full recipe

This recipe is self-contained. You can run the `KeadDeadLetterTopicTest#testProductionJob` class to see the full recipe
in action. That test uses an embedded Apache Kafka and Apache Flink setup, so you can run it directly via Maven or in
your favorite editor such as IntelliJ IDEA or Visual Studio Code.
