# Splitting Apache Kafka® events

## Splitting or routing a stream of events

A common use case is where you will need to split a stream of events or route specific events to a specific location. 

In this recipe, you are going to consume events from Apache Kafka which have a `Priority` value in the payload.
All events with a `Critical` or `Major` priority will be routed to different outputs.

This recipe for Apache Flink is a self-contained recipe that you can directly copy and run from your favorite editor.
There is no need to download Apache Flink or Apache Kafka.

## The JSON input data

The recipe uses Kafka topic `input`, containing JSON-encoded records.

```json
{"id":1,"data":"NL31PJMQ9001080613","priority":"MAJOR"}
{"id":2,"data":"AD5934360143N91217Gfa7hA","priority":"MINOR"}
{"id":3,"data":"ST35710189607894938826568","priority":"CRITICAL"}
{"id":4,"data":"IT90R5678911215DpWA54OpHM17","priority":"CRITICAL"}
{"id":5,"data":"RS93089245532951564296","priority":"MAJOR"}
```

## Define your side output

You are going to use Flink's [Side Output](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/side_output/) 
to split the stream of events. Side Outputs can be used to split a stream in n-ways, into streams of different types
with excellent performance. 

To use Side Outputs, you first define an `OutputTag` to identify your side output stream. 

```java SplitStream.java focus=49:53
        final Map<Event.Priority, OutputTag<Event>> tagsByPriority =
                Map.ofEntries(
                        entry(Event.Priority.CRITICAL, new OutputTag<>("critical") {}),
                        entry(Event.Priority.MAJOR, new OutputTag<>("major") {}),
                        entry(Event.Priority.MINOR, new OutputTag<>("minor") {}));
```

## Emit to side output

You are going to emit to your defined side output using Flink's [`ProcessFunction`](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/process_function/).
By using the `Context` parameter you are emitting data to the previously defined side output.

```java SplitStream.java focus=56:67
        final SingleOutputStreamOperator<Event> process =
                source.process(
                        new ProcessFunction<>() {
                            @Override
                            public void processElement(
                                    Event value,
                                    ProcessFunction<Event, Event>.Context ctx,
                                    Collector<Event> out) {
                                final OutputTag<Event> selectedOutput =
                                        tagsByPriority.get(value.priority);
                                ctx.output(selectedOutput, value);
                            }
                        });
```

## The full recipe

This recipe is self-contained. You can run the `SplitStreamTest#testProductionJob` class to see the full recipe
in action. That test uses an embedded Apache Kafka and Apache Flink setup, so you can run it directly via Maven or in 
your favorite editor such as IntelliJ IDEA or Visual Studio Code.

See the comments included in the code for more
details.
