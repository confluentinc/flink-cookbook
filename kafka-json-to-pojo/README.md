# Deserializing JSON from Apache Kafka®

## Reading Kafka JSON records in Apache Flink

To get started with your first event processing application, you will need to read data from one or multiple sources.

In this recipe, you are going to consume JSON-encoded event data from Apache Kafka and transform this data into your
data model. The data model is defined in a POJO. 

This recipe for Apache Flink is a self-contained recipe that you can directly copy and run from your favorite editor.
There is no need to download Apache Flink or Apache Kafka.

## The JSON input data

The recipe uses Kafka topic `input`, containing JSON-encoded records.

```json
{"id":1,"data":"Frodo Baggins","timestamp":"2022-07-28T08:03:18.819865Z"}
{"id":2,"data":"Meriadoc Brandybuck","timestamp":"2022-07-28T08:03:19.030003Z"}
{"id":3,"data":"Boromir","timestamp":"2022-07-28T08:03:19.144706Z"}
{"id":4,"data":"Gollum","timestamp":"2022-07-28T08:03:19.261407Z"}
{"id":5,"data":"Sauron","timestamp":"2022-07-28T08:03:19.377677Z"}
```

## The data model

You want to consume these records in your Apache Flink application and make them available in the data model. 
The data model is defined in the following POJO:

```java Event.java focus=12:18
public class Event {

    /** A Flink POJO must have public fields, or getters and setters */
    public long id;

    public String data;
    public Instant timestamp;
```

## Connect to Kafka

You are using the Apache Flink [`KafkaSource`](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kafka/#kafka-source)
connector in the application to connect to your Apache Kafka broker.
Next to the necessary connection information, you are configuring the connector
to use a custom deserializer called `EventDeserializationSchema`.

```java KafkaJSONToPOJO.java focus=24:30
        KafkaSource<Event> source =
                KafkaSource.<Event>builder()
                        .setBootstrapServers("localhost:9092")
                        .setTopics(TOPIC)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new EventDeserializationSchema())
                        .build();
```

## The custom deserializer

The custom deserializer `EventDeserializationSchema` uses a Jackson `ObjectMapper` to deserialize each incoming record.

```java EventDeserializationSchema.java focus=9,13:33
public class EventDeserializationSchema extends AbstractDeserializationSchema<Event> {

    private static final long serialVersionUID = 1L;

    private transient ObjectMapper objectMapper;

    /**
     * For performance reasons it's better to create on ObjectMapper in this open method rather than
     * creating a new ObjectMapper for every record.
     */
    @Override
    public void open(InitializationContext context) {
        // JavaTimeModule is needed for Java 8 data time (Instant) support
        objectMapper = JsonMapper.builder().build().registerModule(new JavaTimeModule());
    }

    /**
     * If our deserialize method needed access to the information in the Kafka headers of a
     * KafkaConsumerRecord, we would have implemented a KafkaRecordDeserializationSchema instead of
     * extending AbstractDeserializationSchema.
     */
    @Override
    public Event deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, Event.class);
    }
}
```

## The full recipe

This recipe is self-contained. You can run the `KafkaJSONToPOJOTest#testProductionJob` test to see the full recipe
in action. That test uses an embedded Apache Kafka and Apache Flink setup, so you can run it directly via
Maven or in your favorite editor such as IntelliJ IDEA or Visual Studio Code.
