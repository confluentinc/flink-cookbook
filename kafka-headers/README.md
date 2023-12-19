# Reading Apache Kafka® headers

## Extracting metadata from Apache Kafka record headers

Records headers give you the ability to provide metadata information about your Apache Kafka record. This information is
not added to the key/value pair of the record itself. 

Apache Kafka by default adds metadata to each Apache Kafka record, like the topic name, the partition, offset, timestamp
and [more](https://kafka.apache.org/32/javadoc/org/apache/kafka/clients/consumer/ConsumerRecord.html).

You can also add custom headers yourself, to provide additional metadata to your record. For example, there is the
[OpenTelemetry](https://opentelemetry.io/) project that uses metadata to collect telemetry data such as traces. You
could also use metadata to provide the necessary information that's needed to decrypt the payload of the Kafka record. 

In this recipe, you are consuming events from Apache Kafka which you transform into your data model. This data model 
consists of your business payload plus the metadata from Apache Kafka itself and custom added headers. The data model is
defined in a POJO.

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

## The data model

You want to consume these records in your Apache Flink application and make them available in the data model.
The data model `EnrichedEvent` is built up from three different parts:

1. The business data, which is defined in `Event`
2. The default Apache Kafka headers, which are defined in `Metadata`
3. The custom added Apache Kafka headers, which are defined in `Headers`

```java EnrichedEvent.java focus=4:6
public class EnrichedEvent {
    public Event event;
    public Metadata metadata;
    public Headers headers;
```

### The business data

```java Event.java focus=12,15:18
public class Event {

    /** A Flink POJO must have public fields, or getters and setters */
    public long id;

    public String data;
    public Instant timestamp;

```

### The Apache Kafka metadata

In this recipe, you are going to read the topic name, partition, offset, the timestamp and what type of timestamp was
used when this record was created. 

```java Metadata.java focus=6,9:14
public class Metadata {

    /** A Flink POJO must have public fields, or getters and setters */
    public String metadataTopic;

    public long metadataPartition;
    public long metadataOffset;
    public long metadataTimestamp;
    public String metadataTimestampType;
```

### The custom Kafka headers

In this recipe, two custom headers have been added to the Kafka record: `tracestate` and `traceparent`. Both are defined
in the W3C recommendation for [Trace Context](https://www.w3.org/TR/trace-context/).

```java Headers.java focus=9,12:14
public class Headers {

    /** A Flink POJO must have public fields, or getters and setters */
    public String tracestate;

    public String traceparent;
```

## The custom deserializer

This recipe connects to Apache Kafka in the same way as is described in the 
kafka-json-to-pojo recipe. The difference is that this recipe
uses the custom deserializer `KafkaHeadersEventDeserializationSchema`, which implements a `KafkaRecordDeserializationSchema`.
This provides us with an interface for the deserialization of Kafka records, including all header information. 

This provides direct access to the Kafka metadata information for each record:

```java KafkaHeadersEventDeserializationSchema.java focus=39,42,52:58
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<EnrichedEvent> out)
            throws IOException {
        final Event event = getEvent(record);
        final Metadata metadata = getMetadata(record);
        final Headers headers = getHeaders(record);
        out.collect(new EnrichedEvent(event, metadata, headers));
    }

    private Event getEvent(ConsumerRecord<byte[], byte[]> record) throws IOException {
        return objectMapper.readValue(record.value(), Event.class);
    }

    /** Extracts the Kafka-provided metadata. */
    private static Metadata getMetadata(ConsumerRecord<byte[], byte[]> record) {
        return new Metadata(
                record.topic(),
                record.partition(),
                record.offset(),
                record.timestamp(),
                String.valueOf(record.timestampType()));
    }
```

The Headers interface also allows you to return the last header for a given key via `lastHeader`. You can also return
all headers for the given key by replacing this with `Headers`. 

```java KafkaHeadersEventDeserializationSchema.java focus=39,43,62:70
    private static Headers getHeaders(ConsumerRecord<byte[], byte[]> record) {
        return new Headers(
                getStringHeaderValue(record, HEADER_TRACE_STATE),
                getStringHeaderValue(record, HEADER_TRACE_PARENT));
    }

    private static String getStringHeaderValue(
            ConsumerRecord<byte[], byte[]> record, String header) {
        return new String(record.headers().lastHeader(header).value(), StandardCharsets.UTF_8);
    }
```

When you have all the necessary data collected, it's time to collect it and return all information to your `EnrichedEvent`. 

## The full recipe

This recipe is self-contained. You can run the `KafkaHeadersTest#testProductionJob` class to see the full recipe
in action. That test uses an embedded Apache Kafka and Apache Flink setup, so you can run it directly via Maven or in
your favorite editor such as IntelliJ IDEA or Visual Studio Code.
