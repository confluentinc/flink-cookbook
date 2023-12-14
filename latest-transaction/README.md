# Batch and streaming with the Table and DataStream APIs

## Use case: Keeping track of the most recent transaction for each customer

In this recipe, you are going to keep track of the most recent transaction for each customer.

This recipe contains four different implementations, each working with the same JSON-encoded input. You will see how to setup serialization and deserialization of timestamps for maximum flexibility. If you follow this pattern, your timestamped data can be used with either the DataStream API or the Table/SQL API, and with various source connectors.

In this recipe you'll also get a good look at how batch and streaming relate to one another, because the implementations provided by this recipe cover these cases:

* Streaming from Apache Kafka using Apache Flink's DataStream API
* Streaming from Apache Kafka using Apache Flink's Table API
* Batch from File System using Apache Flink's DataStream API
* Batch from File System using Apache Flink's Table API

This recipe for Apache Flink is a self-contained recipe that you can directly copy and run from your favorite editor.
There is no need to download Apache Flink or Apache Kafka.

## Writing the data

The recipe uses either a Kafka topic `transactions`, or files in a temporary directory. In either case the input is JSON-encoded with timestamps written in ISO-8601 format: 

```json
{"t_time": "2022-07-19T11:46:20.000Z", "t_id": 0, "t_customer_id": 0, "t_amount": 100.00}
{"t_time": "2022-07-19T12:00:00.000Z", "t_id": 1, "t_customer_id": 1, "t_amount": 55.00}
{"t_time": "2022-07-24T12:00:00.000Z", "t_id": 2, "t_customer_id": 0, "t_amount": 500.00}
{"t_time": "2022-07-24T13:00:00.000Z", "t_id": 3, "t_customer_id": 1, "t_amount": 11.00}
{"t_time": "2022-07-24T12:59:00.000Z", "t_id": 4, "t_customer_id": 1, "t_amount": 1.00}
```

All of the source connectors we want to use can handle this ISO-8601 format, but it requires the use of the `jackson-datatype-jsr310` module. For serialization you'll need to use this `@JsonFormat` annotation in the `Transaction` class:

```java Transaction.java focus=10:19
    /**
     * Without this annotation, the timestamps are serialized like this:
     * {"t_time":1658419083.146222000, ...} <br>
     * The StreamingTableJob fails if the timestamps are in that format.
     */
    @JsonFormat(
            shape = JsonFormat.Shape.STRING,
            pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
            timezone = "UTC")
    public Instant t_time;
```

## Streams from Apache Kafka

Two of the implementations are based on connecting to Apache Kafka from Apache Flink using Flink's Kafka connector.
This is done via the Kafka topic `transactions`. 

### DataStream API

The first implementation uses Apache Flink's [`KafkaSource`](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kafka/#kafka-source) 
DataStream API connector. It uses the same implementation as you can find in the kafka-json-to-pojo recipe.

```java StreamingDataStreamJob.java focus=28:34
        KafkaSource<Transaction> unboundedSource =
                KafkaSource.<Transaction>builder()
                        .setBootstrapServers("localhost:9092")
                        .setTopics(kafkaTopic)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new TransactionDeserializer())
                        .build();
```

### Table API

The second implementation uses Apache Flink's [`Kafka`](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/kafka/) 
Table API connector. You first have to create a [`TableEnvironment`](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/common/#create-a-tableenvironment), 
which is the entrypoint for Table API and SQL integration. Since this is a streaming implementation, you are using `inStreamingMode()`.

```java StreamingTableJob.java focus=15:17
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);
```

After you've defined your `TableEnvironment`, you connect to the Apache Kafka topic by creating a dynamic table with SQL. It is necessary to explicitly configure `'json.timestamp-format.standard' = 'ISO-8601'` as shown.

```java StreamingTableJob.java focus=19:36
        tableEnv.executeSql(
                String.format(
                        String.join(
                                "\n",
                                "CREATE TABLE Transactions (",
                                "  t_time TIMESTAMP_LTZ(3),",
                                "  t_id BIGINT,",
                                "  t_customer_id BIGINT,",
                                "  t_amount DECIMAL",
                                ") WITH (",
                                "  'connector' = 'kafka',",
                                "  'topic' = '%s',",
                                "  'properties.bootstrap.servers' = 'localhost:9092',",
                                "  'scan.startup.mode' = 'earliest-offset',",
                                "  'format' = 'json',",
                                "  'json.timestamp-format.standard' = 'ISO-8601'",
                                ")"),
                        kafkaTopic));
```

## Batches from the filesystem

The remaining two implementations are based on consuming files from your filesystem.
You can specify a location by providing the argument `--inputURI`.

### DataStream API

The third implementation uses Apache Flink's [`FileSystem`](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/filesystem/)
DataStream API connector. Since this implementation is a batch implementation, you explicitly have to set the
`RuntimeExecutionMode` to `BATCH`. 

```java BatchDataStreamJob.java focus=32
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
```

Then you can configure the `FileSystem` to read the files. Since Apache Flink lacks a `JsonInputFormat` which you can 
use with the `FileSource`, this recipe uses `JsonPojoInputFormat`. 
This is a custom input format for JSON that's capable of handling any class Jackson can work with, and it includes the `JavaTimeModule` needed for JSR-310 / ISO-8601 encoded timestamps.

```java BatchDataStreamJob.java focus=34:37
        FileSource<Transaction> boundedSource =
                FileSource.forRecordStreamFormat(
                                new JsonPojoInputFormat<>(Transaction.class), new Path(inputURI))
                        .build();
```

### Table API

The fourth implementation uses Apache Flink's [`FileSystem`](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/filesystem/)
Table API connector. You first have to create a [`TableEnvironment`](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/common/#create-a-tableenvironment),
which is the entrypoint for Table API and SQL integration. Since this is a batch implementation, you are using `inBatchMode()`.

```java BatchTableJob.java focus=21:23
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);
```

After you've defined your `TableEnvironment`, you connect to the files by creating a dynamic table with SQL.

```java BatchTableJob.java focus=25:40
        tableEnv.executeSql(
                String.format(
                        String.join(
                                "\n",
                                "CREATE TABLE Transactions (",
                                "  t_time TIMESTAMP_LTZ(3),",
                                "  t_id BIGINT,",
                                "  t_customer_id BIGINT,",
                                "  t_amount DECIMAL",
                                ") WITH (",
                                "  'connector' = 'filesystem',",
                                "  'path' = '%s',",
                                "  'format' = 'json',",
                                "  'json.timestamp-format.standard' = 'ISO-8601'",
                                ")"),
                        uri));
```

## Determining the latest transaction

Two of the recipes are using the DataStream API and two of the recipes are using the Table API as explained previously.
After connecting to your batch or streaming data sources, you can use the same workflow for your DataStream API recipes 
and the same workflow for your Table API recipes. That means that you can easily switch your application between 
a bounded (batch) or unbounded (streaming) source without needing to modify your business logic. 

### The DataStream Workflow

The recipes that use the DataStream API use Apache Flink's [`ValueState`](https://nightlies.apache.org/flink/flink-docs-stable/api/java/org/apache/flink/api/common/state/ValueState.html)
to retrieve or update the latest transaction from the incoming data.

You first define and configure `latestState` which is the implementation of `ValueState`. 

```java LatestTransactionFunction.java focus=13:20
    private ValueState<Transaction> latestState;

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Transaction> stateDescriptor =
                new ValueStateDescriptor<>("latest transaction", Transaction.class);
        latestState = getRuntimeContext().getState(stateDescriptor);
    }
```

Next, you define how each incoming event needs to be handled. In case there's no `latestState` yet for this customer 
or if a new incoming transaction has occurred, you are updating the value in `latestState` to the value from the 
incoming transaction. 

```java LatestTransactionFunction.java focus=23:34
    public void processElement(
            Transaction incoming,
            KeyedProcessFunction<Long, Transaction, Transaction>.Context context,
            Collector<Transaction> out)
            throws Exception {

        Transaction latestTransaction = latestState.value();

        if (latestTransaction == null || (incoming.t_time.isAfter(latestTransaction.t_time))) {
            latestState.update(incoming);
            out.collect(incoming);
        }
    }
```

`ValueState` can only be used with keyed state. In our DataStream API workflow, this means that retrieving and updating
can only be done per key. This recipe uses the `t_customer_id` as the key. 

```java DataStreamWorkflow.java focus=20:23
        DataStream<Transaction> results =
                transactionStream
                        .keyBy(t -> t.t_customer_id)
                        .process(new LatestTransactionFunction());
```

In the case of the DataStream implementations, there's no real difference between the behavior of the batch and streaming versions.

### The Table workflow

The recipes that use the Table API can use exactly the same SQL statement to determine what is the latest transaction 
for each customer.

```java TableWorkflow.java focus=9:19
        String query =
                String.format(
                        String.join(
                                "\n",
                                "SELECT t_time, t_id, t_customer_id, t_amount",
                                "  FROM (",
                                "    SELECT *, ROW_NUMBER()",
                                "      OVER (PARTITION BY t_customer_id ORDER BY t_time DESC) AS rownum",
                                "    FROM %s)",
                                "WHERE rownum <= 1"),
                        inputTable);

        return tableEnv.sqlQuery(query).execute();
```

The only difference between the batch and streaming Table workflows is the output: the batch implementation will produce a final,
materialized result, while the streaming implementation produces a changelog stream. 

## The full recipe

This recipe is self-contained. There are four tests for the four different implementations you can run to see the full
recipe in action. The tests are:

* `LatestTransactionTest#testStreamingDataStreamJob`
* `LatestTransactionTest#testStreamingTableJob`
* `LatestTransactionTest#testBatchDataStreamJob`
* `LatestTransactionTest#testBatchTableJob`

All tests use an embedded Apache Kafka and Apache Flink setup, so you can run it directly via
Maven or in your favorite editor such as IntelliJ IDEA or Visual Studio Code.
