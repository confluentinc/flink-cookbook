# Reading Google Protocol Buffers with Apache Flink®

## Reading Google Protocol Buffers from Apache Kafka®

Google’s Protocol Buffers (Protobuf) are a language/platform neutral and extensible mechanism for serializing 
structured data. Protobuf is an alternative to the well-known formats like JSON and Apache Avro® and is commonly used 
as a serializer for Apache Kafka.

In this recipe, you are going to consume Protobuf-encoded event data from an [Apache Kafka](https://kafka.apache.org/) 
topic and print the data to screen.

You will use both the Table API and the DataStream API. It starts with consuming the events using the Table API Kafka 
connector before switching to the DataStream API for printing the information. This shows that you can use Protobuf 
in either your Table API or in your DataStream API application.

This recipe for Apache Flink is a self contained recipe that you can directly copy and run from your favorite editor.
There is no need to download Apache Flink or Apache Kafka.

## The Protobuf input data

The recipe uses the Kafka topic `transactions`, containing Protobuf-encoded records.

The Protobuf schema for these records is defined in `Transaction.proto`. You can use either `proto2` or `proto3`.

```scheme Transaction.proto
syntax = "proto3";
option java_multiple_files = true;

package io.confluent.developer.cookbook.flink.protobuf;
option java_package = "io.confluent.developer.cookbook.flink";

message Transaction {
  optional string t_time = 1;
  optional int64 t_id = 2;
  optional int64 t_customer_id = 3;
  optional double t_amount = 4;
}
```

## Protobuf project setup

To add support for Protobuf, you need to add dependencies and configure them correctly in the Maven project. 

### Add dependency on flink-protobuf

You start with adding Flink’s Protobuf file format artifact to your POM file.

```xml pom.xml focus=67:71
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-protobuf</artifactId>
            <version>${flink.version}</version>
        </dependency>
```

### Add Protobuf Maven plugin

After adding the dependency on `flink-protobuf`, you need to add the `protoc-jar-maven-plugin`. This Maven plugin
compiles and packages our `Transaction.proto` from `src/main/protobuf` into Java classes at `target/proto-sources`. 
This is done during the `generate-sources` phase of the Maven lifecycle.
These Java classes need to be provided in the classpath of your Flink application. In IntelliJ, you
can do that by [Generating Sources and Update Project Folders](https://www.jetbrains.com/help/idea/maven-projects-tool-window.html).

```xml pom.xml focus=225:237
            <plugin>
                <groupId>com.github.os72</groupId>
                <artifactId>protoc-jar-maven-plugin</artifactId>
                <version>3.11.4</version>
                <executions>
                    <execution>
                        <phase>generate-sources</phase>
                        <goals>
                            <goal>run</goal>
                        </goals>
                        <configuration>
                            <protocArtifact>com.google.protobuf:protoc:${protoc.version}</protocArtifact>
                            <inputDirectories>
                                <include>src/main/protobuf</include>
                            </inputDirectories>
                            <outputTargets>
                                <outputTarget>
                                    <type>java</type>
                                    <addSources>all</addSources>
                                    <outputDirectory>target/proto-sources</outputDirectory>
                                </outputTarget>
                            </outputTargets>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
```

## Connecting to Kafka

You are using the Apache Flink [`Kafka Table API`](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/kafka/)
connector in the application to connect to your Apache Kafka broker.
This is where you define the schema for your incoming Protobuf event, that you are using the Protobuf format and
the class name of your Protobuf schema. 

```java ReadProtobuf.java focus=28:36
        final TableDescriptor transactionStream =
                TableDescriptor.forConnector(KafkaDynamicTableFactory.IDENTIFIER)
                        .schema(
                                Schema.newBuilder()
                                        .column("t_time", DataTypes.STRING())
                                        .column("t_id", DataTypes.BIGINT())
                                        .column("t_customer_id", DataTypes.BIGINT())
                                        .column("t_amount", DataTypes.DOUBLE())
                                        .build())
                        .format("protobuf")
                        .option("protobuf.message-class-name", Transaction.class.getName())
                        .option("topic", TRANSACTION_TOPIC)
                        .option("properties.bootstrap.servers", "localhost:9092")
                        .option("properties.group.id", "ReadProtobuf-Job")
                        .option("scan.startup.mode", "earliest-offset")
                        .build();

```

## Creating and selecting from a dynamic table

After defining your incoming stream, you need to create a temporary dynamic table and select the data from this table.

```java ReadProtobuf.java focus=53:57
        tableEnv.createTemporaryTable("KafkaSourceWithProtobufEncodedEvents", transactionStream);

        Table resultTable =
                tableEnv.sqlQuery(
                        String.join("\n", "SELECT * FROM KafkaSourceWithProtobufEncodedEvents"));
```

## Switching to the DataStream API

You are going to switch back from the previously created Dynamic Table to the DataStream API. Flink currently only 
supports Protobuf when using a Source or a Sink from a Table API job. If required, you can still implement your main 
pipeline or access more low-level operations by switching to the DataStream API.

Because the SQL query is a regular select, the result of the Dynamic Table is an insert-only changelog stream.

```java ReadProtobuf.java focus=62
        DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);
```

You can find the data type mapping from Flink types to Protobuf types in the 
[Flink Protobuf documentation](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/formats/protobuf/#data-type-mapping).

## The full recipe

This recipe is self contained. You can run the `ReadProtobufTest#testProductionJob` test to see the full recipe
in action. That test uses an embedded Apache Kafka and Apache Flink setup, so you can run it directly via
Maven or in your favorite editor such as IntelliJ IDEA or Visual Studio Code.
