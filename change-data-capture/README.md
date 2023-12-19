# Reading Change Data Capture (CDC) with Apache Flink®

## Reading changes from databases in Apache Flink

With Change Data Capture, all inserts, updates, and deletes that are committed to your database are captured. You can 
use this data for use cases such as keeping your standby database in sync with your primary database,
keeping your cache up-to-date, or to stream data in realtime into your data warehouse.

The [CDC Connectors for Apache Flink®](https://github.com/ververica/flink-cdc-connectors) offer a set of source 
connectors for Apache Flink that supports a wide variety of databases. The connectors integrate 
[Debezium®](https://debezium.io/) as the engine to capture the data changes. There are currently CDC Connectors for
MongoDB®, MySQL® (including MariaDB®, AWS Aurora®, AWS RDS®), Oracle®, Postgres®, Microsoft SQL Server®, and many more.
The connectors support exactly-once semantics. 

In this recipe, you are going to consume change data capture from a [Postgres](https://www.postgresql.org/) database.
You will use the DataStream API to connect to the table `transactions.incoming` and print the JSON payload with the 
captured data changes. 

This recipe for Apache Flink is a self contained recipe that you can directly copy and run from your favorite editor.
There is no need to download Apache Flink or Apache Kafka.

## The Postgres table

The recipe uses the Postgres schema `transactions` and the Postgres database `incoming`.

```sql
CREATE schema transactions;
CREATE TABLE transactions.incoming (
	t_id serial PRIMARY KEY,
	t_customer_id serial,
	t_amount REAL
);
```

## Add dependency on Postgres CDC Connector

You start with adding the Postgres CDC Connector to your POM file.

```xml pom.xml focus=53:58
        <dependency>
            <groupId>com.ververica</groupId>
            <artifactId>flink-connector-postgres-cdc</artifactId>
            <version>${flink.cdc.connector.version}</version>
        </dependency>
```

## Connecting and reading data from Postgres

You are using the [Postgres CDC Connector](https://github.com/ververica/flink-cdc-connectors/blob/master/docs/content/connectors/postgres-cdc.md)
in the application to connect to Postgres. 
Next to the necessary connection information, you are configuring the connector
to use the deserializer called `JsonDebeziumDeserializationSchema`. The connector supports different
[Postgres logical decoding plugins](https://wiki.postgresql.org/wiki/Logical_Decoding_Plugins), which can be configured via the `decodingPluginName`.

```java ReadCDC.java focus=19:32
        SourceFunction<String> source =
                PostgreSQLSource.<String>builder()
                        .hostname("localhost")
                        .port(5432)
                        .database("postgres")
                        .schemaList("transactions")
                        .tableList("transactions.incoming")
                        .username("postgres")
                        .password("postgres")
                        // Set the logical decoding plugin to pgoutput which
                        // is used by the embedded postgres database
                        .decodingPluginName("pgoutput")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .build();
```

## Set parallelism for the source

Postgres and other databases have a binary log, that contains all changes to the database. 
The binary log used by the CDC connector. The Postgres CDC source can't read in parallel, because 
there is only one task that can receive binlog events. That requires setting the parallelism for the source to 1. 

```java ReadCDC.java focus=43
        final DataStreamSource<String> postgres = env.addSource(source).setParallelism(1);
```

## The full recipe

This recipe is self contained. You can run the `ReadCDCTest#testProductionJob` test to see the full recipe
in action. That test uses an embedded Postgres and Apache Flink setup, so you can run it directly via
Maven or in your favorite editor such as IntelliJ IDEA or Visual Studio Code.

See the comments and tests in this recipe for more details.
