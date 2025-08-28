[![Try Confluent Cloud - The Data Streaming Platform](https://images.ctfassets.net/8vofjvai1hpv/10bgcSfn5MzmvS4nNqr94J/af43dd2336e3f9e0c0ca4feef4398f6f/confluent-banner-v2.svg)](https://confluent.cloud/signup?utm_source=github&utm_medium=banner&utm_campaign=oss-repos&utm_term=flink-cookbook)

# Recipes for Apache Flink®

This is a collection of examples of Apache Flink applications in the format of "recipes". Each recipe illustrates how you can solve a specific problem by leveraging one or more of the APIs of Apache Flink.

Each of these recipes is a self-contained module. They can be a starting point for solving your application requirements with Apache Flink.

## Requirements

* Maven
* Java 11

## Recipes

### APIs and languages

* [Batch and streaming with the Table and DataStream APIs](latest-transaction)
* [Writing an application in Kotlin™](kotlin)

### Connectors and formats

* [Deserializing JSON from Kafka](kafka-json-to-pojo)
* [Reading Apache Kafka headers](kafka-headers)
* [Continuously reading CSV files](continuous-file-reading)
* [Reading Google Protocol Buffers](read-protobuf)
* [Change data capture](change-data-capture)

### State and application lifecycle

* [Exactly-once with Apache Kafka®](kafka-exactly-once)
* [Upgrading Flink (Table API)](compiled-plan)
* [Migrating state away from Kryo](kryo-migration)

### Event routing

* [Creating dead letter queues](kafka-dead-letter)
* [Capturing late data](late-data-to-sink)
* [Splitting Apache Kafka® event streams](split-stream)

### Use cases

* [Joining and deduplicating data](table-deduplicated-join)
* [Using session windows](session-window)
* [Using CEP to alert when problems persist](pattern-matching-cep)
* [Measuring latency](measuring-latency)
* [An enrichment join that waits for missing data](enrichment-join-with-buffering)
