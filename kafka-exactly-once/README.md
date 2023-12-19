# Exactly-once with Apache Kafka®

## Exactly once processing with Apache Kafka® and Apache Flink®

Apache Flink is able to guarantee that events will be processed exactly once when used with supported sources and sinks.  
This means that even in case of a failure where Flink retries to send the same event, the consumer of that event will 
still receive the event only once. 

In this recipe, you are going to read from and write to Apache Kafka using exactly-once guarantees. 

This recipe for Apache Flink is a self-contained recipe that you can directly copy and run from your favorite editor.
There is no need to download Apache Flink or Apache Kafka.

## Infrastructure requirements

### Enabled High Availability Service

Running Flink with a High Availability service is enabled to avoid that data will be lost in case of a Job Manager 
failure.

### Enabled checkpointing

Checkpointing is enabled to avoid that data will be lost in case of a Task Manager failure.
In this recipe, the checkpointing interval is set to every 10 seconds.

```java KafkaExactlyOnce.java focus=42
        env.enableCheckpointing(10000);
```

The Flink Kafka producer will commit offsets as part of the checkpoint. This is not needed for Flink to guarantee 
exactly-once results, but can be useful for other applications that use offsets for monitoring purposes.  

### Prevent transaction timeouts

Any Kafka transaction that times out will cause data loss. There are two important parameters when enabling 
exactly-once processing. The first one is [`transaction.max.timeout.ms`](https://docs.confluent.io/platform/current/installation/configuration/broker-configs.html#brokerconfigs_transaction.max.timeout.ms) 
which is set at the Kafka broker. The default value is 15 minutes. 

The other parameter is [`transaction.timeout.ms`](https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html#producerconfigs_transaction.timeout.ms)
which is set by Flink, since that is the Kafka producer. 

You need to make sure that:

1. The value for `transaction.max.timeout.ms` on your Kafka broker is beyond the maximum expected 
Flink and/or Kafka downtime. 
2. The value for `transaction.timeout.ms` is lower than `transaction.max.timeout.ms`.

## Your application

### Use `DeliveryGuarantee.EXACTLY_ONCE`

You are using the Apache
Flink [`KafkaSink`](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kafka/#kafka-sink)
connector in the application to connect to your Apache Kafka cluster. 

You use the `DeliveryGuarantee` which is set to `EXACTLY_ONCE`.

### Set `setTransactionalIdPrefix`

You set the value for `setTransactionalIdPrefix`. This value has to be unique for each Flink application that you run in the same Kafka cluster. 

### Set `transaction.timeout.ms`

As explained earlier, you set the value for `transaction.timeout.ms` to a value that matches your requirements.

### Put it all together

```java
        var producerProperties = new Properties();
        producerProperties.setProperty("transaction.timeout.ms", "60000");

        KafkaRecordSerializationSchema<String> serializer =
                KafkaRecordSerializationSchema.builder()
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .setTopic(OUTPUT)
                        .build();

        KafkaSink<String> sink =
                KafkaSink.<String>builder()
                        .setBootstrapServers("localhost:9092")
                        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .setTransactionalIdPrefix("KafkaExactlyOnceRecipe")
                        .setKafkaProducerConfig(producerProperties)
                        .setRecordSerializer(serializer)
                        .build();
```

## The full recipe

This recipe is self-contained. You can run the `KafkaExactlyOnce#testProductionJob` test to see the full recipe
in action. That test uses an embedded Apache Kafka and Apache Flink setup, so you can run it directly via Maven or in
your favorite editor such as IntelliJ IDEA or Visual Studio Code.

The test will log that partitions have been flushed, new transactional producers have been created and that the
checkpoint has been completed successfully. 