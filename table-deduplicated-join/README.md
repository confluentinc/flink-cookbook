# Joining and deduplicating data

## Joining and deduplicating events from Kafka in Apache Flink

There are a lot of situations where you need to combine or enrich your data with information from another source, to get
to meaningful insights. In this recipe, you are going to consume event data from two different Apache Kafka topics, 
join the data and deduplicate it. 

This recipe uses both the DataStream API and the Table API. It starts with consuming the events using the DataStream
Kafka connector, switches to the Table API, uses SQL to perform the joining and deduplication and finally switches
back to the DataStream API for generating the output. 

This recipe for Apache Flink is a self contained recipe that you can directly copy and run from your favorite editor.
There is no need to download Apache Flink or Apache Kafka.

## The JSON input data

The recipe uses Kafka topic `customers` and Kafka topic `transactions`, both containing JSON-encoded records.

```json
{"c_id":1,"c_name":"Ramon Stehr"}
{"c_id":2,"c_name":"Barbie Ledner"}
{"c_id":3,"c_name":"Lore Baumbach"}
{"c_id":4,"c_name":"Deja Crist"}
{"c_id":5,"c_name":"Felix Weimann"}
```

```json
{"t_id":1,"t_customer_id":1,"t_amount":99.08,"timestamp":1657144244.452688000}
{"t_id":2,"t_customer_id":1,"t_amount":405.01,"timestamp":1657144244.572698000}
{"t_id":3,"t_customer_id":2,"t_amount":974.90,"timestamp":1657144244.687627000}
{"t_id":4,"t_customer_id":4,"t_amount":100.19,"timestamp":1657144244.810700000}
{"t_id":5,"t_customer_id":3,"t_amount":161.48,"timestamp":1657144244.947114000}
```

## Connecting and reading data from Apache Kafka

This recipe uses the same implementation as the kafka-json-to-pojo recipe
to connect to the `customers` and `transactions` topic and deserialize the data. It is recommended to read that recipe before continuing.

You create streams `customerStream` and `transactionStream` as a result. 

## Switch from DataStream API to Table API

This recipe could have been written using only the Table API, but we've chosen instead to illustrate how to switch between the DataStream API and the Table API. In this case you are going to switch from the DataStreams to Tables by creating temporary views from the incoming streams created by the DataStream API. 

```java TableDeduplicatedJoin.java focus=69:70
        tableEnv.createTemporaryView("Customers", customerStream);
        tableEnv.createTemporaryView("Transactions", transactionStream);
```

## Deduplicating and joining the events

After creating the views, you will use SQL to actually join and deduplicate the data in the previously created views.
You want to retrieve each transaction, the customer's name, and the transaction amount. This is done by joining the 
records from the `Transactions` view with the `Customers` view. The result is stored in a so-called 
[`Dynamic Table`](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/concepts/dynamic_tables/).  

```java TableDeduplicatedJoin.java focus=74:81
        Table resultTable =
                tableEnv.sqlQuery(
                        String.join(
                                "\n",
                                "SELECT t_id, c_name, CAST(t_amount AS DECIMAL(5, 2))",
                                "FROM Customers",
                                "JOIN (SELECT DISTINCT * FROM Transactions) ON c_id = t_customer_id"));
```

A word of warning: This query is dangerous because Flink's SQL engine will have to store all previous transactions in order to be able to recognize duplicates over the unbounded transaction stream. Moreover, this join is very expensive, as it requires storing all of the customers and transactions (so that the join result can be updated whenever a customer or transaction is updated).

To prevent unbounded state retention in production applications you can either configure [table.exec.state.ttl](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/config/#table-exec-state-ttl), or modify your queries so that Flink's SQL planner can free state that is no longer needed. In this particular case you could modify the query so that it is doing an [interval join](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/joins/#interval-joins), and is [deduplicated using OVER windows with ROW_NUMBER()](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/deduplication/#deduplication). 

## Switching back to the DataStream API

You are going to switch back from the previously created Dynamic Table to the DataStream API. Because the SQL query
is a regular join, the result of the Dynamic Table is an updating changelog table. 

```java TableDeduplicatedJoin.java focus=84
        DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);
```

## The full recipe

This recipe is self contained. You can run the `TableDeduplicatedJoinTest#testProductionJob` test to see the full recipe
in action. That test uses an embedded Apache Kafka and Apache Flink setup, so you can run it directly via
Maven or in your favorite editor such as IntelliJ IDEA or Visual Studio Code.

See the comments and tests included in the code for more details.
