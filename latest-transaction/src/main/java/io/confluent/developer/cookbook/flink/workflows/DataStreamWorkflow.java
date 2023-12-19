package io.confluent.developer.cookbook.flink.workflows;

import io.confluent.developer.cookbook.flink.functions.LatestTransactionFunction;
import io.confluent.developer.cookbook.flink.records.Transaction;
import java.util.function.Consumer;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStreamWorkflow {
    public static void defineWorkflow(
            StreamExecutionEnvironment env,
            Source<Transaction, ?, ?> transactionSource,
            Consumer<DataStream<Transaction>> sinkApplier) {

        DataStream<Transaction> transactionStream =
                env.fromSource(transactionSource, WatermarkStrategy.noWatermarks(), "Transactions");

        DataStream<Transaction> results =
                transactionStream
                        .keyBy(t -> t.t_customer_id)
                        .process(new LatestTransactionFunction());

        // attach the sink
        sinkApplier.accept(results);
    }
}
