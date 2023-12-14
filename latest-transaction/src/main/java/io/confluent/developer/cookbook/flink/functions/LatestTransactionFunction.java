package io.confluent.developer.cookbook.flink.functions;

import io.confluent.developer.cookbook.flink.records.Transaction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class LatestTransactionFunction
        extends KeyedProcessFunction<Long, Transaction, Transaction> {

    private ValueState<Transaction> latestState;

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Transaction> stateDescriptor =
                new ValueStateDescriptor<>("latest transaction", Transaction.class);
        latestState = getRuntimeContext().getState(stateDescriptor);
    }

    @Override
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
}
