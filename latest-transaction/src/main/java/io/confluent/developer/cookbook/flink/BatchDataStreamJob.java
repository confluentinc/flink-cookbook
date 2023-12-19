package io.confluent.developer.cookbook.flink;

import io.confluent.developer.cookbook.flink.records.JsonPojoInputFormat;
import io.confluent.developer.cookbook.flink.records.Transaction;
import io.confluent.developer.cookbook.flink.workflows.DataStreamWorkflow;
import java.net.URI;
import java.util.function.Consumer;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;

public class BatchDataStreamJob {

    public static void main(String[] args) throws Exception {
        final ParameterTool parameters = ParameterTool.fromArgs(args);
        URI inputURI = new URI(parameters.getRequired("inputURI"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        setupJob(env, inputURI, workflow -> workflow.sinkTo(new PrintSink<>()));
        env.execute();
    }

    public static void setupJob(
            StreamExecutionEnvironment env,
            URI inputURI,
            Consumer<DataStream<Transaction>> sinkApplier) {

        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        FileSource<Transaction> boundedSource =
                FileSource.forRecordStreamFormat(
                                new JsonPojoInputFormat<>(Transaction.class), new Path(inputURI))
                        .build();

        DataStreamWorkflow.defineWorkflow(env, boundedSource, sinkApplier);
    }
}
