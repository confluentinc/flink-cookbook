package io.confluent.developer.cookbook.flink;

import io.confluent.developer.cookbook.flink.workflows.TableWorkflow;
import java.net.URI;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

public class BatchTableJob {

    public static void main(String[] args) throws Exception {
        final ParameterTool parameters = ParameterTool.fromArgs(args);

        URI inputURI = new URI(parameters.getRequired("inputURI"));

        produceResults(inputURI).print();
    }

    static TableResult produceResults(URI uri) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

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

        return TableWorkflow.defineWorkflow(tableEnv, "Transactions");
    }
}
