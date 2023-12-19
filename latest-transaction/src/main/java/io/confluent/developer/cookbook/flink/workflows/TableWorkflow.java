package io.confluent.developer.cookbook.flink.workflows;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

public class TableWorkflow {
    public static TableResult defineWorkflow(TableEnvironment tableEnv, String inputTable) {

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
    }
}
