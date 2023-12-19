# Upgrading Flink (Table API)

## Background

This recipe shows how you can upgrade the Flink version used by a Table API job without losing any state.

The standard technique for upgrading a Flink job to a new version of Flink involves restarting the job from a savepoint:

* perform a "stop with savepoint" on the running job
* upgrade to the new version of Flink
* restart the job from that savepoint

This process assumes that each stateful operator in the job being restarted will be able to find and load its state. Jobs written using the DataStream API have enough low-level control to be able to avoid or cope with potential problems (see the Flink documentation on [savepoints](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/savepoints/) for details). But the Table API operates at a higher level of abstraction, and new Flink versions may introduce changes to the SQL planner/optimizer that render the state in a checkpoint or savepoint unrestorable.

This recipe illustrates how to use the compiled plan feature described in [FLIP-190: Support Version Upgrades for Table API & SQL Programs](https://cwiki.apache.org/confluence/x/KZBnCw) and introduced in Flink 1.15 to overcome this problem.

Note, however, that this compiled plan feature is considered experimental.

## Using a compiled plan

The goal of FLIP-190 is to handle cases where you want to upgrade to a newer Flink version while continuing to execute the same SQL query. By transforming the query to a compiled plan, you are guaranteed that exactly the same plan (using the same operators and state) will be executed regardless of which version of Flink is executing that plan. This means that you should be able to upgrade to a newer version of Flink without losing any state.

The query used in this example is deduplicating a stream of transactions using [the technique recommended in the Flink documentation](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/deduplication/):

```java CompiledPlanRecipe.java focus=43:52
    public static final String streamingDeduplication =
            String.join(
                    "\n",
                    "INSERT INTO print_sink",
                    "SELECT t_id, t_customer_id, t_amount",
                    "FROM (",
                    "  SELECT *,",
                    "  ROW_NUMBER() OVER (PARTITION BY t_id ORDER BY t_time ASC) AS row_num",
                    "  FROM Transactions",
                    ") WHERE row_num = 1");
```

If you were about to upgrade Flink, you would use code like this
to compile the SQL query into a JSON plan:

```java CompiledPlanRecipe.java focus=71:84
    public static void compileAndWritePlan(Path planLocation) {
        // Compiling plans is only possible in streaming mode
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // Overwrite an existing plan file, if any (useful for testing/debugging)
        tableEnv.getConfig().set("table.plan.force-recompile", "true");

        tableEnv.executeSql(transactionsDDL);
        tableEnv.executeSql(printSinkDDL);

        // Only queries that describe a complete pipeline (including an INSERT) can be compiled
        tableEnv.compilePlanSql(streamingDeduplication).writeToFile(planLocation);
    }
```

Using the newer Flink version you could then load and execute the compiled plan:

```java CompiledPlanRecipe.java focus=86:95
    public static TableResult runCompiledPlan(Path planLocation) {
        // Compiled plans can only be executed in streaming mode
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // Avoid unbounded state retention (60 seconds is enough for this example)
        tableEnv.getConfig().set("table.exec.state.ttl", "60000");

        return tableEnv.executePlan(PlanReference.fromFile(planLocation));
    }
```

If you want to see what the JSON plan looks like, one of the tests prints it out:

```java CompiledPlanRecipeTest.java focus=86,93:94
    @Test
    public void printPlan() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        tableEnv.executeSql(transactionsDDL);
        tableEnv.executeSql(printSinkDDL);

        tableEnv.compilePlanSql(streamingDeduplication).printJsonString();
    }
```

## The full recipe

See the code and tests in this recipe for more details.

You can run these tests directly via
Maven or in your favorite editor such as IntelliJ IDEA or Visual Studio Code. There is no need to download or install Apache Flink or Apache Kafka.
