from typing import List, Tuple
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, Table, Row
from pyflink.table.expressions import col, call
from pyflink.table.functions import AggregateFunction
from pyflink.table.udf import udaf

# Constants
DEFAULT_PARALLELISM = "1"
SAMPLE_DATA: List[Tuple[str, float, float]] = [
    ("group_a", 10.0, 2.0),
    ("group_a", 20.0, 3.0),
    ("group_a", 15.0, 1.0),
    ("group_b", 30.0, 4.0),
    ("group_b", 25.0, 2.0),
    ("group_b", 35.0, 3.0),
    ("group_c", 5.0, 1.0),
    ("group_c", 10.0, 2.0),
    ("group_c", 8.0, 1.5)
]

# 1. Define an Aggregate UDF (UDAF) using a class
# This UDAF calculates a weighted average: sum(value * weight) / sum(weight)
class WeightedAverage(AggregateFunction):
    def create_accumulator(self) -> Row:
        """Create accumulator to store sum of weighted values and sum of weights"""
        return Row(0.0, 0.0)

    def get_value(self, accumulator: Row) -> float:
        """Calculate final result from accumulator"""
        sum_weighted_values = accumulator[0]
        sum_weights = accumulator[1]
        if sum_weights == 0:
            return 0.0
        else:
            return sum_weighted_values / sum_weights

    def accumulate(self, accumulator: Row, value: float, weight: float):
        """Process input row and update accumulator"""
        if value is not None and weight is not None:
            accumulator[0] += value * weight  # sum_weighted_values
            accumulator[1] += weight          # sum_weights

    def retract(self, accumulator: Row, value: float, weight: float):
        """Remove a value from the accumulator (for streaming scenarios)"""
        if value is not None and weight is not None:
            accumulator[0] -= value * weight
            accumulator[1] -= weight

    def merge(self, accumulator: Row, other_accumulators: list):
        """Merge multiple accumulators (for session windows, batch optimizations)"""
        for other_acc in other_accumulators:
            accumulator[0] += other_acc[0]
            accumulator[1] += other_acc[1]

    def get_accumulator_type(self) -> DataTypes.ROW:
        return DataTypes.ROW([
            DataTypes.FIELD("sum_weighted_values", DataTypes.DOUBLE()),
            DataTypes.FIELD("sum_weights", DataTypes.DOUBLE())
        ])

    def get_result_type(self) -> DataTypes.DOUBLE:
        return DataTypes.DOUBLE()

# 2. UDAF for collecting values into a list
class CollectValues(AggregateFunction):
    def create_accumulator(self) -> Row:
        """Create accumulator to store list of values"""
        return Row([])

    def get_value(self, accumulator: Row) -> str:
        """Return comma-separated string of collected values"""
        values = accumulator[0]
        return ",".join(map(str, values))

    def accumulate(self, accumulator: Row, value: float):
        """Add value to the list"""
        if value is not None:
            accumulator[0].append(value)

    def retract(self, accumulator: Row, value: float):
        """Remove value from the list"""
        if value is not None and value in accumulator[0]:
            accumulator[0].remove(value)

    def merge(self, accumulator: Row, other_accumulators: list):
        """Merge multiple accumulators"""
        for other_acc in other_accumulators:
            accumulator[0].extend(other_acc[0])

    def get_accumulator_type(self) -> DataTypes.ROW:
        return DataTypes.ROW([
            DataTypes.FIELD("values", DataTypes.ARRAY(DataTypes.DOUBLE()))
        ])

    def get_result_type(self) -> DataTypes.STRING:
        return DataTypes.STRING()

# 3. UDAF for calculating running statistics
class RunningStats(AggregateFunction):
    def create_accumulator(self) -> Row:
        """Create accumulator for running statistics"""
        return Row(0, 0.0, 0.0, 0.0)  # count, sum, sum_squares, min

    def get_value(self, accumulator: Row) -> str:
        """Return statistics as JSON-like string"""
        count = accumulator[0]
        sum_val = accumulator[1]
        sum_squares = accumulator[2]
        min_val = accumulator[3]
        
        if count == 0:
            return "{}"
        
        avg = sum_val / count
        variance = (sum_squares / count) - (avg * avg)
        std_dev = variance ** 0.5 if variance >= 0 else 0
        
        return f"{{'count':{count},'avg':{avg:.2f},'min':{min_val},'std_dev':{std_dev:.2f}}}"

    def accumulate(self, accumulator: Row, value: float):
        """Update running statistics"""
        if value is not None:
            accumulator[0] += 1  # count
            accumulator[1] += value  # sum
            accumulator[2] += value * value  # sum_squares
            if accumulator[0] == 1 or value < accumulator[3]:
                accumulator[3] = value  # min

    def retract(self, accumulator: Row, value: float):
        """Remove value from statistics (simplified)"""
        if value is not None and accumulator[0] > 0:
            accumulator[0] -= 1
            accumulator[1] -= value
            accumulator[2] -= value * value

    def merge(self, accumulator: Row, other_accumulators: list):
        """Merge multiple accumulators"""
        for other_acc in other_accumulators:
            accumulator[0] += other_acc[0]
            accumulator[1] += other_acc[1]
            accumulator[2] += other_acc[2]
            if other_acc[3] < accumulator[3] or accumulator[0] == 0:
                accumulator[3] = other_acc[3]

    def get_accumulator_type(self) -> DataTypes.ROW:
        return DataTypes.ROW([
            DataTypes.FIELD("count", DataTypes.INT()),
            DataTypes.FIELD("sum", DataTypes.DOUBLE()),
            DataTypes.FIELD("sum_squares", DataTypes.DOUBLE()),
            DataTypes.FIELD("min", DataTypes.DOUBLE())
        ])

    def get_result_type(self) -> DataTypes.STRING:
        return DataTypes.STRING()

def recipe_07_aggregate_udfs() -> None:
    """
    Demonstrates Aggregate User-Defined Functions (UDAFs) in PyFlink.
    
    This function shows:
    1. Creating UDAFs using class-based approach
    2. Weighted average calculations
    3. Value collection and aggregation
    4. Running statistics calculations
    5. Using UDAFs in Table API
    6. Using UDAFs in SQL queries
    7. Complex aggregation scenarios
    """
    # Setup
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    t_env.get_config().set("parallelism.default", DEFAULT_PARALLELISM)

    print("Recipe 7: Implementing Custom Aggregations with Aggregate UDFs (UDAFs)")

    # Create sample table
    input_table = t_env.from_elements(
        SAMPLE_DATA,
        DataTypes.ROW([
            DataTypes.FIELD("key", DataTypes.STRING()),
            DataTypes.FIELD("value", DataTypes.DOUBLE()),
            DataTypes.FIELD("weight", DataTypes.DOUBLE())
        ])
    )
    print("Input Table:")
    input_table.print_schema()
    print("\nInput Data:")
    input_table.execute().print()

    # Register UDAFs
    weighted_avg_udaf = WeightedAverage()
    collect_values_udaf = CollectValues()
    running_stats_udaf = RunningStats()
    
    t_env.create_temporary_function("WEIGHTED_AVG", weighted_avg_udaf)
    t_env.create_temporary_function("COLLECT_VALUES", collect_values_udaf)
    t_env.create_temporary_function("RUNNING_STATS", running_stats_udaf)

    # 1. Using UDAF in Table API
    print("\n--- 1. Using UDAF in Table API ---")
    result_table_api = input_table.group_by(
        col("key")
    ).select(
        col("key"),
        call("WEIGHTED_AVG", col("value"), col("weight")).alias("weighted_average"),
        col("value").avg.alias("simple_average"),
        col("value").count.alias("value_count")
    )
    print("Table with custom weighted average aggregation (Table API):")
    result_table_api.print_schema()
    print("\nData from Table API UDAF:")
    result_table_api.execute().print()

    # 2. Using UDAF in SQL
    print("\n--- 2. Using UDAF in SQL ---")
    t_env.create_temporary_view("my_data", input_table)
    sql_udaf_result = t_env.sql_query("""
        SELECT
            key,
            WEIGHTED_AVG(value, weight) AS weighted_average,
            AVG(value) AS simple_average,
            COUNT(value) AS value_count
        FROM my_data
        GROUP BY key
    """)
    print("Table with custom weighted average aggregation (SQL):")
    sql_udaf_result.print_schema()
    print("\nData from SQL UDAF:")
    sql_udaf_result.execute().print()

    # 3. Value collection UDAF
    print("\n--- 3. Value Collection UDAF ---")
    collect_result = input_table.group_by(
        col("key")
    ).select(
        col("key"),
        call("COLLECT_VALUES", col("value")).alias("collected_values"),
        call("COLLECT_VALUES", col("weight")).alias("collected_weights")
    )
    print("Table with value collection UDAF:")
    collect_result.print_schema()
    print("\nData from value collection UDAF:")
    collect_result.execute().print()

    # 4. Running statistics UDAF
    print("\n--- 4. Running Statistics UDAF ---")
    stats_result = input_table.group_by(
        col("key")
    ).select(
        col("key"),
        call("RUNNING_STATS", col("value")).alias("value_statistics"),
        call("RUNNING_STATS", col("weight")).alias("weight_statistics")
    )
    print("Table with running statistics UDAF:")
    stats_result.print_schema()
    print("\nData from running statistics UDAF:")
    stats_result.execute().print()

    # 5. Complex aggregation with multiple UDAFs
    print("\n--- 5. Complex Aggregation with Multiple UDAFs ---")
    complex_result = input_table.group_by(
        col("key")
    ).select(
        col("key"),
        call("WEIGHTED_AVG", col("value"), col("weight")).alias("weighted_avg"),
        call("COLLECT_VALUES", col("value")).alias("all_values"),
        call("RUNNING_STATS", col("value")).alias("value_stats"),
        col("value").sum.alias("total_value"),
        col("weight").sum.alias("total_weight")
    )
    print("Table with complex aggregation:")
    complex_result.print_schema()
    print("\nData from complex aggregation:")
    complex_result.execute().print()

    # 6. UDAF with filtering
    print("\n--- 6. UDAF with Filtering ---")
    filtered_result = input_table.filter(
        col("value") > 10.0
    ).group_by(
        col("key")
    ).select(
        col("key"),
        call("WEIGHTED_AVG", col("value"), col("weight")).alias("weighted_avg_filtered"),
        col("value").count.alias("filtered_count")
    )
    print("Table with UDAF and filtering (values > 10):")
    filtered_result.print_schema()
    print("\nData from filtered UDAF:")
    filtered_result.execute().print()

    # 7. SQL with complex UDAF operations
    print("\n--- 7. SQL with Complex UDAF Operations ---")
    complex_sql = t_env.sql_query("""
        SELECT
            key,
            WEIGHTED_AVG(value, weight) AS weighted_average,
            COLLECT_VALUES(value) AS all_values,
            RUNNING_STATS(value) AS statistics,
            SUM(value) AS total_value,
            SUM(weight) AS total_weight,
            COUNT(*) AS record_count
        FROM my_data
        WHERE value > 5.0
        GROUP BY key
        HAVING COUNT(*) > 1
    """)
    print("Table from complex SQL UDAF query:")
    complex_sql.print_schema()
    print("\nData from complex SQL UDAF query:")
    complex_sql.execute().print()

    # 8. UDAF with window functions
    print("\n--- 8. UDAF with Window Functions ---")
    # Create time-based data for windowing
    window_data = [
        ("group_a", 10.0, 2.0, "2023-01-01 10:00:00"),
        ("group_a", 20.0, 3.0, "2023-01-01 10:01:00"),
        ("group_a", 15.0, 1.0, "2023-01-01 10:02:00"),
        ("group_b", 30.0, 4.0, "2023-01-01 10:00:00"),
        ("group_b", 25.0, 2.0, "2023-01-01 10:01:00"),
        ("group_b", 35.0, 3.0, "2023-01-01 10:02:00")
    ]
    
    window_table = t_env.from_elements(
        window_data,
        DataTypes.ROW([
            DataTypes.FIELD("key", DataTypes.STRING()),
            DataTypes.FIELD("value", DataTypes.DOUBLE()),
            DataTypes.FIELD("weight", DataTypes.DOUBLE()),
            DataTypes.FIELD("event_time", DataTypes.STRING())
        ])
    )
    
    t_env.create_temporary_view("window_data", window_table)
    window_sql = t_env.sql_query("""
        SELECT
            key,
            event_time,
            WEIGHTED_AVG(value, weight) AS weighted_avg,
            RUNNING_STATS(value) AS stats
        FROM window_data
        GROUP BY key, event_time
        ORDER BY key, event_time
    """)
    print("Table from windowed UDAF query:")
    window_sql.print_schema()
    print("\nData from windowed UDAF query:")
    window_sql.execute().print()

if __name__ == '__main__':
    recipe_07_aggregate_udfs()