# Recipe 14: Tumbling Window Aggregations
from typing import List, Tuple
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, Table
from pyflink.table.expressions import col, lit
from pyflink.table.window import Tumble
from datetime import datetime, timedelta

# Constants
DEFAULT_PARALLELISM = "1"
SAMPLE_DATA: List[Tuple[int, str, float, datetime]] = [
    (1, "sensor_a", 22.5, datetime(2023, 1, 1, 10, 0, 0)),
    (2, "sensor_b", 23.1, datetime(2023, 1, 1, 10, 1, 0)),
    (3, "sensor_a", 22.8, datetime(2023, 1, 1, 10, 2, 0)),
    (4, "sensor_c", 21.9, datetime(2023, 1, 1, 10, 3, 0)),
    (5, "sensor_b", 23.5, datetime(2023, 1, 1, 10, 4, 0)),
    (6, "sensor_a", 22.2, datetime(2023, 1, 1, 10, 5, 0)),
    (7, "sensor_c", 22.0, datetime(2023, 1, 1, 10, 6, 0)),
    (8, "sensor_b", 23.8, datetime(2023, 1, 1, 10, 7, 0)),
    (9, "sensor_a", 22.9, datetime(2023, 1, 1, 10, 8, 0)),
    (10, "sensor_c", 21.7, datetime(2023, 1, 1, 10, 9, 0)),
    (11, "sensor_b", 24.1, datetime(2023, 1, 1, 10, 10, 0)),
    (12, "sensor_a", 22.4, datetime(2023, 1, 1, 10, 11, 0))
]

def recipe_14_tumbling_windows() -> None:
    """
    Demonstrates tumbling window aggregations in PyFlink.
    
    This function shows:
    1. Creating tumbling windows with different time intervals
    2. Basic aggregations (count, sum, avg, min, max)
    3. Grouping by sensor within windows
    4. Window start and end time extraction
    5. Complex aggregations with multiple functions
    6. SQL-based tumbling window operations
    """
    # Setup
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    t_env.get_config().set("parallelism.default", DEFAULT_PARALLELISM)

    print("Recipe 14: Tumbling Window Aggregations")

    # Create sample table with timestamp
    input_table = t_env.from_elements(
        SAMPLE_DATA,
        DataTypes.ROW([
            DataTypes.FIELD("id", DataTypes.INT()),
            DataTypes.FIELD("sensor_id", DataTypes.STRING()),
            DataTypes.FIELD("temperature", DataTypes.DOUBLE()),
            DataTypes.FIELD("event_time", DataTypes.TIMESTAMP(3))
        ])
    )
    print("Input Table:")
    input_table.print_schema()
    print("\nInput Data:")
    input_table.execute().print()

    # 1. Basic Tumbling Window (5-minute intervals)
    print("\n--- 1. Basic Tumbling Window (5-minute intervals) ---")
    tumbling_5min = input_table.window(
        Tumble.over(lit(5).minutes).on(col("event_time")).alias("w")
    ).group_by(
        col("w")
    ).select(
        col("w").start.alias("window_start"),
        col("w").end.alias("window_end"),
        col("temperature").count.alias("reading_count"),
        col("temperature").avg.alias("avg_temperature"),
        col("temperature").min.alias("min_temperature"),
        col("temperature").max.alias("max_temperature")
    )
    print("Table after 5-minute tumbling window:")
    tumbling_5min.print_schema()
    print("\nData after 5-minute tumbling window:")
    tumbling_5min.execute().print()

    # 2. Tumbling Window with Sensor Grouping
    print("\n--- 2. Tumbling Window with Sensor Grouping ---")
    tumbling_grouped = input_table.window(
        Tumble.over(lit(3).minutes).on(col("event_time")).alias("w")
    ).group_by(
        col("w"),
        col("sensor_id")
    ).select(
        col("w").start.alias("window_start"),
        col("w").end.alias("window_end"),
        col("sensor_id"),
        col("temperature").count.alias("reading_count"),
        col("temperature").avg.alias("avg_temperature"),
        col("temperature").min.alias("min_temperature"),
        col("temperature").max.alias("max_temperature")
    )
    print("Table after grouped tumbling window:")
    tumbling_grouped.print_schema()
    print("\nData after grouped tumbling window:")
    tumbling_grouped.execute().print()

    # 3. Tumbling Window with Complex Aggregations
    print("\n--- 3. Tumbling Window with Complex Aggregations ---")
    tumbling_complex = input_table.window(
        Tumble.over(lit(2).minutes).on(col("event_time")).alias("w")
    ).group_by(
        col("w")
    ).select(
        col("w").start.alias("window_start"),
        col("w").end.alias("window_end"),
        col("temperature").count.alias("total_readings"),
        col("sensor_id").count.distinct.alias("unique_sensors"),
        col("temperature").avg.alias("avg_temperature"),
        col("temperature").stddev.alias("temperature_stddev"),
        (col("temperature").max - col("temperature").min).alias("temperature_range")
    )
    print("Table after complex tumbling window:")
    tumbling_complex.print_schema()
    print("\nData after complex tumbling window:")
    tumbling_complex.execute().print()

    # 4. Tumbling Window with Filtering
    print("\n--- 4. Tumbling Window with Filtering ---")
    tumbling_filtered = input_table.filter(
        col("temperature") > 22.0
    ).window(
        Tumble.over(lit(4).minutes).on(col("event_time")).alias("w")
    ).group_by(
        col("w")
    ).select(
        col("w").start.alias("window_start"),
        col("w").end.alias("window_end"),
        col("temperature").count.alias("high_temp_readings"),
        col("temperature").avg.alias("avg_high_temperature"),
        col("sensor_id").count.distinct.alias("sensors_with_high_temp")
    )
    print("Table after filtered tumbling window:")
    tumbling_filtered.print_schema()
    print("\nData after filtered tumbling window:")
    tumbling_filtered.execute().print()

    # 5. Tumbling Window with Multiple Time Intervals
    print("\n--- 5. Tumbling Window with Multiple Time Intervals ---")
    # 1-minute windows
    tumbling_1min = input_table.window(
        Tumble.over(lit(1).minutes).on(col("event_time")).alias("w")
    ).group_by(
        col("w")
    ).select(
        col("w").start.alias("window_start"),
        col("temperature").count.alias("readings_1min"),
        col("temperature").avg.alias("avg_temp_1min")
    )
    
    # 2-minute windows
    tumbling_2min = input_table.window(
        Tumble.over(lit(2).minutes).on(col("event_time")).alias("w")
    ).group_by(
        col("w")
    ).select(
        col("w").start.alias("window_start"),
        col("temperature").count.alias("readings_2min"),
        col("temperature").avg.alias("avg_temp_2min")
    )
    
    print("Table after 1-minute tumbling window:")
    tumbling_1min.print_schema()
    print("\nData after 1-minute tumbling window:")
    tumbling_1min.execute().print()
    
    print("\nTable after 2-minute tumbling window:")
    tumbling_2min.print_schema()
    print("\nData after 2-minute tumbling window:")
    tumbling_2min.execute().print()

    # 6. SQL-based Tumbling Windows
    print("\n--- 6. SQL-based Tumbling Windows ---")
    t_env.create_temporary_view("sensor_data", input_table)
    
    # Basic SQL tumbling window
    sql_tumbling = t_env.sql_query("""
        SELECT 
            TUMBLE_START(event_time, INTERVAL '3' MINUTE) as window_start,
            TUMBLE_END(event_time, INTERVAL '3' MINUTE) as window_end,
            COUNT(*) as reading_count,
            AVG(temperature) as avg_temperature,
            MIN(temperature) as min_temperature,
            MAX(temperature) as max_temperature
        FROM sensor_data
        GROUP BY TUMBLE(event_time, INTERVAL '3' MINUTE)
    """)
    print("Table from SQL tumbling window:")
    sql_tumbling.print_schema()
    print("\nData from SQL tumbling window:")
    sql_tumbling.execute().print()

    # SQL tumbling window with sensor grouping
    sql_grouped = t_env.sql_query("""
        SELECT 
            TUMBLE_START(event_time, INTERVAL '2' MINUTE) as window_start,
            TUMBLE_END(event_time, INTERVAL '2' MINUTE) as window_end,
            sensor_id,
            COUNT(*) as reading_count,
            AVG(temperature) as avg_temperature,
            STDDEV(temperature) as temperature_stddev
        FROM sensor_data
        GROUP BY TUMBLE(event_time, INTERVAL '2' MINUTE), sensor_id
        ORDER BY window_start, sensor_id
    """)
    print("Table from SQL grouped tumbling window:")
    sql_grouped.print_schema()
    print("\nData from SQL grouped tumbling window:")
    sql_grouped.execute().print()

    # 7. Tumbling Window with Time-based Conditions
    print("\n--- 7. Tumbling Window with Time-based Conditions ---")
    tumbling_time_condition = input_table.window(
        Tumble.over(lit(5).minutes).on(col("event_time")).alias("w")
    ).group_by(
        col("w")
    ).select(
        col("w").start.alias("window_start"),
        col("w").end.alias("window_end"),
        col("temperature").count.alias("total_readings"),
        col("temperature").avg.alias("avg_temperature"),
        # Count readings in specific time ranges within window
        col("temperature").filter(col("event_time").hour == 10).count.alias("readings_at_10am"),
        col("temperature").filter(col("event_time").minute >= 5).count.alias("readings_after_5min")
    )
    print("Table after time-conditioned tumbling window:")
    tumbling_time_condition.print_schema()
    print("\nData after time-conditioned tumbling window:")
    tumbling_time_condition.execute().print()

if __name__ == '__main__':
    recipe_14_tumbling_windows()
