# Recipe 15: Sliding Window Aggregations
from typing import List, Tuple
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, Table
from pyflink.table.expressions import col, lit
from pyflink.table.window import Slide
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
    (12, "sensor_a", 22.4, datetime(2023, 1, 1, 10, 11, 0)),
    (13, "sensor_c", 22.6, datetime(2023, 1, 1, 10, 12, 0)),
    (14, "sensor_b", 23.2, datetime(2023, 1, 1, 10, 13, 0)),
    (15, "sensor_a", 22.7, datetime(2023, 1, 1, 10, 14, 0))
]

def recipe_15_sliding_windows() -> None:
    """
    Demonstrates sliding window aggregations in PyFlink.
    
    This function shows:
    1. Creating sliding windows with different sizes and slide intervals
    2. Basic aggregations over sliding windows
    3. Grouping by sensor within sliding windows
    4. Window start and end time extraction
    5. Complex aggregations with multiple functions
    6. SQL-based sliding window operations
    7. Comparing different slide intervals
    """
    # Setup
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    t_env.get_config().set("parallelism.default", DEFAULT_PARALLELISM)

    print("Recipe 15: Sliding Window Aggregations")

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

    # 1. Basic Sliding Window (5-minute window, 2-minute slide)
    print("\n--- 1. Basic Sliding Window (5-minute window, 2-minute slide) ---")
    sliding_basic = input_table.window(
        Slide.over(lit(5).minutes).every(lit(2).minutes).on(col("event_time")).alias("w")
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
    print("Table after basic sliding window:")
    sliding_basic.print_schema()
    print("\nData after basic sliding window:")
    sliding_basic.execute().print()

    # 2. Sliding Window with Sensor Grouping
    print("\n--- 2. Sliding Window with Sensor Grouping ---")
    sliding_grouped = input_table.window(
        Slide.over(lit(4).minutes).every(lit(2).minutes).on(col("event_time")).alias("w")
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
    print("Table after grouped sliding window:")
    sliding_grouped.print_schema()
    print("\nData after grouped sliding window:")
    sliding_grouped.execute().print()

    # 3. Sliding Window with Complex Aggregations
    print("\n--- 3. Sliding Window with Complex Aggregations ---")
    sliding_complex = input_table.window(
        Slide.over(lit(6).minutes).every(lit(3).minutes).on(col("event_time")).alias("w")
    ).group_by(
        col("w")
    ).select(
        col("w").start.alias("window_start"),
        col("w").end.alias("window_end"),
        col("temperature").count.alias("total_readings"),
        col("sensor_id").count.distinct.alias("unique_sensors"),
        col("temperature").avg.alias("avg_temperature"),
        col("temperature").stddev.alias("temperature_stddev"),
        (col("temperature").max - col("temperature").min).alias("temperature_range"),
        col("temperature").sum.alias("temperature_sum")
    )
    print("Table after complex sliding window:")
    sliding_complex.print_schema()
    print("\nData after complex sliding window:")
    sliding_complex.execute().print()

    # 4. Sliding Window with Filtering
    print("\n--- 4. Sliding Window with Filtering ---")
    sliding_filtered = input_table.filter(
        col("temperature") > 22.0
    ).window(
        Slide.over(lit(5).minutes).every(lit(2).minutes).on(col("event_time")).alias("w")
    ).group_by(
        col("w")
    ).select(
        col("w").start.alias("window_start"),
        col("w").end.alias("window_end"),
        col("temperature").count.alias("high_temp_readings"),
        col("temperature").avg.alias("avg_high_temperature"),
        col("sensor_id").count.distinct.alias("sensors_with_high_temp")
    )
    print("Table after filtered sliding window:")
    sliding_filtered.print_schema()
    print("\nData after filtered sliding window:")
    sliding_filtered.execute().print()

    # 5. Different Slide Intervals Comparison
    print("\n--- 5. Different Slide Intervals Comparison ---")
    # 3-minute window, 1-minute slide (more frequent updates)
    sliding_3_1 = input_table.window(
        Slide.over(lit(3).minutes).every(lit(1).minutes).on(col("event_time")).alias("w")
    ).group_by(
        col("w")
    ).select(
        col("w").start.alias("window_start"),
        col("temperature").count.alias("readings_3min_1min_slide"),
        col("temperature").avg.alias("avg_temp_3min_1min_slide")
    )
    
    # 3-minute window, 3-minute slide (same as tumbling)
    sliding_3_3 = input_table.window(
        Slide.over(lit(3).minutes).every(lit(3).minutes).on(col("event_time")).alias("w")
    ).group_by(
        col("w")
    ).select(
        col("w").start.alias("window_start"),
        col("temperature").count.alias("readings_3min_3min_slide"),
        col("temperature").avg.alias("avg_temp_3min_3min_slide")
    )
    
    print("Table after 3-minute window, 1-minute slide:")
    sliding_3_1.print_schema()
    print("\nData after 3-minute window, 1-minute slide:")
    sliding_3_1.execute().print()
    
    print("\nTable after 3-minute window, 3-minute slide (tumbling equivalent):")
    sliding_3_3.print_schema()
    print("\nData after 3-minute window, 3-minute slide (tumbling equivalent):")
    sliding_3_3.execute().print()

    # 6. SQL-based Sliding Windows
    print("\n--- 6. SQL-based Sliding Windows ---")
    t_env.create_temporary_view("sensor_data", input_table)
    
    # Basic SQL sliding window
    sql_sliding = t_env.sql_query("""
        SELECT 
            HOP_START(event_time, INTERVAL '2' MINUTE, INTERVAL '5' MINUTE) as window_start,
            HOP_END(event_time, INTERVAL '2' MINUTE, INTERVAL '5' MINUTE) as window_end,
            COUNT(*) as reading_count,
            AVG(temperature) as avg_temperature,
            MIN(temperature) as min_temperature,
            MAX(temperature) as max_temperature
        FROM sensor_data
        GROUP BY HOP(event_time, INTERVAL '2' MINUTE, INTERVAL '5' MINUTE)
        ORDER BY window_start
    """)
    print("Table from SQL sliding window:")
    sql_sliding.print_schema()
    print("\nData from SQL sliding window:")
    sql_sliding.execute().print()

    # SQL sliding window with sensor grouping
    sql_grouped = t_env.sql_query("""
        SELECT 
            HOP_START(event_time, INTERVAL '2' MINUTE, INTERVAL '4' MINUTE) as window_start,
            HOP_END(event_time, INTERVAL '2' MINUTE, INTERVAL '4' MINUTE) as window_end,
            sensor_id,
            COUNT(*) as reading_count,
            AVG(temperature) as avg_temperature,
            STDDEV(temperature) as temperature_stddev
        FROM sensor_data
        GROUP BY HOP(event_time, INTERVAL '2' MINUTE, INTERVAL '4' MINUTE), sensor_id
        ORDER BY window_start, sensor_id
    """)
    print("Table from SQL grouped sliding window:")
    sql_grouped.print_schema()
    print("\nData from SQL grouped sliding window:")
    sql_grouped.execute().print()

    # 7. Sliding Window with Time-based Conditions
    print("\n--- 7. Sliding Window with Time-based Conditions ---")
    sliding_time_condition = input_table.window(
        Slide.over(lit(5).minutes).every(lit(2).minutes).on(col("event_time")).alias("w")
    ).group_by(
        col("w")
    ).select(
        col("w").start.alias("window_start"),
        col("w").end.alias("window_end"),
        col("temperature").count.alias("total_readings"),
        col("temperature").avg.alias("avg_temperature"),
        # Count readings in specific time ranges within window
        col("temperature").filter(col("event_time").minute % 2 == 0).count.alias("even_minute_readings"),
        col("temperature").filter(col("event_time").minute % 2 == 1).count.alias("odd_minute_readings")
    )
    print("Table after time-conditioned sliding window:")
    sliding_time_condition.print_schema()
    print("\nData after time-conditioned sliding window:")
    sliding_time_condition.execute().print()

    # 8. Sliding Window with Overlapping Analysis
    print("\n--- 8. Sliding Window with Overlapping Analysis ---")
    sliding_overlap = input_table.window(
        Slide.over(lit(4).minutes).every(lit(1).minutes).on(col("event_time")).alias("w")
    ).group_by(
        col("w")
    ).select(
        col("w").start.alias("window_start"),
        col("w").end.alias("window_end"),
        col("temperature").count.alias("readings_in_window"),
        col("temperature").avg.alias("avg_temperature"),
        # Calculate overlap percentage (simplified)
        (col("temperature").count / lit(4.0)).alias("avg_readings_per_minute")
    )
    print("Table after overlapping sliding window analysis:")
    sliding_overlap.print_schema()
    print("\nData after overlapping sliding window analysis:")
    sliding_overlap.execute().print()

if __name__ == '__main__':
    recipe_15_sliding_windows()
