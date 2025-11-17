# Recipe 16: Session Window Aggregations
from typing import List, Tuple
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, Table
from pyflink.table.expressions import col, lit
from pyflink.table.window import Session
from datetime import datetime, timedelta

# Constants
DEFAULT_PARALLELISM = "1"
SAMPLE_DATA: List[Tuple[int, str, float, datetime]] = [
    # Session 1: User A activity (10:00-10:05)
    (1, "user_a", 22.5, datetime(2023, 1, 1, 10, 0, 0)),
    (2, "user_a", 23.1, datetime(2023, 1, 1, 10, 1, 0)),
    (3, "user_a", 22.8, datetime(2023, 1, 1, 10, 2, 0)),
    (4, "user_a", 21.9, datetime(2023, 1, 1, 10, 3, 0)),
    (5, "user_a", 23.5, datetime(2023, 1, 1, 10, 4, 0)),
    (6, "user_a", 22.2, datetime(2023, 1, 1, 10, 5, 0)),
    
    # Gap of 8 minutes (10:05-10:13) - new session starts
    
    # Session 2: User A activity (10:13-10:16)
    (7, "user_a", 22.0, datetime(2023, 1, 1, 10, 13, 0)),
    (8, "user_a", 23.8, datetime(2023, 1, 1, 10, 14, 0)),
    (9, "user_a", 22.9, datetime(2023, 1, 1, 10, 15, 0)),
    (10, "user_a", 21.7, datetime(2023, 1, 1, 10, 16, 0)),
    
    # Session 3: User B activity (10:00-10:08)
    (11, "user_b", 24.1, datetime(2023, 1, 1, 10, 0, 0)),
    (12, "user_b", 22.4, datetime(2023, 1, 1, 10, 2, 0)),
    (13, "user_b", 23.2, datetime(2023, 1, 1, 10, 4, 0)),
    (14, "user_b", 22.6, datetime(2023, 1, 1, 10, 6, 0)),
    (15, "user_b", 23.0, datetime(2023, 1, 1, 10, 8, 0)),
    
    # Gap of 7 minutes (10:08-10:15) - new session starts
    
    # Session 4: User B activity (10:15-10:18)
    (16, "user_b", 22.7, datetime(2023, 1, 1, 10, 15, 0)),
    (17, "user_b", 23.4, datetime(2023, 1, 1, 10, 16, 0)),
    (18, "user_b", 22.1, datetime(2023, 1, 1, 10, 17, 0)),
    (19, "user_b", 23.9, datetime(2023, 1, 1, 10, 18, 0)),
    
    # Session 5: User C activity (10:05-10:12)
    (20, "user_c", 21.5, datetime(2023, 1, 1, 10, 5, 0)),
    (21, "user_c", 22.3, datetime(2023, 1, 1, 10, 7, 0)),
    (22, "user_c", 21.8, datetime(2023, 1, 1, 10, 9, 0)),
    (23, "user_c", 22.9, datetime(2023, 1, 1, 10, 11, 0)),
    (24, "user_c", 21.6, datetime(2023, 1, 1, 10, 12, 0))
]

def recipe_16_session_windows() -> None:
    """
    Demonstrates session window aggregations in PyFlink.
    
    This function shows:
    1. Creating session windows with different gap intervals
    2. Basic aggregations over session windows
    3. Grouping by user within session windows
    4. Session start and end time extraction
    5. Session duration calculations
    6. SQL-based session window operations
    7. Analyzing user activity patterns
    """
    # Setup
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    t_env.get_config().set("parallelism.default", DEFAULT_PARALLELISM)

    print("Recipe 16: Session Window Aggregations")

    # Create sample table with timestamp
    input_table = t_env.from_elements(
        SAMPLE_DATA,
        DataTypes.ROW([
            DataTypes.FIELD("id", DataTypes.INT()),
            DataTypes.FIELD("user_id", DataTypes.STRING()),
            DataTypes.FIELD("temperature", DataTypes.DOUBLE()),
            DataTypes.FIELD("event_time", DataTypes.TIMESTAMP(3))
        ])
    )
    print("Input Table:")
    input_table.print_schema()
    print("\nInput Data:")
    input_table.execute().print()

    # 1. Basic Session Window (5-minute gap)
    print("\n--- 1. Basic Session Window (5-minute gap) ---")
    session_basic = input_table.window(
        Session.with_gap(lit(5).minutes).on(col("event_time")).alias("w")
    ).group_by(
        col("w")
    ).select(
        col("w").start.alias("session_start"),
        col("w").end.alias("session_end"),
        col("temperature").count.alias("reading_count"),
        col("temperature").avg.alias("avg_temperature"),
        col("temperature").min.alias("min_temperature"),
        col("temperature").max.alias("max_temperature")
    )
    print("Table after basic session window:")
    session_basic.print_schema()
    print("\nData after basic session window:")
    session_basic.execute().print()

    # 2. Session Window with User Grouping
    print("\n--- 2. Session Window with User Grouping ---")
    session_grouped = input_table.window(
        Session.with_gap(lit(5).minutes).on(col("event_time")).alias("w")
    ).group_by(
        col("w"),
        col("user_id")
    ).select(
        col("w").start.alias("session_start"),
        col("w").end.alias("session_end"),
        col("user_id"),
        col("temperature").count.alias("reading_count"),
        col("temperature").avg.alias("avg_temperature"),
        col("temperature").min.alias("min_temperature"),
        col("temperature").max.alias("max_temperature")
    )
    print("Table after grouped session window:")
    session_grouped.print_schema()
    print("\nData after grouped session window:")
    session_grouped.execute().print()

    # 3. Session Window with Duration Analysis
    print("\n--- 3. Session Window with Duration Analysis ---")
    session_duration = input_table.window(
        Session.with_gap(lit(5).minutes).on(col("event_time")).alias("w")
    ).group_by(
        col("w")
    ).select(
        col("w").start.alias("session_start"),
        col("w").end.alias("session_end"),
        col("temperature").count.alias("total_readings"),
        col("user_id").count.distinct.alias("unique_users"),
        col("temperature").avg.alias("avg_temperature"),
        # Calculate session duration in minutes
        ((col("w").end.cast(DataTypes.BIGINT()) - col("w").start.cast(DataTypes.BIGINT())) / 60000).alias("session_duration_minutes")
    )
    print("Table after session duration analysis:")
    session_duration.print_schema()
    print("\nData after session duration analysis:")
    session_duration.execute().print()

    # 4. Session Window with Different Gap Intervals
    print("\n--- 4. Session Window with Different Gap Intervals ---")
    # 3-minute gap
    session_3min = input_table.window(
        Session.with_gap(lit(3).minutes).on(col("event_time")).alias("w")
    ).group_by(
        col("w")
    ).select(
        col("w").start.alias("session_start"),
        col("temperature").count.alias("readings_3min_gap"),
        col("temperature").avg.alias("avg_temp_3min_gap")
    )
    
    # 7-minute gap
    session_7min = input_table.window(
        Session.with_gap(lit(7).minutes).on(col("event_time")).alias("w")
    ).group_by(
        col("w")
    ).select(
        col("w").start.alias("session_start"),
        col("temperature").count.alias("readings_7min_gap"),
        col("temperature").avg.alias("avg_temp_7min_gap")
    )
    
    print("Table after 3-minute gap session window:")
    session_3min.print_schema()
    print("\nData after 3-minute gap session window:")
    session_3min.execute().print()
    
    print("\nTable after 7-minute gap session window:")
    session_7min.print_schema()
    print("\nData after 7-minute gap session window:")
    session_7min.execute().print()

    # 5. Session Window with User Activity Analysis
    print("\n--- 5. Session Window with User Activity Analysis ---")
    session_activity = input_table.window(
        Session.with_gap(lit(5).minutes).on(col("event_time")).alias("w")
    ).group_by(
        col("w"),
        col("user_id")
    ).select(
        col("w").start.alias("session_start"),
        col("w").end.alias("session_end"),
        col("user_id"),
        col("temperature").count.alias("readings_in_session"),
        col("temperature").avg.alias("avg_temperature"),
        # Calculate session duration for each user
        ((col("w").end.cast(DataTypes.BIGINT()) - col("w").start.cast(DataTypes.BIGINT())) / 60000).alias("session_duration_minutes"),
        # Calculate reading frequency (readings per minute)
        (col("temperature").count / ((col("w").end.cast(DataTypes.BIGINT()) - col("w").start.cast(DataTypes.BIGINT())) / 60000)).alias("readings_per_minute")
    )
    print("Table after user activity analysis:")
    session_activity.print_schema()
    print("\nData after user activity analysis:")
    session_activity.execute().print()

    # 6. SQL-based Session Windows
    print("\n--- 6. SQL-based Session Windows ---")
    t_env.create_temporary_view("user_activity", input_table)
    
    # Basic SQL session window
    sql_session = t_env.sql_query("""
        SELECT 
            SESSION_START(event_time, INTERVAL '5' MINUTE) as session_start,
            SESSION_END(event_time, INTERVAL '5' MINUTE) as session_end,
            COUNT(*) as reading_count,
            AVG(temperature) as avg_temperature,
            MIN(temperature) as min_temperature,
            MAX(temperature) as max_temperature
        FROM user_activity
        GROUP BY SESSION(event_time, INTERVAL '5' MINUTE)
        ORDER BY session_start
    """)
    print("Table from SQL session window:")
    sql_session.print_schema()
    print("\nData from SQL session window:")
    sql_session.execute().print()

    # SQL session window with user grouping
    sql_grouped = t_env.sql_query("""
        SELECT 
            SESSION_START(event_time, INTERVAL '5' MINUTE) as session_start,
            SESSION_END(event_time, INTERVAL '5' MINUTE) as session_end,
            user_id,
            COUNT(*) as reading_count,
            AVG(temperature) as avg_temperature,
            (SESSION_END(event_time, INTERVAL '5' MINUTE) - SESSION_START(event_time, INTERVAL '5' MINUTE)) / 60000 as session_duration_minutes
        FROM user_activity
        GROUP BY SESSION(event_time, INTERVAL '5' MINUTE), user_id
        ORDER BY session_start, user_id
    """)
    print("Table from SQL grouped session window:")
    sql_grouped.print_schema()
    print("\nData from SQL grouped session window:")
    sql_grouped.execute().print()

    # 7. Session Window with Filtering
    print("\n--- 7. Session Window with Filtering ---")
    session_filtered = input_table.filter(
        col("temperature") > 22.0
    ).window(
        Session.with_gap(lit(5).minutes).on(col("event_time")).alias("w")
    ).group_by(
        col("w")
    ).select(
        col("w").start.alias("session_start"),
        col("w").end.alias("session_end"),
        col("temperature").count.alias("high_temp_readings"),
        col("temperature").avg.alias("avg_high_temperature"),
        col("user_id").count.distinct.alias("users_with_high_temp")
    )
    print("Table after filtered session window:")
    session_filtered.print_schema()
    print("\nData after filtered session window:")
    session_filtered.execute().print()

    # 8. Session Window with Time-based Analysis
    print("\n--- 8. Session Window with Time-based Analysis ---")
    session_time_analysis = input_table.window(
        Session.with_gap(lit(5).minutes).on(col("event_time")).alias("w")
    ).group_by(
        col("w")
    ).select(
        col("w").start.alias("session_start"),
        col("w").end.alias("session_end"),
        col("temperature").count.alias("total_readings"),
        col("temperature").avg.alias("avg_temperature"),
        # Analyze activity patterns within sessions
        col("temperature").filter(col("event_time").minute % 2 == 0).count.alias("even_minute_readings"),
        col("temperature").filter(col("event_time").minute % 2 == 1).count.alias("odd_minute_readings"),
        # Calculate session intensity
        (col("temperature").count / ((col("w").end.cast(DataTypes.BIGINT()) - col("w").start.cast(DataTypes.BIGINT())) / 60000)).alias("readings_per_minute")
    )
    print("Table after time-based session analysis:")
    session_time_analysis.print_schema()
    print("\nData after time-based session analysis:")
    session_time_analysis.execute().print()

if __name__ == '__main__':
    recipe_16_session_windows()
