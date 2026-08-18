# Recipe 4: Deduplication
from typing import List, Tuple
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, Table
from pyflink.table.expressions import col

# Constants
DEFAULT_PARALLELISM = "1"
EVENT_DATA: List[Tuple[int, str, int]] = [
    (1, "click", 10),
    (2, "view", 5),
    (1, "click", 10),  # Duplicate of first click
    (2, "purchase", 15),
    (3, "click", 10),
    (1, "click", 10)   # Another duplicate
]

def recipe_04_deduplication() -> None:
    """
    Demonstrates basic deduplication techniques in PyFlink.
    
    This function shows:
    1. Basic distinct() on all columns
    2. Distinct on selected columns
    3. Distinct with value aggregation
    """
    # Setup
    env_settings: EnvironmentSettings = EnvironmentSettings.in_streaming_mode()
    t_env: TableEnvironment = TableEnvironment.create(env_settings)
    t_env.get_config().set("parallelism.default", DEFAULT_PARALLELISM)

    print("Recipe 4: Deduplication")

    # Create table
    event_table: Table = t_env.from_elements(
        EVENT_DATA,
        DataTypes.ROW([
            DataTypes.FIELD("id", DataTypes.INT()),
            DataTypes.FIELD("event_type", DataTypes.STRING()),
            DataTypes.FIELD("value", DataTypes.INT())
        ])
    )
    print("Original Event Table Schema:")
    event_table.print_schema()
    print("\nOriginal Event Data:")
    event_table.execute().print()

    # 1. Basic distinct() on all columns
    print("\n--- 1. Basic Distinct on all columns ---")
    distinct_all_cols_table: Table = event_table.distinct()
    print("Table with distinct rows (all columns considered):")
    distinct_all_cols_table.print_schema()
    print("\nDistinct Data (all columns):")
    distinct_all_cols_table.execute().print()

    # 2. Distinct on selected columns
    print("\n--- 2. Distinct on selected columns ---")
    distinct_selected_cols_table: Table = event_table \
        .select(col("id"), col("event_type")) \
        .distinct()
    print("Table with distinct combinations of 'id' and 'event_type':")
    distinct_selected_cols_table.print_schema()
    print("\nDistinct Data (selected columns):")
    distinct_selected_cols_table.execute().print()

    # 3. Distinct with value aggregation
    print("\n--- 3. Distinct with value aggregation ---")
    distinct_with_agg_table: Table = event_table \
        .group_by(col("id"), col("event_type")) \
        .select(
            col("id"),
            col("event_type"),
            col("value").max.alias("max_value")
        )
    print("Table with distinct id/event_type and max value:")
    distinct_with_agg_table.print_schema()
    print("\nDistinct with aggregation results:")
    distinct_with_agg_table.execute().print()

if __name__ == '__main__':
    recipe_04_deduplication()