# Recipe 2: Essential Data Transformations
from typing import List, Tuple
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes
from pyflink.table.expressions import col, lit, if_then_else

# Constants
DEFAULT_PARALLELISM = "1"
INPUT_DATA: List[Tuple[int, str, int, str]] = [
    (1, "sensor_a", 10, "LOW"),
    (2, "sensor_b", 15, "MEDIUM"),
    (3, "sensor_a", 20, "HIGH"),
    (4, "sensor_c", 12, "LOW")
]

ADDITIONAL_DATA: List[Tuple[int, str, int, str]] = [
    (5, "sensor_d", 18, "MEDIUM"),
    (6, "sensor_e", 25, "HIGH")
]

def recipe_02_essential_transformations() -> None:
    """
    Demonstrates essential data transformation operations in PyFlink.
    
    This function shows various transformation techniques:
    1. Selecting and applying expressions
    2. Filtering rows
    3. Aliasing columns
    4. Chaining operations
    5. Sorting (in batch mode)
    6. Limiting rows
    7. Removing duplicates
    8. Combining tables
    9. Conditional transformations
    """
    # Setup
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    t_env.get_config().set("parallelism.default", DEFAULT_PARALLELISM)

    print("Recipe 2: Essential Data Transformations")

    # Create a sample table
    input_table = t_env.from_elements(
        INPUT_DATA,
        DataTypes.ROW([
            DataTypes.FIELD("id", DataTypes.INT()), 
            DataTypes.FIELD("device_name", DataTypes.STRING()), 
            DataTypes.FIELD("reading", DataTypes.INT()), 
            DataTypes.FIELD("status", DataTypes.STRING())
        ])
    )
    print("Original Table Schema:")
    input_table.print_schema()
    print("\nOriginal Data:")
    input_table.execute().print()

    # 1. Select specific columns and apply expressions
    print("\n--- 1. Select and Expressions ---")
    selected_table = input_table.select(
        col("id"),
        col("device_name"),
        (col("reading") + 10).alias("reading_plus_10")
    )
    print("Selected Table Schema:")
    selected_table.print_schema()
    print("\nSelected Data:")
    selected_table.execute().print()

    # 2. Filter rows based on a condition
    print("\n--- 2. Filter ---")
    filtered_table = input_table.filter(
        (col("status") == "HIGH") | (col("reading") > 12)
    )
    print("Filtered Table Schema:")
    filtered_table.print_schema()
    print("\nFiltered Data:")
    filtered_table.execute().print()

    # 3. Alias (rename) columns
    print("\n--- 3. Alias ---")
    aliased_table = input_table.select(
        col("id"),
        col("device_name").alias("sensor_id"),
        col("reading").alias("value"),
        col("status")
    )
    print("Aliased Table Schema:")
    aliased_table.print_schema()

    # 4. Chaining operations and using built-in column functions
    print("\n--- 4. Chaining and Built-in Functions ---")
    transformed_table = input_table \
       .filter(col("device_name") == "sensor_a") \
       .select(
            col("id"),
            col("device_name").upper_case.alias("uppercase_device_name"),
            col("reading")
        )
    print("Transformed (Chained) Table Schema:")
    transformed_table.print_schema()
    print("\nTransformed (Chained) Data:")
    transformed_table.execute().print()

    # 5. Order By (sorting) only available in batch mode
    batch_env_settings = EnvironmentSettings.in_batch_mode()
    batch_t_env = TableEnvironment.create(batch_env_settings)

    batch_input_table = batch_t_env.from_elements(
        INPUT_DATA,
        DataTypes.ROW([
            DataTypes.FIELD("id", DataTypes.INT()), 
            DataTypes.FIELD("device_name", DataTypes.STRING()), 
            DataTypes.FIELD("reading", DataTypes.INT()), 
            DataTypes.FIELD("status", DataTypes.STRING())
        ])
    )

    print("\n--- 5. Order By ---")
    sorted_table = batch_input_table.order_by(col("reading").desc)
    print("Sorted Table Schema:")
    sorted_table.print_schema()
    print("\nSorted Data:")
    sorted_table.execute().print()

    # 6. Limit (restricting number of rows)
    print("\n--- 6. Limit ---")
    limited_table = input_table.limit(2)
    print("Limited Table Schema:")
    limited_table.print_schema()
    print("\nLimited Data:")
    limited_table.execute().print()

    # 7. Distinct (removing duplicates)
    print("\n--- 7. Distinct ---")
    distinct_table = input_table.select(col("status")).distinct()
    print("Distinct Table Schema:")
    distinct_table.print_schema()
    print("\nDistinct Data:")
    distinct_table.execute().print()

    # 8. Union (combining tables)
    print("\n--- 8. Union ---")
    additional_table = t_env.from_elements(
        ADDITIONAL_DATA,
        DataTypes.ROW([
            DataTypes.FIELD("id", DataTypes.INT()), 
            DataTypes.FIELD("device_name", DataTypes.STRING()), 
            DataTypes.FIELD("reading", DataTypes.INT()), 
            DataTypes.FIELD("status", DataTypes.STRING())
        ])
    )
    union_table = input_table.union_all(additional_table)
    print("Union Table Schema:")
    union_table.print_schema()
    print("\nUnion Data:")
    union_table.execute().print()

    # 9. Case/When (conditional transformations)
    print("\n--- 9. Case/When ---")
    case_table = input_table.select(
        col("id"),
        col("device_name"),
        col("reading"),
        col("status"),
        if_then_else(col("reading") > 15, "HIGH",
            if_then_else(col("reading") > 10, "MEDIUM", "LOW")).alias("reading_level")
    )
    print("If/Then/Else Table Schema:")
    case_table.print_schema()
    print("If/Then/Else Data:")
    case_table.execute().print()

if __name__ == '__main__':
    recipe_02_essential_transformations()