# Recipe 1: Creating Tables from Diverse Sources
from typing import List, Tuple, Dict
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, Row
from pyflink.table.expressions import col, lit
import pandas as pd

# Constants
DEFAULT_PARALLELISM = "1"
ELEMENTS_DATA: List[Tuple[int, str, int]] = [
    (1, "John", 85),
    (2, "Jane", 90),
    (3, "Jim", 78)
]

PANDAS_DATA: Dict[str, List] = {
    'sensor_id': ['s1', 's2', 's1', 's3'],
    'temperature': [22.5, 23.1, 22.8, 21.9],
    'humidity': [60.1, 65.5, 62.3, 58.0]
}

def recipe_01_creating_tables() -> None:
    """
    Demonstrates different ways to create tables in PyFlink.
    
    This function shows three main approaches:
    1. Creating tables from in-memory data using from_elements()
    2. Using the Data Generator (datagen) connector
    3. Creating tables from Pandas DataFrames
    """
    # Setup streaming environment
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    t_env.get_config().set("parallelism.default", DEFAULT_PARALLELISM)

    print("Recipe 1: Creating Tables from Diverse Sources")

    # 1. From In-Memory Data using from_elements()
    print("\n--- 1. From In-Memory Data (from_elements) ---")
    elements_data_implicit = ELEMENTS_DATA
    table_implicit_schema = t_env.from_elements(elements_data_implicit)
    print("Table with implicit schema (_1, _2, _3):")
    table_implicit_schema.print_schema()
    # table_implicit_schema.execute().print()

    elements_data_named = ELEMENTS_DATA
    table_named_cols = t_env.from_elements(elements_data_named, ['id', 'name', 'score'])
    print("\nTable with named columns (types inferred):")
    table_named_cols.print_schema()
    # table_named_cols.execute().print()

    elements_data_typed = ELEMENTS_DATA
    schema_explicit = DataTypes.ROW([DataTypes.FIELD("id", DataTypes.INT()), 
                                     DataTypes.FIELD("name", DataTypes.STRING()), 
                                     DataTypes.FIELD("score", DataTypes.INT())])
    table_explicit_schema = t_env.from_elements(elements_data_typed, schema_explicit)
    print("\nTable with explicit schema (DataTypes.ROW):")
    table_explicit_schema.print_schema()
    # table_explicit_schema.execute().print()

    # 2. Using the Data Generator (datagen) Connector
    print("\n--- 2. Using Data Generator (datagen) Connector ---")
    datagen_ddl = """
        CREATE TABLE generated_orders (
            order_id BIGINT,
            product_name STRING,
            order_time TIMESTAMP(3),
            quantity INT
        ) WITH (
            'connector' = 'datagen',
            'rows-per-second' = '1',
            'fields.order_id.kind' = 'sequence',
            'fields.order_id.start' = '1',
            'fields.order_id.end' = '5', -- Generate 5 orders for quick test
            'fields.product_name.length' = '5',
            'fields.quantity.min' = '1',
            'fields.quantity.max' = '10'
        )
    """
    t_env.execute_sql(datagen_ddl)
    table_datagen = t_env.from_path("generated_orders")
    print("Schema from DataGen source:")
    table_datagen.print_schema()
    # print("First 5 generated rows (if job is run):")
    # table_datagen.execute().print() # This will run indefinitely if end not specified or large

    # 3. From Pandas DataFrames using from_pandas()
    print("\n--- 3. From Pandas DataFrames (from_pandas) ---")
    pandas_df = pd.DataFrame(PANDAS_DATA)

    table_from_pandas = t_env.from_pandas(pandas_df)
    # Or with inferred schema and original column names:
    # table_from_pandas_inferred = t_env.from_pandas(pandas_df)
    print("Schema from Pandas DataFrame:")
    table_from_pandas.print_schema()
    # table_from_pandas.execute().print()

if __name__ == '__main__':
    recipe_01_creating_tables()