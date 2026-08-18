# Recipe 5: Custom Logic with Scalar UDFs
from typing import List, Tuple
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, Table
from pyflink.table.udf import udf, ScalarFunction
from pyflink.table.expressions import col

# Constants
DEFAULT_PARALLELISM = "1"
SAMPLE_DATA: List[Tuple[int, str, int]] = [
    (1, "hello pyflink", 100),
    (2, "test udf", -50),
    (3, None, 200),
    (4, "another example", None)
]

# 1. Define a Scalar UDF using a Python function and @udf decorator
@udf(result_type=DataTypes.STRING())
def to_uppercase_udf(s: str) -> str:
    if s is None:
        return None
    return s.upper()

# 2. Define a Scalar UDF using a lambda function
add_value_udf = udf(lambda i, j: i + j if i is not None and j is not None else None,
                      input_types=[DataTypes.INT(), DataTypes.INT()],
                      result_type=DataTypes.BIGINT())

# 3. Define a Scalar UDF using a class
class IsPositiveUDF(ScalarFunction):
    def eval(self, num: int) -> bool:
        if num is None:
            return False
        return num > 0

is_positive_udf = udf(IsPositiveUDF(), result_type=DataTypes.BOOLEAN())

def recipe_05_scalar_udfs() -> None:
    """
    Demonstrates how to create and use Scalar User-Defined Functions (UDFs) in PyFlink.
    
    This function shows:
    1. Different ways to define UDFs (function decorator, lambda, class-based)
    2. Using UDFs in SELECT statements
    3. Using UDFs in FILTER conditions
    4. Using UDFs in SQL queries
    """
    # Setup
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    t_env.get_config().set("parallelism.default", DEFAULT_PARALLELISM)

    print("Recipe 5: Custom Logic with Scalar UDFs")

    # Create sample table
    input_table = t_env.from_elements(
        SAMPLE_DATA,
        DataTypes.ROW([
            DataTypes.FIELD("id", DataTypes.INT()),
            DataTypes.FIELD("text_data", DataTypes.STRING()),
            DataTypes.FIELD("numeric_data", DataTypes.INT())
        ])
    )
    print("Input Table:")
    input_table.print_schema()
    print("\nInput Data:")
    input_table.execute().print()

    # 1. Use the UDFs in Table API select expressions
    print("\n--- 1. Using UDFs in SELECT ---")
    transformed_table_select = input_table.select(
        col("id"),
        col("text_data"),
        to_uppercase_udf(col("text_data")).alias("uppercase_text_api"),
        to_uppercase_udf(col("text_data")).alias("uppercase_text_sql"),
        add_value_udf(col("id"), col("numeric_data")).alias("id_plus_numeric")
    )
    print("Table after applying TO_UPPER and ADD_VAL UDFs in select:")
    transformed_table_select.print_schema()
    print("\nData after SELECT UDFs:")
    transformed_table_select.execute().print()

    # 2. Use the UDF in Table API filter expressions
    print("\n--- 2. Using UDFs in FILTER ---")
    filtered_table_udf = input_table.filter(
        is_positive_udf(col("numeric_data"))
    )
    print("Table after filtering with IS_POSITIVE UDF:")
    filtered_table_udf.print_schema()
    print("\nData after FILTER UDF:")
    filtered_table_udf.execute().print()

    # 3. Example of using UDF in SQL
    print("\n--- 3. Using UDFs in SQL ---")
    # Register the UDFs for SQL
    t_env.create_temporary_function("IS_POSITIVE_API", is_positive_udf)
    t_env.create_temporary_function("TO_UPPER_SQL", to_uppercase_udf)

    # Create a view to query with SQL
    t_env.create_temporary_view("my_table_for_sql", input_table)
    sql_result_table = t_env.sql_query(
        "SELECT id, TO_UPPER_SQL(text_data) AS sql_upper FROM my_table_for_sql WHERE IS_POSITIVE_API(numeric_data)"
    )
    print("Table from SQL query using UDFs:")
    sql_result_table.print_schema()
    print("\nData from SQL UDF query:")
    sql_result_table.execute().print()

if __name__ == '__main__':
    recipe_05_scalar_udfs()