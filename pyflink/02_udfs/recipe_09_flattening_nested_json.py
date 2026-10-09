# Recipe 9: Flattening Nested JSON with UDFs
from typing import List, Tuple, Dict, Any
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, Table, Row
from pyflink.table.udf import udf, ScalarFunction, udtf, TableFunction
from pyflink.table.expressions import col, call
import json

# Constants
DEFAULT_PARALLELISM = "1"
SAMPLE_DATA: List[Tuple[int, str]] = [
    (1, '{"user": {"id": 123, "name": "John", "address": {"street": "123 Main St", "city": "New York", "zip": "10001"}}, "orders": [{"id": "O1", "amount": 100.50}, {"id": "O2", "amount": 75.25}]}'),
    (2, '{"user": {"id": 456, "name": "Jane", "address": {"street": "456 Oak Ave", "city": "Los Angeles", "zip": "90210"}}, "orders": [{"id": "O3", "amount": 200.00}]}'),
    (3, '{"user": {"id": 789, "name": "Bob", "address": {"street": "789 Pine Rd", "city": "Chicago", "zip": "60601"}}, "orders": []}'),
    (4, '{"user": {"id": 101, "name": "Alice", "address": {"street": "101 Elm St", "city": "Boston", "zip": "02101"}}, "orders": [{"id": "O4", "amount": 150.75}, {"id": "O5", "amount": 300.00}, {"id": "O6", "amount": 50.25}]}'),
    (5, None)
]

# 1. Flatten User Information UDF
@udf(result_type=DataTypes.STRING())
def flatten_user_info(json_str: str) -> str:
    """Extract and flatten user information from nested JSON"""
    if json_str is None:
        return None
    try:
        data = json.loads(json_str)
        user = data.get('user', {})
        
        flattened = {
            "user_id": user.get('id'),
            "user_name": user.get('name'),
            "street": user.get('address', {}).get('street'),
            "city": user.get('address', {}).get('city'),
            "zip_code": user.get('address', {}).get('zip')
        }
        return json.dumps(flattened)
    except (json.JSONDecodeError, TypeError):
        return None

# 2. Extract Order Count UDF
@udf(result_type=DataTypes.INT())
def get_order_count(json_str: str) -> int:
    """Get the number of orders from nested JSON"""
    if json_str is None:
        return 0
    try:
        data = json.loads(json_str)
        orders = data.get('orders', [])
        return len(orders)
    except (json.JSONDecodeError, TypeError):
        return 0

# 3. Calculate Total Order Amount UDF
@udf(result_type=DataTypes.DOUBLE())
def calculate_total_amount(json_str: str) -> float:
    """Calculate total amount from all orders"""
    if json_str is None:
        return 0.0
    try:
        data = json.loads(json_str)
        orders = data.get('orders', [])
        total = sum(order.get('amount', 0) for order in orders)
        return total
    except (json.JSONDecodeError, TypeError):
        return 0.0

# 4. Extract Order IDs UDF
@udf(result_type=DataTypes.STRING())
def extract_order_ids(json_str: str) -> str:
    """Extract all order IDs as a comma-separated string"""
    if json_str is None:
        return None
    try:
        data = json.loads(json_str)
        orders = data.get('orders', [])
        order_ids = [order.get('id', '') for order in orders if order.get('id')]
        return ','.join(order_ids) if order_ids else None
    except (json.JSONDecodeError, TypeError):
        return None

# 5. Complex Flattening UDF
class ComplexFlatteningUDF(ScalarFunction):
    """Complex flattening that handles multiple nested levels"""
    
    def eval(self, json_str: str) -> str:
        if json_str is None:
            return None
        try:
            data = json.loads(json_str)
            
            # Extract user info
            user = data.get('user', {})
            address = user.get('address', {})
            
            # Extract order summary
            orders = data.get('orders', [])
            order_count = len(orders)
            total_amount = sum(order.get('amount', 0) for order in orders)
            avg_amount = total_amount / order_count if order_count > 0 else 0
            
            flattened = {
                "user_id": user.get('id'),
                "user_name": user.get('name'),
                "full_address": f"{address.get('street', '')}, {address.get('city', '')} {address.get('zip', '')}",
                "order_count": order_count,
                "total_amount": round(total_amount, 2),
                "average_order_amount": round(avg_amount, 2),
                "has_orders": order_count > 0
            }
            return json.dumps(flattened)
        except (json.JSONDecodeError, TypeError, ZeroDivisionError):
            return None

complex_flattening_udf = udf(ComplexFlatteningUDF(), result_type=DataTypes.STRING())

# 6. Unnest Table Function (UDTF) - Recreating Java API unnest
class UnnestArrayUDF(TableFunction):
    """Table function to unnest arrays from JSON, similar to Java API unnest"""
    
    def eval(self, json_str: str, array_path: str):
        if json_str is None or array_path is None:
            return
        
        try:
            data = json.loads(json_str)
            
            # Navigate to the array using dot notation (e.g., "orders", "user.addresses")
            current = data
            for key in array_path.split('.'):
                if isinstance(current, dict) and key in current:
                    current = current[key]
                else:
                    return  # Path not found
            
            # Ensure we have an array
            if not isinstance(current, list):
                return
            
            # Yield each element in the array
            for i, item in enumerate(current):
                if isinstance(item, dict):
                    # For objects, yield with index
                    yield Row(i, json.dumps(item))
                else:
                    # For primitives, yield with index
                    yield Row(i, str(item))
                    
        except (json.JSONDecodeError, TypeError, KeyError):
            return

# Define result type for the unnest UDTF
unnest_result_type = DataTypes.ROW([
    DataTypes.FIELD("array_index", DataTypes.INT()),
    DataTypes.FIELD("array_element", DataTypes.STRING())
])

unnest_array_udtf = udtf(UnnestArrayUDF(), result_types=unnest_result_type)

# 7. Unnest with Position UDF
@udtf(result_types=[DataTypes.INT(), DataTypes.STRING(), DataTypes.INT()])
def unnest_with_position(json_str: str, array_path: str):
    """Unnest arrays with position information"""
    if json_str is None or array_path is None:
        return
    
    try:
        data = json.loads(json_str)
        
        # Navigate to the array
        current = data
        for key in array_path.split('.'):
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return
        
        if not isinstance(current, list):
            return
        
        # Yield each element with position
        for i, item in enumerate(current):
            if isinstance(item, dict):
                yield i, json.dumps(item), len(current)
            else:
                yield i, str(item), len(current)
                
    except (json.JSONDecodeError, TypeError, KeyError):
        return

def recipe_09_flattening_nested_json() -> None:
    """
    Demonstrates flattening nested JSON structures using UDFs in PyFlink.
    
    This function shows:
    1. Extracting and flattening user information from nested objects
    2. Counting elements in nested arrays
    3. Aggregating values from nested arrays
    4. Extracting specific fields from nested structures
    5. Complex flattening with multiple nested levels
    6. Unnesting arrays using table functions (similar to Java API unnest)
    7. Unnesting with position information
    """
    # Setup
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    t_env.get_config().set("parallelism.default", DEFAULT_PARALLELISM)

    print("Recipe 9: Flattening Nested JSON with UDFs")

    # Create sample table
    input_table = t_env.from_elements(
        SAMPLE_DATA,
        DataTypes.ROW([
            DataTypes.FIELD("id", DataTypes.INT()),
            DataTypes.FIELD("nested_json", DataTypes.STRING())
        ])
    )
    print("Input Table:")
    input_table.print_schema()
    print("\nInput Data:")
    input_table.execute().print()

    # 1. Flatten User Information
    print("\n--- 1. Flatten User Information ---")
    user_result = input_table.select(
        col("id"),
        col("nested_json"),
        flatten_user_info(col("nested_json")).alias("flattened_user")
    )
    print("Table after flattening user info:")
    user_result.print_schema()
    print("\nData after flattening user info:")
    user_result.execute().print()

    # 2. Extract Order Count
    print("\n--- 2. Extract Order Count ---")
    count_result = input_table.select(
        col("id"),
        col("nested_json"),
        get_order_count(col("nested_json")).alias("order_count")
    )
    print("Table after extracting order count:")
    count_result.print_schema()
    print("\nData after extracting order count:")
    count_result.execute().print()

    # 3. Calculate Total Amount
    print("\n--- 3. Calculate Total Amount ---")
    amount_result = input_table.select(
        col("id"),
        col("nested_json"),
        calculate_total_amount(col("nested_json")).alias("total_amount")
    )
    print("Table after calculating total amount:")
    amount_result.print_schema()
    print("\nData after calculating total amount:")
    amount_result.execute().print()

    # 4. Extract Order IDs
    print("\n--- 4. Extract Order IDs ---")
    order_ids_result = input_table.select(
        col("id"),
        col("nested_json"),
        extract_order_ids(col("nested_json")).alias("order_ids")
    )
    print("Table after extracting order IDs:")
    order_ids_result.print_schema()
    print("\nData after extracting order IDs:")
    order_ids_result.execute().print()

    # 5. Complex Flattening
    print("\n--- 5. Complex Flattening ---")
    complex_result = input_table.select(
        col("id"),
        col("nested_json"),
        complex_flattening_udf(col("nested_json")).alias("complex_flattened")
    )
    print("Table after complex flattening:")
    complex_result.print_schema()
    print("\nData after complex flattening:")
    complex_result.execute().print()

    # 6. Unnest Arrays (Table Function)
    print("\n--- 6. Unnest Arrays (Table Function) ---")
    # Register UDTFs before using them in lateral joins
    t_env.create_temporary_function("UNNEST_ARRAYS", unnest_array_udtf)
    t_env.create_temporary_function("UNNEST_WITH_POSITION", unnest_with_position)
    
    unnest_result = input_table.join_lateral(
        call("UNNEST_ARRAYS", col("nested_json"), "orders").alias("array_index", "array_element")
    )
    print("Table after unnesting orders array:")
    unnest_result.print_schema()
    print("\nData after unnesting orders array:")
    unnest_result.execute().print()

    # 7. Unnest with Position Information
    print("\n--- 7. Unnest with Position Information ---")
    unnest_position_result = input_table.left_outer_join_lateral(
        call("UNNEST_WITH_POSITION", col("nested_json"), "orders").alias("array_index", "array_element", "array_size")
    )
    print("Table after unnesting with position:")
    unnest_position_result.print_schema()
    print("\nData after unnesting with position:")
    unnest_position_result.execute().print()

    # 8. Combined Flattening Example
    print("\n--- 8. Combined Flattening ---")
    # Register remaining UDFs for SQL
    t_env.create_temporary_function("FLATTEN_USER", flatten_user_info)
    t_env.create_temporary_function("GET_ORDER_COUNT", get_order_count)
    t_env.create_temporary_function("CALC_TOTAL", calculate_total_amount)
    t_env.create_temporary_function("EXTRACT_ORDER_IDS", extract_order_ids)

    t_env.create_temporary_view("nested_data", input_table)
    combined_result = t_env.sql_query("""
        SELECT 
            id,
            FLATTEN_USER(nested_json) as user_info,
            GET_ORDER_COUNT(nested_json) as order_count,
            CALC_TOTAL(nested_json) as total_amount,
            EXTRACT_ORDER_IDS(nested_json) as order_ids
        FROM nested_data
        WHERE nested_json IS NOT NULL
    """)
    print("Table from combined SQL query:")
    combined_result.print_schema()
    print("\nData from combined SQL query:")
    combined_result.execute().print()

    # 9. SQL Unnest Example
    print("\n--- 9. SQL Unnest Example ---")
    sql_unnest_result = t_env.sql_query("""
        SELECT 
            d.id,
            u.array_index,
            u.array_element,
            u.array_size
        FROM nested_data AS d,
        LATERAL TABLE(UNNEST_WITH_POSITION(d.nested_json, 'orders')) AS u(array_index, array_element, array_size)
        WHERE d.nested_json IS NOT NULL
    """)
    print("Table from SQL unnest query:")
    sql_unnest_result.print_schema()
    print("\nData from SQL unnest query:")
    sql_unnest_result.execute().print()

if __name__ == '__main__':
    recipe_09_flattening_nested_json() 