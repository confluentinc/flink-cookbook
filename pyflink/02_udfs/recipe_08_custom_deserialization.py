# Recipe 8: Custom Deserialization with UDFs
from typing import List, Tuple, Dict, Any
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, Table
from pyflink.table.udf import udf, ScalarFunction
from pyflink.table.expressions import col
import json
import base64
import struct

# Constants
DEFAULT_PARALLELISM = "1"
SAMPLE_DATA: List[Tuple[int, str, str, str]] = [
    (1, '{"user_id": 123, "name": "John", "age": 30}', "eyJ1c2VyX2lkIjogMTIzLCAibmFtZSI6ICJKb2huIiwgImFnZSI6IDMwfQ==", "010000007b000000"),
    (2, '{"user_id": 456, "name": "Jane", "age": 25}', "eyJ1c2VyX2lkIjogNDU2LCAibmFtZSI6ICJKYW5lIiwgImFnZSI6IDI1fQ==", "01000000c8010000"),
    (3, '{"user_id": 789, "name": "Bob", "age": 35}', "eyJ1c2VyX2lkIjogNzg5LCAibmFtZSI6ICJCb2IiLCAiYWdlIjogMzV9", "0100000015030000"),
    (4, None, None, None)
]

# 1. JSON Deserialization UDF
@udf(result_type=DataTypes.STRING())
def extract_user_name_from_json(json_str: str) -> str:
    """Extract user name from JSON string"""
    if json_str is None:
        return None
    try:
        data = json.loads(json_str)
        return data.get('name')
    except (json.JSONDecodeError, KeyError, TypeError):
        return None

# 2. Base64 Decoding UDF
@udf(result_type=DataTypes.STRING())
def decode_base64_json(base64_str: str) -> str:
    """Decode base64 string and return as JSON string"""
    if base64_str is None:
        return None
    try:
        decoded_bytes = base64.b64decode(base64_str)
        decoded_str = decoded_bytes.decode('utf-8')
        # Validate it's valid JSON
        json.loads(decoded_str)
        return decoded_str
    except (base64.binascii.Error, UnicodeDecodeError, json.JSONDecodeError):
        return None

# 3. Binary Data Deserialization UDF
class BinaryDeserializerUDF(ScalarFunction):
    """Deserialize binary data containing user_id and age"""
    
    def eval(self, binary_data: str) -> str:
        if binary_data is None:
            return None
        try:
            # Convert hex string to bytes
            binary_bytes = bytes.fromhex(binary_data)
            # Assuming format: 4 bytes for user_id (little endian)
            if len(binary_bytes) >= 4:
                user_id = struct.unpack('<I', binary_bytes[:4])[0]
                return json.dumps({"user_id": user_id, "source": "binary"})
            return None
        except (ValueError, struct.error):
            return None

binary_deserializer_udf = udf(BinaryDeserializerUDF(), result_type=DataTypes.STRING())

# 4. Complex JSON Parsing UDF
@udf(result_type=DataTypes.STRING())
def parse_nested_json(json_str: str) -> str:
    """Parse nested JSON and extract specific fields"""
    if json_str is None:
        return None
    try:
        data = json.loads(json_str)
        # Extract and format specific fields
        user_id = data.get('user_id', 'unknown')
        name = data.get('name', 'unknown')
        age = data.get('age', 'unknown')
        
        return json.dumps({
            "parsed_user_id": user_id,
            "parsed_name": name,
            "parsed_age": age,
            "status": "success"
        })
    except (json.JSONDecodeError, TypeError):
        return json.dumps({
            "parsed_user_id": "error",
            "parsed_name": "error", 
            "parsed_age": "error",
            "status": "failed"
        })

def recipe_08_custom_deserialization() -> None:
    """
    Demonstrates custom deserialization using UDFs in PyFlink.
    
    This function shows:
    1. JSON string parsing and field extraction
    2. Base64 decoding and validation
    3. Binary data deserialization
    4. Complex nested JSON parsing with error handling
    """
    # Setup
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    t_env.get_config().set("parallelism.default", DEFAULT_PARALLELISM)

    print("Recipe 8: Custom Deserialization with UDFs")

    # Create sample table
    input_table = t_env.from_elements(
        SAMPLE_DATA,
        DataTypes.ROW([
            DataTypes.FIELD("id", DataTypes.INT()),
            DataTypes.FIELD("json_data", DataTypes.STRING()),
            DataTypes.FIELD("base64_data", DataTypes.STRING()),
            DataTypes.FIELD("binary_data", DataTypes.STRING())
        ])
    )
    print("Input Table:")
    input_table.print_schema()
    print("\nInput Data:")
    input_table.execute().print()

    # 1. JSON Deserialization
    print("\n--- 1. JSON Deserialization ---")
    json_result = input_table.select(
        col("id"),
        col("json_data"),
        extract_user_name_from_json(col("json_data")).alias("extracted_name")
    )
    print("Table after JSON deserialization:")
    json_result.print_schema()
    print("\nData after JSON deserialization:")
    json_result.execute().print()

    # 2. Base64 Decoding
    print("\n--- 2. Base64 Decoding ---")
    base64_result = input_table.select(
        col("id"),
        col("base64_data"),
        decode_base64_json(col("base64_data")).alias("decoded_json")
    )
    print("Table after Base64 decoding:")
    base64_result.print_schema()
    print("\nData after Base64 decoding:")
    base64_result.execute().print()

    # 3. Binary Deserialization
    print("\n--- 3. Binary Deserialization ---")
    binary_result = input_table.select(
        col("id"),
        col("binary_data"),
        binary_deserializer_udf(col("binary_data")).alias("deserialized_binary")
    )
    print("Table after binary deserialization:")
    binary_result.print_schema()
    print("\nData after binary deserialization:")
    binary_result.execute().print()

    # 4. Complex JSON Parsing
    print("\n--- 4. Complex JSON Parsing ---")
    complex_result = input_table.select(
        col("id"),
        col("json_data"),
        parse_nested_json(col("json_data")).alias("parsed_json")
    )
    print("Table after complex JSON parsing:")
    complex_result.print_schema()
    print("\nData after complex JSON parsing:")
    complex_result.execute().print()

    # 5. Combined Deserialization Example
    print("\n--- 5. Combined Deserialization ---")
    # Register UDFs for SQL
    t_env.create_temporary_function("EXTRACT_NAME", extract_user_name_from_json)
    t_env.create_temporary_function("DECODE_BASE64", decode_base64_json)
    t_env.create_temporary_function("PARSE_JSON", parse_nested_json)

    t_env.create_temporary_view("input_data", input_table)
    combined_result = t_env.sql_query("""
        SELECT 
            id,
            json_data,
            EXTRACT_NAME(json_data) as name_from_json,
            DECODE_BASE64(base64_data) as decoded_data,
            PARSE_JSON(json_data) as parsed_data
        FROM input_data
        WHERE json_data IS NOT NULL
    """)
    print("Table from combined SQL query:")
    combined_result.print_schema()
    print("\nData from combined SQL query:")
    combined_result.execute().print()

if __name__ == '__main__':
    recipe_08_custom_deserialization() 