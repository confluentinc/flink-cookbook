# Recipe 6: Generating Multiple Rows with Table UDFs (UDTFs)
from typing import List, Tuple
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, Table, Row
from pyflink.table.expressions import col, call
from pyflink.table.udf import udtf, TableFunction

# Constants
DEFAULT_PARALLELISM = "1"
SAMPLE_DATA: List[Tuple[int, str]] = [
    (1, "Hello PyFlink"),
    (2, "Table UDFs are powerful"),
    (3, "Generating multiple rows is easy"),
    (4, "Streaming data processing"),
    (5, "Real-time analytics with Flink")
]

# 1. Define a Table UDF (UDTF) using a class extending TableFunction
class SplitWords(TableFunction):
    def eval(self, text_string: str):
        """Split text into words and yield each word with its length"""
        if text_string is None:
            return
        for word in text_string.split(" "):
            if word:  # ensure non-empty words
                # Yield a Row object for each word and its length
                yield Row(word, len(word))

# Define the result type of the UDTF (schema of the yielded rows)
split_udtf_result_type = DataTypes.ROW([
    DataTypes.FIELD("word", DataTypes.STRING()),
    DataTypes.FIELD("length", DataTypes.INT())
])

# 2. Alternative: UDTF using @udtf decorator on a generator function
@udtf(result_types=[DataTypes.STRING(), DataTypes.INT()])
def split_fn_udtf(text_string: str):
    """Split text into words using function decorator"""
    if text_string:
        for word in text_string.split(" "):
            if word:
                yield word, len(word)

# 3. UDTF for extracting key-value pairs
@udtf(result_types=[DataTypes.STRING(), DataTypes.STRING()])
def extract_key_value(text_string: str):
    """Extract key-value pairs from text like 'key=value'"""
    if text_string is None:
        return
    
    for part in text_string.split(","):
        part = part.strip()
        if "=" in part:
            key, value = part.split("=", 1)
            yield key.strip(), value.strip()

# 4. UDTF for generating sequence numbers
@udtf(result_types=[DataTypes.INT(), DataTypes.INT()])
def generate_sequence(start: int, count: int):
    """Generate a sequence of numbers"""
    if start is None or count is None or count <= 0:
        return
    
    for i in range(count):
        yield start + i, i

def recipe_06_table_udfs() -> None:
    """
    Demonstrates Table User-Defined Functions (UDTFs) in PyFlink.
    
    This function shows:
    1. Creating UDTFs using class-based approach
    2. Creating UDTFs using function decorators
    3. Using UDTFs with join_lateral
    4. Using UDTFs with left_outer_join_lateral
    5. Using UDTFs in SQL queries
    6. Different types of UDTF use cases
    """
    # Setup
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    t_env.get_config().set("parallelism.default", DEFAULT_PARALLELISM)

    print("Recipe 6: Generating Multiple Rows with Table UDFs (UDTFs)")

    # Create sample table
    input_table = t_env.from_elements(
        SAMPLE_DATA,
        DataTypes.ROW([
            DataTypes.FIELD("id", DataTypes.INT()),
            DataTypes.FIELD("line_content", DataTypes.STRING())
        ])
    )
    print("Input Table:")
    input_table.print_schema()
    print("\nInput Data:")
    input_table.execute().print()

    # Register UDTFs
    split_words_udtf_instance = SplitWords()
    t_env.create_temporary_function("SPLIT_WORDS_CLASS", udtf(split_words_udtf_instance, result_types=split_udtf_result_type))
    t_env.create_temporary_function("SPLIT_FN", split_fn_udtf)
    t_env.create_temporary_function("EXTRACT_KV", extract_key_value)
    t_env.create_temporary_function("GENERATE_SEQ", generate_sequence)

    # 1. Using class-based UDTF with join_lateral
    print("\n--- 1. Class-based UDTF with join_lateral ---")
    output_table_join_lateral_class = input_table.join_lateral(
        call("SPLIT_WORDS_CLASS", col("line_content")).alias("word", "length")
    )
    print("Table after class-based UDTF with join_lateral:")
    output_table_join_lateral_class.print_schema()
    print("\nData from class-based UDTF (join_lateral):")
    output_table_join_lateral_class.execute().print()

    # 2. Using function-based UDTF with left_outer_join_lateral
    print("\n--- 2. Function-based UDTF with left_outer_join_lateral ---")
    output_table_left_join_fn = input_table.left_outer_join_lateral(
        call("SPLIT_FN", col("line_content")).alias("word", "length")
    )
    print("Table after function-based UDTF with left_outer_join_lateral:")
    output_table_left_join_fn.print_schema()
    print("\nData from function-based UDTF (left_outer_join_lateral):")
    output_table_left_join_fn.execute().print()

    # 3. Using UDTF in SQL
    print("\n--- 3. Using UDTF in SQL ---")
    t_env.create_temporary_view("input_table", input_table)
    sql_udtf_result = t_env.sql_query("""
        SELECT d.id, s.word, s.length
        FROM input_table AS d, LATERAL TABLE(SPLIT_WORDS_CLASS(d.line_content)) AS s(word, length)
        WHERE d.line_content IS NOT NULL
    """)
    print("Table from SQL query using UDTF:")
    sql_udtf_result.print_schema()
    print("\nData from SQL UDTF query:")
    sql_udtf_result.execute().print()

    # 4. UDTF with filtering
    print("\n--- 4. UDTF with filtering ---")
    filtered_udtf = input_table.join_lateral(
        call("SPLIT_FN", col("line_content")).alias("word", "length")
    ).filter(
        col("length") > 4
    ).select(
        col("id"),
        col("word"),
        col("length")
    )
    print("Table after UDTF with filtering (words longer than 4 characters):")
    filtered_udtf.print_schema()
    print("\nData after UDTF filtering:")
    filtered_udtf.execute().print()

    # 5. UDTF with aggregations
    print("\n--- 5. UDTF with aggregations ---")
    aggregated_udtf = input_table.join_lateral(
        call("SPLIT_FN", col("line_content")).alias("word", "length")
    ).group_by(
        col("id")
    ).select(
        col("id"),
        col("word").count.alias("word_count"),
        col("length").avg.alias("avg_word_length"),
        col("length").max.alias("max_word_length")
    )
    print("Table after UDTF with aggregations:")
    aggregated_udtf.print_schema()
    print("\nData after UDTF aggregations:")
    aggregated_udtf.execute().print()

    # 6. Multiple UDTFs in sequence
    print("\n--- 6. Multiple UDTFs in sequence ---")
    # Create additional sample data with key-value pairs
    kv_data = [
        (1, "name=John,age=30,city=NYC"),
        (2, "name=Jane,age=25,city=LA"),
        (3, "name=Bob,age=35,city=CHI")
    ]
    kv_table = t_env.from_elements(
        kv_data,
        DataTypes.ROW([
            DataTypes.FIELD("id", DataTypes.INT()),
            DataTypes.FIELD("kv_string", DataTypes.STRING())
        ])
    )
    
    kv_result = kv_table.join_lateral(
        call("EXTRACT_KV", col("kv_string")).alias("key", "value")
    )
    print("Table after key-value extraction UDTF:")
    kv_result.print_schema()
    print("\nData after key-value extraction:")
    kv_result.execute().print()

    # 7. UDTF with parameters
    print("\n--- 7. UDTF with parameters ---")
    # Create sample data for sequence generation
    seq_data = [
        (1, 10, 5),  # start=10, count=5
        (2, 20, 3),  # start=20, count=3
        (3, 100, 2)  # start=100, count=2
    ]
    seq_table = t_env.from_elements(
        seq_data,
        DataTypes.ROW([
            DataTypes.FIELD("id", DataTypes.INT()),
            DataTypes.FIELD("start_num", DataTypes.INT()),
            DataTypes.FIELD("count", DataTypes.INT())
        ])
    )
    
    seq_result = seq_table.join_lateral(
        call("GENERATE_SEQ", col("start_num"), col("count")).alias("sequence_num", "index")
    )
    print("Table after sequence generation UDTF:")
    seq_result.print_schema()
    print("\nData after sequence generation:")
    seq_result.execute().print()

    # 8. Complex UDTF example
    print("\n--- 8. Complex UDTF example ---")
    t_env.create_temporary_view("kv_data", kv_table)
    complex_sql = t_env.sql_query("""
        SELECT 
            d.id,
            k.key,
            k.value,
            CASE 
                WHEN k.key = 'age' THEN CAST(k.value AS INT)
                ELSE NULL
            END as age_value
        FROM kv_data AS d, LATERAL TABLE(EXTRACT_KV(d.kv_string)) AS k(key, value)
        WHERE k.key = 'age'
    """)
    print("Table from complex SQL UDTF query:")
    complex_sql.print_schema()
    print("\nData from complex SQL UDTF query:")
    complex_sql.execute().print()

if __name__ == '__main__':
    recipe_06_table_udfs()