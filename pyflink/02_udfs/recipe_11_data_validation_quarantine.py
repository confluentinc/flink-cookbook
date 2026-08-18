# Recipe 11: Data Validation and Quarantine/Dead Letter with UDFs
from typing import List, Tuple, Dict, Any
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, Table
from pyflink.table.udf import udf, ScalarFunction
from pyflink.table.expressions import col
import json
import re
from datetime import datetime

# Constants
DEFAULT_PARALLELISM = "1"
SAMPLE_DATA: List[Tuple[int, str, str, str, str, str, str]] = [
    (1, "john.doe@example.com", "123-45-6789", "555-123-4567", "25", "2023-01-15", '{"user_id": 123, "name": "John", "age": 25}'),
    (2, "invalid-email", "987-65-4321", "555-987-6543", "35", "2023-13-45", '{"user_id": 456, "name": "Jane", "age": 35}'),
    (3, "bob@test.com", "111-22-333", "invalid-phone", "150", "2023-02-30", '{"user_id": 789, "name": "Bob", "age": 150}'),
    (4, "alice@demo.net", "444-55-6666", "555-444-5555", "30", "2023-03-15", '{"invalid_json": "missing_quotes}'),
    (5, None, None, None, None, None, None)
]

# 1. Email Validation UDF
@udf(result_type=DataTypes.BOOLEAN())
def is_valid_email(email: str) -> bool:
    """Validate email format"""
    if email is None:
        return False
    try:
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(email_pattern, email))
    except (AttributeError, TypeError):
        return False

# 2. SSN Validation UDF
@udf(result_type=DataTypes.BOOLEAN())
def is_valid_ssn(ssn: str) -> bool:
    """Validate SSN format (XXX-XX-XXXX)"""
    if ssn is None:
        return False
    try:
        # Remove any non-digit characters
        digits_only = re.sub(r'\D', '', ssn)
        return len(digits_only) == 9
    except (AttributeError, TypeError):
        return False

# 3. Phone Validation UDF
@udf(result_type=DataTypes.BOOLEAN())
def is_valid_phone(phone: str) -> bool:
    """Validate phone number format"""
    if phone is None:
        return False
    try:
        # Remove any non-digit characters
        digits_only = re.sub(r'\D', '', phone)
        return len(digits_only) in [10, 11]  # 10 digits or 11 with country code
    except (AttributeError, TypeError):
        return False

# 4. Age Validation UDF
@udf(result_type=DataTypes.BOOLEAN())
def is_valid_age(age_str: str) -> bool:
    """Validate age is reasonable (0-120)"""
    if age_str is None:
        return False
    try:
        age = int(age_str)
        return 0 <= age <= 120
    except (ValueError, TypeError):
        return False

# 5. Date Validation UDF
@udf(result_type=DataTypes.BOOLEAN())
def is_valid_date(date_str: str) -> bool:
    """Validate date format (YYYY-MM-DD)"""
    if date_str is None:
        return False
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except (ValueError, TypeError):
        return False

# 6. JSON Validation UDF
@udf(result_type=DataTypes.BOOLEAN())
def is_valid_json(json_str: str) -> bool:
    """Validate JSON format"""
    if json_str is None:
        return False
    try:
        json.loads(json_str)
        return True
    except (json.JSONDecodeError, TypeError):
        return False

# 7. Comprehensive Validation UDF
class ComprehensiveValidatorUDF(ScalarFunction):
    """Comprehensive validation with detailed error reporting"""
    
    def eval(self, email: str, ssn: str, phone: str, age: str, date: str, json_data: str) -> str:
        errors = []
        
        # Validate each field
        if not is_valid_email(email):
            errors.append("invalid_email")
        
        if not is_valid_ssn(ssn):
            errors.append("invalid_ssn")
            
        if not is_valid_phone(phone):
            errors.append("invalid_phone")
            
        if not is_valid_age(age):
            errors.append("invalid_age")
            
        if not is_valid_date(date):
            errors.append("invalid_date")
            
        if not is_valid_json(json_data):
            errors.append("invalid_json")
        
        if errors:
            return json.dumps({
                "valid": False,
                "errors": errors,
                "status": "quarantine"
            })
        else:
            return json.dumps({
                "valid": True,
                "errors": [],
                "status": "valid"
            })

comprehensive_validator_udf = udf(ComprehensiveValidatorUDF(), result_type=DataTypes.STRING())

# 8. Data Quality Score UDF
@udf(result_type=DataTypes.DOUBLE())
def calculate_quality_score(email: str, ssn: str, phone: str, age: str, date: str, json_data: str) -> float:
    """Calculate data quality score (0.0 to 1.0)"""
    total_fields = 6
    valid_fields = 0
    
    if is_valid_email(email):
        valid_fields += 1
    if is_valid_ssn(ssn):
        valid_fields += 1
    if is_valid_phone(phone):
        valid_fields += 1
    if is_valid_age(age):
        valid_fields += 1
    if is_valid_date(date):
        valid_fields += 1
    if is_valid_json(json_data):
        valid_fields += 1
    
    return round(valid_fields / total_fields, 2)

def recipe_11_data_validation_quarantine() -> None:
    """
    Demonstrates data validation and quarantine/dead letter handling using UDFs in PyFlink.
    
    This function shows:
    1. Individual field validation (email, SSN, phone, age, date, JSON)
    2. Comprehensive validation with error reporting
    3. Data quality scoring
    4. Quarantine/dead letter classification
    5. Validation-based filtering and routing
    """
    # Setup
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    t_env.get_config().set("parallelism.default", DEFAULT_PARALLELISM)

    print("Recipe 11: Data Validation and Quarantine/Dead Letter with UDFs")

    # Create sample table
    input_table = t_env.from_elements(
        SAMPLE_DATA,
        DataTypes.ROW([
            DataTypes.FIELD("id", DataTypes.INT()),
            DataTypes.FIELD("email", DataTypes.STRING()),
            DataTypes.FIELD("ssn", DataTypes.STRING()),
            DataTypes.FIELD("phone", DataTypes.STRING()),
            DataTypes.FIELD("age", DataTypes.STRING()),
            DataTypes.FIELD("date", DataTypes.STRING()),
            DataTypes.FIELD("json_data", DataTypes.STRING())
        ])
    )
    print("Input Table:")
    input_table.print_schema()
    print("\nInput Data:")
    input_table.execute().print()

    # 1. Individual Field Validation
    print("\n--- 1. Individual Field Validation ---")
    validation_result = input_table.select(
        col("id"),
        col("email"),
        is_valid_email(col("email")).alias("valid_email"),
        is_valid_ssn(col("ssn")).alias("valid_ssn"),
        is_valid_phone(col("phone")).alias("valid_phone"),
        is_valid_age(col("age")).alias("valid_age"),
        is_valid_date(col("date")).alias("valid_date"),
        is_valid_json(col("json_data")).alias("valid_json")
    )
    print("Table after individual validation:")
    validation_result.print_schema()
    print("\nData after individual validation:")
    validation_result.execute().print()

    # 2. Comprehensive Validation
    print("\n--- 2. Comprehensive Validation ---")
    comprehensive_result = input_table.select(
        col("id"),
        col("email"),
        col("ssn"),
        col("phone"),
        col("age"),
        col("date"),
        col("json_data"),
        comprehensive_validator_udf(
            col("email"), col("ssn"), col("phone"), 
            col("age"), col("date"), col("json_data")
        ).alias("validation_result")
    )
    print("Table after comprehensive validation:")
    comprehensive_result.print_schema()
    print("\nData after comprehensive validation:")
    comprehensive_result.execute().print()

    # 3. Data Quality Scoring
    print("\n--- 3. Data Quality Scoring ---")
    quality_result = input_table.select(
        col("id"),
        col("email"),
        col("ssn"),
        col("phone"),
        col("age"),
        col("date"),
        col("json_data"),
        calculate_quality_score(
            col("email"), col("ssn"), col("phone"), 
            col("age"), col("date"), col("json_data")
        ).alias("quality_score")
    )
    print("Table after quality scoring:")
    quality_result.print_schema()
    print("\nData after quality scoring:")
    quality_result.execute().print()

    # 4. Valid Data Filtering
    print("\n--- 4. Valid Data Filtering ---")
    valid_data = input_table.filter(
        is_valid_email(col("email")) &
        is_valid_ssn(col("ssn")) &
        is_valid_phone(col("phone")) &
        is_valid_age(col("age")) &
        is_valid_date(col("date")) &
        is_valid_json(col("json_data"))
    )
    print("Table with valid data only:")
    valid_data.print_schema()
    print("\nValid data:")
    valid_data.execute().print()

    # 5. Quarantine Data Filtering
    print("\n--- 5. Quarantine Data Filtering ---")
    quarantine_data = input_table.filter(
        ~(is_valid_email(col("email")) &
          is_valid_ssn(col("ssn")) &
          is_valid_phone(col("phone")) &
          is_valid_age(col("age")) &
          is_valid_date(col("date")) &
          is_valid_json(col("json_data")))
    )
    print("Table with quarantine data only:")
    quarantine_data.print_schema()
    print("\nQuarantine data:")
    quarantine_data.execute().print()

    # 6. Quality-based Routing
    print("\n--- 6. Quality-based Routing ---")
    # Register UDFs for SQL
    t_env.create_temporary_function("IS_VALID_EMAIL", is_valid_email)
    t_env.create_temporary_function("IS_VALID_SSN", is_valid_ssn)
    t_env.create_temporary_function("IS_VALID_PHONE", is_valid_phone)
    t_env.create_temporary_function("IS_VALID_AGE", is_valid_age)
    t_env.create_temporary_function("IS_VALID_DATE", is_valid_date)
    t_env.create_temporary_function("IS_VALID_JSON", is_valid_json)
    t_env.create_temporary_function("CALC_QUALITY_SCORE", calculate_quality_score)

    t_env.create_temporary_view("validation_data", input_table)
    routing_result = t_env.sql_query("""
        SELECT 
            id,
            email,
            ssn,
            phone,
            age,
            date,
            json_data,
            CALC_QUALITY_SCORE(email, ssn, phone, age, date, json_data) as quality_score,
            CASE 
                WHEN CALC_QUALITY_SCORE(email, ssn, phone, age, date, json_data) = 1.0 THEN 'valid'
                WHEN CALC_QUALITY_SCORE(email, ssn, phone, age, date, json_data) >= 0.5 THEN 'partial'
                ELSE 'quarantine'
            END as data_status
        FROM validation_data
        WHERE email IS NOT NULL
    """)
    print("Table from quality-based routing:")
    routing_result.print_schema()
    print("\nData from quality-based routing:")
    routing_result.execute().print()

if __name__ == '__main__':
    recipe_11_data_validation_quarantine() 