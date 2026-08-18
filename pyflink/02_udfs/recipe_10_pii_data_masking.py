# Recipe 10: PII Data Masking with UDFs
from typing import List, Tuple, Dict, Any
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, Table
from pyflink.table.udf import udf, ScalarFunction
from pyflink.table.expressions import col
import re
import hashlib
import base64

# Constants
DEFAULT_PARALLELISM = "1"
SAMPLE_DATA: List[Tuple[int, str, str, str, str, str]] = [
    (1, "john.doe@example.com", "123-45-6789", "555-123-4567", "123 Main St, New York, NY 10001", "John Doe"),
    (2, "jane.smith@company.com", "987-65-4321", "555-987-6543", "456 Oak Ave, Los Angeles, CA 90210", "Jane Smith"),
    (3, "bob.wilson@test.org", "111-22-3333", "555-111-2222", "789 Pine Rd, Chicago, IL 60601", "Bob Wilson"),
    (4, "alice.johnson@demo.net", "444-55-6666", "555-444-5555", "101 Elm St, Boston, MA 02101", "Alice Johnson"),
    (5, None, None, None, None, None)
]

# 1. Email Masking UDF
@udf(result_type=DataTypes.STRING())
def mask_email(email: str) -> str:
    """Mask email addresses while preserving domain"""
    if email is None:
        return None
    try:
        if '@' in email:
            username, domain = email.split('@', 1)
            if len(username) <= 2:
                masked_username = username
            else:
                masked_username = username[0] + '*' * (len(username) - 2) + username[-1]
            return f"{masked_username}@{domain}"
        return email
    except (AttributeError, ValueError):
        return email

# 2. SSN Masking UDF
@udf(result_type=DataTypes.STRING())
def mask_ssn(ssn: str) -> str:
    """Mask Social Security Numbers"""
    if ssn is None:
        return None
    try:
        # Remove any non-digit characters
        digits_only = re.sub(r'\D', '', ssn)
        if len(digits_only) == 9:
            return f"***-**-{digits_only[-4:]}"
        return ssn
    except (AttributeError, TypeError):
        return ssn

# 3. Phone Number Masking UDF
@udf(result_type=DataTypes.STRING())
def mask_phone(phone: str) -> str:
    """Mask phone numbers"""
    if phone is None:
        return None
    try:
        # Remove any non-digit characters
        digits_only = re.sub(r'\D', '', phone)
        if len(digits_only) == 10:
            return f"***-***-{digits_only[-4:]}"
        elif len(digits_only) == 11 and digits_only[0] == '1':
            return f"***-***-{digits_only[-4:]}"
        return phone
    except (AttributeError, TypeError):
        return phone

# 4. Address Masking UDF
@udf(result_type=DataTypes.STRING())
def mask_address(address: str) -> str:
    """Mask street addresses while preserving city and state"""
    if address is None:
        return None
    try:
        # Split by comma to separate street from city/state/zip
        parts = address.split(',')
        if len(parts) >= 2:
            street = parts[0].strip()
            city_state_zip = ','.join(parts[1:]).strip()
            
            # Mask street number and name
            street_words = street.split()
            if len(street_words) >= 2:
                # Keep first letter of street name, mask the rest
                street_name = street_words[1]
                masked_street_name = street_name[0] + '*' * (len(street_name) - 1)
                masked_street = f"*** {masked_street_name}"
            else:
                masked_street = "***"
            
            return f"{masked_street}, {city_state_zip}"
        return address
    except (AttributeError, IndexError):
        return address

# 5. Name Masking UDF
@udf(result_type=DataTypes.STRING())
def mask_name(name: str) -> str:
    """Mask full names"""
    if name is None:
        return None
    try:
        name_parts = name.split()
        if len(name_parts) >= 2:
            first_name = name_parts[0]
            last_name = name_parts[-1]
            
            # Keep first letter of first and last name
            masked_first = first_name[0] + '*' * (len(first_name) - 1)
            masked_last = last_name[0] + '*' * (len(last_name) - 1)
            
            return f"{masked_first} {masked_last}"
        elif len(name_parts) == 1:
            # Single name
            name = name_parts[0]
            return name[0] + '*' * (len(name) - 1)
        return name
    except (AttributeError, IndexError):
        return name

# 6. Hash-based Anonymization UDF
class HashAnonymizerUDF(ScalarFunction):
    """Hash-based anonymization for sensitive data"""
    
    def eval(self, data: str) -> str:
        if data is None:
            return None
        try:
            # Create a hash of the data
            hash_object = hashlib.sha256(data.encode('utf-8'))
            hash_hex = hash_object.hexdigest()
            # Return first 8 characters of hash
            return f"hash_{hash_hex[:8]}"
        except (AttributeError, UnicodeEncodeError):
            return None

hash_anonymizer_udf = udf(HashAnonymizerUDF(), result_type=DataTypes.STRING())

# 7. Comprehensive PII Masking UDF
@udf(result_type=DataTypes.STRING())
def comprehensive_pii_mask(data: str, data_type: str) -> str:
    """Comprehensive PII masking based on data type"""
    if data is None:
        return None
    
    try:
        data_type_lower = data_type.lower()
        
        if data_type_lower == 'email':
            return mask_email(data)
        elif data_type_lower == 'ssn':
            return mask_ssn(data)
        elif data_type_lower == 'phone':
            return mask_phone(data)
        elif data_type_lower == 'address':
            return mask_address(data)
        elif data_type_lower == 'name':
            return mask_name(data)
        elif data_type_lower == 'hash':
            return hash_anonymizer_udf(data)
        else:
            # Default: mask with asterisks
            if len(data) <= 2:
                return data
            return data[0] + '*' * (len(data) - 2) + data[-1]
    except (AttributeError, TypeError):
        return data

def recipe_10_pii_data_masking() -> None:
    """
    Demonstrates PII (Personally Identifiable Information) data masking using UDFs in PyFlink.
    
    This function shows:
    1. Email address masking while preserving domain
    2. Social Security Number masking
    3. Phone number masking
    4. Address masking while preserving location
    5. Name masking with partial visibility
    6. Hash-based anonymization
    7. Comprehensive PII masking based on data type
    """
    # Setup
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    t_env.get_config().set("parallelism.default", DEFAULT_PARALLELISM)

    print("Recipe 10: PII Data Masking with UDFs")

    # Create sample table
    input_table = t_env.from_elements(
        SAMPLE_DATA,
        DataTypes.ROW([
            DataTypes.FIELD("id", DataTypes.INT()),
            DataTypes.FIELD("email", DataTypes.STRING()),
            DataTypes.FIELD("ssn", DataTypes.STRING()),
            DataTypes.FIELD("phone", DataTypes.STRING()),
            DataTypes.FIELD("address", DataTypes.STRING()),
            DataTypes.FIELD("name", DataTypes.STRING())
        ])
    )
    print("Input Table:")
    input_table.print_schema()
    print("\nInput Data:")
    input_table.execute().print()

    # 1. Email Masking
    print("\n--- 1. Email Masking ---")
    email_result = input_table.select(
        col("id"),
        col("email"),
        mask_email(col("email")).alias("masked_email")
    )
    print("Table after email masking:")
    email_result.print_schema()
    print("\nData after email masking:")
    email_result.execute().print()

    # 2. SSN Masking
    print("\n--- 2. SSN Masking ---")
    ssn_result = input_table.select(
        col("id"),
        col("ssn"),
        mask_ssn(col("ssn")).alias("masked_ssn")
    )
    print("Table after SSN masking:")
    ssn_result.print_schema()
    print("\nData after SSN masking:")
    ssn_result.execute().print()

    # 3. Phone Number Masking
    print("\n--- 3. Phone Number Masking ---")
    phone_result = input_table.select(
        col("id"),
        col("phone"),
        mask_phone(col("phone")).alias("masked_phone")
    )
    print("Table after phone masking:")
    phone_result.print_schema()
    print("\nData after phone masking:")
    phone_result.execute().print()

    # 4. Address Masking
    print("\n--- 4. Address Masking ---")
    address_result = input_table.select(
        col("id"),
        col("address"),
        mask_address(col("address")).alias("masked_address")
    )
    print("Table after address masking:")
    address_result.print_schema()
    print("\nData after address masking:")
    address_result.execute().print()

    # 5. Name Masking
    print("\n--- 5. Name Masking ---")
    name_result = input_table.select(
        col("id"),
        col("name"),
        mask_name(col("name")).alias("masked_name")
    )
    print("Table after name masking:")
    name_result.print_schema()
    print("\nData after name masking:")
    name_result.execute().print()

    # 6. Hash-based Anonymization
    print("\n--- 6. Hash-based Anonymization ---")
    hash_result = input_table.select(
        col("id"),
        col("email"),
        hash_anonymizer_udf(col("email")).alias("hashed_email")
    )
    print("Table after hash anonymization:")
    hash_result.print_schema()
    print("\nData after hash anonymization:")
    hash_result.execute().print()

    # 7. Comprehensive PII Masking
    print("\n--- 7. Comprehensive PII Masking ---")
    # Register UDFs for SQL
    t_env.create_temporary_function("MASK_EMAIL", mask_email)
    t_env.create_temporary_function("MASK_SSN", mask_ssn)
    t_env.create_temporary_function("MASK_PHONE", mask_phone)
    t_env.create_temporary_function("MASK_ADDRESS", mask_address)
    t_env.create_temporary_function("MASK_NAME", mask_name)
    t_env.create_temporary_function("COMPREHENSIVE_MASK", comprehensive_pii_mask)

    t_env.create_temporary_view("pii_data", input_table)
    comprehensive_result = t_env.sql_query("""
        SELECT 
            id,
            MASK_EMAIL(email) as masked_email,
            MASK_SSN(ssn) as masked_ssn,
            MASK_PHONE(phone) as masked_phone,
            MASK_ADDRESS(address) as masked_address,
            MASK_NAME(name) as masked_name
        FROM pii_data
        WHERE email IS NOT NULL
    """)
    print("Table from comprehensive SQL query:")
    comprehensive_result.print_schema()
    print("\nData from comprehensive SQL query:")
    comprehensive_result.execute().print()

if __name__ == '__main__':
    recipe_10_pii_data_masking() 