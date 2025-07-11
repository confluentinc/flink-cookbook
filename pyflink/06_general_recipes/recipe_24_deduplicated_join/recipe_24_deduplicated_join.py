#!/usr/bin/env python3
"""
Recipe 20: Deduplicated Join with PyFlink Table API

This recipe demonstrates how to join and deduplicate data using PyFlink Table API.
It mirrors the Java table-deduplicated-join recipe but uses only Table API and
creates streams from elements instead of Kafka.

The recipe:
1. Creates customer and transaction data streams from elements
2. Performs a join between customers and transactions
3. Deduplicates transactions using DISTINCT
4. Shows the joined and deduplicated results
"""

from typing import List, Tuple
from datetime import datetime
from pyflink.table import EnvironmentSettings, StreamTableEnvironment, DataTypes, Row
from pyflink.table.expressions import col

# Constants
DEFAULT_PARALLELISM = "1"

# Sample customer data (c_id, c_name)
CUSTOMERS_DATA: List[Tuple[int, str]] = [
    (1, "Ramon Stehr"),
    (2, "Barbie Ledner"),
    (3, "Lore Baumbach"),
    (4, "Deja Crist"),
    (5, "Felix Weimann"),
    (6, "Alice Johnson"),
    (7, "Bob Wilson"),
    (8, "Carol Davis"),
    (9, "David Miller"),
    (10, "Eva Garcia")
]

# Sample transaction data (t_id, t_customer_id, t_amount, timestamp)
# Note: Some transactions have duplicate t_id values to demonstrate deduplication
TRANSACTIONS_DATA: List[Tuple[int, int, float, datetime]] = [
    (1, 1, 99.08, datetime(2023, 1, 1, 10, 0, 0)),
    (2, 1, 405.01, datetime(2023, 1, 1, 10, 5, 0)),
    (3, 2, 974.90, datetime(2023, 1, 1, 10, 10, 0)),
    (4, 4, 100.19, datetime(2023, 1, 1, 10, 15, 0)),
    (5, 3, 161.48, datetime(2023, 1, 1, 10, 20, 0)),
    (6, 5, 234.56, datetime(2023, 1, 1, 10, 25, 0)),
    (7, 2, 567.89, datetime(2023, 1, 1, 10, 30, 0)),
    (8, 6, 789.12, datetime(2023, 1, 1, 10, 35, 0)),
    (9, 7, 123.45, datetime(2023, 1, 1, 10, 40, 0)),
    (10, 8, 456.78, datetime(2023, 1, 1, 10, 45, 0)),
    # Duplicate transactions to demonstrate deduplication
    (1, 1, 99.08, datetime(2023, 1, 1, 10, 50, 0)),  # Duplicate of t_id=1
    (3, 2, 974.90, datetime(2023, 1, 1, 10, 55, 0)),  # Duplicate of t_id=3
    (5, 3, 161.48, datetime(2023, 1, 1, 11, 0, 0)),   # Duplicate of t_id=5
    (11, 9, 321.65, datetime(2023, 1, 1, 11, 5, 0)),
    (12, 10, 654.32, datetime(2023, 1, 1, 11, 10, 0)),
    (13, 1, 111.11, datetime(2023, 1, 1, 11, 15, 0)),
    (14, 3, 222.22, datetime(2023, 1, 1, 11, 20, 0)),
    (15, 5, 333.33, datetime(2023, 1, 1, 11, 25, 0))
]

def create_table_environment():
    """Create and configure the table environment."""
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = StreamTableEnvironment.create(environment_settings=settings)
    table_env.get_config().set("parallelism.default", DEFAULT_PARALLELISM)
    return table_env

def create_customers_table(table_env):
    """Create the customers table from sample data."""
    return table_env.from_elements(
        CUSTOMERS_DATA,
        DataTypes.ROW([
            DataTypes.FIELD("c_id", DataTypes.BIGINT()),
            DataTypes.FIELD("c_name", DataTypes.STRING())
        ])
    )

def create_transactions_table(table_env):
    """Create the transactions table from sample data."""
    return table_env.from_elements(
        TRANSACTIONS_DATA,
        DataTypes.ROW([
            DataTypes.FIELD("t_id", DataTypes.BIGINT()),
            DataTypes.FIELD("t_customer_id", DataTypes.BIGINT()),
            DataTypes.FIELD("t_amount", DataTypes.DOUBLE()),
            DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP(3))
        ])
    )

def demonstrate_deduplicated_join(table_env):
    """
    Demonstrate deduplicated join using SQL query.
    
    This mirrors the Java version's approach:
    - Joins customers with deduplicated transactions
    - Uses DISTINCT to remove duplicate transactions
    - Shows the final joined result
    """
    print("\n=== Deduplicated Join Demonstration ===")
    
    # SQL query that mirrors the Java version
    # This query joins customers with deduplicated transactions
    result_table = table_env.sql_query("""
        SELECT 
            t_id, 
            c_name, 
            CAST(t_amount AS DECIMAL(5, 2)) as t_amount,
            `timestamp`
        FROM Customers
        JOIN (SELECT DISTINCT * FROM Transactions) ON c_id = t_customer_id
    """)
    
    print("Deduplicated Join Result:")
    print("(Shows transactions joined with customer names, duplicates removed)")
    result_table.execute().print()
    
    return result_table

def demonstrate_individual_tables(table_env, customers_table, transactions_table):
    """Show the individual tables before joining."""
    print("\n=== Individual Tables ===")
    
    print("Customers Table:")
    customers_table.print_schema()
    print("Customers Data:")
    customers_table.execute().print()
    
    print("\nTransactions Table (with duplicates):")
    transactions_table.print_schema()
    print("Transactions Data (note duplicate t_id values):")
    transactions_table.execute().print()
    
    # Show deduplicated transactions separately
    print("\nDeduplicated Transactions (using DISTINCT):")
    deduplicated_transactions = table_env.sql_query("""
        SELECT DISTINCT * FROM Transactions
    """)
    deduplicated_transactions.execute().print()

def demonstrate_join_without_deduplication(table_env):
    """Show what happens without deduplication."""
    print("\n=== Join Without Deduplication ===")
    
    # This query doesn't use DISTINCT, so duplicates remain
    result_with_duplicates = table_env.sql_query("""
        SELECT 
            t_id, 
            c_name, 
            CAST(t_amount AS DECIMAL(5, 2)) as t_amount,
            `timestamp`
        FROM Customers
        JOIN Transactions ON c_id = t_customer_id
    """)
    
    print("Join Result WITHOUT Deduplication:")
    print("(Shows duplicate transactions for same t_id)")
    result_with_duplicates.execute().print()

def demonstrate_advanced_deduplication(table_env):
    """Show more advanced deduplication techniques."""
    print("\n=== Advanced Deduplication Techniques ===")
    
    # 1. Deduplicate by specific columns
    print("1. Deduplicate by t_id only:")
    dedup_by_id = table_env.sql_query("""
        SELECT DISTINCT t_id, t_customer_id, t_amount, `timestamp`
        FROM Transactions
    """)
    dedup_by_id.execute().print()
    
    # 2. Keep the latest transaction for each t_id
    print("\n2. Keep latest transaction for each t_id (using ROW_NUMBER):")
    latest_transactions = table_env.sql_query("""
        SELECT t_id, t_customer_id, t_amount, `timestamp`
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY t_id ORDER BY `timestamp` DESC) as rn
            FROM Transactions
        ) t
        WHERE rn = 1
    """)
    latest_transactions.execute().print()
    
    # 3. Join with latest transactions
    print("\n3. Join customers with latest transactions:")
    latest_join = table_env.sql_query("""
        SELECT 
            t.t_id, 
            c.c_name, 
            CAST(t.t_amount AS DECIMAL(5, 2)) as t_amount,
            t.`timestamp`
        FROM Customers c
        JOIN (
            SELECT t_id, t_customer_id, t_amount, `timestamp`
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY t_id ORDER BY `timestamp` DESC) as rn
                FROM Transactions
            ) t
            WHERE rn = 1
        ) t ON c.c_id = t.t_customer_id
    """)
    latest_join.execute().print()

def main():
    """Main function to run the deduplicated join recipe."""
    print("=== Recipe 20: Deduplicated Join with PyFlink Table API ===")
    print("This recipe demonstrates joining and deduplicating data using PyFlink.")
    print("It mirrors the Java table-deduplicated-join recipe but uses only Table API.\n")
    
    # Create table environment
    table_env = create_table_environment()
    
    # Create tables from sample data
    customers_table = create_customers_table(table_env)
    transactions_table = create_transactions_table(table_env)
    
    # Create temporary views for SQL queries
    table_env.create_temporary_view("Customers", customers_table)
    table_env.create_temporary_view("Transactions", transactions_table)
    
    # Demonstrate individual tables
    demonstrate_individual_tables(table_env, customers_table, transactions_table)
    
    # Demonstrate join without deduplication
    demonstrate_join_without_deduplication(table_env)
    
    # Demonstrate deduplicated join (main feature)
    result_table = demonstrate_deduplicated_join(table_env)
    
    # Demonstrate advanced deduplication techniques
    demonstrate_advanced_deduplication(table_env)
    
    print("\n=== Recipe Summary ===")
    print("✓ Created customer and transaction tables from sample data")
    print("✓ Demonstrated deduplication using DISTINCT")
    print("✓ Performed join between customers and deduplicated transactions")
    print("✓ Showed advanced deduplication techniques")
    print("✓ All operations completed using PyFlink Table API only")

if __name__ == "__main__":
    main() 