#!/usr/bin/env python3
"""
Reading and Writing Parquet Files to/from S3 with PyFlink

This recipe demonstrates how to read from and write to Amazon S3 using PyFlink 
Table API with the filesystem connector and Parquet format.

The recipe:
1. Reads Parquet files from S3 using the filesystem connector
2. Processes the data using SQL queries
3. Writes the results back to S3 in Parquet format
4. Demonstrates S3 authentication and configuration

This recipe shows how to use Flink's filesystem connector with S3 and Parquet format.
"""

import os
import sys
import json
import time
from pathlib import Path

from pyflink.table import EnvironmentSettings, StreamTableEnvironment, DataTypes
from pyflink.common import Types

# Constants
S3_BUCKET = "pyflink-recipe-data"
S3_INPUT_PATH = f"s3://{S3_BUCKET}/input/"
S3_OUTPUT_PATH = f"s3://{S3_BUCKET}/output/"
LOCAL_INPUT_DIR = "s3_input_data"
LOCAL_OUTPUT_DIR = "s3_output_data"

def create_table_environment():
    """Create and configure the table environment."""
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = StreamTableEnvironment.create(environment_settings=settings)
    return table_env

def create_sample_parquet_data():
    """Create sample Parquet files for processing."""
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    
    input_path = Path(LOCAL_INPUT_DIR)
    input_path.mkdir(exist_ok=True)
    
    # Create sample data
    sample_data = [
        {
            "id": 1,
            "name": "Product A",
            "category": "electronics",
            "price": 299.99,
            "rating": 4.5,
            "tags": ["new", "featured"],
            "created_at": "2024-01-01T12:00:00"
        },
        {
            "id": 2,
            "name": "Product B", 
            "category": "clothing",
            "price": 89.99,
            "rating": 4.2,
            "tags": ["sale", "popular"],
            "created_at": "2024-01-01T12:00:01"
        },
        {
            "id": 3,
            "name": "Product C",
            "category": "books",
            "price": 24.99,
            "rating": 4.8,
            "tags": ["bestseller"],
            "created_at": "2024-01-01T12:00:02"
        },
        {
            "id": 4,
            "name": "Product D",
            "category": "electronics",
            "price": 599.99,
            "rating": 4.7,
            "tags": ["premium", "featured"],
            "created_at": "2024-01-01T12:00:03"
        },
        {
            "id": 5,
            "name": "Product E",
            "category": "clothing",
            "price": 129.99,
            "rating": 4.1,
            "tags": ["trending"],
            "created_at": "2024-01-01T12:00:04"
        }
    ]
    
    # Convert to DataFrame and write as Parquet
    df = pd.DataFrame(sample_data)
    
    # Write sample files
    for i in range(0, len(sample_data), 2):
        batch_df = df.iloc[i:i+2]
        file_path = input_path / f"products_batch_{i//2 + 1}.parquet"
        
        # Convert to PyArrow table and write as Parquet
        table = pa.Table.from_pandas(batch_df)
        pq.write_table(table, file_path)
    
    print(f"Created {len(sample_data)//2} sample Parquet files in {LOCAL_INPUT_DIR}/")

def define_s3_tables(table_env):
    """Define S3 tables for reading and writing Parquet files."""
    
    # Check if S3 credentials are available
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    
    if not aws_access_key or not aws_secret_key:
        print("AWS credentials not found. Using local filesystem for demonstration.")
        input_path = LOCAL_INPUT_DIR
        output_path = LOCAL_OUTPUT_DIR
    else:
        print("AWS credentials found. Using S3 for storage.")
        input_path = S3_INPUT_PATH
        output_path = S3_OUTPUT_PATH
    
    # Input table - reads Parquet files
    table_env.execute_sql(f'''
        CREATE TABLE input_products (
            id BIGINT,
            name STRING,
            category STRING,
            price DOUBLE,
            rating DOUBLE,
            tags ARRAY<STRING>,
            created_at TIMESTAMP(3)
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{input_path}',
            'format' = 'parquet'
        )
    ''')
    
    # Output table - writes processed data as Parquet
    table_env.execute_sql(f'''
        CREATE TABLE output_products (
            id BIGINT,
            name STRING,
            category STRING,
            price DOUBLE,
            rating DOUBLE,
            tags ARRAY<STRING>,
            created_at TIMESTAMP(3),
            price_category STRING,
            rating_category STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{output_path}',
            'format' = 'parquet'
        )
    ''')

def run_workflow(table_env):
    """Run the main workflow."""
    print("Setting up Table API workflow with S3 Parquet connector...")
    
    # Define the S3 tables
    define_s3_tables(table_env)
    
    # Process the data with SQL queries
    result_table = table_env.sql_query('''
        SELECT 
            id,
            name,
            category,
            price,
            rating,
            tags,
            created_at,
            CASE 
                WHEN price >= 500 THEN 'premium'
                WHEN price >= 100 THEN 'mid-range'
                ELSE 'budget'
            END AS price_category,
            CASE 
                WHEN rating >= 4.5 THEN 'excellent'
                WHEN rating >= 4.0 THEN 'good'
                WHEN rating >= 3.5 THEN 'average'
                ELSE 'poor'
            END AS rating_category
        FROM input_products
        WHERE price > 0 AND rating > 0
    ''')
    
    print("Table API workflow configured. Starting execution...")
    
    # Execute the query and write to output
    result_table.execute_insert('output_products')
    
    # Also print the results
    result_table.execute().print()

def main():
    """Main function."""
    print("=== Reading and Writing Parquet Files to/from S3 with PyFlink ===")
    print(f"Input path: {S3_INPUT_PATH if os.getenv('AWS_ACCESS_KEY_ID') else LOCAL_INPUT_DIR}")
    print(f"Output path: {S3_OUTPUT_PATH if os.getenv('AWS_ACCESS_KEY_ID') else LOCAL_OUTPUT_DIR}")
    print()
    
    # Create sample data if it doesn't exist
    if not Path(LOCAL_INPUT_DIR).exists() or not any(Path(LOCAL_INPUT_DIR).iterdir()):
        print("Creating sample Parquet data...")
        create_sample_parquet_data()
    
    # Create output directory
    Path(LOCAL_OUTPUT_DIR).mkdir(exist_ok=True)
    
    # Create table environment
    table_env = create_table_environment()

    run_workflow(table_env)

if __name__ == "__main__":
    main() 