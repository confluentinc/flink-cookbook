#!/usr/bin/env python3
"""
Reading and Writing Files with PyFlink Filesystem Connector

This recipe demonstrates how to read from and write to the filesystem using PyFlink 
Table API with the filesystem connector.

The recipe:
1. Reads JSON data from a local filesystem directory
2. Processes the data using SQL queries
3. Writes the results to a different directory
4. Demonstrates both batch and streaming modes

This recipe shows how to use Flink's filesystem connector for local file processing.
"""

import os
import sys
import json
import time
from pathlib import Path

from pyflink.table import EnvironmentSettings, StreamTableEnvironment, DataTypes
from pyflink.common import Types

# Constants
INPUT_DIR = "input_data"
OUTPUT_DIR = "output_data"
TEMP_DIR = "temp_data"

def create_table_environment():
    """Create and configure the table environment."""
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = StreamTableEnvironment.create(environment_settings=settings)
    return table_env

def create_sample_data():
    """Create sample JSON files for processing."""
    input_path = Path(INPUT_DIR)
    input_path.mkdir(exist_ok=True)
    
    # Create sample JSON files
    sample_data = [
        {
            "id": 1,
            "name": "Product A",
            "category": "electronics",
            "price": 299.99,
            "tags": ["new", "featured"]
        },
        {
            "id": 2,
            "name": "Product B", 
            "category": "clothing",
            "price": 89.99,
            "tags": ["sale", "popular"]
        },
        {
            "id": 3,
            "name": "Product C",
            "category": "books",
            "price": 24.99,
            "tags": ["bestseller"]
        }
    ]
    
    # Write sample files
    for i, data in enumerate(sample_data):
        file_path = input_path / f"product_{i+1}.json"
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)
    
    print(f"Created {len(sample_data)} sample files in {INPUT_DIR}/")

def define_filesystem_tables(table_env):
    """Define filesystem tables for reading and writing."""
    # Input table - reads JSON files
    table_env.execute_sql(f'''
        CREATE TABLE input_products (
            id BIGINT,
            name STRING,
            category STRING,
            price DOUBLE,
            tags ARRAY<STRING>
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{INPUT_DIR}',
            'format' = 'json'
        )
    ''')
    
    # Output table - writes processed data
    table_env.execute_sql(f'''
        CREATE TABLE output_products (
            id BIGINT,
            name STRING,
            category STRING,
            price DOUBLE,
            tags ARRAY<STRING>,
            price_category STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{OUTPUT_DIR}',
            'format' = 'json'
        )
    ''')

def run_workflow(table_env):
    """Run the main workflow."""
    print("Setting up Table API workflow with filesystem connector...")
    
    # Define the filesystem tables
    define_filesystem_tables(table_env)
    
    # Process the data with SQL queries
    result_table = table_env.sql_query('''
        SELECT 
            id,
            name,
            category,
            price,
            tags,
            CASE 
                WHEN price >= 200 THEN 'premium'
                WHEN price >= 50 THEN 'mid-range'
                ELSE 'budget'
            END AS price_category
        FROM input_products
        WHERE price > 0
    ''')
    
    print("Table API workflow configured. Starting execution...")
    
    # Execute the query and write to output
    result_table.execute_insert('output_products')
    
    # Also print the results
    result_table.execute().print()

def main():
    """Main function."""
    print("=== Reading and Writing Files with PyFlink Filesystem Connector ===")
    print(f"Input directory: {INPUT_DIR}")
    print(f"Output directory: {OUTPUT_DIR}")
    print()
    
    # Create sample data if it doesn't exist
    if not Path(INPUT_DIR).exists() or not any(Path(INPUT_DIR).iterdir()):
        print("Creating sample data...")
        create_sample_data()
    
    # Create output directory
    Path(OUTPUT_DIR).mkdir(exist_ok=True)
    
    # Create table environment
    table_env = create_table_environment()

    run_workflow(table_env)

if __name__ == "__main__":
    main()
