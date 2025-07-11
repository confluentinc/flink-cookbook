#!/usr/bin/env python3
"""
Reading JSON Data from Apache Kafka with PyFlink Table API

This recipe demonstrates how to consume JSON-encoded event data from an Apache Kafka® 
topic and process it using PyFlink Table API with built-in JSON format support.

The recipe:
1. Connects to Kafka topic 'events' containing JSON-encoded records
2. Uses Flink's built-in JSON format for automatic deserialization
3. Processes the data using SQL queries
4. Prints the decoded data to screen

This recipe shows how to use Flink's native JSON format support without custom UDFs.
"""

import os
import sys
import json
import time
from pathlib import Path

from pyflink.table import EnvironmentSettings, StreamTableEnvironment, DataTypes
from pyflink.common import Types

# Constants
EVENTS_TOPIC = "events"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
GROUP_ID = "ReadJSON-Job"

def create_table_environment():
    """Create and configure the table environment."""
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = StreamTableEnvironment.create(environment_settings=settings)
    return table_env

def define_kafka_json_table(table_env):
    """Define the Kafka table with JSON format."""
    table_env.execute_sql(f'''
        CREATE TABLE kafka_events (
            event_id STRING,
            event_type STRING,
            user_id STRING,
            timestamp TIMESTAMP(3),
            data ROW<
                value DOUBLE,
                category STRING,
                tags ARRAY<STRING>
            >
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{EVENTS_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'properties.group.id' = '{GROUP_ID}',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    ''')

def run_workflow(table_env):
    """Run the main workflow."""
    print("Setting up Table API workflow with JSON format...")
    
    # Define the Kafka table
    define_kafka_json_table(table_env)
    
    # Process the JSON data with SQL queries
    result_table = table_env.sql_query('''
        SELECT 
            event_id,
            event_type,
            user_id,
            timestamp,
            data.value,
            data.category,
            data.tags
        FROM kafka_events
        WHERE data.value > 0
    ''')
    
    print("Table API workflow configured. Starting execution...")
    # Execute and print the results
    result_table.execute().print()

def main():
    """Main function."""
    print("=== Reading JSON Data from Apache Kafka with PyFlink Table API ===")
    print(f"Connecting to Kafka topic: {EVENTS_TOPIC}")
    print(f"Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print()
    
    # Create table environment and set pipeline.jars
    table_env = create_table_environment()

    # Set pipeline.jars so Flink loads the required connectors
    jars_dir = Path(__file__).parent / "jars"
    jars = [
        str(jars_dir / "flink-connector-kafka-4.0.0-2.0.jar"),
        str(jars_dir / "kafka-clients-3.0.0.jar"),
        str(jars_dir / "flink-json-1.18.0.jar"),
    ]
    table_env.get_config().set("pipeline.jars", ";".join([f"file://{j}" for j in jars]))

    run_workflow(table_env)

if __name__ == "__main__":
    main()
