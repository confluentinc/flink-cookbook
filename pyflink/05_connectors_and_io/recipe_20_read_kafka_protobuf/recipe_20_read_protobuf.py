#!/usr/bin/env python3
"""
Reading Protocol Buffers with Apache Flink (UDF Deserialization)

This recipe demonstrates how to consume Protobuf-encoded event data from an Apache Kafka® 
topic and process it using PyFlink Table API with a Python UDF for protobuf deserialization.

The recipe:
1. Connects to Kafka topic 'transactions' containing Protobuf-encoded records
2. Reads raw bytes from Kafka
3. Uses a Python UDF to deserialize protobuf bytes into columns
4. Processes the data using SQL queries
5. Prints the decoded data to screen

This is a Python version of the Java read-protobuf recipe, using a pure Python approach.
"""

import os
import sys
from pathlib import Path

# Add the proto_data directory to the path for imports
sys.path.insert(0, str(Path(__file__).parent / "proto_data"))

from pyflink.table import EnvironmentSettings, StreamTableEnvironment, DataTypes, Row
from pyflink.table.udf import udf
from pyflink.common import Types
from pyflink.datastream.functions import MapFunction

# Import the generated protobuf class
from transaction_pb2 import Transaction

# Constants
TRANSACTION_TOPIC = "transactions"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
GROUP_ID = "ReadProtobuf-Job"

class TransactionPrinter(MapFunction):
    """
    Map function to print transaction data in a readable format.
    """
    def map(self, row):
        return f"Transaction: ID={row[1]}, Customer={row[2]}, Amount=${row[3]:.2f}, Time={row[0]}"

# UDF to parse protobuf bytes into columns
def transaction_row_type():
    return DataTypes.ROW([
        DataTypes.FIELD("t_time", DataTypes.STRING()),
        DataTypes.FIELD("t_id", DataTypes.BIGINT()),
        DataTypes.FIELD("t_customer_id", DataTypes.BIGINT()),
        DataTypes.FIELD("t_amount", DataTypes.DOUBLE()),
    ])

@udf(result_type=transaction_row_type())
def parse_transaction(raw_bytes: bytes):
    txn = Transaction()
    txn.ParseFromString(raw_bytes)
    return Row(txn.t_time, txn.t_id, txn.t_customer_id, txn.t_amount)

def create_table_environment():
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = StreamTableEnvironment.create(environment_settings=settings)
    return table_env

def define_kafka_raw_table(table_env):
    table_env.execute_sql(f'''
        CREATE TABLE kafka_raw (
            raw_bytes BYTES
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{TRANSACTION_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'properties.group.id' = '{GROUP_ID}',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'raw'
        )
    ''')

def run_workflow(table_env):
    print("Setting up Table API workflow with Python UDF protobuf deserializer...")
    
    # Register the UDF
    table_env.create_temporary_function("parse_transaction", parse_transaction)
    
    # Define the Kafka table
    define_kafka_raw_table(table_env)
    
    # Use the UDF to parse the raw bytes
    result_table = table_env.sql_query('''
        SELECT
            parsed.t_time,
            parsed.t_id,
            parsed.t_customer_id,
            parsed.t_amount
        FROM (
            SELECT parse_transaction(raw_bytes) AS parsed
            FROM kafka_raw
        )
    ''')
    
    print("Table API workflow configured. Starting execution...")
    # Execute and print the results
    result_table.execute().print()

def main():
    print("=== Reading Protocol Buffers with Apache Flink (Python UDF Deserializer) ===")
    print(f"Connecting to Kafka topic: {TRANSACTION_TOPIC}")
    print(f"Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print()
    
    # Create table environment and set pipeline.jars
    table_env = create_table_environment()

    # Set pipeline.jars so Flink loads the required connectors
    jars_dir = Path(__file__).parent / "jars"
    jars = [
        str(jars_dir / "flink-connector-kafka-4.0.0-2.0.jar"),
        str(jars_dir / "kafka-clients-3.4.0.jar"),
    ]
    table_env.get_config().set("pipeline.jars", ";".join([f"file://{j}" for j in jars]))

    run_workflow(table_env)

if __name__ == "__main__":
    main() 