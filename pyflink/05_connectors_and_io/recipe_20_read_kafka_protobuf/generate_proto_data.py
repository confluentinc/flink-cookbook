#!/usr/bin/env python3
"""
Script to generate protobuf transaction data and send it to Kafka.
This simulates the Java TransactionSupplier functionality.
"""

import time
import random
import json
from datetime import datetime
from confluent_kafka import Producer, KafkaError
import subprocess
import sys
import os

# Import the generated protobuf class
try:
    from proto_data.transaction_pb2 import Transaction
except ImportError:
    print("Protobuf classes not found. Generating them first...")
    # Generate protobuf classes
    subprocess.run([
        "python", "-m", "grpc_tools.protoc",
        "--python_out=proto_data",
        "--proto_path=proto_data",
        "proto_data/transaction.proto"
    ], check=True)
    from proto_data.transaction_pb2 import Transaction

def create_transaction(transaction_id):
    """
    Create a transaction protobuf message.
    
    Args:
        transaction_id: Unique ID for the transaction
        
    Returns:
        Transaction protobuf message
    """
    transaction = Transaction()
    transaction.t_time = datetime.now().isoformat()
    transaction.t_id = transaction_id
    transaction.t_customer_id = random.randint(0, 24)  # 25 customers (0-24)
    transaction.t_amount = round(random.uniform(0, 1000), 2)
    
    return transaction

def delivery_report(err, msg):
    """
    Delivery report callback for Kafka producer.
    
    Args:
        err: Error object if delivery failed
        msg: Message object if delivery succeeded
    """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Transaction sent to {msg.topic()} "
              f"[partition {msg.partition()}, offset {msg.offset()}]")

def send_to_kafka(producer, topic, transaction, key=None):
    """
    Send transaction to Kafka topic.
    
    Args:
        producer: Producer instance
        topic: Kafka topic name
        transaction: Transaction protobuf message
        key: Optional message key
    """
    try:
        # Serialize the protobuf message
        value = transaction.SerializeToString()
        
        # Prepare key
        key_bytes = key.encode('utf-8') if key else None
        
        # Send to Kafka
        producer.produce(
            topic=topic,
            key=key_bytes,
            value=value,
            callback=delivery_report
        )
        
        # Trigger any available delivery reports
        producer.poll(0)
        
    except Exception as e:
        print(f"Failed to send transaction {transaction.t_id}: {e}")

def main():
    """Main function to generate and send transaction data."""
    
    # Kafka configuration
    kafka_config = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'protobuf-producer'
    }
    topic_name = "transactions"
    
    # Create Kafka producer
    try:
        producer = Producer(kafka_config)
        print(f"Connected to Kafka at {kafka_config['bootstrap.servers']}")
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        print("Make sure Kafka is running on localhost:9092")
        return
    
    # Generate and send transactions
    transaction_id = 0
    print(f"Starting to generate transactions and send to topic '{topic_name}'...")
    print("Press Ctrl+C to stop")
    
    try:
        while True:
            # Create transaction
            transaction = create_transaction(transaction_id)
            
            # Send to Kafka
            send_to_kafka(producer, topic_name, transaction, f"key_{transaction_id}")
            
            transaction_id += 1
            
            # Wait a bit between transactions
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nStopping transaction generation...")
    finally:
        # Wait for any outstanding messages to be delivered
        producer.flush()
        print("Kafka producer closed")

if __name__ == "__main__":
    main() 