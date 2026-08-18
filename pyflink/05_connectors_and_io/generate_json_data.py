#!/usr/bin/env python3
"""
JSON Data Generator for PyFlink Recipes

This script generates sample JSON event data and sends it to Kafka
for testing PyFlink Table API recipes.
"""

import json
import time
import random
import uuid
from datetime import datetime, timedelta
from confluent_kafka import Producer
import sys

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
EVENTS_TOPIC = "events"

def create_producer():
    """Create Kafka producer."""
    return Producer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'json-data-generator'
    })

def generate_event():
    """Generate a random event."""
    event_types = ['page_view', 'click', 'purchase', 'login', 'logout']
    categories = ['electronics', 'clothing', 'books', 'food', 'sports']
    tags = ['urgent', 'featured', 'new', 'sale', 'trending', 'popular']
    
    event_id = str(uuid.uuid4())
    event_type = random.choice(event_types)
    user_id = f"user_{random.randint(1000, 9999)}"
    timestamp = datetime.now() - timedelta(seconds=random.randint(0, 3600))
    
    # Generate nested data structure
    data = {
        "value": round(random.uniform(10.0, 1000.0), 2),
        "category": random.choice(categories),
        "tags": random.sample(tags, random.randint(1, 3))
    }
    
    event = {
        "event_id": event_id,
        "event_type": event_type,
        "user_id": user_id,
        "timestamp": timestamp.isoformat(),
        "data": data
    }
    
    return event

def delivery_report(err, msg):
    """Delivery report callback."""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def main():
    """Main function to generate and send JSON events."""
    print("=== JSON Data Generator for PyFlink Recipes ===")
    print(f"Connecting to Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {EVENTS_TOPIC}")
    print("Press Ctrl+C to stop the generator")
    print()
    
    producer = create_producer()
    
    try:
        message_count = 0
        while True:
            # Generate event
            event = generate_event()
            message = json.dumps(event)
            
            # Send to Kafka
            producer.produce(
                EVENTS_TOPIC,
                message.encode('utf-8'),
                callback=delivery_report
            )
            
            # Flush to ensure delivery
            producer.poll(0)
            
            message_count += 1
            print(f"Generated event {message_count}: {event['event_type']} by {event['user_id']}")
            
            # Wait between messages
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nStopping JSON data generator...")
    finally:
        # Wait for any outstanding messages
        producer.flush()
        print("Generator stopped")

if __name__ == "__main__":
    main() 