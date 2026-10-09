# Reading JSON Data from Kafka with PyFlink Table API

This recipe demonstrates how to consume JSON-encoded event data from Apache Kafka® and process it using PyFlink Table API with built-in JSON format support.

## Overview

This recipe shows how to:
1. Use Flink's native JSON format for automatic deserialization
2. Read JSON data from Kafka using PyFlink Table API
3. Process structured JSON data with nested objects and arrays
4. Apply SQL queries to filter and transform JSON data

## Project Structure

```
05_connectors_and_io/
├── recipe_21_kafka_connector.py     # Main PyFlink recipe (JSON format)
├── generate_json_data.py            # Data generator script
├── setup_json_recipe.py             # Setup script for JAR dependencies
├── tools/
│   ├── download_jars.py             # JAR download utility
│   └── start_kafka.py               # Kafka startup tool
├── jars/                           # Downloaded JAR files (created by setup)
│   ├── flink-connector-kafka-4.0.0-2.0.jar
│   ├── kafka-clients-3.0.0.jar
│   ├── flink-json-1.18.0.jar
│   └── classpath.txt
└── README_json_recipe.md           # This file
```

## Prerequisites

1. **uv**
2. **Python 3.11+**
3. **PyFlink** and related dependencies
4. **confluent-kafka** for data generation

## Installation

1. **Install dependencies:**
   ```bash
   uv add apache-flink confluent-kafka
   ```

2. **Run the setup script:**
   ```bash
   python setup_json_recipe.py
   ```
   This will download the required Flink JAR files for Kafka and JSON format support.

3. **Start Kafka environment:**
   ```bash
   python tools/start_kafka.py
   ```

## Usage

### Step 1: Generate Test Data

Start the data generator to create JSON events and send them to Kafka:

```bash
python generate_json_data.py
```

This script will:
- Generate random event data with nested JSON structures
- Serialize it as JSON messages
- Send the messages to the `events` Kafka topic
- Print confirmation messages for each sent event

Press `Ctrl+C` to stop the data generator.

### Step 2: Process Data with PyFlink

Then run the PyFlink recipe to consume and process the JSON data:

```bash
python recipe_21_kafka_connector.py
```

This will:
- Connect to the `events` Kafka topic
- Use Flink's built-in JSON format for automatic deserialization
- Process the data using SQL queries
- Print the decoded event information

## JSON Schema

The recipe processes JSON events with the following structure:

```json
{
  "event_id": "uuid-string",
  "event_type": "page_view|click|purchase|login|logout",
  "user_id": "user_1234",
  "timestamp": "2024-01-01T12:00:00",
  "data": {
    "value": 123.45,
    "category": "electronics|clothing|books|food|sports",
    "tags": ["urgent", "featured", "new"]
  }
}
```

## Configuration

### Kafka Configuration

The recipe connects to Kafka with these default settings:
- **Bootstrap servers:** `localhost:9092`
- **Topic:** `events`
- **Group ID:** `ReadJSON-Job`
- **Startup mode:** `earliest-offset`
- **Format:** `json` (automatic JSON deserialization)

### PyFlink Table Configuration

The recipe uses PyFlink's Table API with built-in JSON format support:

```sql
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
    'topic' = 'events',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'ReadJSON-Job',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
)
```

The JSON format approach provides:
- **Simplicity**: No custom UDFs required
- **Performance**: Native Flink JSON parsing
- **Type Safety**: Structured schema definition
- **Flexibility**: Support for nested objects and arrays

## Data Flow

1. **Data Generation** (`generate_json_data.py`):
   - Creates random event data with nested JSON structures
   - Serializes as JSON messages
   - Sends to Kafka topic using `confluent-kafka` producer

2. **Data Processing** (`recipe_21_kafka_connector.py`):
   - Reads JSON from Kafka topic using Table API
   - Uses Flink's built-in JSON format for deserialization
   - Processes with SQL queries
   - Prints formatted output

## Example Output

When running the recipe, you should see output like:

```
=== Reading JSON Data from Apache Kafka with PyFlink Table API ===
Connecting to Kafka topic: events
Kafka bootstrap servers: localhost:9092

Setting up Table API workflow with JSON format...
Table API workflow configured. Starting execution...
+----+--------------------------------------+-----------+----------+-------------------------+-------+----------+------------------+
| op |                            event_id | event_type | user_id |                timestamp | value | category |             tags |
+----+--------------------------------------+-----------+----------+-------------------------+-------+----------+------------------+
| +I | 550e8400-e29b-41d4-a716-446655440000 | page_view | user_1234 | 2024-01-01 12:00:00.000 | 456.78 | electronics | [urgent, featured] |
| +I | 550e8400-e29b-41d4-a716-446655440001 | purchase  | user_5678 | 2024-01-01 12:00:01.000 | 234.56 | clothing  | [sale, new]      |
| +I | 550e8400-e29b-41d4-a716-446655440002 | click     | user_9012 | 2024-01-01 12:00:02.000 | 789.12 | books     | [trending]        |
...
```

## Troubleshooting

### Common Issues

1. **Kafka Connection Error:**
   - Ensure Kafka is running on `localhost:9092`
   - Check that the `events` topic exists

2. **JSON Format Error:**
   - Verify JSON schema matches the table definition
   - Check that JSON messages are valid

3. **PyFlink Import Error:**
   - Install PyFlink: `uv add apache-flink`
   - Check Python version compatibility

4. **Kafka Connector Missing Error:**
   - Run `python setup_json_recipe.py` to download required JAR files
   - Ensure the JAR files are in the `jars/` directory

5. **JSON Schema Mismatch:**
   - Verify the JSON structure matches the table schema
   - Check for required vs optional fields
   - Ensure data types match (e.g., numbers vs strings)

### Debug Mode

To see more detailed output, you can modify the scripts to include debug logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Extending the Recipe

### Adding New Fields

To add new fields to the JSON schema:

1. Update the table definition in `recipe_21_kafka_connector.py`
2. Update the data generator in `generate_json_data.py`
3. Modify SQL queries as needed

### Using Different JSON Structures

The recipe can be extended to support different JSON structures by:
- Modifying the table schema definition
- Updating the data generator
- Adjusting SQL queries for the new structure

### Performance Optimization

For better performance with large datasets:
- Consider using batch processing
- Implement JSON schema validation
- Use optimized JSON parsing libraries
- Consider partitioning strategies

## Comparison with Protobuf Recipe

This JSON recipe differs from the protobuf recipe in several ways:

| Aspect | JSON Recipe | Protobuf Recipe |
|--------|-------------|-----------------|
| **Format** | Built-in JSON format | Custom UDF deserialization |
| **Schema** | Flexible JSON schema | Strict protobuf schema |
| **Performance** | Native Flink parsing | Python UDF processing |
| **Complexity** | Simple setup | Requires protobuf generation |
| **Flexibility** | Dynamic schema | Schema-first approach |

Both approaches are valid depending on your use case and requirements. 