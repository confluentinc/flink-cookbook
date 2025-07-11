# Reading Google Protocol Buffers with PyFlink

This recipe demonstrates how to consume Protobuf-encoded event data from Apache Kafka® and process it using PyFlink Table API with Python UDFs for protobuf deserialization.

## Overview

This recipe shows how to:
1. Generate protobuf classes from a `.proto` file
2. Create test data and send it to Kafka
3. Read raw protobuf bytes from Kafka using PyFlink Table API
4. Use Python UDFs to deserialize protobuf data into structured columns
5. Process the data using SQL queries

## Project Structure

```
read-protobuf/
├── proto_data/
│   └── transaction.proto          # Protobuf schema definition
├── recipe_20_read_protobuf.py     # Main PyFlink recipe (UDF-based deserialization)
├── generate_proto_data.py         # Data generator script
├── setup_proto.py                 # Protobuf setup script
├── setup_jars.py                  # JAR dependencies setup script
├── start_kafka.sh                 # Kafka startup script (macOS/Linux)
├── start_kafka.bat                # Kafka startup script (Windows)
├── pyproject.toml                # Python project configuration
├── jars/                         # Downloaded JAR files (created by setup)
│   ├── flink-connector-kafka-4.0.0-2.0.jar
│   ├── kafka-clients-3.4.0.jar
│   └── classpath.txt
├── set_classpath.sh              # Environment setup script (created by setup)
└── README.md                     # This file
```

**Note**: The `tools/` directory containing the JAR download utility is located at `flink-cookbook/pyflink/tools/` for reuse across multiple recipes.

## Prerequisites

1. **uv**
2. **Python 3.11+**
3. **Podman** (will be automatically installed by the startup script)
4. **PyFlink** and related dependencies

## Installation

1. **Install dependencies:**
   ```bash
   uv add -e .
   ```

2. **Run the setup script:**
   ```bash
   uv run setup_proto.py
   ```

3. **Setup JAR dependencies:**
   ```bash
   python setup_jars.py
   ```
   This will download the required Flink JAR files for Kafka connector support.

4. **Start Kafka environment:**
   ```bash
   # On macOS/Linux:
   ./start_kafka.sh
   
   # On Windows:
   start_kafka.bat
   ```

## Usage

### Step 1: Generate Test Data

Start the data generator to create protobuf transactions and send them to Kafka:

```bash
uv run generate_proto_data.py
```

This script will:
- Generate random transaction data
- Serialize it as protobuf messages
- Send the messages to the `transactions` Kafka topic
- Print confirmation messages for each sent transaction

Press `Ctrl+C` to stop the data generator.

### Step 2: Process Data with PyFlink

Then run the PyFlink recipe to consume and process the protobuf data:

```bash
uv run recipe_20_read_protobuf.py
```

This will:
- Connect to the `transactions` Kafka topic
- Read raw protobuf bytes from Kafka
- Use Python UDFs to deserialize protobuf messages into structured columns
- Process the data using SQL queries
- Print the decoded transaction information

## Protobuf Schema

The recipe uses the following protobuf schema (`proto_data/transaction.proto`):

```protobuf
syntax = "proto3";

package transaction;

message Transaction {
  optional string t_time = 1;
  optional int64 t_id = 2;
  optional int64 t_customer_id = 3;
  optional double t_amount = 4;
}
```

## Configuration

### Kafka Configuration

The recipe connects to Kafka with these default settings:
- **Bootstrap servers:** `localhost:9092`
- **Topic:** `transactions`
- **Group ID:** `ReadProtobuf-Job`
- **Startup mode:** `earliest-offset`
- **Format:** `raw` (reads raw bytes for UDF processing)

### PyFlink UDF Configuration

The recipe uses PyFlink's Table API with Python UDFs for protobuf deserialization:

```python
# UDF to parse protobuf bytes into columns
@udf(result_type=transaction_row_type())
def parse_transaction(raw_bytes: bytes):
    txn = Transaction()
    txn.ParseFromString(raw_bytes)
    return Row(txn.t_time, txn.t_id, txn.t_customer_id, txn.t_amount)
```

The UDF approach provides:
- **Flexibility**: Custom deserialization logic
- **Type Safety**: Structured return types with PyFlink DataTypes
- **Performance**: Direct protobuf parsing in Python
- **Debugging**: Easy to inspect and modify deserialization logic

## Data Flow

1. **Data Generation** (`generate_proto_data.py`):
   - Creates random transaction data
   - Serializes as protobuf messages
   - Sends to Kafka topic using `confluent-kafka` producer

2. **Data Processing** (`recipe_20_read_protobuf.py`):
   - Reads raw bytes from Kafka topic using Table API
   - Uses Python UDF to deserialize protobuf messages
   - Processes with SQL queries
   - Prints formatted output

## Example Output

When running the recipe, you should see output like:

```
=== Reading Protocol Buffers with Apache Flink (Python UDF Deserializer) ===
Connecting to Kafka topic: transactions
Kafka bootstrap servers: locallocalhost:9092

Setting up Table API workflow with Python UDF protobuf deserializer...
Table API workflow configured. Starting execution...
+----+----------------------+------+---------------+----------+
| op |               t_time | t_id | t_customer_id | t_amount |
+----+----------------------+------+---------------+----------+
| +I | 2024-01-01T12:00:00 |    1 |            12 |   456.78 |
| +I | 2024-01-01T12:00:01 |    2 |             8 |   234.56 |
| +I | 2024-01-01T12:00:02 |    3 |            19 |   789.12 |
...
```

## Troubleshooting

### Common Issues

1. **Kafka Connection Error:**
   - Ensure Kafka is running on `localhost:9092`
   - Check that the `transactions` topic exists

2. **Protobuf Import Error:**
   - Run `python setup_proto.py` to generate the protobuf classes
   - Ensure `grpcio-tools` is installed

3. **PyFlink Import Error:**
   - Install PyFlink: with `uv pip install apache-flink` or `uv add apache-flink`
   - Check Python version compatibility

4. **Kafka Connector Missing Error:**
   - Run `python setup_jars.py` to download required JAR files
   - Source the classpath: `source set_classpath.sh`
   - Ensure the JAR files are in the `jars/` directory

5. **UDF Serialization Error:**
   - Ensure UDF returns `Row` objects, not tuples
   - Check that the return type matches the UDF definition
   - Verify protobuf message parsing logic

6. **Classpath Issues:**
   - Verify `set_classpath.sh` exists and is executable
   - Check that all JAR files are downloaded in the `jars/` directory
   - Ensure the classpath environment variable is set correctly

### Debug Mode

To see more detailed output, you can modify the scripts to include debug logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Extending the Recipe

### Adding New Fields

To add new fields to the protobuf schema:

1. Update `proto_data/transaction.proto`
2. Regenerate classes: `python setup_proto.py`
3. Update the UDF return type in `recipe_20_read_protobuf.py`
4. Update the data generator in `generate_proto_data.py`

### Using Different Serialization Formats

The recipe can be extended to support other serialization formats by creating additional UDFs for different deserialization logic.

### Performance Optimization

For better performance with large datasets:
- Consider using batch processing UDFs
- Implement caching for frequently accessed protobuf schemas
- Use optimized protobuf parsing libraries