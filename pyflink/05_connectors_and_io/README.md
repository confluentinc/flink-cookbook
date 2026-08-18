# PyFlink Connectors and I/O Recipes

This section contains recipes demonstrating how to use various connectors and I/O operations with PyFlink Table API.

## Overview

These recipes show how to:
1. Read from and write to different data sources using PyFlink Table API
2. Use built-in formats and custom UDFs for data processing
3. Handle various data formats including JSON, Protocol Buffers, and files
4. Process both streaming and batch data

## Recipes

### 1. Protocol Buffers with Kafka (Recipe 20)

**File:** `recipe_20_read_kafka_protobuf/recipe_20_read_protobuf.py`

Demonstrates reading Protocol Buffer-encoded data from Kafka using custom Python UDFs for deserialization.

**Key Features:**
- Custom UDF-based protobuf deserialization
- Schema-first approach with strict typing
- High performance with Python UDF processing
- Complex nested data structures

**Use Case:** When you need strict schema validation and high-performance protobuf processing.

### 2. JSON with Kafka (Recipe 21)

**File:** `recipe_21_kafka_connector.py`

Demonstrates reading JSON-encoded data from Kafka using Flink's built-in JSON format.

**Key Features:**
- Native Flink JSON format support
- Automatic JSON deserialization
- Support for nested objects and arrays
- Simple setup without custom UDFs

**Use Case:** When you need flexible JSON processing with minimal setup.

### 3. Filesystem Connector (Recipe 22)

**File:** `recipe_22_filesystem_connector.py`

Demonstrates reading from and writing to the filesystem using PyFlink's filesystem connector.

**Key Features:**
- Local filesystem processing
- JSON file reading and writing
- Batch and streaming processing modes
- File monitoring capabilities

**Use Case:** When you need to process files locally or in batch mode.

### 4. S3 Parquet Connector (Recipe 23)

**File:** `recipe_23_s3_parquet_connector.py`

Demonstrates reading from and writing to Amazon S3 using PyFlink's filesystem connector with Parquet format.

**Key Features:**
- Cloud storage processing with S3
- Columnar Parquet format for efficient analytics
- Automatic local fallback when S3 credentials are not available
- Support for S3 authentication and configuration

**Use Case:** When you need to process large datasets in the cloud with columnar storage format.

## Project Structure

```
05_connectors_and_io/
├── recipe_20_read_kafka_protobuf/     # Protobuf Kafka recipe
│   ├── recipe_20_read_protobuf.py
│   ├── generate_proto_data.py
│   ├── setup_proto.py
│   ├── setup_jars.py
│   ├── proto_data/
│   │   └── transaction.proto
│   ├── jars/
│   └── README.md
├── recipe_21_kafka_connector.py       # JSON Kafka recipe
├── generate_json_data.py              # JSON data generator
├── setup_json_recipe.py               # JSON recipe setup
├── recipe_22_filesystem_connector.py  # Filesystem recipe
├── recipe_23_s3_parquet_connector.py # S3 Parquet recipe
├── tools/
│   ├── download_jars.py               # JAR download utility
│   └── start_kafka.py                 # Kafka startup tool
├── jars/                             # Shared JAR files
├── pyproject.toml                    # Python dependencies
├── README.md                         # This file
├── README_json_recipe.md             # JSON recipe documentation
├── README_filesystem_recipe.md       # Filesystem recipe documentation
└── README_s3_parquet_recipe.md      # S3 Parquet recipe documentation
```

## Prerequisites

1. **uv** - Python package manager
2. **Python 3.11+**
3. **PyFlink** and related dependencies
4. **confluent-kafka** (for data generation)

## Installation

### 1. Install Dependencies

```bash
# Install PyFlink and other dependencies
uv add apache-flink confluent-kafka
```

### 2. Setup Tools

The recipes use shared tools for downloading JARs and starting Kafka:

```bash
# Download required JAR files
python tools/download_jars.py jars/ all

# Start Kafka environment (if needed for Kafka recipes)
python tools/start_kafka.py
```

### 3. Setup Individual Recipes

Each recipe may have its own setup requirements:

```bash
# Setup protobuf recipe
cd recipe_20_read_kafka_protobuf
python setup_proto.py
python setup_jars.py

# Setup JSON recipe
python setup_json_recipe.py
```

## Usage

### Protocol Buffers Recipe

1. **Start Kafka:**
   ```bash
   python tools/start_kafka.py
   ```

2. **Generate protobuf data:**
   ```bash
   cd recipe_20_read_kafka_protobuf
   python generate_proto_data.py
   ```

3. **Run the recipe:**
   ```bash
   python recipe_20_read_protobuf.py
   ```

### JSON Recipe

1. **Start Kafka:**
   ```bash
   python tools/start_kafka.py
   ```

2. **Generate JSON data:**
   ```bash
   python generate_json_data.py
   ```

3. **Run the recipe:**
   ```bash
   python recipe_21_kafka_connector.py
   ```

### Filesystem Recipe

1. **Run the recipe:**
   ```bash
   python recipe_22_filesystem_connector.py
   ```

   This will automatically create sample data and process it.

### S3 Parquet Recipe

1. **Setup the recipe:**
   ```bash
   python setup_s3_parquet_recipe.py
   ```

2. **Run the recipe:**
   ```bash
   python recipe_23_s3_parquet_connector.py
   ```

   This will automatically create sample Parquet data and process it. Set AWS credentials for S3 access.

## Data Formats Comparison

| Format | Recipe | Approach | Pros | Cons |
|--------|--------|----------|------|------|
| **Protocol Buffers** | Recipe 20 | Custom UDF | Strict schema, high performance | Complex setup, requires UDF |
| **JSON** | Recipe 21 | Built-in format | Simple setup, flexible schema | Less strict validation |
| **Files** | Recipe 22 | Filesystem connector | Local processing, batch support | Limited scalability |
| **Parquet** | Recipe 23 | Columnar format | Efficient analytics, compression | Requires specific tools |

## Connector Types

### Kafka Connector

Used in recipes 20 and 21 for streaming data processing:

```sql
CREATE TABLE kafka_table (
    -- schema definition
) WITH (
    'connector' = 'kafka',
    'topic' = 'topic_name',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json'  -- or 'raw' for custom UDFs
)
```

### Filesystem Connector

Used in recipe 22 for file-based processing:

```sql
CREATE TABLE filesystem_table (
    -- schema definition
) WITH (
    'connector' = 'filesystem',
    'path' = '/path/to/files',
    'format' = 'json'
)
```

### S3 Connector

Used in recipe 23 for cloud storage processing:

```sql
CREATE TABLE s3_table (
    -- schema definition
) WITH (
    'connector' = 'filesystem',
    'path' = 's3://bucket/path/',
    'format' = 'parquet'
)
```

## Format Support

### Built-in Formats

- **JSON**: Automatic parsing and serialization
- **CSV**: Comma-separated values
- **Parquet**: Columnar storage
- **Avro**: Schema-based serialization
- **ORC**: Optimized Row Columnar

### Custom Formats

- **Protocol Buffers**: Using custom UDFs
- **Custom binary formats**: Using raw format with UDFs
- **Domain-specific formats**: Using custom deserialization logic
- **Parquet**: Columnar storage format for analytics

## Performance Considerations

### Kafka Recipes

- **Protocol Buffers**: Best performance, strict schema
- **JSON**: Good performance, flexible schema
- **Batch size**: Adjust based on data volume
- **Parallelism**: Configure based on topic partitions

### Filesystem Recipe

- **File size**: Consider splitting large files
- **Format**: Use Parquet for better compression
- **Partitioning**: Implement for large datasets
- **Monitoring**: Use streaming mode for real-time processing

### S3 Parquet Recipe

- **S3 partitioning**: Use for large datasets
- **Parquet optimization**: Choose appropriate compression
- **Cost optimization**: Use appropriate S3 storage classes
- **Authentication**: Use IAM roles when possible

## Troubleshooting

### Common Issues

1. **Kafka Connection Errors:**
   - Ensure Kafka is running on `localhost:9092`
   - Check topic existence and permissions
   - Verify JAR files are properly loaded

2. **Format Errors:**
   - Verify data format matches table schema
   - Check for encoding issues
   - Validate JSON/protobuf syntax

3. **Performance Issues:**
   - Adjust batch sizes and parallelism
   - Monitor memory usage
   - Consider using optimized formats

4. **File System Errors:**
   - Check file permissions
   - Verify directory existence
   - Ensure sufficient disk space

5. **S3 Connection Errors:**
   - Verify AWS credentials are set correctly
   - Check S3 bucket permissions
   - Ensure the bucket exists and is accessible
   - Verify the region is correct

### Debug Mode

Enable debug logging for troubleshooting:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Extending the Recipes

### Adding New Connectors

1. **Download required JARs** using the tools
2. **Define table schema** matching your data format
3. **Configure connector properties** for your use case
4. **Test with sample data** before production use

### Adding New Formats

1. **Use built-in formats** when possible
2. **Create custom UDFs** for unsupported formats
3. **Implement proper error handling** for malformed data
4. **Add validation** for data quality

### Performance Optimization

1. **Use appropriate formats** for your data characteristics
2. **Configure parallelism** based on data volume
3. **Implement partitioning** for large datasets
4. **Monitor resource usage** and adjust accordingly

## Best Practices

1. **Schema Design**: Define clear, consistent schemas
2. **Error Handling**: Implement robust error handling
3. **Monitoring**: Add logging and metrics
4. **Testing**: Test with realistic data volumes
5. **Documentation**: Document schema changes and configurations

## Next Steps

After mastering these recipes, consider exploring:

1. **Advanced UDFs**: Custom aggregation and window functions
2. **Complex Joins**: Multi-table joins and lookups
3. **State Management**: Using state backends for complex processing
4. **Time-based Processing**: Event time and watermarks
5. **Sinks**: Writing to various output systems

These recipes provide a foundation for building production-ready PyFlink applications with various data sources and formats. 