# PyFlink Table API Cookbook Recipes

This repository contains Python scripts for essential PyFlink Table API recipes, organized by category and demonstrating various data processing patterns.

## Structure

The repository is organized into directories based on the category of operations:

- **00_setting_the_environment/**: Environment setup and configuration
- **01_core_operations/**: Basic table creation, transformations, aggregations, and deduplication
- **02_udfs/**: User-Defined Functions (Scalar, Table, Aggregate)
- **03_windowing/**: Tumbling, Sliding, and Session window aggregations
- **04_joins/**: Relational, Interval, and Temporal joins
- **05_connectors_and_io/**: Connectors and I/O operations with various data sources
- **06_general_recipes/**: Additional general-purpose recipes
- **tools/**: Shared utilities for JAR downloads and environment setup

## Prerequisites

- **uv** - Modern Python package manager
- **Python 3.11+**
- **Java (OpenJDK) 8+**
- **Apache Flink**

## Installation

### 1. Install uv (if not already installed)

```bash
# On macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# On Windows
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### 2. Install Dependencies

```bash
# Install PyFlink and other dependencies
uv add apache-flink pandas pyarrow confluent-kafka
```

### 3. Setup Environment

```bash
# Activate the virtual environment
uv shell

# Or run commands directly with uv
uv run python --version
```

## Running the Recipes

### Core Operations

```bash
# Basic table operations
uv run python 01_core_operations/recipe_01_creating_tables.py
uv run python 01_core_operations/recipe_02_transformations.py
uv run python 01_core_operations/recipe_03_aggregations.py
uv run python 01_core_operations/recipe_04_deduplication.py
```

### User-Defined Functions

```bash
# UDF examples
uv run python 02_udfs/recipe_05_scalar_udfs.py
uv run python 02_udfs/recipe_06_table_udfs.py
uv run python 02_udfs/recipe_07_aggregate_udfs.py
```

### Windowing

```bash
# Window aggregations
uv run python 03_windowing/recipe_08_tumbling_windows.py
uv run python 03_windowing/recipe_09_sliding_windows.py
uv run python 03_windowing/recipe_10_session_windows.py
```

### Joins

```bash
# Join operations
uv run python 04_joins/recipe_11_relational_joins.py
uv run python 04_joins/recipe_12_interval_joins.py
uv run python 04_joins/recipe_13_temporal_joins.py
```

### Connectors and I/O

The connectors and I/O section contains recipes for working with various data sources:

#### Protocol Buffers with Kafka (Recipe 20)

```bash
# Setup
cd 05_connectors_and_io/recipe_20_read_kafka_protobuf
uv run python setup_proto.py
uv run python setup_jars.py

# Start Kafka
uv run python ../../tools/start_kafka.py

# Generate data and run recipe
uv run python generate_proto_data.py
uv run python recipe_20_read_protobuf.py
```

#### JSON with Kafka (Recipe 21)

```bash
# Setup
cd 05_connectors_and_io
uv run python setup_json_recipe.py

# Start Kafka
uv run python tools/start_kafka.py

# Generate data and run recipe
uv run python generate_json_data.py
uv run python recipe_21_kafka_connector.py
```

#### Filesystem Connector (Recipe 22)

```bash
cd 05_connectors_and_io
uv run python recipe_22_filesystem_connector.py
```

#### S3 Parquet Connector (Recipe 23)

```bash
cd 05_connectors_and_io
uv run python setup_s3_parquet_recipe.py
uv run python recipe_23_s3_parquet_connector.py
```

## Connectors and I/O Overview

The `05_connectors_and_io/` section demonstrates various data source integrations:

### Data Formats

| Format | Recipe | Approach | Use Case |
|--------|--------|----------|----------|
| **Protocol Buffers** | Recipe 20 | Custom UDF | Strict schema validation |
| **JSON** | Recipe 21 | Built-in format | Flexible schema processing |
| **Files** | Recipe 22 | Filesystem connector | Local file processing |
| **Parquet** | Recipe 23 | Columnar format | Cloud analytics with S3 |

### Connector Types

- **Kafka Connector**: Streaming data from Apache Kafka
- **Filesystem Connector**: Local file processing
- **S3 Connector**: Cloud storage with Amazon S3

### Shared Tools

- **`tools/download_jars.py`**: Downloads required JAR files for connectors
- **`tools/start_kafka.py`**: Cross-platform Kafka startup utility

## Important Notes

### JAR Dependencies

Recipes involving external systems require additional connector JARs:

1. **Automatic Download**: Use the setup scripts in each recipe directory
2. **Manual Setup**: Download JARs and place in Flink's `lib/` directory
3. **Configuration**: Set `pipeline.jars` in TableEnvironment

### External Systems

- **Kafka**: Ensure Kafka broker is running and topics exist
- **S3**: Set AWS credentials for cloud storage access
- **Filesystem**: Ensure output paths are writable

### Execution

Most scripts include `execute().print()` or `execute_insert().wait()` lines. Uncomment these to see output or execute the pipeline.

## Recipes Overview

### Core Operations (01_core_operations/)
1. **Creating Tables**: from_elements, datagen, from_pandas
2. **Transformations**: select, filter, alias, column expressions
3. **Aggregations**: group_by with built-in aggregate functions
4. **Deduplication**: Using distinct()

### User-Defined Functions (02_udfs/)
5. **Scalar UDFs**: Custom row-wise transformations
6. **Table UDFs (UDTFs)**: Generating multiple rows from single input
7. **Aggregate UDFs (UDAFs)**: Custom aggregation logic

### Windowing (03_windowing/)
8. **Tumbling Windows**: Fixed-size, non-overlapping windows
9. **Sliding Windows**: Fixed-size, overlapping windows
10. **Session Windows**: Activity-based windows with inactivity gaps

### Joins (04_joins/)
11. **Relational Joins**: Inner, Left, Right, Full Outer joins
12. **Interval Joins**: Time-bound joins for streaming data
13. **Temporal Table Joins**: Point-in-time lookups against versioned tables

### Connectors and I/O (05_connectors_and_io/)
14. **Protocol Buffers with Kafka**: Custom UDF deserialization
15. **JSON with Kafka**: Built-in JSON format processing
16. **Filesystem Connector**: Local file reading and writing
17. **S3 Parquet Connector**: Cloud storage with columnar format

## Development

### Adding New Recipes

1. Create a new directory in the appropriate category
2. Follow the naming convention: `recipe_XX_description.py`
3. Include comprehensive documentation
4. Add setup scripts for dependencies if needed

### Contributing

1. Use `uv` for dependency management
2. Follow the existing code structure and documentation style
3. Include setup scripts for external dependencies
4. Test with both local and cloud environments

## Troubleshooting

### Common Issues

1. **JAR Dependencies**: Use the provided setup scripts
2. **Kafka Connection**: Ensure Kafka is running and topics exist
3. **S3 Access**: Set AWS credentials for cloud storage
4. **Python Version**: Ensure Python 3.11+ is used

### Debug Mode

Enable debug logging for troubleshooting:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

This cookbook provides a comprehensive foundation for building production-ready PyFlink applications with various data sources and processing patterns.
