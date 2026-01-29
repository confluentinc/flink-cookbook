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
- **06_general_recipes/**: Advanced general-purpose recipes and patterns
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

### 2. Setup the environment

```bash
uv venv --python 3.11
```

### 3. Install Dependencies

```bash
# Install PyFlink and other dependencies
uv add apache-flink pandas pyarrow confluent-kafka
# or uv pip sync

# or 
source .venv/bin/activate
uv pip install apache-flink pandas pyarrow confluent-kafka
```

## Running the Recipes

### Core Operations

```bash
# Basic table operations
uv run 00_setting_the_environment/recipe_01_setting_up_the_environment.py
```

### Connectors and I/O

The connectors and I/O section contains recipes for working with various data sources and has additional requirements that will need to be set up:

#### Protocol Buffers with Kafka (Recipe 19)

```bash
# Setup
cd 05_connectors_and_io/recipe_20_read_kafka_protobuf
uv run setup_proto.py
uv run setup_jars.py

# Start Kafka
uv run ../../tools/start_kafka.py

# Generate data and run recipe
uv run generate_proto_data.py &&
uv run recipe_20_read_protobuf.py
```

#### JSON with Kafka (Recipe 20)

```bash
# Setup
cd 05_connectors_and_io
uv run setup_json_recipe.py

# Start Kafka
uv run tools/start_kafka.py

# Generate data and run recipe
uv run generate_json_data.py
uv run recipe_21_kafka_connector.py
```

#### Filesystem Connector (Recipe 21)

```bash
cd 05_connectors_and_io
uv run recipe_22_filesystem_connector.py
```

#### S3 Parquet Connector (Recipe 22)

```bash
cd 05_connectors_and_io
uv run setup_s3_parquet_recipe.py
uv run recipe_23_s3_parquet_connector.py
```

### General Recipes

#### Deduplicated Join (Recipe 23)

```bash
cd 06_general_recipes/recipe_24_deduplicated_join
uv run recipe_24_deduplicated_join.py
```

## Connectors and I/O Overview

The `05_connectors_and_io/` section demonstrates various data source integrations:

### Data Formats

| Format | Recipe | Approach | Use Case |
|--------|--------|----------|----------|
| **Protocol Buffers** | Recipe 19 | Custom UDF | Strict schema validation |
| **JSON** | Recipe 20 | Built-in format | Flexible schema processing |
| **Files** | Recipe 21 | Filesystem connector | Local file processing |
| **Parquet** | Recipe 22 | Columnar format | Cloud analytics with S3 |

### Connector Types

- **Kafka Connector**: Streaming data from Apache Kafka
- **Filesystem Connector**: Local file processing
- **S3 Connector**: Cloud storage with Amazon S3

### General Recipes Overview

The `06_general_recipes/` section contains advanced patterns and techniques:

| Recipe | Pattern | Description |
|--------|---------|-------------|
| **Recipe 23** | Deduplicated Join | Advanced join operations with deduplication techniques |

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


### Setting up the Environment (00_setting_the_environment/)
1. **Setting up the Environment**: Basic environment configuration
2. **Using Catalogs and Databases**: Working with catalogs and databases in Flink
3. **Adding JARs to Path**: Managing JAR dependencies for connectors

### Core Operations (01_core_operations/)
1. **Creating Tables**: from_elements, datagen, from_pandas
2. **Essential Transformations**: select, filter, alias, column expressions
3. **Aggregating Data**: group_by with built-in aggregate functions
4. **Deduplication**: Using distinct() for data deduplication

### User-Defined Functions (02_udfs/)
5. **Scalar UDFs**: Custom row-wise transformations
6. **Table UDFs (UDTFs)**: Generating multiple rows from single input
7. **Aggregate UDFs (UDAFs)**: Custom aggregation logic
8. **Custom Deserialization**: Building custom deserializers for data formats
9. **Flattening Nested JSON**: Processing complex JSON structures
10. **PII Data Masking**: Protecting sensitive information in data streams
11. **Data Validation and Quarantine**: Implementing data quality checks
12. **Sentiment Analysis and Categorization**: Text processing and classification

### Windowing (03_windowing/)
13. **Tumbling Windows**: Fixed-size, non-overlapping windows
14. **Sliding Windows**: Fixed-size, overlapping windows
15. **Session Windows**: Activity-based windows with inactivity gaps

### Joins (04_joins/)
16. **Relational Joins**: Inner, Left, Right, Full Outer joins
17. **Interval Joins**: Time-bound joins for streaming data
18. **Temporal Table Joins**: Point-in-time lookups against versioned tables

### Connectors and I/O (05_connectors_and_io/)
19. **Protocol Buffers with Kafka**: Custom UDF deserialization
20. **JSON with Kafka**: Built-in JSON format processing
21. **Filesystem Connector**: Local file reading and writing
22. **S3 Parquet Connector**: Cloud storage with columnar format

### General Recipes (06_general_recipes/)
23. **Deduplicated Join**: Advanced join operations with deduplication techniques

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
