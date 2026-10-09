# Reading and Writing Files with PyFlink Filesystem Connector

This recipe demonstrates how to read from and write to the filesystem using PyFlink Table API with the filesystem connector.

## Overview

This recipe shows how to:
1. Use Flink's filesystem connector for local file processing
2. Read JSON files from a directory using Table API
3. Process the data with SQL queries
4. Write results to a different directory
5. Demonstrate both batch and streaming processing modes

## Project Structure

```
05_connectors_and_io/
├── recipe_22_filesystem_connector.py  # Main PyFlink recipe (filesystem connector)
├── input_data/                        # Input directory with JSON files (created by recipe)
│   ├── product_1.json
│   ├── product_2.json
│   └── product_3.json
├── output_data/                       # Output directory for processed results (created by recipe)
│   └── *.json                        # Processed files
└── README_filesystem_recipe.md       # This file
```

## Prerequisites

1. **uv**
2. **Python 3.11+**
3. **PyFlink** and related dependencies

## Installation

1. **Install dependencies:**
   ```bash
   uv add apache-flink
   ```

## Usage

### Step 1: Run the Filesystem Recipe

Run the PyFlink recipe to process files:

```bash
python recipe_22_filesystem_connector.py
```

This will:
- Create sample JSON files in the `input_data/` directory (if they don't exist)
- Read the JSON files using the filesystem connector
- Process the data with SQL queries
- Write the results to the `output_data/` directory
- Print the processed data to the console

## Data Schema

The recipe processes JSON files with the following structure:

```json
{
  "id": 1,
  "name": "Product A",
  "category": "electronics",
  "price": 299.99,
  "tags": ["new", "featured"]
}
```

## Configuration

### Filesystem Configuration

The recipe uses the filesystem connector with these settings:
- **Input path:** `input_data/`
- **Output path:** `output_data/`
- **Format:** `json` (automatic JSON deserialization/serialization)
- **Processing mode:** Streaming

### PyFlink Table Configuration

The recipe uses PyFlink's Table API with filesystem connector:

```sql
-- Input table
CREATE TABLE input_products (
    id BIGINT,
    name STRING,
    category STRING,
    price DOUBLE,
    tags ARRAY<STRING>
) WITH (
    'connector' = 'filesystem',
    'path' = 'input_data',
    'format' = 'json'
)

-- Output table
CREATE TABLE output_products (
    id BIGINT,
    name STRING,
    category STRING,
    price DOUBLE,
    tags ARRAY<STRING>,
    price_category STRING
) WITH (
    'connector' = 'filesystem',
    'path' = 'output_data',
    'format' = 'json'
)
```

## Data Processing

The recipe applies the following transformations:

1. **Data Filtering:** Only processes products with price > 0
2. **Price Categorization:** Adds a `price_category` field based on price ranges:
   - `premium`: price >= $200
   - `mid-range`: price >= $50 and < $200
   - `budget`: price < $50

## Example Output

When running the recipe, you should see output like:

```
=== Reading and Writing Files with PyFlink Filesystem Connector ===
Input directory: input_data
Output directory: output_data

Creating sample data...
Created 3 sample files in input_data/

Setting up Table API workflow with filesystem connector...
Table API workflow configured. Starting execution...
+----+----+-----------+----------+-------+----------+-------------+
| op | id |      name | category | price |     tags | price_category |
+----+----+-----------+----------+-------+----------+-------------+
| +I |  1 | Product A | electronics | 299.99 | [new, featured] | premium |
| +I |  2 | Product B | clothing | 89.99 | [sale, popular] | mid-range |
| +I |  3 | Product C | books | 24.99 | [bestseller] | budget |
+----+----+-----------+----------+-------+----------+-------------+
```

The processed files will be written to the `output_data/` directory with the additional `price_category` field.

## Filesystem Connector Features

### Supported Formats

The filesystem connector supports various formats:
- **JSON**: Automatic JSON parsing and serialization
- **CSV**: Comma-separated values
- **Parquet**: Columnar storage format
- **Avro**: Schema-based serialization
- **ORC**: Optimized Row Columnar format

### Processing Modes

1. **Batch Mode**: Process all files at once
2. **Streaming Mode**: Monitor directory for new files (used in this recipe)

### File Monitoring

The filesystem connector can:
- Monitor directories for new files
- Process files as they arrive
- Handle file deletions and modifications
- Support various file naming patterns

## Troubleshooting

### Common Issues

1. **File Not Found Error:**
   - Ensure the input directory exists
   - Check file permissions
   - Verify file format matches the table schema

2. **JSON Format Error:**
   - Verify JSON files are valid
   - Check that JSON structure matches the table definition
   - Ensure data types match (e.g., numbers vs strings)

3. **Output Directory Error:**
   - Ensure write permissions to the output directory
   - Check available disk space

4. **PyFlink Import Error:**
   - Install PyFlink: `uv add apache-flink`
   - Check Python version compatibility

### Debug Mode

To see more detailed output, you can modify the script to include debug logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Extending the Recipe

### Adding New Fields

To add new fields to the JSON schema:

1. Update the table definitions in `recipe_22_filesystem_connector.py`
2. Modify the SQL query to include the new fields
3. Update the sample data generation if needed

### Using Different File Formats

The recipe can be extended to support different file formats by:
- Changing the `format` parameter in table definitions
- Adjusting the table schema for the new format
- Modifying data processing logic as needed

### Processing Large Files

For better performance with large files:
- Consider using batch processing mode
- Implement file partitioning strategies
- Use optimized file formats like Parquet
- Consider parallel processing configurations

## Comparison with Kafka Recipes

This filesystem recipe differs from the Kafka recipes in several ways:

| Aspect | Filesystem Recipe | Kafka Recipe |
|--------|-------------------|--------------|
| **Data Source** | Local filesystem | Apache Kafka |
| **Processing** | File-based | Stream-based |
| **Latency** | Higher (file-based) | Lower (real-time) |
| **Scalability** | Limited by filesystem | Highly scalable |
| **Use Case** | Batch processing | Real-time streaming |

Both approaches are valid depending on your data processing requirements and infrastructure. 