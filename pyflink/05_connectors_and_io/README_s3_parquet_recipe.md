# Reading and Writing Parquet Files to/from S3 with PyFlink

This recipe demonstrates how to read from and write to Amazon S3 using PyFlink Table API with the filesystem connector and Parquet format.

## Overview

This recipe shows how to:
1. Use Flink's filesystem connector with S3 for cloud storage
2. Read Parquet files from S3 using Table API
3. Process the data with SQL queries
4. Write results back to S3 in Parquet format
5. Handle S3 authentication and configuration
6. Fall back to local filesystem when S3 credentials are not available

## Project Structure

```
05_connectors_and_io/
├── recipe_23_s3_parquet_connector.py  # Main PyFlink recipe (S3 Parquet)
├── setup_s3_parquet_recipe.py         # Setup script for JAR dependencies
├── tools/
│   ├── download_jars.py               # JAR download utility
│   └── start_kafka.py                 # Kafka startup tool
├── jars/                             # Downloaded JAR files (created by setup)
│   ├── flink-connector-files-1.18.0.jar
│   ├── flink-parquet-1.18.0.jar
│   ├── hadoop-aws-3.3.4.jar
│   ├── aws-java-sdk-s3-1.12.261.jar
│   └── classpath.txt
├── s3_input_data/                    # Local input directory (created by recipe)
│   └── *.parquet                     # Sample Parquet files
├── s3_output_data/                   # Local output directory (created by recipe)
│   └── *.parquet                     # Processed Parquet files
└── README_s3_parquet_recipe.md      # This file
```

## Prerequisites

1. **uv**
2. **Python 3.11+**
3. **PyFlink** and related dependencies
4. **pandas** and **pyarrow** for Parquet file creation
5. **AWS credentials** (optional, for S3 access)

## Installation

1. **Install dependencies:**
   ```bash
   uv add apache-flink pandas pyarrow
   ```

2. **Run the setup script:**
   ```bash
   python setup_s3_parquet_recipe.py
   ```
   This will download the required Flink JAR files for S3 and Parquet support.

3. **Set AWS credentials (optional):**
   ```bash
   export AWS_ACCESS_KEY_ID=your_access_key
   export AWS_SECRET_ACCESS_KEY=your_secret_key
   export AWS_DEFAULT_REGION=us-east-1
   ```

## Usage

### Step 1: Run the S3 Parquet Recipe

Run the PyFlink recipe to process Parquet files:

```bash
python recipe_23_s3_parquet_connector.py
```

This will:
- Create sample Parquet files in the `s3_input_data/` directory (if they don't exist)
- Read the Parquet files using the filesystem connector
- Process the data with SQL queries
- Write the results to the `s3_output_data/` directory (or S3 if credentials are set)
- Print the processed data to the console

## Data Schema

The recipe processes Parquet files with the following structure:

```json
{
  "id": 1,
  "name": "Product A",
  "category": "electronics",
  "price": 299.99,
  "rating": 4.5,
  "tags": ["new", "featured"],
  "created_at": "2024-01-01T12:00:00"
}
```

## Configuration

### S3 Configuration

The recipe uses the filesystem connector with S3 support:

- **Input path:** `s3://pyflink-recipe-data/input/` (with AWS credentials) or `s3_input_data/` (local fallback)
- **Output path:** `s3://pyflink-recipe-data/output/` (with AWS credentials) or `s3_output_data/` (local fallback)
- **Format:** `parquet` (columnar storage format)
- **Processing mode:** Streaming

### PyFlink Table Configuration

The recipe uses PyFlink's Table API with filesystem connector and Parquet format:

```sql
-- Input table
CREATE TABLE input_products (
    id BIGINT,
    name STRING,
    category STRING,
    price DOUBLE,
    rating DOUBLE,
    tags ARRAY<STRING>,
    created_at TIMESTAMP(3)
) WITH (
    'connector' = 'filesystem',
    'path' = 's3://bucket/input/',
    'format' = 'parquet'
)

-- Output table
CREATE TABLE output_products (
    id BIGINT,
    name STRING,
    category STRING,
    price DOUBLE,
    rating DOUBLE,
    tags ARRAY<STRING>,
    created_at TIMESTAMP(3),
    price_category STRING,
    rating_category STRING
) WITH (
    'connector' = 'filesystem',
    'path' = 's3://bucket/output/',
    'format' = 'parquet'
)
```

## Data Processing

The recipe applies the following transformations:

1. **Data Filtering:** Only processes products with price > 0 and rating > 0
2. **Price Categorization:** Adds a `price_category` field based on price ranges:
   - `premium`: price >= $500
   - `mid-range`: price >= $100 and < $500
   - `budget`: price < $100
3. **Rating Categorization:** Adds a `rating_category` field based on rating ranges:
   - `excellent`: rating >= 4.5
   - `good`: rating >= 4.0 and < 4.5
   - `average`: rating >= 3.5 and < 4.0
   - `poor`: rating < 3.5

## Example Output

When running the recipe, you should see output like:

```
=== Reading and Writing Parquet Files to/from S3 with PyFlink ===
Input path: s3_input_data
Output path: s3_output_data

Creating sample Parquet data...
Created 3 sample Parquet files in s3_input_data/

Setting up Table API workflow with S3 Parquet connector...
Table API workflow configured. Starting execution...
+----+----+-----------+----------+-------+-------+----------+-------------------------+----------------+------------------+
| op | id |      name | category | price | rating |     tags |                created_at | price_category | rating_category |
+----+----+-----------+----------+-------+-------+----------+-------------------------+----------------+------------------+
| +I |  1 | Product A | electronics | 299.99 | 4.5 | [new, featured] | 2024-01-01 12:00:00.000 | mid-range | excellent |
| +I |  2 | Product B | clothing | 89.99 | 4.2 | [sale, popular] | 2024-01-01 12:00:01.000 | budget | good |
| +I |  3 | Product C | books | 24.99 | 4.8 | [bestseller] | 2024-01-01 12:00:02.000 | budget | excellent |
| +I |  4 | Product D | electronics | 599.99 | 4.7 | [premium, featured] | 2024-01-01 12:00:03.000 | premium | excellent |
| +I |  5 | Product E | clothing | 129.99 | 4.1 | [trending] | 2024-01-01 12:00:04.000 | mid-range | good |
+----+----+-----------+----------+-------+-------+----------+-------------------------+----------------+------------------+
```

The processed files will be written to the output directory in Parquet format with the additional categorization fields.

## S3 Integration Features

### Authentication

The recipe supports multiple authentication methods:
- **Environment variables:** `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
- **AWS credentials file:** `~/.aws/credentials`
- **IAM roles:** When running on AWS infrastructure
- **Local fallback:** Uses local filesystem when S3 credentials are not available

### S3 Configuration

The filesystem connector supports various S3 configurations:
- **Region:** Set via `AWS_DEFAULT_REGION` environment variable
- **Endpoint:** Configurable for different S3-compatible services
- **Encryption:** Supports S3 server-side encryption
- **Partitioning:** Supports S3 partitioning strategies

### Parquet Format Benefits

Parquet format provides several advantages:
- **Columnar storage:** Efficient for analytical queries
- **Compression:** Built-in compression reduces storage costs
- **Schema evolution:** Supports schema changes over time
- **Predicate pushdown:** Filters are applied at storage level

## Troubleshooting

### Common Issues

1. **S3 Connection Error:**
   - Verify AWS credentials are set correctly
   - Check S3 bucket permissions
   - Ensure the bucket exists and is accessible
   - Verify the region is correct

2. **Parquet Format Error:**
   - Verify Parquet files are valid
   - Check that schema matches the table definition
   - Ensure data types match (e.g., numbers vs strings)

3. **Authentication Error:**
   - Check AWS credentials are properly configured
   - Verify IAM permissions for S3 access
   - Test with AWS CLI to confirm credentials work

4. **PyFlink Import Error:**
   - Install PyFlink: `uv add apache-flink`
   - Check Python version compatibility

5. **JAR Dependencies Error:**
   - Run `python setup_s3_parquet_recipe.py` to download required JAR files
   - Ensure the JAR files are in the `jars/` directory

### Debug Mode

To see more detailed output, you can modify the script to include debug logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Extending the Recipe

### Adding New Fields

To add new fields to the Parquet schema:

1. Update the table definitions in `recipe_23_s3_parquet_connector.py`
2. Modify the SQL query to include the new fields
3. Update the sample data generation if needed

### Using Different S3 Configurations

The recipe can be extended to support different S3 configurations by:
- Adding S3-specific connector properties
- Configuring different authentication methods
- Setting up S3 partitioning strategies
- Implementing custom S3 endpoints

### Processing Large Datasets

For better performance with large datasets:
- Consider using S3 partitioning
- Implement batch processing strategies
- Use S3 Select for filtering at storage level
- Configure appropriate parallelism settings

## Comparison with Other Recipes

This S3 Parquet recipe differs from the other recipes in several ways:

| Aspect | S3 Parquet Recipe | Kafka Recipes | Filesystem Recipe |
|--------|-------------------|---------------|-------------------|
| **Storage** | Cloud storage (S3) | Message queue (Kafka) | Local filesystem |
| **Format** | Columnar (Parquet) | JSON/Protobuf | JSON |
| **Processing** | Batch-oriented | Stream-oriented | File-based |
| **Scalability** | Highly scalable | Highly scalable | Limited by local storage |
| **Cost** | Pay-per-use | Infrastructure cost | Free (local) |

## Best Practices

1. **S3 Configuration:**
   - Use appropriate S3 storage classes for cost optimization
   - Implement lifecycle policies for data management
   - Consider S3 Intelligent Tiering for automatic optimization

2. **Parquet Optimization:**
   - Choose appropriate compression algorithms
   - Set optimal row group sizes
   - Use column pruning for better performance

3. **Security:**
   - Use IAM roles instead of access keys when possible
   - Implement least-privilege access policies
   - Enable S3 server-side encryption

4. **Monitoring:**
   - Monitor S3 costs and usage
   - Track processing performance
   - Implement error handling and retry logic

## Next Steps

After mastering this recipe, consider exploring:

1. **S3 Partitioning:** Implement partitioned data storage
2. **S3 Select:** Use S3 Select for server-side filtering
3. **Glue Catalog:** Integrate with AWS Glue Data Catalog
4. **Athena Integration:** Query processed data with Amazon Athena
5. **Lake Formation:** Use AWS Lake Formation for data lake management

This recipe provides a foundation for building cloud-native data processing pipelines with PyFlink and S3. 