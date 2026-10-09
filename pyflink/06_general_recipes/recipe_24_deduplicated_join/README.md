# Recipe 20: Deduplicated Join with PyFlink Table API

This recipe demonstrates how to join and deduplicate data using PyFlink Table API. It mirrors the Java `table-deduplicated-join` recipe but uses only Table API and creates streams from elements instead of Kafka.

## Overview

This recipe shows how to:
1. Create customer and transaction data streams from elements
2. Perform joins between tables with deduplication
3. Use SQL queries to remove duplicate records
4. Demonstrate advanced deduplication techniques
5. Show the difference between joins with and without deduplication

## Key Concepts

### Deduplication
Deduplication is the process of removing duplicate records from a dataset. In streaming applications, this is crucial because:
- Duplicate events can occur due to network retries, system failures, or intentional reprocessing
- Duplicates can skew analytics and business logic
- Removing duplicates ensures data quality and consistency

### Join with Deduplication
The recipe demonstrates joining two tables while deduplicating one of them:
- **Customers table**: Contains customer information (id, name)
- **Transactions table**: Contains transaction data (id, customer_id, amount, timestamp)
- **Join operation**: Links transactions to customers while removing duplicate transactions

## Sample Data

### Customers Data
```python
CUSTOMERS_DATA = [
    (1, "Ramon Stehr"),
    (2, "Barbie Ledner"),
    (3, "Lore Baumbach"),
    (4, "Deja Crist"),
    (5, "Felix Weimann"),
    # ... more customers
]
```

### Transactions Data
```python
TRANSACTIONS_DATA = [
    (1, 1, 99.08, datetime(2023, 1, 1, 10, 0, 0)),
    (2, 1, 405.01, datetime(2023, 1, 1, 10, 5, 0)),
    (3, 2, 974.90, datetime(2023, 1, 1, 10, 10, 0)),
    # ... more transactions
    # Note: Some transactions have duplicate t_id values
    (1, 1, 99.08, datetime(2023, 1, 1, 10, 50, 0)),  # Duplicate of t_id=1
]
```

## Core SQL Query

The main deduplicated join query mirrors the Java version:

```sql
SELECT 
    t_id, 
    c_name, 
    CAST(t_amount AS DECIMAL(5, 2)) as t_amount,
    timestamp
FROM Customers
JOIN (SELECT DISTINCT * FROM Transactions) ON c_id = t_customer_id
ORDER BY t_id
```

This query:
1. Uses `SELECT DISTINCT * FROM Transactions` to remove duplicates
2. Joins the deduplicated transactions with customers
3. Returns transaction details with customer names

## Advanced Deduplication Techniques

### 1. Basic Deduplication
```sql
SELECT DISTINCT * FROM Transactions
ORDER BY t_id
```

### 2. Keep Latest Transaction per ID
```sql
SELECT t_id, t_customer_id, t_amount, timestamp
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY t_id ORDER BY timestamp DESC) as rn
    FROM Transactions
) t
WHERE rn = 1
ORDER BY t_id
```

### 3. Join with Latest Transactions
```sql
SELECT 
    t.t_id, 
    c.c_name, 
    CAST(t.t_amount AS DECIMAL(5, 2)) as t_amount,
    t.timestamp
FROM Customers c
JOIN (
    SELECT t_id, t_customer_id, t_amount, timestamp
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY t_id ORDER BY timestamp DESC) as rn
        FROM Transactions
    ) t
    WHERE rn = 1
) t ON c.c_id = t.t_customer_id
ORDER BY t.t_id
```

## Usage

### Running the Recipe

```bash
python recipe_20_deduplicated_join.py
```

### Expected Output

The recipe will show:

1. **Individual Tables**: Customers and transactions data
2. **Join Without Deduplication**: Shows duplicate transactions
3. **Deduplicated Join**: Main feature - joins with duplicates removed
4. **Advanced Techniques**: Various deduplication approaches

Example output:
```
=== Deduplicated Join Demonstration ===
Deduplicated Join Result:
(Shows transactions joined with customer names, duplicates removed)
+----+----------------------+------+---------------+----------+
| op |                 t_id | c_name |      t_amount | timestamp |
+----+----------------------+------+---------------+----------+
| +I |                    1 | Ramon Stehr |        99.08 | 2023-01-01 10:00:00 |
| +I |                    2 | Ramon Stehr |       405.01 | 2023-01-01 10:05:00 |
| +I |                    3 | Barbie Ledner |       974.90 | 2023-01-01 10:10:00 |
...
```

## Key Features

### Self-Contained
- No external dependencies (Kafka, databases, etc.)
- Uses PyFlink's `from_elements()` to create streams
- All data is generated in-memory

### Table API Only
- Uses only PyFlink Table API (no DataStream API)
- Demonstrates SQL queries for complex operations
- Shows seamless integration between Python and SQL

### Educational
- Shows before/after scenarios
- Demonstrates multiple deduplication techniques
- Explains the impact of deduplication on joins

## Comparison with Java Version

| Feature | Java Version | Python Version |
|---------|-------------|----------------|
| Data Source | Kafka topics | In-memory elements |
| API Usage | DataStream + Table API | Table API only |
| Deduplication | SQL DISTINCT | SQL DISTINCT |
| Join Type | Regular join | Regular join |
| Output | PrintSink | PrintSink |

## Performance Considerations

### State Management
- The `DISTINCT` operation requires storing all previous records
- For unbounded streams, consider using TTL (Time To Live) settings
- Monitor memory usage in production applications

### Alternative Approaches
For production applications, consider:
1. **Interval Joins**: Limit join window to reduce state
2. **Deduplication with TTL**: Configure `table.exec.state.ttl`
3. **Window-based Deduplication**: Use time windows for bounded state

## Extending the Recipe

### Adding More Tables
```python
# Add product information
products_table = table_env.from_elements(
    PRODUCTS_DATA,
    DataTypes.ROW([
        DataTypes.FIELD("p_id", DataTypes.BIGINT()),
        DataTypes.FIELD("p_name", DataTypes.STRING()),
        DataTypes.FIELD("p_category", DataTypes.STRING())
    ])
)
```

### Custom Deduplication Logic
```python
# Custom deduplication based on business rules
custom_dedup = table_env.sql_query("""
    SELECT t_id, t_customer_id, t_amount, timestamp
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY t_id, t_customer_id 
                   ORDER BY timestamp DESC
               ) as rn
        FROM Transactions
    ) t
    WHERE rn = 1
""")
```

### Time-based Deduplication
```python
# Deduplicate within time windows
time_based_dedup = table_env.sql_query("""
    SELECT t_id, t_customer_id, t_amount, timestamp
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY t_id 
                   ORDER BY timestamp DESC
               ) as rn
        FROM Transactions
        WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR
    ) t
    WHERE rn = 1
""")
```

## Troubleshooting

### Common Issues

1. **Memory Issues**: Large datasets may cause memory problems
   - Solution: Use TTL settings or window-based processing

2. **Performance**: Complex joins can be slow
   - Solution: Optimize join conditions and use appropriate indexes

3. **Duplicate Logic**: Ensure deduplication logic matches business requirements
   - Solution: Test with various data scenarios

### Debug Tips

1. **Check Data**: Verify input data structure and content
2. **Monitor State**: Watch memory usage during processing
3. **Test Incrementally**: Start with small datasets and scale up

## License

This recipe is part of the Flink Cookbook and follows the same license as the original Java version. 