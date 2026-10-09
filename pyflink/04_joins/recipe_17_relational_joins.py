# Recipe 17: Standard Relational Joins
from typing import List, Tuple
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, Table
from pyflink.table.expressions import col, lit
from datetime import datetime

# Constants
DEFAULT_PARALLELISM = "1"

# Sample data for orders
ORDERS_DATA: List[Tuple[int, int, str, float, datetime]] = [
    (1, 101, "laptop", 999.99, datetime(2023, 1, 1, 10, 0, 0)),
    (2, 102, "mouse", 29.99, datetime(2023, 1, 1, 10, 5, 0)),
    (3, 101, "keyboard", 89.99, datetime(2023, 1, 1, 10, 10, 0)),
    (4, 103, "monitor", 299.99, datetime(2023, 1, 1, 10, 15, 0)),
    (5, 102, "headphones", 79.99, datetime(2023, 1, 1, 10, 20, 0)),
    (6, 104, "tablet", 399.99, datetime(2023, 1, 1, 10, 25, 0)),
    (7, 101, "webcam", 59.99, datetime(2023, 1, 1, 10, 30, 0))
]

# Sample data for customers
CUSTOMERS_DATA: List[Tuple[int, str, str, str]] = [
    (101, "John Doe", "john@example.com", "New York"),
    (102, "Jane Smith", "jane@example.com", "Los Angeles"),
    (103, "Bob Wilson", "bob@example.com", "Chicago"),
    (105, "Alice Johnson", "alice@example.com", "Boston")  # Note: customer 104 is missing
]

# Sample data for products
PRODUCTS_DATA: List[Tuple[str, str, float, str]] = [
    ("laptop", "Electronics", 999.99, "High-end laptop"),
    ("mouse", "Accessories", 29.99, "Wireless mouse"),
    ("keyboard", "Accessories", 89.99, "Mechanical keyboard"),
    ("monitor", "Electronics", 299.99, "4K monitor"),
    ("headphones", "Accessories", 79.99, "Noise-canceling headphones"),
    ("tablet", "Electronics", 399.99, "10-inch tablet"),
    ("webcam", "Accessories", 59.99, "HD webcam"),
    ("speakers", "Accessories", 129.99, "Bluetooth speakers")  # Not in orders
]

def recipe_17_relational_joins() -> None:
    """
    Demonstrates standard relational joins in PyFlink.
    
    This function shows:
    1. Inner joins between tables
    2. Left outer joins
    3. Right outer joins
    4. Full outer joins
    5. Cross joins
    6. Self joins
    7. Multiple table joins
    8. SQL-based join operations
    """
    # Setup
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    t_env.get_config().set("parallelism.default", DEFAULT_PARALLELISM)

    print("Recipe 17: Standard Relational Joins")

    # Create tables
    orders_table = t_env.from_elements(
        ORDERS_DATA,
        DataTypes.ROW([
            DataTypes.FIELD("order_id", DataTypes.INT()),
            DataTypes.FIELD("customer_id", DataTypes.INT()),
            DataTypes.FIELD("product_name", DataTypes.STRING()),
            DataTypes.FIELD("amount", DataTypes.DOUBLE()),
            DataTypes.FIELD("order_time", DataTypes.TIMESTAMP(3))
        ])
    )

    customers_table = t_env.from_elements(
        CUSTOMERS_DATA,
        DataTypes.ROW([
            DataTypes.FIELD("customer_id", DataTypes.INT()),
            DataTypes.FIELD("customer_name", DataTypes.STRING()),
            DataTypes.FIELD("email", DataTypes.STRING()),
            DataTypes.FIELD("city", DataTypes.STRING())
        ])
    )

    products_table = t_env.from_elements(
        PRODUCTS_DATA,
        DataTypes.ROW([
            DataTypes.FIELD("product_name", DataTypes.STRING()),
            DataTypes.FIELD("category", DataTypes.STRING()),
            DataTypes.FIELD("price", DataTypes.DOUBLE()),
            DataTypes.FIELD("description", DataTypes.STRING())
        ])
    )

    print("Orders Table:")
    orders_table.print_schema()
    print("\nOrders Data:")
    orders_table.execute().print()

    print("\nCustomers Table:")
    customers_table.print_schema()
    print("\nCustomers Data:")
    customers_table.execute().print()

    print("\nProducts Table:")
    products_table.print_schema()
    print("\nProducts Data:")
    products_table.execute().print()

    # 1. Inner Join
    print("\n--- 1. Inner Join (Orders + Customers) ---")
    # Rename columns to avoid ambiguity using proper PyFlink column aliasing
    orders_renamed = orders_table.select(
        col("order_id"),
        col("customer_id").alias("order_customer_id"),
        col("product_name"),
        col("amount"),
        col("order_time")
    )
    customers_renamed = customers_table.select(
        col("customer_id").alias("customer_customer_id"),
        col("customer_name"),
        col("email"),
        col("city")
    )
    
    inner_join = orders_renamed.join(
        customers_renamed,
        col("order_customer_id") == col("customer_customer_id")
    ).select(
        col("order_id"),
        col("customer_name"),
        col("product_name"),
        col("amount"),
        col("city")
    )
    print("Table after inner join:")
    inner_join.print_schema()
    print("\nData after inner join:")
    inner_join.execute().print()

    # 2. Left Outer Join
    print("\n--- 2. Left Outer Join (Orders + Customers) ---")
    left_join = orders_renamed.left_outer_join(
        customers_renamed,
        col("order_customer_id") == col("customer_customer_id")
    ).select(
        col("order_id"),
        col("order_customer_id"),
        col("product_name"),
        col("amount"),
        col("customer_name"),
        col("city")
    )
    print("Table after left outer join:")
    left_join.print_schema()
    print("\nData after left outer join:")
    left_join.execute().print()

    # 3. Right Outer Join
    print("\n--- 3. Right Outer Join (Orders + Customers) ---")
    right_join = orders_renamed.right_outer_join(
        customers_renamed,
        col("order_customer_id") == col("customer_customer_id")
    ).select(
        col("order_id"),
        col("customer_customer_id"),
        col("customer_name"),
        col("email"),
        col("city"),
        col("product_name"),
        col("amount")
    )
    print("Table after right outer join:")
    right_join.print_schema()
    print("\nData after right outer join:")
    right_join.execute().print()

    # 4. Full Outer Join
    print("\n--- 4. Full Outer Join (Orders + Customers) ---")
    full_join = orders_renamed.full_outer_join(
        customers_renamed,
        col("order_customer_id") == col("customer_customer_id")
    ).select(
        col("order_id"),
        col("order_customer_id"),
        col("product_name"),
        col("amount"),
        col("customer_name"),
        col("email"),
        col("city")
    )
    print("Table after full outer join:")
    full_join.print_schema()
    print("\nData after full outer join:")
    full_join.execute().print()

    # 5. Cross Join
    print("\n--- 5. Cross Join (Customers + Products) ---")
    # Create temporary views for SQL cross join
    t_env.create_temporary_view("customers_cross", customers_table)
    t_env.create_temporary_view("products_cross", products_table)
    
    cross_join = t_env.sql_query("""
        SELECT 
            c.customer_name,
            c.city,
            p.product_name,
            p.category,
            p.price
        FROM customers_cross c
        CROSS JOIN products_cross p
        LIMIT 10
    """)
    print("Table after cross join (limited to 10 rows):")
    cross_join.print_schema()
    print("\nData after cross join:")
    cross_join.execute().print()

    # 6. Multiple Table Join
    print("\n--- 6. Multiple Table Join (Orders + Customers + Products) ---")
    # Create renamed products table to avoid ambiguity
    products_renamed = products_table.select(
        col("product_name").alias("product_product_name"),
        col("category"),
        col("price"),
        col("description")
    )
    
    multi_join = orders_renamed.join(
        customers_renamed,
        col("order_customer_id") == col("customer_customer_id")
    ).join(
        products_renamed,
        col("product_name") == col("product_product_name")
    ).select(
        col("order_id"),
        col("customer_name"),
        col("product_name"),
        col("category"),
        col("amount"),
        col("price"),
        col("city")
    )
    print("Table after multiple table join:")
    multi_join.print_schema()
    print("\nData after multiple table join:")
    multi_join.execute().print()

    # 7. Join with Conditions
    print("\n--- 7. Join with Additional Conditions ---")
    conditional_join = orders_renamed.join(
        customers_renamed,
        (col("order_customer_id") == col("customer_customer_id")) & 
        (col("amount") > 50.0)
    ).select(
        col("order_id"),
        col("customer_name"),
        col("product_name"),
        col("amount"),
        col("city")
    )
    print("Table after conditional join:")
    conditional_join.print_schema()
    print("\nData after conditional join:")
    conditional_join.execute().print()

    # 8. Self Join (Orders with same customer)
    print("\n--- 8. Self Join (Orders with same customer) ---")
    # Create a view for self join
    t_env.create_temporary_view("orders_view", orders_table)
    self_join = t_env.sql_query("""
        SELECT 
            o1.order_id as order1_id,
            o1.product_name as product1,
            o1.amount as amount1,
            o2.order_id as order2_id,
            o2.product_name as product2,
            o2.amount as amount2,
            o1.customer_id
        FROM orders_view o1
        JOIN orders_view o2 ON o1.customer_id = o2.customer_id 
        AND o1.order_id < o2.order_id
    """)
    print("Table after self join:")
    self_join.print_schema()
    print("\nData after self join:")
    self_join.execute().print()

    # 9. SQL-based Joins
    print("\n--- 9. SQL-based Joins ---")
    t_env.create_temporary_view("orders", orders_table)
    t_env.create_temporary_view("customers", customers_table)
    t_env.create_temporary_view("products", products_table)
    
    # SQL Inner Join
    sql_inner = t_env.sql_query("""
        SELECT 
            o.order_id,
            c.customer_name,
            o.product_name,
            o.amount,
            c.city
        FROM orders o
        INNER JOIN customers c ON o.customer_id = c.customer_id
    """)
    print("Table from SQL inner join:")
    sql_inner.print_schema()
    print("\nData from SQL inner join:")
    sql_inner.execute().print()

    # SQL Left Join
    sql_left = t_env.sql_query("""
        SELECT 
            o.order_id,
            o.customer_id,
            o.product_name,
            o.amount,
            c.customer_name,
            c.city
        FROM orders o
        LEFT JOIN customers c ON o.customer_id = c.customer_id
    """)
    print("Table from SQL left join:")
    sql_left.print_schema()
    print("\nData from SQL left join:")
    sql_left.execute().print()

    # SQL Multiple Join
    sql_multi = t_env.sql_query("""
        SELECT 
            o.order_id,
            c.customer_name,
            o.product_name,
            p.category,
            o.amount,
            p.price,
            c.city
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        JOIN products p ON o.product_name = p.product_name
    """)
    print("Table from SQL multiple join:")
    sql_multi.print_schema()
    print("\nData from SQL multiple join:")
    sql_multi.execute().print()

    # 10. Join with Aggregations
    print("\n--- 10. Join with Aggregations ---")
    join_aggregation = orders_renamed.join(
        customers_renamed,
        col("order_customer_id") == col("customer_customer_id")
    ).group_by(
        col("customer_name"),
        col("city")
    ).select(
        col("customer_name"),
        col("city"),
        col("amount").count.alias("total_orders"),
        col("amount").sum.alias("total_spent"),
        col("amount").avg.alias("avg_order_value")
    )
    print("Table after join with aggregations:")
    join_aggregation.print_schema()
    print("\nData after join with aggregations:")
    join_aggregation.execute().print()

if __name__ == '__main__':
    recipe_17_relational_joins()
