# Recipe 18: Interval Joins
from typing import List, Tuple
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes, Table
from pyflink.table.expressions import col, lit, timestamp_diff, if_then_else
from pyflink.table.expression import TimePointUnit
from datetime import datetime, timedelta

# Constants
DEFAULT_PARALLELISM = "1"

# Sample data for orders (left table)
ORDERS_DATA: List[Tuple[int, int, str, float, datetime]] = [
    (1, 101, "laptop", 999.99, datetime(2023, 1, 1, 10, 0, 0)),
    (2, 102, "mouse", 29.99, datetime(2023, 1, 1, 10, 5, 0)),
    (3, 101, "keyboard", 89.99, datetime(2023, 1, 1, 10, 10, 0)),
    (4, 103, "monitor", 299.99, datetime(2023, 1, 1, 10, 15, 0)),
    (5, 102, "headphones", 79.99, datetime(2023, 1, 1, 10, 20, 0)),
    (6, 104, "tablet", 399.99, datetime(2023, 1, 1, 10, 25, 0)),
    (7, 101, "webcam", 59.99, datetime(2023, 1, 1, 10, 30, 0)),
    (8, 105, "speakers", 129.99, datetime(2023, 1, 1, 10, 35, 0)),
    (9, 103, "printer", 199.99, datetime(2023, 1, 1, 10, 40, 0)),
    (10, 102, "scanner", 89.99, datetime(2023, 1, 1, 10, 45, 0))
]

# Sample data for shipments (right table)
SHIPMENTS_DATA: List[Tuple[int, int, str, datetime, datetime]] = [
    (1, 101, "shipped", datetime(2023, 1, 1, 10, 2, 0), datetime(2023, 1, 1, 10, 2, 0)),
    (2, 102, "shipped", datetime(2023, 1, 1, 10, 7, 0), datetime(2023, 1, 1, 10, 7, 0)),
    (3, 101, "shipped", datetime(2023, 1, 1, 10, 12, 0), datetime(2023, 1, 1, 10, 12, 0)),
    (4, 103, "shipped", datetime(2023, 1, 1, 10, 17, 0), datetime(2023, 1, 1, 10, 17, 0)),
    (5, 102, "shipped", datetime(2023, 1, 1, 10, 22, 0), datetime(2023, 1, 1, 10, 22, 0)),
    (6, 104, "shipped", datetime(2023, 1, 1, 10, 27, 0), datetime(2023, 1, 1, 10, 27, 0)),
    (7, 101, "shipped", datetime(2023, 1, 1, 10, 32, 0), datetime(2023, 1, 1, 10, 32, 0)),
    (8, 105, "shipped", datetime(2023, 1, 1, 10, 37, 0), datetime(2023, 1, 1, 10, 37, 0)),
    (9, 103, "shipped", datetime(2023, 1, 1, 10, 42, 0), datetime(2023, 1, 1, 10, 42, 0)),
    (10, 102, "shipped", datetime(2023, 1, 1, 10, 47, 0), datetime(2023, 1, 1, 10, 47, 0))
]

# Sample data for price changes (for time-bounded joins)
PRICE_CHANGES_DATA: List[Tuple[str, float, datetime]] = [
    ("laptop", 999.99, datetime(2023, 1, 1, 10, 0, 0)),
    ("laptop", 949.99, datetime(2023, 1, 1, 10, 8, 0)),  # Price drop
    ("laptop", 899.99, datetime(2023, 1, 1, 10, 16, 0)), # Further drop
    ("mouse", 29.99, datetime(2023, 1, 1, 10, 0, 0)),
    ("mouse", 24.99, datetime(2023, 1, 1, 10, 12, 0)),   # Price drop
    ("keyboard", 89.99, datetime(2023, 1, 1, 10, 0, 0)),
    ("keyboard", 79.99, datetime(2023, 1, 1, 10, 20, 0)), # Price drop
    ("monitor", 299.99, datetime(2023, 1, 1, 10, 0, 0)),
    ("monitor", 279.99, datetime(2023, 1, 1, 10, 25, 0)), # Price drop
    ("headphones", 79.99, datetime(2023, 1, 1, 10, 0, 0))
]

def recipe_18_interval_joins() -> None:
    """
    Demonstrates interval joins in PyFlink.
    
    This function shows:
    1. Basic interval joins with time bounds
    2. Interval joins with different time ranges
    3. Interval joins with additional conditions
    4. Multiple interval joins
    5. SQL-based interval joins
    6. Time-bounded lookups
    7. Analyzing temporal relationships
    """
    # Setup
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    t_env.get_config().set("parallelism.default", DEFAULT_PARALLELISM)

    print("Recipe 18: Interval Joins")

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

    shipments_table = t_env.from_elements(
        SHIPMENTS_DATA,
        DataTypes.ROW([
            DataTypes.FIELD("shipment_id", DataTypes.INT()),
            DataTypes.FIELD("customer_id", DataTypes.INT()),
            DataTypes.FIELD("status", DataTypes.STRING()),
            DataTypes.FIELD("shipment_time", DataTypes.TIMESTAMP(3)),
            DataTypes.FIELD("processed_time", DataTypes.TIMESTAMP(3))
        ])
    )

    price_changes_table = t_env.from_elements(
        PRICE_CHANGES_DATA,
        DataTypes.ROW([
            DataTypes.FIELD("product_name", DataTypes.STRING()),
            DataTypes.FIELD("new_price", DataTypes.DOUBLE()),
            DataTypes.FIELD("change_time", DataTypes.TIMESTAMP(3))
        ])
    )

    print("Orders Table:")
    orders_table.print_schema()
    print("\nOrders Data:")
    orders_table.execute().print()

    print("\nShipments Table:")
    shipments_table.print_schema()
    print("\nShipments Data:")
    shipments_table.execute().print()

    print("\nPrice Changes Table:")
    price_changes_table.print_schema()
    print("\nPrice Changes Data:")
    price_changes_table.execute().print()

    # 1. Basic Interval Join (Orders + Shipments within 5 minutes)
    print("\n--- 1. Basic Interval Join (Orders + Shipments within 5 minutes) ---")
    # Rename columns to avoid ambiguity
    orders_renamed = orders_table.select(
        col("order_id"),
        col("customer_id").alias("order_customer_id"),
        col("product_name"),
        col("amount"),
        col("order_time")
    )
    shipments_renamed = shipments_table.select(
        col("shipment_id"),
        col("customer_id").alias("shipment_customer_id"),
        col("status"),
        col("shipment_time"),
        col("processed_time")
    )
    
    interval_join_basic = orders_renamed.join(shipments_renamed).where(
        (col("order_customer_id") == col("shipment_customer_id")) &
        (col("order_time") >= col("shipment_time") - lit(5).minutes) &
        (col("order_time") <= col("shipment_time") + lit(5).minutes)
    ).select(
        col("order_id"),
        col("order_customer_id").alias("customer_id"),
        col("product_name"),
        col("amount"),
        col("order_time"),
        col("shipment_id"),
        col("status"),
        col("shipment_time")
    )
    print("Table after basic interval join:")
    interval_join_basic.print_schema()
    print("\nData after basic interval join:")
    interval_join_basic.execute().print()

    # 2. Interval Join with Different Time Ranges
    print("\n--- 2. Interval Join with Different Time Ranges ---")
    # 2-minute interval
    interval_2min = orders_renamed.join(shipments_renamed).where(
        (col("order_customer_id") == col("shipment_customer_id")) &
        (col("order_time") >= col("shipment_time") - lit(2).minutes) &
        (col("order_time") <= col("shipment_time") + lit(2).minutes)
    ).select(
        col("order_id"),
        col("shipment_id"),
        col("order_time"),
        col("shipment_time")
    )
    
    # 10-minute interval
    interval_10min = orders_renamed.join(shipments_renamed).where(
        (col("order_customer_id") == col("shipment_customer_id")) &
        (col("order_time") >= col("shipment_time") - lit(10).minutes) &
        (col("order_time") <= col("shipment_time") + lit(10).minutes)
    ).select(
        col("order_id"),
        col("shipment_id"),
        col("order_time"),
        col("shipment_time")
    )
    
    print("Table after 2-minute interval join:")
    interval_2min.print_schema()
    print("\nData after 2-minute interval join:")
    interval_2min.execute().print()
    
    print("\nTable after 10-minute interval join:")
    interval_10min.print_schema()
    print("\nData after 10-minute interval join:")
    interval_10min.execute().print()

    # 3. Interval Join with Additional Conditions
    print("\n--- 3. Interval Join with Additional Conditions ---")
    interval_conditional = orders_renamed.join(shipments_renamed).where(
        (col("order_customer_id") == col("shipment_customer_id")) &
        (col("order_time") >= col("shipment_time") - lit(5).minutes) &
        (col("order_time") <= col("shipment_time") + lit(5).minutes) &
        (col("amount") > 50.0)
    ).select(
        col("order_id"),
        col("order_customer_id").alias("customer_id"),
        col("product_name"),
        col("amount"),
        col("order_time"),
        col("shipment_id"),
        col("status"),
        col("shipment_time")
    )
    print("Table after conditional interval join:")
    interval_conditional.print_schema()
    print("\nData after conditional interval join:")
    interval_conditional.execute().print()

    # 4. Time-bounded Price Lookup
    print("\n--- 4. Time-bounded Price Lookup ---")
    # Rename price changes table to avoid ambiguity
    price_changes_renamed = price_changes_table.select(
        col("product_name").alias("price_product_name"),
        col("new_price"),
        col("change_time")
    )
    
    price_lookup = orders_renamed.join(price_changes_renamed).where(
        (col("product_name") == col("price_product_name")) &
        (col("order_time") >= col("change_time") - lit(30).minutes) &
        (col("order_time") <= col("change_time") + lit(30).minutes)
    ).select(
        col("order_id"),
        col("product_name"),
        col("amount").alias("order_price"),
        col("new_price").alias("current_price"),
        col("order_time"),
        col("change_time"),
        (col("amount") - col("new_price")).alias("price_difference")
    )
    print("Table after price lookup interval join:")
    price_lookup.print_schema()
    print("\nData after price lookup interval join:")
    price_lookup.execute().print()

    # 5. Multiple Interval Joins
    print("\n--- 5. Multiple Interval Joins ---")
    # First join: orders with shipments
    orders_shipments = orders_renamed.join(shipments_renamed).where(
        (col("order_customer_id") == col("shipment_customer_id")) &
        (col("order_time") >= col("shipment_time") - lit(5).minutes) &
        (col("order_time") <= col("shipment_time") + lit(5).minutes)
    )
    
    # Second join: with price changes
    multi_interval = orders_shipments.join(price_changes_renamed).where(
        (col("product_name") == col("price_product_name")) &
        (col("order_time") >= col("change_time") - lit(15).minutes) &
        (col("order_time") <= col("change_time") + lit(15).minutes)
    ).select(
        col("order_id"),
        col("order_customer_id").alias("customer_id"),
        col("product_name"),
        col("amount"),
        col("order_time"),
        col("shipment_id"),
        col("status"),
        col("shipment_time"),
        col("new_price"),
        col("change_time")
    )
    print("Table after multiple interval joins:")
    multi_interval.print_schema()
    print("\nData after multiple interval joins:")
    multi_interval.execute().print()

    # 6. SQL-based Interval Joins
    print("\n--- 6. SQL-based Interval Joins ---")
    t_env.create_temporary_view("orders", orders_table)
    t_env.create_temporary_view("shipments", shipments_table)
    t_env.create_temporary_view("price_changes", price_changes_table)
    
    # SQL Interval Join
    sql_interval = t_env.sql_query("""
        SELECT 
            o.order_id,
            o.customer_id,
            o.product_name,
            o.amount,
            o.order_time,
            s.shipment_id,
            s.status,
            s.shipment_time
        FROM orders o
        JOIN shipments s ON o.customer_id = s.customer_id
        AND o.order_time BETWEEN s.shipment_time - INTERVAL '5' MINUTE 
                             AND s.shipment_time + INTERVAL '5' MINUTE
    """)
    print("Table from SQL interval join:")
    sql_interval.print_schema()
    print("\nData from SQL interval join:")
    sql_interval.execute().print()

    # SQL Multiple Interval Join
    sql_multi_interval = t_env.sql_query("""
        SELECT 
            o.order_id,
            o.customer_id,
            o.product_name,
            o.amount as order_price,
            o.order_time,
            s.shipment_id,
            s.status,
            s.shipment_time,
            p.new_price,
            p.change_time
        FROM orders o
        JOIN shipments s ON o.customer_id = s.customer_id
        AND o.order_time BETWEEN s.shipment_time - INTERVAL '5' MINUTE 
                             AND s.shipment_time + INTERVAL '5' MINUTE
        JOIN price_changes p ON o.product_name = p.product_name
        AND o.order_time BETWEEN p.change_time - INTERVAL '15' MINUTE 
                             AND p.change_time + INTERVAL '15' MINUTE
    """)
    print("Table from SQL multiple interval join:")
    sql_multi_interval.print_schema()
    print("\nData from SQL multiple interval join:")
    sql_multi_interval.execute().print()

    # 7. Temporal Analysis with Interval Joins
    print("\n--- 7. Temporal Analysis with Interval Joins ---")
    temporal_analysis = orders_renamed.join(shipments_renamed).where(
        (col("order_customer_id") == col("shipment_customer_id")) &
        (col("order_time") >= col("shipment_time") - lit(5).minutes) &
        (col("order_time") <= col("shipment_time") + lit(5).minutes)
    ).group_by(
        col("order_customer_id")
    ).select(
        col("order_customer_id").alias("customer_id"),
        col("amount").count.alias("total_orders"),
        col("amount").sum.alias("total_spent"),
        # Calculate average time between order and shipment using timestamp_diff
        timestamp_diff(TimePointUnit.MINUTE, col("order_time"), col("shipment_time")).avg.alias("avg_processing_time_minutes")
    )
    print("Table after temporal analysis:")
    temporal_analysis.print_schema()
    print("\nData after temporal analysis:")
    temporal_analysis.execute().print()

    # 8. Interval Join with Time Window Analysis
    print("\n--- 8. Interval Join with Time Window Analysis ---")
    time_window_analysis = orders_renamed.join(shipments_renamed).where(
        (col("order_customer_id") == col("shipment_customer_id")) &
        (col("order_time") >= col("shipment_time") - lit(5).minutes) &
        (col("order_time") <= col("shipment_time") + lit(5).minutes)
    ).select(
        col("order_id"),
        col("order_customer_id").alias("customer_id"),
        col("product_name"),
        col("order_time"),
        col("shipment_time"),
        # Calculate processing time in minutes using timestamp_diff
        timestamp_diff(TimePointUnit.MINUTE, col("order_time"), col("shipment_time")).alias("processing_time_minutes"),
        # Categorize processing speed using proper case_when syntax
        if_then_else(
            timestamp_diff(TimePointUnit.MINUTE, col("order_time"), col("shipment_time")) < 2, "fast",
            if_then_else(
                timestamp_diff(TimePointUnit.MINUTE, col("order_time"), col("shipment_time")) < 5, "normal",
                "slow"
            )
        ).alias("processing_speed")
    )
    print("Table after time window analysis:")
    time_window_analysis.print_schema()
    print("\nData after time window analysis:")
    time_window_analysis.execute().print()

if __name__ == '__main__':
    recipe_18_interval_joins()
