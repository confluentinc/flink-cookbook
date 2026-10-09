# Recipe 3: Aggregating Data
from typing import List, Tuple
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes
from pyflink.table.expressions import col

# Constants
DEFAULT_PARALLELISM = "1"
SALES_DATA: List[Tuple[str, int, int]] = [
    ("electronics", 101, 100),
    ("electronics", 102, 200),
    ("electronics", 103, 150),
    ("clothing", 201, 50),
    ("clothing", 202, 75),
    ("clothing", 203, 60),
    ("electronics", 101, 100),
    ("clothing", 203, 60),
    ("electronics", 102, 200),
    ("clothing", 202, 75),
    ("electronics", 101, 100)
]

def recipe_03_aggregating_data() -> None:
    """
    Demonstrates data aggregation operations in PyFlink.
    
    This function:
    1. Sets up a streaming table environment
    2. Creates a sample sales table
    3. Performs various aggregations on the sales data
    """
    # Setup
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    t_env.get_config().set("parallelism.default", DEFAULT_PARALLELISM)

    print("Recipe 3: Aggregating Data")

    # Create sales table
    sales_table = t_env.from_elements(
        SALES_DATA,
        DataTypes.ROW([
            DataTypes.FIELD("category", DataTypes.STRING()), 
            DataTypes.FIELD("product_id", DataTypes.INT()), 
            DataTypes.FIELD("amount", DataTypes.INT())
        ])
    )
    
    print("Original Sales Table:")
    sales_table.print_schema()
    print("\nOriginal Sales Data:")
    sales_table.execute().print()

    # Aggregate: Calculate total sales amount and count of products per category
    print("\n--- Aggregation by Category ---")
    grouped_sales = sales_table.group_by(col("category"))
    aggregated_table = grouped_sales.select(
        col("category"),
        col("amount").sum.alias("total_sales"),
        col("product_id").count.alias("product_count"),
        col("amount").avg.alias("average_sale_amount")
    )
    
    print("Aggregated Sales by Category:")
    aggregated_table.print_schema()
    print("\nAggregated Sales Data by Category:")
    aggregated_table.execute().print()

    # Aggregate: Calculate max sale amount for each product_id within each category
    print("\n--- Aggregation by Category and Product ---")
    aggregated_by_product = sales_table \
       .group_by(col("category"), col("product_id")) \
       .select(
            col("category"),
            col("product_id"),
            col("amount").max.alias("max_sale_for_product")
        )
    
    print("Max Sale Amount by Category and Product:")
    aggregated_by_product.print_schema()
    print("\nMax Sale Amount Data by Category and Product:")
    aggregated_by_product.execute().print()

if __name__ == '__main__':
    recipe_03_aggregating_data()