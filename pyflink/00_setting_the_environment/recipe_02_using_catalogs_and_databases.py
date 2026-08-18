from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    # Create a TableEnvironment
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)

    # Create a database
    database_name = "my_database"
    table_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

    # Use the database
    table_env.use_database(database_name)

    # Create a table in the database
    table_env.execute_sql("""
        CREATE TABLE my_table (
            id INT,
            name STRING,
            PRIMARY KEY (id) NOT ENFORCED
        ) WITH (
            'connector' = 'filesystem',
            'path' = '/tmp/my_table',
            'format' = 'csv'
        )
    """)

    # List all catalogs
    print("Available catalogs:")
    for catalog in table_env.list_catalogs():
        print(f"- {catalog}")

    # List all databases in the current catalog
    print("\nDatabases in current catalog:")
    for database in table_env.list_databases():
        print(f"- {database}")

    # List all tables in the current database
    print("\nTables in current database:")
    for table in table_env.list_tables():
        print(f"- {table}")

if __name__ == "__main__":
    main() 