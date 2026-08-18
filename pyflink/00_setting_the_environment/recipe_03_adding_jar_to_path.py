from pyflink.table import EnvironmentSettings, TableEnvironment
import os
from download_jars import download_required_jars

def main():
    # Download required JAR files first
    print("Ensuring required JAR files are available...")
    downloaded_jars = download_required_jars()
    
    # Create a TableEnvironment
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)

    # Get the current directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Example: Add Flink Kafka connector JAR
    kafka_connector_jar = os.path.join(current_dir, "flink-connector-kafka-4.0.0-2.0.jar")
    if os.path.exists(kafka_connector_jar):
        # Convert to file:// URL
        jar_url = f"file://{kafka_connector_jar}"
        table_env.get_config().set("pipeline.jars", jar_url)
        print(f"Added Kafka connector JAR: {jar_url}")
    else:
        print(f"Warning: Kafka connector JAR not found at {kafka_connector_jar}")
        print("Please ensure the download script has run successfully")

    # Example: Add multiple JARs if they exist
    jar_paths = [
        os.path.join(current_dir, "flink-connector-kafka-4.0.0-2.0.jar"),
        os.path.join(current_dir, "flink-json-1.18.0.jar"),
        os.path.join(current_dir, "kafka-clients-3.0.0.jar")
    ]
    
    # Filter out non-existent paths and convert to URLs
    existing_jars = [f"file://{jar}" for jar in jar_paths if os.path.exists(jar)]
    if existing_jars:
        table_env.get_config().set("pipeline.jars", ";".join(existing_jars))
        print("\nAdded multiple JAR files:")
        for jar in existing_jars:
            print(f"- {jar}")

    # Method 3: Add JAR file using SQL
    if os.path.exists(kafka_connector_jar):
        table_env.execute_sql(f"""
            ADD JAR 'file://{kafka_connector_jar}'
        """)

if __name__ == "__main__":
    main() 