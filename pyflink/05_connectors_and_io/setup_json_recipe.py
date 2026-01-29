#!/usr/bin/env python3
"""
Setup script for JSON Kafka recipe.

This script downloads the required JAR files and sets up the environment
for the PyFlink JSON Kafka recipe.
"""

import os
import sys
from pathlib import Path

# Add tools directory to path
sys.path.insert(0, str(Path(__file__).parent / "tools"))

from download_jars import download_jar

def setup_json_recipe():
    """Setup the JSON recipe environment."""
    print("=== Setting up JSON Kafka Recipe ===")
    
    # Create jars directory
    jars_dir = Path(__file__).parent / "jars"
    jars_dir.mkdir(exist_ok=True)
    
    # Define required JARs for JSON recipe
    jars_to_download = [
        {
            "group_id": "org.apache.flink",
            "artifact_id": "flink-connector-kafka",
            "version": "4.0.0-2.0"
        },
        {
            "group_id": "org.apache.kafka",
            "artifact_id": "kafka-clients",
            "version": "3.0.0"
        },
        {
            "group_id": "org.apache.flink",
            "artifact_id": "flink-json",
            "version": "1.18.0"
        }
    ]
    
    downloaded_jars = []
    
    for jar_info in jars_to_download:
        jar_path = download_jar(
            jar_info["group_id"],
            jar_info["artifact_id"],
            jar_info["version"],
            jars_dir
        )
        if jar_path:
            downloaded_jars.append(jar_path)
    
    print(f"\nDownloaded {len(downloaded_jars)} JAR files:")
    for jar in downloaded_jars:
        print(f"- {Path(jar).name}")
    
    # Create classpath file
    classpath_file = jars_dir / "classpath.txt"
    with open(classpath_file, 'w') as f:
        f.write(":".join(downloaded_jars))
    
    print(f"\nClasspath saved to: {classpath_file}")
    print("\nSetup complete! You can now run the JSON recipe.")
    print("\nTo start Kafka:")
    print("  python tools/start_kafka.py")
    print("\nTo generate test data:")
    print("  python generate_json_data.py")
    print("\nTo run the recipe:")
    print("  python recipe_21_kafka_connector.py")

if __name__ == "__main__":
    setup_json_recipe() 