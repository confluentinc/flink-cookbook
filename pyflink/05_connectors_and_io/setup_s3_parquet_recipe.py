#!/usr/bin/env python3
"""
Setup script for S3 Parquet recipe.

This script downloads the required JAR files and sets up the environment
for the PyFlink S3 Parquet recipe.
"""

import os
import sys
from pathlib import Path

# Add tools directory to path
sys.path.insert(0, str(Path(__file__).parent / "tools"))

from download_jars import download_jar

def setup_s3_parquet_recipe():
    """Setup the S3 Parquet recipe environment."""
    print("=== Setting up S3 Parquet Recipe ===")
    
    # Create jars directory
    jars_dir = Path(__file__).parent / "jars"
    jars_dir.mkdir(exist_ok=True)
    
    # Define required JARs for S3 Parquet recipe
    jars_to_download = [
        {
            "group_id": "org.apache.flink",
            "artifact_id": "flink-connector-files",
            "version": "1.18.0"
        },
        {
            "group_id": "org.apache.flink",
            "artifact_id": "flink-parquet",
            "version": "1.18.0"
        },
        {
            "group_id": "org.apache.hadoop",
            "artifact_id": "hadoop-aws",
            "version": "3.3.4"
        },
        {
            "group_id": "com.amazonaws",
            "artifact_id": "aws-java-sdk-s3",
            "version": "1.12.261"
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
    print("\nSetup complete! You can now run the S3 Parquet recipe.")
    print("\nTo run the recipe:")
    print("  python recipe_23_s3_parquet_connector.py")
    print("\nNote: Set AWS credentials for S3 access:")
    print("  export AWS_ACCESS_KEY_ID=your_access_key")
    print("  export AWS_SECRET_ACCESS_KEY=your_secret_key")
    print("  export AWS_DEFAULT_REGION=us-east-1")

if __name__ == "__main__":
    setup_s3_parquet_recipe() 