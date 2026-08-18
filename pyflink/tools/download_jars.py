import os
import urllib.request
import sys

def download_jar(group_id, artifact_id, version, target_dir):
    """
    Download a JAR file from Maven Central
    
    Args:
        group_id: Maven group ID (e.g., 'org.apache.flink')
        artifact_id: Maven artifact ID (e.g., 'flink-connector-kafka')
        version: Version to download (e.g., '4.0.0-2.0')
        target_dir: Directory to save the JAR file
    """
    # Construct Maven Central URL
    group_path = group_id.replace('.', '/')
    jar_name = f"{artifact_id}-{version}.jar"
    url = f"https://repo1.maven.org/maven2/{group_path}/{artifact_id}/{version}/{jar_name}"
    
    # Create target directory if it doesn't exist
    os.makedirs(target_dir, exist_ok=True)
    
    # Full path for the JAR file
    jar_path = os.path.join(target_dir, jar_name)
    
    # Check if file already exists
    if os.path.exists(jar_path):
        print(f"JAR file already exists: {jar_path}")
        return jar_path
    
    try:
        print(f"Downloading {jar_name} from {url}")
        urllib.request.urlretrieve(url, jar_path)
        print(f"Successfully downloaded: {jar_path}")
        return jar_path
    except Exception as e:
        print(f"Error downloading {jar_name}: {e}")
        return None

def download_flink_connectors(target_dir):
    """
    Download Flink connector JARs needed for various recipes.
    """
    # Define the JARs we need for Flink recipes
    jars_to_download = [
        {
            "group_id": "org.apache.flink",
            "artifact_id": "flink-connector-kafka",
            "version": "4.0.0-2.0"
        },
        {
            "group_id": "org.apache.flink",
            "artifact_id": "flink-json",
            "version": "1.18.0"
        },
        {
            "group_id": "org.apache.kafka",
            "artifact_id": "kafka-clients",
            "version": "3.0.0"
        },
        {
            "group_id": "org.apache.flink",
            "artifact_id": "flink-protobuf",
            "version": "1.18.0"
        },
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
            target_dir
        )
        if jar_path:
            downloaded_jars.append(jar_path)
    
    return downloaded_jars

def download_protobuf_jars(target_dir):
    """
    Download JARs specifically needed for protobuf recipes.
    """
    # Define the JARs we need for protobuf recipes
    jars_to_download = [
        {
            "group_id": "org.apache.flink",
            "artifact_id": "flink-connector-kafka",
            "version": "4.0.0-2.0"
        },
        {
            "group_id": "org.apache.flink",
            "artifact_id": "flink-protobuf",
            "version": "1.18.0"
        },
        {
            "group_id": "com.google.protobuf",
            "artifact_id": "protobuf-java",
            "version": "3.21.7"
        },
        {
            "group_id": "org.apache.kafka",
            "artifact_id": "kafka-clients",
            "version": "3.4.0"
        }
    ]
    
    downloaded_jars = []
    
    for jar_info in jars_to_download:
        jar_path = download_jar(
            jar_info["group_id"],
            jar_info["artifact_id"], 
            jar_info["version"],
            target_dir
        )
        if jar_path:
            downloaded_jars.append(jar_path)
    
    return downloaded_jars

def main():
    """
    Main function to download JAR files.
    """
    if len(sys.argv) < 2:
        print("Usage: python download_jars.py <target_directory> [recipe_type]")
        print("  recipe_type: 'protobuf' (default) or 'all'")
        sys.exit(1)
    
    target_dir = sys.argv[1]
    recipe_type = sys.argv[2] if len(sys.argv) > 2 else "protobuf"
    
    print(f"Downloading JAR files to: {target_dir}")
    print(f"Recipe type: {recipe_type}")
    
    if recipe_type == "protobuf":
        downloaded_jars = download_protobuf_jars(target_dir)
    elif recipe_type == "all":
        downloaded_jars = download_flink_connectors(target_dir)
    else:
        print(f"Unknown recipe type: {recipe_type}")
        print("Available types: 'protobuf', 'all'")
        sys.exit(1)
    
    print(f"\nDownloaded {len(downloaded_jars)} JAR files:")
    for jar in downloaded_jars:
        print(f"- {os.path.basename(jar)}")
    
    # Create a classpath file for easy use
    classpath_file = os.path.join(target_dir, "classpath.txt")
    with open(classpath_file, 'w') as f:
        f.write(":".join(downloaded_jars))
    
    print(f"\nClasspath saved to: {classpath_file}")
    print("\nTo use these JARs with PyFlink, set the PYTHONPATH:")
    print(f"export PYTHONPATH=\"{':'.join(downloaded_jars)}:$PYTHONPATH\"")

if __name__ == "__main__":
    main() 