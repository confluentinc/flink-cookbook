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

def download_required_jars():
    """
    Download all required JAR files for the Flink Kafka connector example
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Define the JARs we need
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
        }
    ]
    
    downloaded_jars = []
    
    for jar_info in jars_to_download:
        jar_path = download_jar(
            jar_info["group_id"],
            jar_info["artifact_id"], 
            jar_info["version"],
            current_dir
        )
        if jar_path:
            downloaded_jars.append(jar_path)
    
    return downloaded_jars

if __name__ == "__main__":
    print("Downloading required JAR files...")
    downloaded = download_required_jars()
    print(f"\nDownloaded {len(downloaded)} JAR files:")
    for jar in downloaded:
        print(f"- {os.path.basename(jar)}") 