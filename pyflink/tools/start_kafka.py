#!/usr/bin/env python3
"""
Kafka startup tool for PyFlink recipes.

This script provides a cross-platform way to start Kafka and Zookeeper
for PyFlink recipe development and testing.
"""

import os
import sys
import subprocess
import platform
import time
import signal
import atexit
from pathlib import Path

# Kafka configuration
KAFKA_VERSION = "3.6.1"
KAFKA_DIR = "kafka"
KAFKA_BINARY = f"kafka_2.13-{KAFKA_VERSION}"
KAFKA_DOWNLOAD_URL = f"https://downloads.apache.org/kafka/{KAFKA_VERSION}/{KAFKA_BINARY}.tgz"

def download_kafka():
    """Download and extract Kafka if not already present."""
    kafka_path = Path(KAFKA_DIR) / KAFKA_BINARY
    
    if kafka_path.exists():
        print(f"Kafka already exists at: {kafka_path}")
        return kafka_path
    
    print(f"Downloading Kafka {KAFKA_VERSION}...")
    
    # Create kafka directory
    os.makedirs(KAFKA_DIR, exist_ok=True)
    
    # Download and extract Kafka
    import urllib.request
    import tarfile
    
    tgz_path = Path(KAFKA_DIR) / f"{KAFKA_BINARY}.tgz"
    
    print(f"Downloading from: {KAFKA_DOWNLOAD_URL}")
    urllib.request.urlretrieve(KAFKA_DOWNLOAD_URL, tgz_path)
    
    print("Extracting Kafka...")
    with tarfile.open(tgz_path, 'r:gz') as tar:
        tar.extractall(KAFKA_DIR)
    
    # Clean up downloaded file
    tgz_path.unlink()
    
    print(f"Kafka extracted to: {kafka_path}")
    return kafka_path

def get_kafka_scripts(kafka_path):
    """Get paths to Kafka scripts."""
    if platform.system() == "Windows":
        zk_script = kafka_path / "bin" / "windows" / "zookeeper-server-start.bat"
        kafka_script = kafka_path / "bin" / "windows" / "kafka-server-start.bat"
    else:
        zk_script = kafka_path / "bin" / "zookeeper-server-start.sh"
        kafka_script = kafka_path / "bin" / "kafka-server-start.sh"
    
    return zk_script, kafka_script

def create_kafka_config(kafka_path):
    """Create Kafka configuration files."""
    config_dir = kafka_path / "config"
    
    # Create server.properties
    server_props = config_dir / "server.properties"
    if not server_props.exists():
        with open(server_props, 'w') as f:
            f.write("""# Kafka server configuration
broker.id=0
listeners=PLAINTEXT://localhost:9092
log.dirs=./kafka-logs
num.partitions=1
default.replication.factor=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
zookeeper.connect=localhost:2181
zookeeper.connection.timeout.ms=18000
group.initial.rebalance.delay.ms=0
""")
    
    # Create zookeeper.properties
    zk_props = config_dir / "zookeeper.properties"
    if not zk_props.exists():
        with open(zk_props, 'w') as f:
            f.write("""# Zookeeper configuration
dataDir=./zookeeper-data
clientPort=2181
maxClientCnxns=0
admin.enableServer=false
""")

def start_zookeeper(kafka_path):
    """Start Zookeeper server."""
    zk_script, _ = get_kafka_scripts(kafka_path)
    config_dir = kafka_path / "config"
    zk_props = config_dir / "zookeeper.properties"
    
    print("Starting Zookeeper...")
    
    if platform.system() == "Windows":
        process = subprocess.Popen([
            str(zk_script), str(zk_props)
        ], cwd=kafka_path)
    else:
        # Make script executable
        os.chmod(zk_script, 0o755)
        process = subprocess.Popen([
            str(zk_script), str(zk_props)
        ], cwd=kafka_path)
    
    # Wait a bit for Zookeeper to start
    time.sleep(5)
    return process

def start_kafka(kafka_path):
    """Start Kafka server."""
    _, kafka_script = get_kafka_scripts(kafka_path)
    config_dir = kafka_path / "config"
    server_props = config_dir / "server.properties"
    
    print("Starting Kafka...")
    
    if platform.system() == "Windows":
        process = subprocess.Popen([
            str(kafka_script), str(server_props)
        ], cwd=kafka_path)
    else:
        # Make script executable
        os.chmod(kafka_script, 0o755)
        process = subprocess.Popen([
            str(kafka_script), str(server_props)
        ], cwd=kafka_path)
    
    # Wait a bit for Kafka to start
    time.sleep(10)
    return process

def create_topic(topic_name, kafka_path):
    """Create a Kafka topic."""
    _, kafka_script = get_kafka_scripts(kafka_path)
    create_topic_script = kafka_script.parent / ("kafka-topics.bat" if platform.system() == "Windows" else "kafka-topics.sh")
    
    print(f"Creating topic: {topic_name}")
    
    try:
        if platform.system() == "Windows":
            subprocess.run([
                str(create_topic_script),
                "--bootstrap-server", "localhost:9092",
                "--create",
                "--topic", topic_name,
                "--partitions", "1",
                "--replication-factor", "1"
            ], cwd=kafka_path, check=True)
        else:
            os.chmod(create_topic_script, 0o755)
            subprocess.run([
                str(create_topic_script),
                "--bootstrap-server", "localhost:9092",
                "--create",
                "--topic", topic_name,
                "--partitions", "1",
                "--replication-factor", "1"
            ], cwd=kafka_path, check=True)
        print(f"Topic '{topic_name}' created successfully")
    except subprocess.CalledProcessError as e:
        print(f"Topic '{topic_name}' might already exist or error occurred: {e}")

def cleanup_processes(processes):
    """Clean up running processes."""
    print("\nShutting down Kafka and Zookeeper...")
    for process in processes:
        if process and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()

def main():
    """Main function to start Kafka environment."""
    print("=== Starting Kafka Environment for PyFlink Recipes ===")
    
    # Download and setup Kafka
    kafka_path = download_kafka()
    create_kafka_config(kafka_path)
    
    # Start Zookeeper and Kafka
    processes = []
    
    try:
        zk_process = start_zookeeper(kafka_path)
        processes.append(zk_process)
        
        kafka_process = start_kafka(kafka_path)
        processes.append(kafka_process)
        
        # Create default topics
        create_topic("transactions", kafka_path)
        create_topic("events", kafka_path)
        create_topic("test", kafka_path)
        
        print("\nKafka environment is ready!")
        print("Kafka is running on: localhost:9092")
        print("Zookeeper is running on: localhost:2181")
        print("\nAvailable topics:")
        print("- transactions")
        print("- events") 
        print("- test")
        print("\nPress Ctrl+C to stop Kafka and Zookeeper")
        
        # Register cleanup function
        atexit.register(cleanup_processes, processes)
        
        # Wait for interrupt
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nReceived interrupt signal")
    finally:
        cleanup_processes(processes)
        print("Kafka environment stopped")

if __name__ == "__main__":
    main() 