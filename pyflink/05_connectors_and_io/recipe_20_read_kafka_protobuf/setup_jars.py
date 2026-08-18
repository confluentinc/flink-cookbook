#!/usr/bin/env python3
"""
Setup script to download and configure JAR files for the protobuf recipe.
"""

import os
import sys
import subprocess
from pathlib import Path

def setup_jars():
    """
    Download and set up JAR files for the protobuf recipe.
    """
    # Get the directory containing this script
    script_dir = Path(__file__).parent
    jars_dir = script_dir / "jars"
    
    # Create jars directory
    jars_dir.mkdir(exist_ok=True)
    
    print("Setting up JAR files for protobuf recipe...")
    
    # Download JARs using the tools script
    # Current path: flink-cookbook/pyflink/05_connectors_and_io/recipe_16_read_protobuf/
    # Tools path: flink-cookbook/pyflink/tools/
    tools_dir = Path(__file__).parent.parent.parent / "tools"
    download_script = tools_dir / "download_jars.py"
    
    if not download_script.exists():
        print(f"Error: Download script not found at {download_script}")
        return False
    
    try:
        # Run the download script
        result = subprocess.run([
            sys.executable, str(download_script), str(jars_dir), "protobuf"
        ], capture_output=True, text=True, check=True)
        
        print(result.stdout)
        
        # Read the classpath file
        classpath_file = jars_dir / "classpath.txt"
        if classpath_file.exists():
            with open(classpath_file, 'r') as f:
                classpath = f.read().strip()
            
            print(f"\n✓ JAR files downloaded successfully")
            
            
    except subprocess.CalledProcessError as e:
        print(f"Error downloading JARs: {e}")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        return False

def main():
    """
    Main setup function.
    """
    print("=== PyFlink Protobuf Recipe JAR Setup ===")
    
    if setup_jars():
        print("\n✓ JAR setup completed successfully!")
        print("\nTo run the recipe with JARs:")
        print("1. Source the environment: source set_classpath.sh")
        print("2. Run the recipe: uv run recipe_16_read_protobuf.py")
    else:
        print("\n✗ JAR setup failed!")
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 