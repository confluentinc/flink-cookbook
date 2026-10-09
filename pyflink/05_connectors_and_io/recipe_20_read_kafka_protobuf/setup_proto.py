#!/usr/bin/env python3
"""
Setup script for the PyFlink Protobuf recipe.
This script generates the protobuf classes and sets up the environment.
"""

import subprocess
import sys
import os
from pathlib import Path

def generate_protobuf_classes():
    """
    Generate Python classes from the protobuf definition.
    """
    print("Generating protobuf classes...")
    
    # Get the directory containing this script
    script_dir = Path(__file__).parent
    proto_dir = script_dir / "proto_data"
    proto_file = proto_dir / "transaction.proto"
    
    if not proto_file.exists():
        print(f"Error: Proto file not found at {proto_file}")
        return False
    
    try:
        # Generate Python classes from protobuf
        result = subprocess.run([
            sys.executable, "-m", "grpc_tools.protoc",
            "--python_out", str(proto_dir),
            "--proto_path", str(proto_dir),
            str(proto_file)
        ], capture_output=True, text=True, check=True)
        
        print("✓ Protobuf classes generated successfully")
        print(f"  Generated files in: {proto_dir}")
        
        # List generated files
        for file in proto_dir.glob("*.py"):
            print(f"  - {file.name}")
        
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"Error generating protobuf classes: {e}")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        return False
    except FileNotFoundError:
        print("Error: grpc_tools.protoc not found. Please install grpcio-tools:")
        print("  pip install grpcio-tools")
        return False

def test_protobuf_generation():
    """
    Test that the protobuf classes can be imported and used.
    """
    print("\nTesting protobuf classes...")
    
    try:
        # Add proto_data to Python path
        script_dir = Path(__file__).parent
        proto_dir = script_dir / "proto_data"
        sys.path.insert(0, str(proto_dir))
        
        # Import the generated class
        from transaction_pb2 import Transaction
        
        # Create a test transaction
        transaction = Transaction()
        transaction.t_time = "2024-01-01T12:00:00"
        transaction.t_id = 1
        transaction.t_customer_id = 5
        transaction.t_amount = 100.50
        
        # Serialize and deserialize to test
        serialized = transaction.SerializeToString()
        new_transaction = Transaction()
        new_transaction.ParseFromString(serialized)
        
        print("✓ Protobuf classes working correctly")
        print(f"  Test transaction: ID={new_transaction.t_id}, "
              f"Customer={new_transaction.t_customer_id}, "
              f"Amount=${new_transaction.t_amount}")
        
        return True
        
    except ImportError as e:
        print(f"Error importing protobuf classes: {e}")
        return False
    except Exception as e:
        print(f"Error testing protobuf classes: {e}")
        return False

def main():
    """
    Main setup function.
    """
    print("=== PyFlink Protobuf Recipe Setup ===")
    
    # Generate protobuf classes
    if not generate_protobuf_classes():
        print("Setup failed!")
        return False
    
    # Test the generated classes
    if not test_protobuf_generation():
        print("Setup failed!")
        return False
    
    print("\n✓ Setup completed successfully!")
    print("\nNext steps:")
    print("1. Start Kafka (if not already running)")
    print("2. Run: python generate_proto_data.py (to generate test data)")
    print("3. Run: python recipe_16_read_protobuf.py (to process the data)")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 