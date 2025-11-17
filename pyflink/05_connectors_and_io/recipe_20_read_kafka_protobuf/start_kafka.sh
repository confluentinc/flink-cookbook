#!/bin/bash

# Script to start Kafka and Zookeeper in Podman containers
# This script sets up a local Kafka cluster for development and testing
# Podman is a drop-in replacement for Docker that doesn't require a daemon

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
KAFKA_VERSION="7.4.0"
ZOOKEEPER_VERSION="7.4.0"
NETWORK_NAME="kafka-network"
ZOOKEEPER_CONTAINER="zookeeper"
KAFKA_CONTAINER="kafka"
KAFKA_PORT=9092
ZOOKEEPER_PORT=2181

echo -e "${BLUE}=== Starting Kafka Development Environment ===${NC}"

# Function to check and install Podman
check_podman() {
    # Check if podman is installed
    if ! command -v podman &> /dev/null; then
        echo -e "${YELLOW}Podman not found. Installing Podman...${NC}"
        
        # Detect OS and install Podman
        if [[ "$OSTYPE" == "darwin"* ]]; then
            # macOS
            if command -v brew &> /dev/null; then
                brew install podman
            else
                echo -e "${RED}Error: Homebrew not found. Please install Homebrew first:${NC}"
                echo -e "  /bin/bash -c \"\$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)\""
                exit 1
            fi
        elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
            # Linux
            if command -v apt-get &> /dev/null; then
                # Ubuntu/Debian
                sudo apt-get update
                sudo apt-get install -y podman
            elif command -v dnf &> /dev/null; then
                # Fedora/RHEL/CentOS
                sudo dnf install -y podman
            elif command -v yum &> /dev/null; then
                # Older RHEL/CentOS
                sudo yum install -y podman
            else
                echo -e "${RED}Error: Unsupported Linux distribution. Please install Podman manually.${NC}"
                echo -e "  Visit: https://podman.io/getting-started/installation"
                exit 1
            fi
        else
            echo -e "${RED}Error: Unsupported operating system. Please install Podman manually.${NC}"
            echo -e "  Visit: https://podman.io/getting-started/installation"
            exit 1
        fi
    fi
    
    # Check if podman is working
    if ! podman info > /dev/null 2>&1; then
        echo -e "${YELLOW}Initializing Podman...${NC}"
        podman machine init 2>/dev/null || true
        podman machine start 2>/dev/null || true
    fi
    
    echo -e "${GREEN}✓ Podman is ready${NC}"
}

# Function to create network if it doesn't exist
create_network() {
    if ! podman network ls | grep -q $NETWORK_NAME; then
        echo -e "${YELLOW}Creating Podman network: $NETWORK_NAME${NC}"
        podman network create $NETWORK_NAME
    else
        echo -e "${GREEN}✓ Network $NETWORK_NAME already exists${NC}"
    fi
}

# Function to stop and remove existing containers
cleanup_containers() {
    echo -e "${YELLOW}Cleaning up existing containers...${NC}"
    
    # Stop and remove Kafka container
    if podman ps -a | grep -q $KAFKA_CONTAINER; then
        podman stop $KAFKA_CONTAINER 2>/dev/null || true
        podman rm $KAFKA_CONTAINER 2>/dev/null || true
        echo -e "${GREEN}✓ Removed existing Kafka container${NC}"
    fi
    
    # Stop and remove Zookeeper container
    if podman ps -a | grep -q $ZOOKEEPER_CONTAINER; then
        podman stop $ZOOKEEPER_CONTAINER 2>/dev/null || true
        podman rm $ZOOKEEPER_CONTAINER 2>/dev/null || true
        echo -e "${GREEN}✓ Removed existing Zookeeper container${NC}"
    fi
}

# Function to start Zookeeper
start_zookeeper() {
    echo -e "${YELLOW}Starting Zookeeper...${NC}"
    
    podman run -d \
        --name $ZOOKEEPER_CONTAINER \
        --network $NETWORK_NAME \
        -p $ZOOKEEPER_PORT:2181 \
        -e ZOOKEEPER_CLIENT_PORT=2181 \
        -e ZOOKEEPER_TICK_TIME=2000 \
        confluentinc/cp-zookeeper:$ZOOKEEPER_VERSION
    
    echo -e "${GREEN}✓ Zookeeper started on port $ZOOKEEPER_PORT${NC}"
}

# Function to start Kafka
start_kafka() {
    echo -e "${YELLOW}Starting Kafka...${NC}"
    
    # Wait for Zookeeper to be ready
    echo -e "${YELLOW}Waiting for Zookeeper to be ready...${NC}"
    sleep 10
    
    podman run -d \
        --name $KAFKA_CONTAINER \
        --network $NETWORK_NAME \
        -p $KAFKA_PORT:9092 \
        -e KAFKA_BROKER_ID=1 \
        -e KAFKA_ZOOKEEPER_CONNECT=$ZOOKEEPER_CONTAINER:2181 \
        -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT \
        -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092,PLAINTEXT_HOST://$KAFKA_CONTAINER:29092 \
        -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
        -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
        -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
        -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
        confluentinc/cp-kafka:$KAFKA_VERSION
    
    echo -e "${GREEN}✓ Kafka started on port $KAFKA_PORT${NC}"
}

# Function to create topics
create_topics() {
    echo -e "${YELLOW}Creating Kafka topics...${NC}"
    
    # Wait for Kafka to be ready
    echo -e "${YELLOW}Waiting for Kafka to be ready...${NC}"
    sleep 15
    
    # Create transactions topic
    podman exec $KAFKA_CONTAINER kafka-topics \
        --create \
        --topic transactions \
        --bootstrap-server localhost:9092 \
        --partitions 1 \
        --replication-factor 1 \
        2>/dev/null || echo -e "${YELLOW}Topic 'transactions' already exists${NC}"
    
    echo -e "${GREEN}✓ Topic 'transactions' created${NC}"
}

# Function to show status
show_status() {
    echo -e "${BLUE}=== Kafka Environment Status ===${NC}"
    
    # Check if containers are running
    if podman ps | grep -q $ZOOKEEPER_CONTAINER; then
        echo -e "${GREEN}✓ Zookeeper is running${NC}"
    else
        echo -e "${RED}✗ Zookeeper is not running${NC}"
    fi
    
    if podman ps | grep -q $KAFKA_CONTAINER; then
        echo -e "${GREEN}✓ Kafka is running${NC}"
    else
        echo -e "${RED}✗ Kafka is not running${NC}"
    fi
    
    echo -e "${BLUE}Connection Details:${NC}"
    echo -e "  Kafka: localhost:$KAFKA_PORT"
    echo -e "  Zookeeper: localhost:$ZOOKEEPER_PORT"
    echo -e "  Topic: transactions"
}

# Function to show usage
show_usage() {
    echo -e "${BLUE}Usage: $0 [COMMAND]${NC}"
    echo -e ""
    echo -e "${YELLOW}Commands:${NC}"
    echo -e "  start     Start Kafka and Zookeeper (default)"
    echo -e "  stop      Stop Kafka and Zookeeper"
    echo -e "  restart   Restart Kafka and Zookeeper"
    echo -e "  status    Show status of containers"
    echo -e "  logs      Show logs from containers"
    echo -e "  cleanup   Stop and remove containers"
    echo -e "  help      Show this help message"
    echo -e ""
    echo -e "${YELLOW}Examples:${NC}"
    echo -e "  $0 start      # Start the environment"
    echo -e "  $0 stop       # Stop the environment"
    echo -e "  $0 status     # Check status"
    echo -e ""
    echo -e "${YELLOW}Note:${NC}"
    echo -e "  This script uses Podman instead of Docker for better security and performance."
    echo -e "  Podman will be automatically installed if not present."
}

# Function to stop containers
stop_containers() {
    echo -e "${YELLOW}Stopping Kafka and Zookeeper...${NC}"
    
    if podman ps | grep -q $KAFKA_CONTAINER; then
        podman stop $KAFKA_CONTAINER
        echo -e "${GREEN}✓ Kafka stopped${NC}"
    fi
    
    if podman ps | grep -q $ZOOKEEPER_CONTAINER; then
        podman stop $ZOOKEEPER_CONTAINER
        echo -e "${GREEN}✓ Zookeeper stopped${NC}"
    fi
}

# Function to show logs
show_logs() {
    echo -e "${BLUE}=== Container Logs ===${NC}"
    echo -e "${YELLOW}Zookeeper logs:${NC}"
    podman logs $ZOOKEEPER_CONTAINER --tail 10 2>/dev/null || echo "No Zookeeper logs found"
    echo -e ""
    echo -e "${YELLOW}Kafka logs:${NC}"
    podman logs $KAFKA_CONTAINER --tail 10 2>/dev/null || echo "No Kafka logs found"
}

# Main script logic
main() {
    local command=${1:-start}
    
    case $command in
        start)
            check_podman
            create_network
            cleanup_containers
            start_zookeeper
            start_kafka
            create_topics
            show_status
            echo -e "${GREEN}✓ Kafka environment is ready!${NC}"
            echo -e "${BLUE}You can now run:${NC}"
            echo -e "  uv run generate_proto_data.py"
            echo -e "  uv run recipe_16_read_protobuf.py"
            ;;
        stop)
            stop_containers
            ;;
        restart)
            stop_containers
            sleep 2
            $0 start
            ;;
        status)
            show_status
            ;;
        logs)
            show_logs
            ;;
        cleanup)
            cleanup_containers
            echo -e "${GREEN}✓ Cleanup completed${NC}"
            ;;
        help|--help|-h)
            show_usage
            ;;
        *)
            echo -e "${RED}Unknown command: $command${NC}"
            show_usage
            exit 1
            ;;
    esac
}

# Run main function with all arguments
main "$@" 