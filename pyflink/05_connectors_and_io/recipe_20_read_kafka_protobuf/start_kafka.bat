@echo off
REM Script to start Kafka and Zookeeper in Podman containers
REM This script sets up a local Kafka cluster for development and testing
REM Podman is a drop-in replacement for Docker that doesn't require a daemon

setlocal enabledelayedexpansion

REM Configuration
set KAFKA_VERSION=7.4.0
set ZOOKEEPER_VERSION=7.4.0
set NETWORK_NAME=kafka-network
set ZOOKEEPER_CONTAINER=zookeeper
set KAFKA_CONTAINER=kafka
set KAFKA_PORT=9092
set ZOOKEEPER_PORT=2181

echo === Starting Kafka Development Environment ===

REM Check if Podman is installed
where podman >nul 2>&1
if %errorlevel% neq 0 (
    echo Podman not found. Installing Podman...
    echo Please install Podman manually from: https://podman.io/getting-started/installation
    echo For Windows, you can use: winget install RedHat.Podman
    pause
    exit /b 1
)

REM Check if Podman is working
podman info >nul 2>&1
if %errorlevel% neq 0 (
    echo Initializing Podman...
    podman machine init 2>nul
    podman machine start 2>nul
)

echo ✓ Podman is ready

REM Create network if it doesn't exist
podman network ls | findstr %NETWORK_NAME% >nul 2>&1
if %errorlevel% neq 0 (
    echo Creating Podman network: %NETWORK_NAME%
    podman network create %NETWORK_NAME%
) else (
    echo ✓ Network %NETWORK_NAME% already exists
)

REM Clean up existing containers
echo Cleaning up existing containers...
podman ps -a | findstr %KAFKA_CONTAINER% >nul 2>&1
if %errorlevel% equ 0 (
    podman stop %KAFKA_CONTAINER% 2>nul
    podman rm %KAFKA_CONTAINER% 2>nul
    echo ✓ Removed existing Kafka container
)

podman ps -a | findstr %ZOOKEEPER_CONTAINER% >nul 2>&1
if %errorlevel% equ 0 (
    podman stop %ZOOKEEPER_CONTAINER% 2>nul
    podman rm %ZOOKEEPER_CONTAINER% 2>nul
    echo ✓ Removed existing Zookeeper container
)

REM Start Zookeeper
echo Starting Zookeeper...
podman run -d --name %ZOOKEEPER_CONTAINER% --network %NETWORK_NAME% -p %ZOOKEEPER_PORT%:2181 -e ZOOKEEPER_CLIENT_PORT=2181 -e ZOOKEEPER_TICK_TIME=2000 confluentinc/cp-zookeeper:%ZOOKEEPER_VERSION%
echo ✓ Zookeeper started on port %ZOOKEEPER_PORT%

REM Wait for Zookeeper to be ready
echo Waiting for Zookeeper to be ready...
timeout /t 10 /nobreak >nul

REM Start Kafka
echo Starting Kafka...
podman run -d --name %KAFKA_CONTAINER% --network %NETWORK_NAME% -p %KAFKA_PORT%:9092 -e KAFKA_BROKER_ID=1 -e KAFKA_ZOOKEEPER_CONNECT=%ZOOKEEPER_CONTAINER%:2181 -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:%KAFKA_PORT%,PLAINTEXT_HOST://%KAFKA_CONTAINER%:29092 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 confluentinc/cp-kafka:%KAFKA_VERSION%
echo ✓ Kafka started on port %KAFKA_PORT%

REM Wait for Kafka to be ready
echo Waiting for Kafka to be ready...
timeout /t 15 /nobreak >nul

REM Create topics
echo Creating Kafka topics...
podman exec %KAFKA_CONTAINER% kafka-topics --create --topic transactions --bootstrap-server localhost:%KAFKA_PORT% --partitions 1 --replication-factor 1 2>nul || echo Topic 'transactions' already exists
echo ✓ Topic 'transactions' created

REM Show status
echo === Kafka Environment Status ===
podman ps | findstr %ZOOKEEPER_CONTAINER% >nul 2>&1
if %errorlevel% equ 0 (
    echo ✓ Zookeeper is running
) else (
    echo ✗ Zookeeper is not running
)

podman ps | findstr %KAFKA_CONTAINER% >nul 2>&1
if %errorlevel% equ 0 (
    echo ✓ Kafka is running
) else (
    echo ✗ Kafka is not running
)

echo Connection Details:
echo   Kafka: localhost:%KAFKA_PORT%
echo   Zookeeper: localhost:%ZOOKEEPER_PORT%
echo   Topic: transactions

echo ✓ Kafka environment is ready!
echo You can now run:
echo   python generate_proto_data.py
echo   python recipe_16_read_protobuf.py

pause 