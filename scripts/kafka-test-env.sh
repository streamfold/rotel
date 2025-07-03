#!/bin/bash

# Kafka Test Environment Management Script

COMPOSE_FILE="docker-compose.kafka-test.yml"

function start_kafka() {
    echo "Starting Kafka test environment..."
    docker compose -f $COMPOSE_FILE up -d
    
    echo "Waiting for Kafka to be ready..."
    max_attempts=30
    attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        if docker exec rotel-test-kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
            echo "Kafka is ready!"
            break
        fi
        
        attempt=$((attempt + 1))
        echo "Attempt $attempt/$max_attempts - waiting for Kafka..."
        sleep 2
    done
    
    if [ $attempt -eq $max_attempts ]; then
        echo "ERROR: Kafka failed to start within timeout"
        exit 1
    fi
    
    echo "Creating test topics..."
    docker exec rotel-test-kafka kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --topic otlp_traces
    docker exec rotel-test-kafka kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --topic otlp_metrics
    docker exec rotel-test-kafka kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --topic otlp_logs
    
    echo "Kafka test environment is ready!"
    echo "Broker available at: localhost:9092"
}

function stop_kafka() {
    echo "Stopping Kafka test environment..."
    docker compose -f $COMPOSE_FILE down -v
    echo "Kafka test environment stopped."
}

function status_kafka() {
    echo "Kafka test environment status:"
    docker compose -f $COMPOSE_FILE ps
}

function consume_topic() {
    local topic=${1:-otlp_traces}
    echo "Consuming messages from topic: $topic"
    echo "Press Ctrl+C to stop..."
    docker exec -it rotel-test-kafka kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic $topic \
        --from-beginning \
        --property print.headers=true \
        --property print.key=true \
        --property print.partition=true \
        --property print.offset=true
}

function logs_kafka() {
    docker compose -f $COMPOSE_FILE logs -f kafka
}

function help_menu() {
    echo "Kafka Test Environment Manager"
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  start                Start Kafka test environment"
    echo "  stop                 Stop Kafka test environment"
    echo "  status               Show status of test environment"
    echo "  consume [topic]      Consume messages from topic (default: otlp_traces)"
    echo "  logs                 Show Kafka logs"
    echo "  help                 Show this help message"
    echo ""
    echo "Topics available: otlp_traces, otlp_metrics, otlp_logs"
}

case "${1:-help}" in
    start)
        start_kafka
        ;;
    stop)
        stop_kafka
        ;;
    status)
        status_kafka
        ;;
    consume)
        consume_topic $2
        ;;
    logs)
        logs_kafka
        ;;
    help|*)
        help_menu
        ;;
esac