#!/bin/bash

# Kmsg Test Environment Management Script
#
# This script helps run kmsg receiver integration tests in a Docker container.
# Since /dev/kmsg is Linux-specific, Docker is used to run tests on macOS/Windows.

IMAGE_NAME="rotel-kmsg-test"
CONTAINER_NAME="rotel-kmsg-test-runner"

function build_image() {
    echo "Building kmsg test Docker image..."
    docker build -t $IMAGE_NAME -f - . <<'EOF'
FROM rust:latest
RUN apt-get update && apt-get install -y cmake libclang-dev protobuf-compiler && rm -rf /var/lib/apt/lists/*
EOF

    if [ $? -eq 0 ]; then
        echo "Image '$IMAGE_NAME' built successfully!"
    else
        echo "ERROR: Failed to build Docker image"
        exit 1
    fi
}

function run_tests() {
    echo "Running kmsg integration tests in Docker..."

    # Check if image exists
    if ! docker image inspect $IMAGE_NAME >/dev/null 2>&1; then
        echo "Image not found. Building first..."
        build_image
    fi

    docker run --rm \
        -v "$(pwd)":/workspace \
        -v cargo-registry:/usr/local/cargo/registry \
        -v cargo-target:/workspace/target \
        -w /workspace \
        --privileged \
        --name $CONTAINER_NAME \
        $IMAGE_NAME \
        cargo test --test kmsg_integration_tests --features "integration-tests,kmsg_receiver" "$@"
}

function run_shell() {
    echo "Starting interactive shell in kmsg test container..."

    # Check if image exists
    if ! docker image inspect $IMAGE_NAME >/dev/null 2>&1; then
        echo "Image not found. Building first..."
        build_image
    fi

    docker run --rm -it \
        -v "$(pwd)":/workspace \
        -v cargo-registry:/usr/local/cargo/registry \
        -v cargo-target:/workspace/target \
        -w /workspace \
        --privileged \
        --name $CONTAINER_NAME \
        $IMAGE_NAME \
        /bin/bash
}

function read_kmsg() {
    echo "Reading kernel messages from Docker's Linux VM..."
    echo "Press Ctrl+C to stop..."

    # Check if image exists
    if ! docker image inspect $IMAGE_NAME >/dev/null 2>&1; then
        echo "Image not found. Building first..."
        build_image
    fi

    docker run --rm -it \
        --privileged \
        $IMAGE_NAME \
        sh -c 'cat /dev/kmsg'
}

function cleanup() {
    echo "Cleaning up kmsg test environment..."

    # Stop and remove any running containers
    if docker ps -q --filter "name=$CONTAINER_NAME" | grep -q .; then
        echo "Stopping running container..."
        docker stop $CONTAINER_NAME
    fi

    # Remove the image
    if docker image inspect $IMAGE_NAME >/dev/null 2>&1; then
        echo "Removing image..."
        docker rmi $IMAGE_NAME
    fi

    echo "Cleanup complete."
}

function status() {
    echo "Kmsg test environment status:"
    echo ""

    echo "Image:"
    if docker image inspect $IMAGE_NAME >/dev/null 2>&1; then
        docker image ls $IMAGE_NAME
    else
        echo "  Not built (run '$0 build' to create)"
    fi

    echo ""
    echo "Running containers:"
    if docker ps -q --filter "name=$CONTAINER_NAME" | grep -q .; then
        docker ps --filter "name=$CONTAINER_NAME"
    else
        echo "  None"
    fi

    echo ""
    echo "Cargo cache volumes:"
    docker volume ls --filter "name=cargo-registry" --filter "name=cargo-target" 2>/dev/null || echo "  None"
}

function help_menu() {
    echo "Kmsg Test Environment Manager"
    echo ""
    echo "This script helps run kmsg receiver integration tests in Docker."
    echo "Docker is required because /dev/kmsg is Linux-specific."
    echo ""
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "Commands:"
    echo "  build          Build the Docker image with Rust and build dependencies"
    echo "  test [args]    Run kmsg integration tests (extra args passed to cargo test)"
    echo "  shell          Start an interactive shell in the test container"
    echo "  read-kmsg      Read kernel messages from Docker's Linux VM"
    echo "  status         Show status of test environment"
    echo "  cleanup        Remove the Docker image and stop containers"
    echo "  help           Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 build                    # Build the test image"
    echo "  $0 test                     # Run all kmsg integration tests"
    echo "  $0 test -- --nocapture      # Run tests with output"
    echo "  $0 test test_can_open_kmsg  # Run a specific test"
    echo "  $0 shell                    # Get a shell for manual testing"
    echo "  $0 read-kmsg                # View kernel messages"
}

case "${1:-help}" in
    build)
        build_image
        ;;
    test)
        shift
        run_tests "$@"
        ;;
    shell)
        run_shell
        ;;
    read-kmsg)
        read_kmsg
        ;;
    status)
        status
        ;;
    cleanup)
        cleanup
        ;;
    help|*)
        help_menu
        ;;
esac
