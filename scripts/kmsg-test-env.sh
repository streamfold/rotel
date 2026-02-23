#!/bin/bash

# Kmsg Test Environment Management Script
#
# This script helps run kmsg receiver integration tests in a Docker container.
# Since /dev/kmsg is Linux-specific, Docker is used to run tests on macOS/Windows.

IMAGE_NAME="rotel-kmsg-test"
CONTAINER_NAME="rotel-kmsg-test-runner"

function build_image() {
    echo "Building kmsg test Docker image..."
    # Note: Using <<EOF (not <<'EOF') to allow COPY from build context.
    # Any $ in Dockerfile content would be shell-interpolated.
    docker build -t $IMAGE_NAME -f - . <<EOF
FROM rust:latest
RUN apt-get update && apt-get install -y cmake libclang-dev protobuf-compiler && rm -rf /var/lib/apt/lists/*
# Install the project's pinned toolchain with clippy
COPY rust-toolchain.toml /tmp/rust-toolchain.toml
RUN cd /tmp && rustup show && rustup component add clippy
EOF

    if [ $? -eq 0 ]; then
        echo "Image '$IMAGE_NAME' built successfully!"
    else
        echo "ERROR: Failed to build Docker image"
        exit 1
    fi
}

function ensure_image() {
    if ! docker image inspect $IMAGE_NAME >/dev/null 2>&1; then
        echo "Image not found. Building first..."
        build_image
    fi
}

# Run cargo command in docker container
# Usage: docker_cargo [--privileged] <cargo args...>
function docker_cargo() {
    local privileged=""
    if [ "$1" = "--privileged" ]; then
        privileged="--privileged"
        shift
    fi

    docker run --rm \
        -v "$(pwd)":/workspace \
        -v cargo-registry:/usr/local/cargo/registry \
        -v cargo-target:/workspace/target \
        -w /workspace \
        $privileged \
        --name $CONTAINER_NAME \
        $IMAGE_NAME \
        cargo "$@"
}

# Internal functions (no echo, no ensure_image)
function _unit_tests() {
    docker_cargo test --features kmsg_receiver --lib receivers::kmsg "$@"
}

function _integration_tests() {
    docker_cargo --privileged test --test kmsg_integration_tests --features "integration-tests,kmsg_receiver" "$@"
}

function _clippy() {
    docker_cargo clippy --features kmsg_receiver --lib "$@"
}

# Public functions
function run_unit_tests() {
    echo "Running kmsg unit tests in Docker..."
    ensure_image
    _unit_tests "$@"
}

function run_integration_tests() {
    echo "Running kmsg integration tests in Docker..."
    ensure_image
    _integration_tests "$@"
}

function run_all_tests() {
    echo "Running all kmsg tests in Docker..."
    ensure_image

    echo ""
    echo "=== Unit Tests ==="
    _unit_tests "$@"

    echo ""
    echo "=== Integration Tests ==="
    _integration_tests "$@"
}

function run_clippy() {
    echo "Running clippy in Docker..."
    ensure_image
    _clippy "$@"
}

function run_check() {
    echo "Running full verification (clippy + all tests) in Docker..."
    ensure_image

    echo ""
    echo "=== Clippy ==="
    if ! _clippy; then
        echo "ERROR: Clippy failed"
        exit 1
    fi

    echo ""
    echo "=== Unit Tests ==="
    if ! _unit_tests "$@"; then
        echo "ERROR: Unit tests failed"
        exit 1
    fi

    echo ""
    echo "=== Integration Tests ==="
    if ! _integration_tests "$@"; then
        echo "ERROR: Integration tests failed"
        exit 1
    fi

    echo ""
    echo "All checks passed!"
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
    echo "This script helps run kmsg receiver tests in Docker."
    echo "Docker is required because /dev/kmsg is Linux-specific."
    echo ""
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "Commands:"
    echo "  build                   Build the Docker image with Rust and build dependencies"
    echo "  check [args]            Run full verification (clippy + all tests)"
    echo "  clippy [args]           Run clippy lints only"
    echo "  test [args]             Run all kmsg tests (unit + integration)"
    echo "  unit-test [args]        Run kmsg unit tests only"
    echo "  integration-test [args] Run kmsg integration tests only"
    echo "  shell                   Start an interactive shell in the test container"
    echo "  read-kmsg               Read kernel messages from Docker's Linux VM"
    echo "  status                  Show status of test environment"
    echo "  cleanup                 Remove the Docker image and stop containers"
    echo "  help                    Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 build                    # Build the test image"
    echo "  $0 check                    # Run clippy + all tests"
    echo "  $0 clippy                   # Run clippy only"
    echo "  $0 test                     # Run all kmsg tests"
    echo "  $0 unit-test                # Run unit tests only"
    echo "  $0 integration-test         # Run integration tests only"
    echo "  $0 test -- --nocapture      # Run tests with output"
    echo "  $0 shell                    # Get a shell for manual testing"
    echo "  $0 read-kmsg                # View kernel messages"
}

case "${1:-help}" in
    build)
        build_image
        ;;
    check|verify)
        shift
        run_check "$@"
        ;;
    clippy)
        shift
        run_clippy "$@"
        ;;
    test)
        shift
        run_all_tests "$@"
        ;;
    unit-test)
        shift
        run_unit_tests "$@"
        ;;
    integration-test)
        shift
        run_integration_tests "$@"
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
