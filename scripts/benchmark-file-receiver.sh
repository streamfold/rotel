#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# benchmark-file-receiver.sh - Benchmark file receiver performance
#
# This script measures:
#   1. Event detection latency (time from log write to processing)
#   2. Throughput (logs per second)
#   3. CPU and memory usage
#
# Usage: ./benchmark-file-receiver.sh [options]
#   -c, --collector TYPE   Collector to test: rotel, otel, or both (default: rotel)
#   -d, --duration SECS    Test duration in seconds (default: 60)
#   -r, --rate RATE        Log lines per second (default: 1000)
#   -o, --output DIR       Output directory for results (default: /tmp/benchmark)
#   -h, --help             Show this help message

set -e

# Defaults
COLLECTOR="rotel"
DURATION=60
RATE=1000
OUTPUT_DIR="/tmp/benchmark"
LOG_FILE="/tmp/benchmark-nginx.log"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -c|--collector)
            COLLECTOR="$2"
            shift 2
            ;;
        -d|--duration)
            DURATION="$2"
            shift 2
            ;;
        -r|--rate)
            RATE="$2"
            shift 2
            ;;
        -o|--output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -h|--help)
            head -17 "$0" | tail -14
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROTEL_DIR="$(dirname "$SCRIPT_DIR")"

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Calculate write interval from rate
calculate_interval() {
    local rate=$1
    # Interval in microseconds
    echo $((1000000 / rate))
}

# High-performance log generator using timestamps for latency measurement
generate_logs_fast() {
    local output_file=$1
    local rate=$2
    local duration=$3
    local interval_us=$(calculate_interval "$rate")

    log_info "Generating logs at ${rate}/s for ${duration}s (interval: ${interval_us}μs)"

    local end_time=$(($(date +%s) + duration))
    local count=0
    local batch_size=10

    # Pre-generate some random data
    local methods=("GET" "POST" "PUT" "DELETE")
    local paths=("/api/users" "/api/orders" "/api/products" "/api/health")
    local statuses=(200 201 204 400 404 500)

    > "$output_file"  # Clear file

    while [[ $(date +%s) -lt $end_time ]]; do
        # Write a batch of logs
        for ((i = 0; i < batch_size; i++)); do
            local ts=$(date +%s)
            local method=${methods[$((RANDOM % ${#methods[@]}))]}
            local path=${paths[$((RANDOM % ${#paths[@]}))]}
            local status=${statuses[$((RANDOM % ${#statuses[@]}))]}
            local ip="$((RANDOM % 256)).$((RANDOM % 256)).$((RANDOM % 256)).$((RANDOM % 256))"
            local timestamp=$(date "+%d/%b/%Y:%H:%M:%S %z")

            # Include timestamp in user agent for tracking
            echo "$ip - - [$timestamp] \"$method $path HTTP/1.1\" $status 1234 \"-\" \"benchmark/$ts\"" >> "$output_file"
            ((count++))
        done

        # Sleep for batch interval (simplified for macOS compatibility)
        local sleep_ms=$((interval_us * batch_size / 1000))
        if [[ $sleep_ms -gt 0 ]]; then
            sleep "0.$(printf '%03d' $sleep_ms)"
        fi
    done

    echo "$count"
}

# Monitor process resource usage
monitor_resources() {
    local pid=$1
    local output_file=$2
    local duration=$3

    log_info "Monitoring PID $pid for ${duration}s"

    echo "timestamp,cpu_percent,memory_mb,rss_kb" > "$output_file"

    local end_time=$(($(date +%s) + duration))
    while [[ $(date +%s) -lt $end_time ]] && kill -0 "$pid" 2>/dev/null; do
        local ts=$(date +%s)

        # Get CPU and memory (macOS compatible)
        if [[ "$(uname)" == "Darwin" ]]; then
            local stats=$(ps -p "$pid" -o %cpu=,rss= 2>/dev/null || echo "0 0")
            local cpu=$(echo "$stats" | awk '{print $1}')
            local rss=$(echo "$stats" | awk '{print $2}')
            local mem_mb=$(echo "scale=2; $rss / 1024" | bc)
        else
            local stats=$(ps -p "$pid" -o %cpu=,rss= 2>/dev/null || echo "0 0")
            local cpu=$(echo "$stats" | awk '{print $1}')
            local rss=$(echo "$stats" | awk '{print $2}')
            local mem_mb=$(echo "scale=2; $rss / 1024" | bc)
        fi

        echo "$ts,$cpu,$mem_mb,$rss" >> "$output_file"
        sleep 1
    done
}

# Start Rotel file receiver - sets COLLECTOR_PID global
start_rotel() {
    local log_file=$1

    log_info "Building Rotel..."
    (cd "$ROTEL_DIR" && cargo build --release --features file_receiver 2>/dev/null)

    log_info "Starting Rotel file receiver..."

    # Create a minimal config pointing to our benchmark log
    "$ROTEL_DIR/target/release/rotel" start \
        --receiver file \
        --file-receiver-include "$log_file" \
        --file-receiver-parser nginx_access \
        --file-receiver-start-at beginning \
        --file-receiver-watch-mode native \
        --file-receiver-offsets-path /tmp/benchmark-rotel-offsets.json \
        --exporter blackhole \
        > "$OUTPUT_DIR/rotel.log" 2>&1 &

    COLLECTOR_PID=$!
}

# Start OTel Collector
start_otel() {
    local log_file=$1
    local config_file="$OUTPUT_DIR/otel-config.yaml"

    # Check for custom build first, then otelcol-contrib
    local otelcol_bin=""
    local custom_build="$ROTEL_DIR/benchmark/otel-collector/otelcol-benchmark/otelcol-benchmark"

    if [[ -x "$custom_build" ]]; then
        otelcol_bin="$custom_build"
        log_info "Using custom OTel Collector build"
    elif command -v otelcol-contrib &> /dev/null; then
        otelcol_bin="otelcol-contrib"
        log_info "Using system otelcol-contrib"
    else
        log_warn "OTel Collector not found."
        log_warn "Either build it: cd benchmark/otel-collector && make build"
        log_warn "Or install: brew install open-telemetry/opentelemetry-collector/otelcol-contrib"
        return 1
    fi

    log_info "Creating OTel Collector config..."

    cat > "$config_file" << EOF
receivers:
  filelog:
    include:
      - $log_file
    start_at: beginning
    poll_interval: 50ms
    operators:
      - type: regex_parser
        regex: '^(?P<remote_addr>\S+) - (?P<remote_user>\S+) \[(?P<time_local>[^\]]+)\] "(?P<request>[^"]+)" (?P<status>\d+) (?P<body_bytes_sent>\d+) "(?P<http_referer>[^"]*)" "(?P<http_user_agent>[^"]*)"'
        parse_from: body
        parse_to: attributes
      - type: add
        field: attributes.source
        value: "nginx"

processors:
  memory_limiter:
    check_interval: 1s
    limit_mib: 512
  batch:
    timeout: 100ms
    send_batch_size: 1000

exporters:
  debug:
    verbosity: basic
  nop: {}

service:
  pipelines:
    logs:
      receivers: [filelog]
      processors: [memory_limiter, batch]
      exporters: [nop]
EOF

    log_info "Starting OTel Collector..."

    "$otelcol_bin" --config "$config_file" > "$OUTPUT_DIR/otel.log" 2>&1 &

    COLLECTOR_PID=$!
}

# Run benchmark for a specific collector
run_benchmark() {
    local collector_name=$1
    local start_func=$2

    log_info "============================================"
    log_info "Benchmarking: $collector_name"
    log_info "============================================"

    local result_dir="$OUTPUT_DIR/$collector_name"
    mkdir -p "$result_dir"

    # Clean up any previous state
    rm -f "$LOG_FILE" /tmp/benchmark-*-offsets.json
    touch "$LOG_FILE"

    # Start the collector (sets COLLECTOR_PID global)
    COLLECTOR_PID=""
    $start_func "$LOG_FILE"

    local pid=$COLLECTOR_PID

    if [[ -z "$pid" ]] || ! kill -0 "$pid" 2>/dev/null; then
        log_error "Failed to start $collector_name"
        return 1
    fi

    log_info "$collector_name started with PID $pid"

    # Give it a moment to initialize
    sleep 2

    # Start resource monitoring in background
    monitor_resources "$pid" "$result_dir/resources.csv" "$DURATION" &
    local monitor_pid=$!

    # Generate logs
    local start_time=$(python3 -c "import time; print(time.time())")
    local count
    count=$(generate_logs_fast "$LOG_FILE" "$RATE" "$DURATION")
    local end_time=$(python3 -c "import time; print(time.time())")

    # Calculate actual throughput
    local duration_s=$(python3 -c "print(f'{$end_time - $start_time:.3f}')")
    local actual_rate=$(python3 -c "print(f'{$count / ($end_time - $start_time):.2f}')")

    log_info "Generated $count log lines in ${duration_s}s (${actual_rate}/s actual)"

    # Wait for processing to complete (give extra time)
    log_info "Waiting for processing to complete..."
    sleep 5

    # Stop resource monitoring
    kill "$monitor_pid" 2>/dev/null || true
    wait "$monitor_pid" 2>/dev/null || true

    # Stop the collector
    log_info "Stopping $collector_name..."
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true

    # Analyze results
    analyze_results "$collector_name" "$result_dir" "$count" "$duration_s"
}

# Analyze benchmark results
analyze_results() {
    local collector_name=$1
    local result_dir=$2
    local log_count=$3
    local duration=$4

    log_info "Analyzing results for $collector_name..."

    local resources_file="$result_dir/resources.csv"

    if [[ -f "$resources_file" ]]; then
        # Calculate averages (skip header)
        local avg_cpu=$(tail -n +2 "$resources_file" | awk -F',' '{sum+=$2; count++} END {if(count>0) printf "%.2f", sum/count; else print "N/A"}')
        local max_cpu=$(tail -n +2 "$resources_file" | awk -F',' 'BEGIN{max=0} {if($2>max)max=$2} END {printf "%.2f", max}')
        local avg_mem=$(tail -n +2 "$resources_file" | awk -F',' '{sum+=$3; count++} END {if(count>0) printf "%.2f", sum/count; else print "N/A"}')
        local max_mem=$(tail -n +2 "$resources_file" | awk -F',' 'BEGIN{max=0} {if($3>max)max=$3} END {printf "%.2f", max}')

        local throughput=$(echo "scale=2; $log_count / $duration" | bc)

        # Save summary
        cat > "$result_dir/summary.txt" << EOF
Benchmark Results: $collector_name
=====================================
Duration:        ${duration}s
Log Count:       $log_count
Throughput:      ${throughput} logs/s
Avg CPU:         ${avg_cpu}%
Max CPU:         ${max_cpu}%
Avg Memory:      ${avg_mem} MB
Max Memory:      ${max_mem} MB
EOF

        cat "$result_dir/summary.txt"
    else
        log_warn "No resource data collected"
    fi
}

# Generate comparison report
generate_report() {
    log_info "============================================"
    log_info "BENCHMARK COMPARISON REPORT"
    log_info "============================================"

    echo ""
    echo "Test Parameters:"
    echo "  Duration:     ${DURATION}s"
    echo "  Target Rate:  ${RATE} logs/s"
    echo "  Log File:     $LOG_FILE"
    echo ""

    for collector in rotel otel; do
        local summary="$OUTPUT_DIR/$collector/summary.txt"
        if [[ -f "$summary" ]]; then
            echo ""
            cat "$summary"
        fi
    done

    echo ""
    echo "Detailed results saved to: $OUTPUT_DIR"
}

# Main
main() {
    log_info "File Receiver Benchmark"
    log_info "======================="
    log_info "Collector:  $COLLECTOR"
    log_info "Duration:   ${DURATION}s"
    log_info "Rate:       ${RATE} logs/s"
    log_info "Output:     $OUTPUT_DIR"
    echo ""

    # Create output directory
    mkdir -p "$OUTPUT_DIR"

    case "$COLLECTOR" in
        rotel)
            run_benchmark "rotel" start_rotel
            ;;
        otel)
            run_benchmark "otel" start_otel
            ;;
        both)
            run_benchmark "rotel" start_rotel
            echo ""
            sleep 5  # Cool down between tests
            run_benchmark "otel" start_otel
            generate_report
            ;;
        *)
            log_error "Unknown collector: $COLLECTOR"
            exit 1
            ;;
    esac

    log_success "Benchmark complete!"
}

main
