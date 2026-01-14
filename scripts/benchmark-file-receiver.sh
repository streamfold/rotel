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
#   -n, --files NUM        Number of log files to write to (default: 1)
#   -m, --mode MODE        Watch mode: poll or native (default: poll)
#   -o, --output DIR       Output directory for results (default: /tmp/benchmark)
#   -h, --help             Show this help message

set -e

# Defaults
COLLECTOR="rotel"
DURATION=60
RATE=1000
NUM_FILES=1
WATCH_MODE="poll"
OUTPUT_DIR="/tmp/benchmark"
LOG_BASE="/tmp/benchmark-nginx"

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
        -n|--files)
            NUM_FILES="$2"
            shift 2
            ;;
        -m|--mode)
            WATCH_MODE="$2"
            shift 2
            ;;
        -o|--output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -h|--help)
            head -18 "$0" | tail -15
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

# High-performance log generator using Python for speed
generate_logs_fast() {
    local log_base=$1
    local rate=$2
    local duration=$3
    local num_files=$4

    log_info "Generating logs at ${rate}/s for ${duration}s across ${num_files} file(s)" >&2

    # Use Python for much faster log generation with rotation
    python3 << EOF
import time
import random
import os

log_base = "$log_base"
rate = $rate
duration = $duration
num_files = $num_files
rotate_interval = 5  # Rotate every 5 seconds

methods = ["GET", "POST", "PUT", "DELETE"]
paths = ["/api/users", "/api/orders", "/api/products", "/api/health"]
statuses = [200, 201, 204, 400, 404, 500]

# Calculate batch size and interval
# Write in batches of 1000 lines, sleep between batches
batch_size = min(1000, rate)
batches_per_second = rate / batch_size
sleep_interval = 1.0 / batches_per_second if batches_per_second > 0 else 0

start_time = time.time()
end_time = start_time + duration
count = 0

# Build list of output files: {base}-0.log, {base}-1.log, etc.
output_files = [f"{log_base}-{i}.log" for i in range(num_files)]

# Track rotation state per file
rotation_counts = [0] * num_files
last_rotates = [start_time] * num_files

# Clear any existing rotated files for all log files
for output_file in output_files:
    for i in range(100):
        rotated = f"{output_file}.{i}"
        if os.path.exists(rotated):
            os.remove(rotated)

# Open all files
file_handles = [open(f, 'w') for f in output_files]

# Current file index for round-robin distribution
current_file_idx = 0

while time.time() < end_time:
    batch_start = time.time()

    # Check if any files need rotation
    for idx in range(num_files):
        if batch_start - last_rotates[idx] >= rotate_interval:
            file_handles[idx].close()
            # Rotate: rename current to .N, create new file
            rotated_name = f"{output_files[idx]}.{rotation_counts[idx]}"
            if os.path.exists(output_files[idx]):
                os.rename(output_files[idx], rotated_name)
            file_handles[idx] = open(output_files[idx], 'w')
            rotation_counts[idx] += 1
            last_rotates[idx] = batch_start

    # Generate a batch of log lines
    lines = []
    timestamp = time.strftime("%d/%b/%Y:%H:%M:%S %z")
    for _ in range(batch_size):
        ip = f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}"
        method = random.choice(methods)
        path = random.choice(paths)
        status = random.choice(statuses)
        lines.append(f'{ip} - - [{timestamp}] "{method} {path} HTTP/1.1" {status} 1234 "-" "benchmark/{int(time.time())}"')

    # Write batch to current file (round-robin across files)
    file_handles[current_file_idx].write('\n'.join(lines) + '\n')
    file_handles[current_file_idx].flush()
    count += batch_size

    # Move to next file for next batch
    current_file_idx = (current_file_idx + 1) % num_files

    # Sleep to maintain rate
    elapsed = time.time() - batch_start
    if sleep_interval > elapsed:
        time.sleep(sleep_interval - elapsed)

# Close all files
for f in file_handles:
    f.close()

print(count)
EOF
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
    local log_base=$1

    log_info "Building Rotel..."
    (cd "$ROTEL_DIR" && cargo build --release --features file_receiver 2>/dev/null)

    log_info "Starting Rotel file receiver..."

    # Create a minimal config pointing to our benchmark logs
    # Use glob pattern to match all log files: {base}-*.log
    "$ROTEL_DIR/target/release/rotel" start \
        --receiver file \
        --file-receiver-include "${log_base}-*.log" \
        --file-receiver-parser nginx_access \
        --file-receiver-start-at beginning \
        --file-receiver-watch-mode "$WATCH_MODE" \
        --file-receiver-offsets-path /tmp/benchmark-rotel-offsets.json \
        --exporter blackhole \
        > "$OUTPUT_DIR/rotel.log" 2>&1 &

    COLLECTOR_PID=$!
}

# Start OTel Collector
start_otel() {
    local log_base=$1
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

    # Use glob pattern to match all log files: {base}-*.log
    cat > "$config_file" << EOF
receivers:
  filelog:
    include:
      - ${log_base}-*.log
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

    # Clean up any previous state - remove all matching log files and rotations
    rm -f "${LOG_BASE}"-*.log "${LOG_BASE}"-*.log.* /tmp/benchmark-*-offsets.json

    # Create initial log files: {base}-0.log, {base}-1.log, etc.
    for ((i=0; i<NUM_FILES; i++)); do
        touch "${LOG_BASE}-${i}.log"
    done

    # Start the collector (sets COLLECTOR_PID global)
    COLLECTOR_PID=""
    $start_func "$LOG_BASE"

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

    # Generate logs across multiple files
    local start_time=$(python3 -c "import time; print(time.time())")
    local count
    count=$(generate_logs_fast "$LOG_BASE" "$RATE" "$DURATION" "$NUM_FILES")
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
Num Files:       $NUM_FILES
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
    echo "  Num Files:    ${NUM_FILES}"
    echo "  Log Pattern:  ${LOG_BASE}-*.log"
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
    log_info "Num Files:  $NUM_FILES"
    log_info "Watch Mode: $WATCH_MODE"
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
