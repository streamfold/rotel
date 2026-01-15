#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# benchmark-file-receiver.sh - Benchmark file receiver performance
#
# This script measures:
#   1. End-to-end throughput (logs per second through full pipeline)
#   2. Processing time (time to receive all exported messages)
#   3. CPU and memory usage
#
# Usage: ./benchmark-file-receiver.sh [options]
#   -c, --collector TYPE   Collector to test: rotel, otel, or both (default: rotel)
#   -d, --duration SECS    Test duration in seconds (default: 60)
#   -r, --rate RATE        Log lines per second (default: 1000)
#   -n, --files NUM        Number of log files to write to (default: 1)
#   -m, --mode MODE        Watch mode: poll or native (default: poll)
#   -D, --debounce MS      Debounce interval in ms for native watcher (default: 200) [rotel only]
#   -p, --protocol PROTO   OTLP protocol: grpc or http (default: grpc) [verify mode only]
#   -t, --timeout SECS     Max seconds to wait for processing (default: 300) [verify mode only]
#   -v, --verify           Enable verify mode: export to OTLP sink and count messages
#   -o, --output DIR       Output directory for results (default: /tmp/benchmark)
#   -h, --help             Show this help message

set -e

# Defaults
COLLECTOR="rotel"
DURATION=60
RATE=1000
NUM_FILES=1
WATCH_MODE="poll"
DEBOUNCE_MS=200
PROTOCOL="grpc"
TIMEOUT=300
VERIFY=false
OUTPUT_DIR="/tmp/benchmark"
LOG_BASE="/tmp/benchmark-nginx"
SINK_PID=""

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
        -D|--debounce)
            DEBOUNCE_MS="$2"
            shift 2
            ;;
        -p|--protocol)
            PROTOCOL="$2"
            shift 2
            ;;
        -t|--timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        -v|--verify)
            VERIFY=true
            shift
            ;;
        -o|--output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -h|--help)
            head -23 "$0" | tail -20
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Set sink port based on protocol
if [[ "$PROTOCOL" == "grpc" ]]; then
    SINK_PORT=14317
else
    SINK_PORT=14318
fi

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

# Cleanup function
cleanup() {
    log_info "Cleaning up..."
    if [[ -n "$COLLECTOR_PID" ]] && kill -0 "$COLLECTOR_PID" 2>/dev/null; then
        kill -TERM "$COLLECTOR_PID" 2>/dev/null || true
    fi
    if [[ -n "$SINK_PID" ]] && kill -0 "$SINK_PID" 2>/dev/null; then
        kill -TERM "$SINK_PID" 2>/dev/null || true
    fi
    if [[ -n "$MONITOR_PID" ]] && kill -0 "$MONITOR_PID" 2>/dev/null; then
        kill -TERM "$MONITOR_PID" 2>/dev/null || true
    fi
}

trap cleanup EXIT

# Start gRPC OTLP sink that counts received log records
start_grpc_sink() {
    local port=$1
    local count_file=$2

    log_info "Starting gRPC OTLP sink on port $port..."

    # Check for required packages
    if ! python3 -c "import grpc; from opentelemetry.proto.collector.logs.v1 import logs_service_pb2_grpc" 2>/dev/null; then
        log_error "gRPC sink requires: pip install grpcio opentelemetry-proto"
        log_error "Or use HTTP protocol: -p http"
        return 1
    fi

    # Kill any existing process on the port
    lsof -ti:"$port" | xargs kill -9 2>/dev/null || true
    sleep 1

    # Initialize count file
    echo "0" > "$count_file"

    python3 - "$port" "$count_file" << 'GRPC_SINK_EOF' &
import sys
import grpc
from concurrent import futures
import threading

from opentelemetry.proto.collector.logs.v1 import logs_service_pb2
from opentelemetry.proto.collector.logs.v1 import logs_service_pb2_grpc

PORT = int(sys.argv[1])
COUNT_FILE = sys.argv[2]

# Global counter with lock
counter = 0
counter_lock = threading.Lock()

def update_count(delta):
    global counter
    with counter_lock:
        counter += delta
        with open(COUNT_FILE, 'w') as f:
            f.write(str(counter))

class LogsServiceServicer(logs_service_pb2_grpc.LogsServiceServicer):
    def Export(self, request, context):
        # Count log records in the request
        count = 0
        for resource_logs in request.resource_logs:
            for scope_logs in resource_logs.scope_logs:
                count += len(scope_logs.log_records)

        if count > 0:
            update_count(count)

        return logs_service_pb2.ExportLogsServiceResponse()

def serve():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        options=[
            ('grpc.max_receive_message_length', 64 * 1024 * 1024),
            ('grpc.max_send_message_length', 64 * 1024 * 1024),
        ]
    )
    logs_service_pb2_grpc.add_LogsServiceServicer_to_server(LogsServiceServicer(), server)
    server.add_insecure_port(f'[::]:{PORT}')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
GRPC_SINK_EOF

    SINK_PID=$!
    sleep 1

    if ! kill -0 "$SINK_PID" 2>/dev/null; then
        log_error "Failed to start gRPC sink"
        return 1
    fi

    log_info "gRPC sink started with PID $SINK_PID"
}

# Start HTTP OTLP sink that counts received log records
start_http_sink() {
    local port=$1
    local count_file=$2

    log_info "Starting HTTP OTLP sink on port $port..."

    # Kill any existing process on the port
    lsof -ti:"$port" | xargs kill -9 2>/dev/null || true
    sleep 1

    # Initialize count file
    echo "0" > "$count_file"

    python3 - "$port" "$count_file" << 'HTTP_SINK_EOF' &
import http.server
import socketserver
import sys
import gzip
import threading

PORT = int(sys.argv[1])
COUNT_FILE = sys.argv[2]

# Global counter with lock
counter = 0
counter_lock = threading.Lock()

def read_varint(data, pos):
    """Read a varint from data starting at pos"""
    result = 0
    shift = 0
    while pos < len(data):
        b = data[pos]
        result |= (b & 0x7f) << shift
        pos += 1
        if (b & 0x80) == 0:
            break
        shift += 7
    return result, pos

def count_log_records(data):
    """Count log records in ExportLogsServiceRequest protobuf"""
    count = 0
    pos = 0

    while pos < len(data):
        if pos >= len(data):
            break
        tag, pos = read_varint(data, pos)
        field_num = tag >> 3
        wire_type = tag & 0x7

        if wire_type == 2:
            length, pos = read_varint(data, pos)
            field_data = data[pos:pos+length]
            pos += length

            if field_num == 1:  # ResourceLogs
                rl_pos = 0
                while rl_pos < len(field_data):
                    rl_tag, rl_pos = read_varint(field_data, rl_pos)
                    rl_field = rl_tag >> 3
                    rl_wire = rl_tag & 0x7

                    if rl_wire == 2:
                        rl_len, rl_pos = read_varint(field_data, rl_pos)
                        rl_field_data = field_data[rl_pos:rl_pos+rl_len]
                        rl_pos += rl_len

                        if rl_field == 2:  # ScopeLogs
                            sl_pos = 0
                            while sl_pos < len(rl_field_data):
                                sl_tag, sl_pos = read_varint(rl_field_data, sl_pos)
                                sl_field = sl_tag >> 3
                                sl_wire = sl_tag & 0x7

                                if sl_wire == 2:
                                    sl_len, sl_pos = read_varint(rl_field_data, sl_pos)
                                    sl_pos += sl_len
                                    if sl_field == 2:  # LogRecord
                                        count += 1
                                elif sl_wire == 0:
                                    _, sl_pos = read_varint(rl_field_data, sl_pos)
                                elif sl_wire == 1:
                                    sl_pos += 8
                                elif sl_wire == 5:
                                    sl_pos += 4
                                else:
                                    break
                    elif rl_wire == 0:
                        _, rl_pos = read_varint(field_data, rl_pos)
                    elif rl_wire == 1:
                        rl_pos += 8
                    elif rl_wire == 5:
                        rl_pos += 4
                    else:
                        break
        elif wire_type == 0:
            _, pos = read_varint(data, pos)
        elif wire_type == 1:
            pos += 8
        elif wire_type == 5:
            pos += 4
        else:
            break

    return count

def update_count(delta):
    global counter
    with counter_lock:
        counter += delta
        with open(COUNT_FILE, 'w') as f:
            f.write(str(counter))

class OTLPHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def do_POST(self):
        content_length = int(self.headers.get('Content-Length', 0))
        content_encoding = self.headers.get('Content-Encoding', '')

        body = self.rfile.read(content_length)

        if content_encoding == 'gzip':
            body = gzip.decompress(body)

        try:
            count = count_log_records(body)
            if count > 0:
                update_count(count)
        except Exception as e:
            pass

        self.send_response(200)
        self.send_header('Content-Type', 'application/x-protobuf')
        self.end_headers()
        self.wfile.write(b'')

with socketserver.TCPServer(("", PORT), OTLPHandler) as httpd:
    httpd.serve_forever()
HTTP_SINK_EOF

    SINK_PID=$!
    sleep 1

    if ! kill -0 "$SINK_PID" 2>/dev/null; then
        log_error "Failed to start HTTP sink"
        return 1
    fi

    log_info "HTTP sink started with PID $SINK_PID"
}

# Wait for all messages to be received, returns processing time in ms
# Sets PROCESSING_TIME_MS and RECEIVED_COUNT globals
wait_for_processing() {
    local expected_count=$1
    local count_file=$2
    local timeout_secs=$3

    log_info "Waiting for $expected_count messages (timeout: ${timeout_secs}s)..."

    local start_time=$(python3 -c "import time; print(int(time.time() * 1000))")
    local deadline=$(($(date +%s) + timeout_secs))
    local last_count=0
    local stable_iterations=0

    while [[ $(date +%s) -lt $deadline ]]; do
        local current_count=$(cat "$count_file" 2>/dev/null || echo 0)

        if [[ $current_count -ge $expected_count ]]; then
            local end_time=$(python3 -c "import time; print(int(time.time() * 1000))")
            PROCESSING_TIME_MS=$((end_time - start_time))
            RECEIVED_COUNT=$current_count
            log_info "All $expected_count messages received"
            return 0
        fi

        if [[ $current_count -ne $last_count ]]; then
            local pct=$((current_count * 100 / expected_count))
            log_info "Progress: $current_count / $expected_count ($pct%)"
            last_count=$current_count
            stable_iterations=0
        else
            ((stable_iterations++))
            if [[ $stable_iterations -ge 10 && $current_count -gt 0 ]]; then
                log_warn "No progress for 10s at $current_count messages"
                stable_iterations=0
            fi
        fi

        sleep 1
    done

    local end_time=$(python3 -c "import time; print(int(time.time() * 1000))")
    PROCESSING_TIME_MS=$((end_time - start_time))
    RECEIVED_COUNT=$(cat "$count_file" 2>/dev/null || echo 0)
    log_warn "Timeout after ${timeout_secs}s with $RECEIVED_COUNT / $expected_count messages"
    return 1
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
    local sink_port=$2

    log_info "Building Rotel..."
    (cd "$ROTEL_DIR" && cargo build --release --features file_receiver 2>/dev/null)

    if [[ "$VERIFY" == "true" ]]; then
        log_info "Starting Rotel file receiver (verify mode, protocol: $PROTOCOL, debounce: ${DEBOUNCE_MS}ms)..."
        "$ROTEL_DIR/target/release/rotel" start \
            --receiver file \
            --file-receiver-include "${log_base}-*.log" \
            --file-receiver-parser nginx_access \
            --file-receiver-start-at beginning \
            --file-receiver-watch-mode "$WATCH_MODE" \
            --file-receiver-debounce-interval-ms "$DEBOUNCE_MS" \
            --file-receiver-offsets-path /tmp/benchmark-rotel-offsets.json \
            --exporter otlp \
            --otlp-exporter-endpoint "localhost:$sink_port" \
            --otlp-exporter-protocol "$PROTOCOL" \
            > "$OUTPUT_DIR/rotel.log" 2>&1 &
    else
        log_info "Starting Rotel file receiver (blackhole mode, debounce: ${DEBOUNCE_MS}ms)..."
        "$ROTEL_DIR/target/release/rotel" start \
            --receiver file \
            --file-receiver-include "${log_base}-*.log" \
            --file-receiver-parser nginx_access \
            --file-receiver-start-at beginning \
            --file-receiver-watch-mode "$WATCH_MODE" \
            --file-receiver-debounce-interval-ms "$DEBOUNCE_MS" \
            --file-receiver-offsets-path /tmp/benchmark-rotel-offsets.json \
            --exporter blackhole \
            > "$OUTPUT_DIR/rotel.log" 2>&1 &
    fi

    COLLECTOR_PID=$!
}

# Start OTel Collector
start_otel() {
    local log_base=$1
    local sink_port=$2
    local config_file="$OUTPUT_DIR/otel-config.yaml"

    # Check for custom build first, then otelcol-contrib
    local otelcol_bin=""
    local custom_build="$ROTEL_DIR/benchmark/otel-collector/otelcol-benchmark/otelcol-benchmark"
    local use_custom=false

    if [[ -x "$custom_build" ]]; then
        otelcol_bin="$custom_build"
        use_custom=true
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

    # Configure exporter based on mode
    local exporter_config=""
    local exporter_name=""

    if [[ "$VERIFY" == "true" ]]; then
        # Custom build only has 'otlp' (gRPC) exporter, not 'otlphttp'
        if [[ "$use_custom" == "true" && "$PROTOCOL" == "http" ]]; then
            log_warn "Custom OTel build only supports gRPC. Use otelcol-contrib for HTTP."
            log_warn "Install: brew install open-telemetry/opentelemetry-collector/otelcol-contrib"
            return 1
        fi

        log_info "Creating OTel Collector config (verify mode, protocol: $PROTOCOL)..."

        if [[ "$PROTOCOL" == "grpc" ]]; then
            exporter_name="otlp"
            exporter_config="  otlp:
    endpoint: localhost:$sink_port
    tls:
      insecure: true"
        else
            exporter_name="otlphttp"
            exporter_config="  otlphttp:
    endpoint: http://localhost:$sink_port
    tls:
      insecure: true"
        fi
    else
        log_info "Creating OTel Collector config (nop mode)..."
        exporter_name="nop"
        exporter_config="  nop: {}"
    fi

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
$exporter_config

service:
  pipelines:
    logs:
      receivers: [filelog]
      processors: [memory_limiter, batch]
      exporters: [$exporter_name]
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

    # Count file for the sink (only used in verify mode)
    local count_file="$result_dir/received_count.txt"

    # Start the sink only in verify mode
    SINK_PID=""
    if [[ "$VERIFY" == "true" ]]; then
        if [[ "$PROTOCOL" == "grpc" ]]; then
            start_grpc_sink "$SINK_PORT" "$count_file"
        else
            start_http_sink "$SINK_PORT" "$count_file"
        fi

        if [[ -z "$SINK_PID" ]] || ! kill -0 "$SINK_PID" 2>/dev/null; then
            log_error "Failed to start sink"
            return 1
        fi
    fi

    # Start the collector (sets COLLECTOR_PID global)
    COLLECTOR_PID=""
    $start_func "$LOG_BASE" "$SINK_PORT"

    local pid=$COLLECTOR_PID

    if [[ -z "$pid" ]] || ! kill -0 "$pid" 2>/dev/null; then
        log_error "Failed to start $collector_name"
        if [[ -n "$SINK_PID" ]]; then
            kill "$SINK_PID" 2>/dev/null || true
            SINK_PID=""
        fi
        return 1
    fi

    log_info "$collector_name started with PID $pid"

    # Give it a moment to initialize
    sleep 2

    # Start resource monitoring in background
    local monitor_duration=$DURATION
    if [[ "$VERIFY" == "true" ]]; then
        monitor_duration=$((DURATION + TIMEOUT))
    fi
    monitor_resources "$pid" "$result_dir/resources.csv" "$((monitor_duration + 10))" &
    MONITOR_PID=$!

    # Record start time (after initialization)
    local total_start_time=$(python3 -c "import time; print(int(time.time() * 1000))")

    # Generate logs across multiple files
    local gen_start_time=$(python3 -c "import time; print(int(time.time() * 1000))")
    local count
    count=$(generate_logs_fast "$LOG_BASE" "$RATE" "$DURATION" "$NUM_FILES")
    local gen_end_time=$(python3 -c "import time; print(int(time.time() * 1000))")
    local gen_time_ms=$((gen_end_time - gen_start_time))

    log_info "Generated $count log lines in $((gen_time_ms / 1000))s"

    # Wait for processing
    PROCESSING_TIME_MS=0
    RECEIVED_COUNT=0

    if [[ "$VERIFY" == "true" ]]; then
        # Wait for all messages to be received by the sink
        wait_for_processing "$count" "$count_file" "$TIMEOUT"
    else
        # In blackhole mode, wait a fixed time for collector to process
        log_info "Waiting for processing to complete..."
        sleep 5
        RECEIVED_COUNT="N/A"
        local proc_end_time=$(python3 -c "import time; print(int(time.time() * 1000))")
        PROCESSING_TIME_MS=$((proc_end_time - gen_end_time))
    fi

    local total_end_time=$(python3 -c "import time; print(int(time.time() * 1000))")
    local total_time_ms=$((total_end_time - total_start_time))

    # Stop resource monitoring
    kill "$MONITOR_PID" 2>/dev/null || true
    wait "$MONITOR_PID" 2>/dev/null || true
    MONITOR_PID=""

    # Stop the collector
    log_info "Stopping $collector_name..."
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    COLLECTOR_PID=""

    # Stop the sink if running
    if [[ -n "$SINK_PID" ]]; then
        log_info "Stopping sink..."
        kill "$SINK_PID" 2>/dev/null || true
        wait "$SINK_PID" 2>/dev/null || true
        SINK_PID=""
    fi

    # Analyze results
    analyze_results "$collector_name" "$result_dir" "$count" "$RECEIVED_COUNT" "$gen_time_ms" "$PROCESSING_TIME_MS" "$total_time_ms"

    # Show rotel logs if requested
    if [[ "$collector_name" == "rotel" ]]; then
        echo ""
        log_info "=== Rotel Logs (last 100 lines) ==="
        if [[ -f "$OUTPUT_DIR/rotel.log" ]]; then
            tail -100 "$OUTPUT_DIR/rotel.log"
        else
            log_warn "No rotel log file found"
        fi
        echo ""
        log_info "=== Full logs at: $OUTPUT_DIR/rotel.log ==="
    fi
}

# Analyze benchmark results
analyze_results() {
    local collector_name=$1
    local result_dir=$2
    local expected_count=$3
    local received_count=$4
    local gen_time_ms=$5
    local processing_time_ms=$6
    local total_time_ms=$7

    log_info "Analyzing results for $collector_name..."

    local resources_file="$result_dir/resources.csv"

    # Calculate throughput and status based on mode
    local throughput="N/A"
    local status="N/A"
    local drop_rate="N/A"

    if [[ "$received_count" == "N/A" ]]; then
        # Blackhole mode - calculate generation rate as throughput proxy
        if [[ $gen_time_ms -gt 0 ]]; then
            throughput=$(python3 -c "print(f'{$expected_count * 1000 / $gen_time_ms:.2f}')")
        fi
        status="(blackhole)"
    else
        # Verify mode - calculate actual throughput from received messages
        if [[ $processing_time_ms -gt 0 ]]; then
            throughput=$(python3 -c "print(f'{$received_count * 1000 / $processing_time_ms:.2f}')")
        fi

        # Check if all messages were received
        if [[ $received_count -ge $expected_count ]]; then
            status="PASS"
            drop_rate="0.00"
        else
            status="INCOMPLETE"
            drop_rate=$(python3 -c "print(f'{($expected_count - $received_count) * 100 / $expected_count:.2f}')")
        fi
    fi

    local avg_cpu="N/A"
    local max_cpu="N/A"
    local avg_mem="N/A"
    local max_mem="N/A"

    if [[ -f "$resources_file" ]]; then
        # Calculate averages (skip header)
        avg_cpu=$(tail -n +2 "$resources_file" | awk -F',' '{sum+=$2; count++} END {if(count>0) printf "%.2f", sum/count; else print "N/A"}')
        max_cpu=$(tail -n +2 "$resources_file" | awk -F',' 'BEGIN{max=0} {if($2>max)max=$2} END {printf "%.2f", max}')
        avg_mem=$(tail -n +2 "$resources_file" | awk -F',' '{sum+=$3; count++} END {if(count>0) printf "%.2f", sum/count; else print "N/A"}')
        max_mem=$(tail -n +2 "$resources_file" | awk -F',' 'BEGIN{max=0} {if($3>max)max=$3} END {printf "%.2f", max}')
    fi

    # Determine mode string
    local mode_str="blackhole"
    if [[ "$VERIFY" == "true" ]]; then
        mode_str="verify ($PROTOCOL)"
    fi

    # Save summary
    cat > "$result_dir/summary.txt" << EOF
Benchmark Results: $collector_name
=====================================
Status:          $status
Mode:            $mode_str
Watch Mode:      $WATCH_MODE
Num Files:       $NUM_FILES

Messages:
  Expected:      $expected_count
  Received:      $received_count
  Drop Rate:     $drop_rate

Timing:
  Generation:    ${gen_time_ms}ms ($(python3 -c "print(f'{$gen_time_ms/1000:.2f}')")s)
  Processing:    ${processing_time_ms}ms ($(python3 -c "print(f'{$processing_time_ms/1000:.2f}')")s)
  Total:         ${total_time_ms}ms ($(python3 -c "print(f'{$total_time_ms/1000:.2f}')")s)

Performance:
  Throughput:    ${throughput} msgs/s
  Avg CPU:       ${avg_cpu}%
  Max CPU:       ${max_cpu}%
  Avg Memory:    ${avg_mem} MB
  Max Memory:    ${max_mem} MB
EOF

    echo ""
    cat "$result_dir/summary.txt"
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
    echo "  Watch Mode:   $WATCH_MODE"
    if [[ "$VERIFY" == "true" ]]; then
        echo "  Mode:         verify (OTLP export)"
        echo "  Protocol:     $PROTOCOL"
    else
        echo "  Mode:         blackhole (no export)"
    fi
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
    if [[ "$COLLECTOR" == "rotel" || "$COLLECTOR" == "both" ]]; then
        log_info "Debounce:   ${DEBOUNCE_MS}ms (rotel only)"
    fi
    if [[ "$VERIFY" == "true" ]]; then
        log_info "Mode:       verify (OTLP export)"
        log_info "Protocol:   $PROTOCOL"
        log_info "Timeout:    ${TIMEOUT}s"
    else
        log_info "Mode:       blackhole (no export)"
    fi
    log_info "Output:     $OUTPUT_DIR"
    echo ""

    # Validate protocol (only needed in verify mode)
    if [[ "$VERIFY" == "true" && "$PROTOCOL" != "grpc" && "$PROTOCOL" != "http" ]]; then
        log_error "Invalid protocol: $PROTOCOL (must be 'grpc' or 'http')"
        exit 1
    fi

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
