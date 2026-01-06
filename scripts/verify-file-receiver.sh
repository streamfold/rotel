#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# verify-file-receiver.sh - Verify file receiver processes all messages
#
# This script verifies that rotel's file receiver:
#   1. Processes every log message (no drops)
#   2. Handles file rotation correctly
#   3. Does not produce duplicates
#
# Usage: ./verify-file-receiver.sh [options]
#   -c, --count COUNT      Total log lines to generate (default: 10000)
#   -r, --rotations NUM    Number of file rotations (default: 4)
#   -m, --mode MODE        Watch mode: poll or native (default: native)
#   -h, --help             Show this help message

set -e

# Defaults
LOG_COUNT=10000
ROTATIONS=4
WATCH_MODE="native"
OUTPUT_DIR="/tmp/verify-file-receiver"
LOG_FILE="/tmp/verify-test.log"
SINK_PORT=14318

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -c|--count)
            LOG_COUNT="$2"
            shift 2
            ;;
        -r|--rotations)
            ROTATIONS="$2"
            shift 2
            ;;
        -m|--mode)
            WATCH_MODE="$2"
            shift 2
            ;;
        -h|--help)
            head -15 "$0" | tail -12
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

# Cleanup function
cleanup() {
    log_info "Cleaning up..."
    # Kill any background processes
    if [[ -n "$ROTEL_PID" ]] && kill -0 "$ROTEL_PID" 2>/dev/null; then
        kill -TERM "$ROTEL_PID" 2>/dev/null || true
    fi
    if [[ -n "$SINK_PID" ]] && kill -0 "$SINK_PID" 2>/dev/null; then
        kill -TERM "$SINK_PID" 2>/dev/null || true
    fi
}

trap cleanup EXIT

# Start a simple HTTP sink that captures OTLP log requests
start_http_sink() {
    local port=$1
    local output_file=$2

    log_info "Starting HTTP sink on port $port..."

    # Kill any existing process on the port
    lsof -ti:"$port" | xargs kill -9 2>/dev/null || true
    sleep 1

    python3 - "$port" "$output_file" << 'SINK_EOF' &
import http.server
import socketserver
import sys
import gzip

# Minimal protobuf parser for OTLP LogsData
# Wire format: field_number << 3 | wire_type
# Wire types: 0=varint, 2=length-delimited

def read_varint(data, pos):
    """Read a varint from data starting at pos, return (value, new_pos)"""
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

def parse_protobuf(data, handlers, path=""):
    """Parse protobuf data and call handlers for each field"""
    pos = 0
    while pos < len(data):
        if pos >= len(data):
            break
        tag, pos = read_varint(data, pos)
        field_num = tag >> 3
        wire_type = tag & 0x7

        if wire_type == 0:  # varint
            value, pos = read_varint(data, pos)
            if (path, field_num) in handlers:
                handlers[(path, field_num)](value)
        elif wire_type == 2:  # length-delimited
            length, pos = read_varint(data, pos)
            value = data[pos:pos+length]
            pos += length
            if (path, field_num) in handlers:
                handlers[(path, field_num)](value)
        elif wire_type == 5:  # 32-bit
            pos += 4
        elif wire_type == 1:  # 64-bit
            pos += 8
        else:
            break  # unknown wire type

def extract_log_bodies(data):
    """Extract log body strings from OTLP ExportLogsServiceRequest protobuf"""
    bodies = []

    def parse_any_value(av_data):
        """Parse AnyValue to extract stringValue (field 1)"""
        def handle_string(val):
            try:
                bodies.append(val.decode('utf-8'))
            except:
                pass
        parse_protobuf(av_data, {("", 1): handle_string})

    def parse_log_record(lr_data):
        """Parse LogRecord - body is field 5"""
        def handle_body(val):
            parse_any_value(val)
        parse_protobuf(lr_data, {("", 5): handle_body})

    def parse_scope_logs(sl_data):
        """Parse ScopeLogs - log_records is field 2"""
        def handle_log_record(val):
            parse_log_record(val)
        parse_protobuf(sl_data, {("", 2): handle_log_record})

    def parse_resource_logs(rl_data):
        """Parse ResourceLogs - scope_logs is field 2"""
        def handle_scope_logs(val):
            parse_scope_logs(val)
        parse_protobuf(rl_data, {("", 2): handle_scope_logs})

    # ExportLogsServiceRequest - resource_logs is field 1
    def handle_resource_logs(val):
        parse_resource_logs(val)

    parse_protobuf(data, {("", 1): handle_resource_logs})
    return bodies

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 14318
OUTPUT_FILE = sys.argv[2] if len(sys.argv) > 2 else "/tmp/sink-output.jsonl"

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
            # Parse protobuf and extract log bodies
            log_bodies = extract_log_bodies(body)
            if log_bodies:
                with open(OUTPUT_FILE, 'a') as f:
                    for log_body in log_bodies:
                        f.write(log_body + '\n')
        except Exception as e:
            with open(OUTPUT_FILE + '.error', 'a') as f:
                f.write(f"Error: {e}\n")

        # Return empty ExportLogsServiceResponse protobuf (empty message = empty bytes)
        self.send_response(200)
        self.send_header('Content-Type', 'application/x-protobuf')
        self.end_headers()
        self.wfile.write(b'')

with socketserver.TCPServer(("", PORT), OTLPHandler) as httpd:
    httpd.serve_forever()
SINK_EOF

    SINK_PID=$!

    # Wait for sink to start
    sleep 1

    if ! kill -0 "$SINK_PID" 2>/dev/null; then
        log_error "Failed to start HTTP sink"
        return 1
    fi

    log_info "HTTP sink started with PID $SINK_PID"
}

# Generate logs with unique sequence numbers, rotating periodically
generate_sequenced_logs() {
    local output_file=$1
    local total_count=$2
    local num_rotations=$3

    log_info "Generating $total_count log lines with $num_rotations rotations..."

    python3 - "$output_file" "$total_count" "$num_rotations" << 'GENEOF'
import time
import os
import sys

output_file = sys.argv[1]
total_count = int(sys.argv[2])
num_rotations = int(sys.argv[3])

# Calculate lines per rotation segment
lines_per_segment = total_count // (num_rotations + 1)

# Clean up any existing rotated files
for i in range(100):
    rotated = f"{output_file}.{i}"
    if os.path.exists(rotated):
        os.remove(rotated)

if os.path.exists(output_file):
    os.remove(output_file)

rotation_count = 0
lines_in_segment = 0

f = open(output_file, 'w')

for seq in range(1, total_count + 1):
    # Each log line is valid JSON with a unique sequence number
    line = f'{{"seq": {seq}, "msg": "test message {seq}"}}'
    f.write(line + '\n')
    lines_in_segment += 1

    # Rotate at specific intervals (but not on the last segment)
    if rotation_count < num_rotations and lines_in_segment >= lines_per_segment:
        f.flush()
        # Delay must be longer than poll interval (250ms) to ensure rotel discovers the file
        time.sleep(0.5)
        f.close()
        # Rotate: rename current to .N, create new file
        rotated_name = f"{output_file}.{rotation_count}"
        os.rename(output_file, rotated_name)
        f = open(output_file, 'w')
        rotation_count += 1
        lines_in_segment = 0

f.flush()
f.close()

print(f"Generated {total_count} log lines with {rotation_count} rotations")
GENEOF
}

# Start rotel pointing to the HTTP sink
start_rotel() {
    local log_file=$1
    local sink_port=$2

    log_info "Building Rotel..."
    (cd "$ROTEL_DIR" && cargo build --release --features file_receiver 2>/dev/null)

    log_info "Starting Rotel file receiver..."

    "$ROTEL_DIR/target/release/rotel" start \
        --receiver file \
        --file-receiver-include "$log_file" \
        --file-receiver-parser json \
        --file-receiver-start-at beginning \
        --file-receiver-watch-mode "$WATCH_MODE" \
        --file-receiver-offsets-path "$OUTPUT_DIR/offsets.json" \
        --exporter otlp \
        --otlp-exporter-endpoint "localhost:$sink_port" \
        --otlp-exporter-protocol http \
        > "$OUTPUT_DIR/rotel.log" 2>&1 &

    ROTEL_PID=$!
}

# Verify all sequences were processed exactly once
verify_sequences() {
    local expected_count=$1
    local captured_file=$2

    log_info "Verifying sequences..."

    python3 - "$expected_count" "$captured_file" << 'VERIFYEOF'
import sys
import json
import re
from collections import Counter

expected_count = int(sys.argv[1])
captured_file = sys.argv[2]

# Extract sequence numbers from captured log bodies
sequences = []
seq_pattern = re.compile(r'"seq":\s*(\d+)')

try:
    with open(captured_file, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            # Try to extract seq from JSON or raw text
            match = seq_pattern.search(line)
            if match:
                sequences.append(int(match.group(1)))
except FileNotFoundError:
    print(f"ERROR: Captured file not found: {captured_file}")
    sys.exit(1)

# Count occurrences of each sequence number
counts = Counter(sequences)

# Find issues
expected_set = set(range(1, expected_count + 1))
received_set = set(counts.keys())

missing = expected_set - received_set
unexpected = received_set - expected_set
duplicates = {seq: count for seq, count in counts.items() if count > 1}

# Print results
print(f"\n{'='*50}")
print(f"VERIFICATION RESULTS")
print(f"{'='*50}")
print(f"Expected messages:  {expected_count}")
print(f"Received messages:  {len(sequences)}")
print(f"Unique sequences:   {len(received_set)}")
print(f"Missing:            {len(missing)}")
print(f"Duplicates:         {len(duplicates)}")
print(f"Unexpected:         {len(unexpected)}")

if missing:
    missing_list = sorted(missing)
    print(f"\nMISSING sequences ({len(missing)}):")
    if len(missing_list) <= 20:
        print(f"  {missing_list}")
    else:
        print(f"  First 10: {missing_list[:10]}")
        print(f"  Last 10:  {missing_list[-10:]}")

if duplicates:
    print(f"\nDUPLICATE sequences ({len(duplicates)}):")
    dup_list = sorted(duplicates.items(), key=lambda x: -x[1])[:20]
    for seq, count in dup_list:
        print(f"  seq {seq}: {count} times")

if unexpected:
    unexpected_list = sorted(unexpected)
    print(f"\nUNEXPECTED sequences ({len(unexpected)}):")
    if len(unexpected_list) <= 20:
        print(f"  {unexpected_list}")
    else:
        print(f"  First 10: {unexpected_list[:10]}")

print(f"{'='*50}")

if len(missing) == 0 and len(duplicates) == 0 and len(unexpected) == 0:
    print("✓ ALL MESSAGES PROCESSED EXACTLY ONCE")
    sys.exit(0)
else:
    print("✗ VERIFICATION FAILED")
    sys.exit(1)
VERIFYEOF
}

# Main
main() {
    log_info "File Receiver Verification Test"
    log_info "================================"
    log_info "Log Count:   $LOG_COUNT"
    log_info "Rotations:   $ROTATIONS"
    log_info "Watch Mode:  $WATCH_MODE"
    log_info "Output:      $OUTPUT_DIR"
    echo ""

    # Create output directory
    rm -rf "$OUTPUT_DIR"
    mkdir -p "$OUTPUT_DIR"

    # Clean up any previous log files
    rm -f "$LOG_FILE" "$LOG_FILE".*

    # Initialize captured output file
    local captured_file="$OUTPUT_DIR/captured-logs.jsonl"
    > "$captured_file"

    # Start the HTTP sink first
    start_http_sink "$SINK_PORT" "$captured_file"

    # Start rotel
    start_rotel "$LOG_FILE" "$SINK_PORT"

    if [[ -z "$ROTEL_PID" ]] || ! kill -0 "$ROTEL_PID" 2>/dev/null; then
        log_error "Failed to start rotel"
        cat "$OUTPUT_DIR/rotel.log" 2>/dev/null || true
        exit 1
    fi

    log_info "Rotel started with PID $ROTEL_PID"

    # Give rotel a moment to initialize
    sleep 2

    # Now generate the log files (rotel will watch and process them)
    generate_sequenced_logs "$LOG_FILE" "$LOG_COUNT" "$ROTATIONS"

    # Wait for rotel to process all files
    # Calculate reasonable wait time based on count
    local wait_time=$((LOG_COUNT / 2000 + 10))
    log_info "Waiting ${wait_time}s for processing to complete..."
    sleep "$wait_time"

    # Stop rotel gracefully
    log_info "Stopping rotel..."
    kill -TERM "$ROTEL_PID" 2>/dev/null || true

    # Wait for graceful shutdown
    local timeout=10
    while kill -0 "$ROTEL_PID" 2>/dev/null && [[ $timeout -gt 0 ]]; do
        sleep 1
        ((timeout--))
    done

    # Force kill if still running
    if kill -0 "$ROTEL_PID" 2>/dev/null; then
        log_warn "Force killing rotel..."
        kill -9 "$ROTEL_PID" 2>/dev/null || true
    fi

    ROTEL_PID=""

    # Give sink a moment to flush
    sleep 1

    # Stop the sink
    log_info "Stopping HTTP sink..."
    kill -TERM "$SINK_PID" 2>/dev/null || true
    sleep 1
    SINK_PID=""

    # Verify the results
    echo ""
    if verify_sequences "$LOG_COUNT" "$captured_file"; then
        log_success "Verification PASSED!"
        exit 0
    else
        log_error "Verification FAILED!"
        log_info "Rotel log: $OUTPUT_DIR/rotel.log"
        log_info "Captured logs: $captured_file"
        exit 1
    fi
}

main
