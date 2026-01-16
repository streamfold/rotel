#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# measure-latency.sh - Measure file event detection latency
#
# This script measures how quickly the file receiver detects new log lines
# by writing timestamped logs and checking when they appear in the output.
#
# Usage: ./measure-latency.sh [options]
#   -n, --count NUM       Number of latency samples to collect (default: 100)
#   -i, --interval MS     Interval between samples in ms (default: 100)
#   -w, --watch-mode MODE Watch mode: auto, native, poll (default: native)
#   -h, --help            Show this help message

set -e

COUNT=100
INTERVAL_MS=100
WATCH_MODE="native"
LOG_FILE="/tmp/latency-test.log"
OUTPUT_FILE="/tmp/latency-results.txt"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

while [[ $# -gt 0 ]]; do
    case $1 in
        -n|--count) COUNT="$2"; shift 2 ;;
        -i|--interval) INTERVAL_MS="$2"; shift 2 ;;
        -w|--watch-mode) WATCH_MODE="$2"; shift 2 ;;
        -h|--help) head -15 "$0" | tail -12; exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROTEL_DIR="$(dirname "$SCRIPT_DIR")"

echo -e "${BLUE}Latency Measurement Tool${NC}"
echo "========================"
echo "Samples:     $COUNT"
echo "Interval:    ${INTERVAL_MS}ms"
echo "Watch Mode:  $WATCH_MODE"
echo ""

# Build Rotel
echo "Building Rotel..."
(cd "$ROTEL_DIR" && cargo build --release --features file_receiver 2>/dev/null)

# Clean up
rm -f "$LOG_FILE" "$OUTPUT_FILE" /tmp/latency-offsets.json
touch "$LOG_FILE"

# Start Rotel with debug logging to capture processing times
echo "Starting Rotel..."
"$ROTEL_DIR/target/release/rotel" start \
    --receiver file \
    --file-receiver-include "$LOG_FILE" \
    --file-receiver-start-at beginning \
    --file-receiver-watch-mode "$WATCH_MODE" \
    --file-receiver-poll-interval-ms 50 \
    --file-receiver-offsets-path /tmp/latency-offsets.json \
    --exporter blackhole \
    --debug-log logs \
    2>&1 > /tmp/rotel-latency.log &

ROTEL_PID=$!
sleep 2

if ! kill -0 "$ROTEL_PID" 2>/dev/null; then
    echo "Failed to start Rotel"
    exit 1
fi

echo "Rotel started (PID: $ROTEL_PID)"
echo ""

# Function to get current time in nanoseconds
get_time_ns() {
    if [[ "$(uname)" == "Darwin" ]]; then
        # macOS: use python for nanosecond precision
        python3 -c "import time; print(int(time.time_ns()))"
    else
        date +%s%N
    fi
}

# Collect latency samples
echo "Collecting $COUNT latency samples..."
echo ""

latencies=()
interval_sec=$(echo "scale=3; $INTERVAL_MS / 1000" | bc)

for ((i = 1; i <= COUNT; i++)); do
    # Record write time
    write_time=$(get_time_ns)

    # Write a unique log line with embedded timestamp
    echo "LATENCY_TEST write_ts=$write_time sample=$i" >> "$LOG_FILE"

    # Check when it appears in the Rotel output (with timeout)
    timeout=2
    detected=false
    while [[ $timeout -gt 0 ]]; do
        if grep -q "write_ts=$write_time" /tmp/rotel-latency.log 2>/dev/null; then
            detect_time=$(get_time_ns)
            detected=true
            break
        fi
        sleep 0.01
        timeout=$((timeout - 1))
    done

    if $detected; then
        latency_ns=$((detect_time - write_time))
        latency_ms=$(echo "scale=3; $latency_ns / 1000000" | bc)
        latencies+=("$latency_ms")

        # Progress indicator
        if ((i % 10 == 0)); then
            echo -ne "\rProgress: $i/$COUNT (last latency: ${latency_ms}ms)    "
        fi
    else
        echo -ne "\rProgress: $i/$COUNT (timeout!)           "
    fi

    sleep "$interval_sec"
done

echo ""
echo ""

# Stop Rotel
echo "Stopping Rotel..."
kill "$ROTEL_PID" 2>/dev/null || true
wait "$ROTEL_PID" 2>/dev/null || true

# Calculate statistics
if [[ ${#latencies[@]} -gt 0 ]]; then
    # Sort latencies
    IFS=$'\n' sorted=($(sort -n <<<"${latencies[*]}")); unset IFS

    count=${#sorted[@]}
    min=${sorted[0]}
    max=${sorted[$((count-1))]}

    # Calculate percentiles
    p50_idx=$((count * 50 / 100))
    p90_idx=$((count * 90 / 100))
    p99_idx=$((count * 99 / 100))

    p50=${sorted[$p50_idx]}
    p90=${sorted[$p90_idx]}
    p99=${sorted[$p99_idx]}

    # Calculate mean
    sum=0
    for lat in "${sorted[@]}"; do
        sum=$(echo "$sum + $lat" | bc)
    done
    mean=$(echo "scale=3; $sum / $count" | bc)

    echo -e "${GREEN}Results (Watch Mode: $WATCH_MODE)${NC}"
    echo "================================"
    echo "Samples:     $count"
    echo "Min:         ${min}ms"
    echo "Max:         ${max}ms"
    echo "Mean:        ${mean}ms"
    echo "P50:         ${p50}ms"
    echo "P90:         ${p90}ms"
    echo "P99:         ${p99}ms"

    # Save results
    echo "watch_mode=$WATCH_MODE" > "$OUTPUT_FILE"
    echo "samples=$count" >> "$OUTPUT_FILE"
    echo "min_ms=$min" >> "$OUTPUT_FILE"
    echo "max_ms=$max" >> "$OUTPUT_FILE"
    echo "mean_ms=$mean" >> "$OUTPUT_FILE"
    echo "p50_ms=$p50" >> "$OUTPUT_FILE"
    echo "p90_ms=$p90" >> "$OUTPUT_FILE"
    echo "p99_ms=$p99" >> "$OUTPUT_FILE"

    echo ""
    echo "Results saved to: $OUTPUT_FILE"
else
    echo "No latency samples collected!"
fi

# Cleanup
rm -f "$LOG_FILE" /tmp/latency-offsets.json
