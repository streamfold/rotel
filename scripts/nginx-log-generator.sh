#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# nginx-log-generator.sh - Generate simulated nginx logs with rotation
#
# Usage: ./nginx-log-generator.sh [options]
#   -f, --file PATH       Log file path (default: /tmp/nginx-access.log)
#   -r, --rotate SECONDS  Rotation interval in seconds (default: 10)
#   -i, --interval MS     Write interval in milliseconds (default: 500)
#   -k, --keep COUNT      Number of rotated files to keep (default: 3)
#   -h, --help            Show this help message

set -e

# Defaults
LOG_FILE="/tmp/nginx-access.log"
ROTATE_INTERVAL=10
WRITE_INTERVAL_MS=500
KEEP_COUNT=3

# Sample data for generating realistic logs
METHODS=("GET" "POST" "PUT" "DELETE" "PATCH")
PATHS=("/api/users" "/api/orders" "/api/products" "/api/health" "/api/metrics" "/static/js/app.js" "/static/css/style.css" "/index.html" "/favicon.ico" "/api/v2/data")
STATUS_CODES=(200 200 200 200 201 204 301 302 400 401 403 404 500 502 503)
USER_AGENTS=(
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    "curl/7.68.0"
    "python-requests/2.28.0"
    "Go-http-client/1.1"
    "Apache-HttpClient/4.5.13"
)
REFERRERS=("-" "https://example.com" "https://google.com" "https://github.com")

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -f|--file)
            LOG_FILE="$2"
            shift 2
            ;;
        -r|--rotate)
            ROTATE_INTERVAL="$2"
            shift 2
            ;;
        -i|--interval)
            WRITE_INTERVAL_MS="$2"
            shift 2
            ;;
        -k|--keep)
            KEEP_COUNT="$2"
            shift 2
            ;;
        -h|--help)
            head -14 "$0" | tail -11
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Generate a random IP address
random_ip() {
    echo "$((RANDOM % 256)).$((RANDOM % 256)).$((RANDOM % 256)).$((RANDOM % 256))"
}

# Pick a random element from an array
random_choice() {
    local arr=("$@")
    echo "${arr[RANDOM % ${#arr[@]}]}"
}

# Generate a random nginx access log line
generate_log_line() {
    local ip=$(random_ip)
    local user="-"
    local timestamp=$(date "+%d/%b/%Y:%H:%M:%S %z")
    local method=$(random_choice "${METHODS[@]}")
    local path=$(random_choice "${PATHS[@]}")
    local status=$(random_choice "${STATUS_CODES[@]}")
    local bytes=$((RANDOM % 10000 + 100))
    local referrer=$(random_choice "${REFERRERS[@]}")
    local user_agent=$(random_choice "${USER_AGENTS[@]}")

    echo "$ip - $user [$timestamp] \"$method $path HTTP/1.1\" $status $bytes \"$referrer\" \"$user_agent\""
}

# Rotate log files
rotate_logs() {
    if [[ ! -f "$LOG_FILE" ]]; then
        return
    fi

    # Remove oldest if at limit
    local oldest="${LOG_FILE}.${KEEP_COUNT}"
    if [[ -f "$oldest" ]]; then
        rm -f "$oldest"
    fi

    # Shift existing rotated files
    for ((i = KEEP_COUNT - 1; i >= 1; i--)); do
        local src="${LOG_FILE}.${i}"
        local dst="${LOG_FILE}.$((i + 1))"
        if [[ -f "$src" ]]; then
            mv "$src" "$dst"
        fi
    done

    # Rotate current log
    mv "$LOG_FILE" "${LOG_FILE}.1"

    echo "[$(date '+%H:%M:%S')] Rotated $LOG_FILE -> ${LOG_FILE}.1"
}

# Cleanup on exit
cleanup() {
    echo ""
    echo "Stopping log generator..."
    exit 0
}
trap cleanup SIGINT SIGTERM

# Main
echo "nginx log generator"
echo "==================="
echo "Log file:        $LOG_FILE"
echo "Rotate interval: ${ROTATE_INTERVAL}s"
echo "Write interval:  ${WRITE_INTERVAL_MS}ms"
echo "Keep count:      $KEEP_COUNT"
echo ""
echo "Press Ctrl+C to stop"
echo ""

# Ensure directory exists
mkdir -p "$(dirname "$LOG_FILE")"

# Calculate write interval in seconds (for sleep)
WRITE_INTERVAL_SEC=$(echo "scale=3; $WRITE_INTERVAL_MS / 1000" | bc)

# Track time for rotation
LAST_ROTATE=$(date +%s)
LINE_COUNT=0

while true; do
    # Generate and write a log line
    LINE=$(generate_log_line)
    echo "$LINE" >> "$LOG_FILE"
    ((LINE_COUNT++))

    # Check if it's time to rotate
    NOW=$(date +%s)
    ELAPSED=$((NOW - LAST_ROTATE))

    if [[ $ELAPSED -ge $ROTATE_INTERVAL ]]; then
        echo "[$(date '+%H:%M:%S')] Wrote $LINE_COUNT lines in ${ELAPSED}s"
        rotate_logs
        LAST_ROTATE=$NOW
        LINE_COUNT=0
    fi

    # Sleep before next write
    sleep "$WRITE_INTERVAL_SEC"
done
