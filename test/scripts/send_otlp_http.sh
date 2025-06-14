#!/bin/bash

# test script to send OTLP JSON metrics, logs, or spans over HTTP
#
# Usage:
#   ./send_otlp_http.sh <log|metric|span> [--host HOST] [--port PORT]
#
# Example:
#   ./send_otlp_http.sh log
#   ./send_otlp_http.sh metric --host 127.0.0.1 --port 4318

set -euo pipefail

# Default values
HOST="localhost"
PORT=4318
SERVICE_NAME="test-service"

print_usage() {
  echo "Usage: $0 <log|metric|span> [--host HOST] [--port PORT] [--service-name SERVICE_NAME]" >&2
  exit 1
}

# Ensure at least one argument is provided
[[ $# -lt 1 ]] && print_usage

TYPE="$1"
shift

# Parse optional flags
while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)
      HOST="$2"
      shift 2
      ;;
    --port)
      PORT="$2"
      shift 2
      ;;
    --service-name|--service)
      SERVICE_NAME="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1" >&2
      print_usage
      ;;
  esac
done

# Generate current timestamp once (nanoseconds)
NOW_NANO="$(date +%s%N 2>/dev/null || true)"
if [[ "$NOW_NANO" == *N || -z "$NOW_NANO" ]]; then
  NOW_SEC=$(date +%s)
  NOW_NANO="${NOW_SEC}000000000"
fi

# Slightly later timestamp for span end (1ms later) to ensure end > start
END_NANO="$NOW_NANO"
if [[ "$END_NANO" =~ ^[0-9]+$ ]]; then
  END_NANO=$((END_NANO + 1000000))
fi

# Construct endpoint and payload based on TYPE
case "$TYPE" in
  log)
    ENDPOINT="/v1/logs"

    PAYLOAD=$(cat <<JSON
{
  "resourceLogs": [
    {
      "resource": {
        "attributes": [
          {
            "key": "service.name",
            "value": { "stringValue": "${SERVICE_NAME}" }
          }
        ]
      },
      "scopeLogs": [
        {
          "scope": { "name": "example", "version": "0.1" },
          "logRecords": [
            {
              "timeUnixNano": "${NOW_NANO}",
              "severityNumber": 9,
              "severityText": "Info",
              "body": { "stringValue": "Hello, OTLP log!" },
              "attributes": [
                {
                  "key": "example.attribute",
                  "value": { "stringValue": "value" }
                }
              ]
            }
          ]
        }
      ]
    }
  ]
}
JSON
)
    ;;
  metric|metrics)
    ENDPOINT="/v1/metrics"
    PAYLOAD=$(cat <<JSON
{
  "resourceMetrics": [
    {
      "resource": {
        "attributes": [
          {
            "key": "service.name",
            "value": { "stringValue": "${SERVICE_NAME}" }
          }
        ]
      },
      "scopeMetrics": [
        {
          "scope": { "name": "example", "version": "0.1" },
          "metrics": [
            {
              "name": "test_metric",
              "unit": "1",
              "sum": {
                "aggregationTemporality": 2,
                "isMonotonic": false,
                "dataPoints": [
                  {
                    "timeUnixNano": "${NOW_NANO}",
                    "asDouble": 123.45
                  }
                ]
              }
            }
          ]
        }
      ]
    }
  ]
}
JSON
)
    ;;
  span|spans|trace|traces)
    ENDPOINT="/v1/traces"
    PAYLOAD=$(cat <<JSON
{
  "resourceSpans": [
    {
      "resource": {
        "attributes": [
          {
            "key": "service.name",
            "value": { "stringValue": "${SERVICE_NAME}" }
          }
        ]
      },
      "scopeSpans": [
        {
          "scope": { "name": "example", "version": "0.1" },
          "spans": [
            {
              "traceId": "0102030405060708090a0b0c0d0e0f10",
              "spanId": "1112131415161718",
              "name": "test-span",
              "kind": 1,
              "startTimeUnixNano": "${NOW_NANO}",
              "endTimeUnixNano": "${END_NANO}",
              "attributes": [
                {
                  "key": "example.attribute",
                  "value": { "stringValue": "value" }
                }
              ]
            }
          ]
        }
      ]
    }
  ]
}
JSON
)
    ;;
  *)
    echo "Unknown type: $TYPE" >&2
    print_usage
    ;;
esac

URL="http://${HOST}:${PORT}${ENDPOINT}"

# Send the request and capture response body and status code
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "$URL" \
  -H "Content-Type: application/json" \
  -H "Expect:" \
  -d "$PAYLOAD")

# Extract status code (last line) and body (all but last line)
STATUS_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')

echo "Status Code: $STATUS_CODE"

# Print body if it exists
[[ -n "$BODY" ]] && echo -e "Response Body:\n$BODY"



