# Kafka Integration Tests

This document explains how to run the Kafka integration tests that verify the actual end-to-end functionality of the Kafka exporter.

## Prerequisites

- Docker and Docker Compose installed
- Rust toolchain with cargo
- The integration tests feature enabled

## Quick Start

1. **Start the Kafka test environment:**
   ```bash
   ./scripts/kafka-test-env.sh start
   ```

2. **Run the integration tests:**
   ```bash
   export OPENSSL_DIR=$(brew --prefix openssl@3)
   export OPENSSL_ROOT_DIR=$(brew --prefix openssl@3)
   export PKG_CONFIG_PATH="$(brew --prefix openssl@3)/lib/pkgconfig:$PKG_CONFIG_PATH"
   
   cargo test --test kafka_integration_tests --features integration-tests
   ```

3. **Stop the Kafka test environment:**
   ```bash
   ./scripts/kafka-test-env.sh stop
   ```

## Test Environment Details

The test environment includes:
- **Kafka broker** on `localhost:9092`
- **Zookeeper** on `localhost:2181`
- **Pre-created topics**: `otlp_traces`, `otlp_metrics`, `otlp_logs`

## Integration Tests

The integration tests verify:

### 1. `test_kafka_exporter_traces_json`
- Sends trace data to Kafka in JSON format
- Verifies message is received and contains valid JSON
- Validates trace structure (resource, scope_spans)

### 2. `test_kafka_exporter_metrics_protobuf`
- Sends metrics data in Protobuf format
- Verifies binary message is received
- Confirms non-empty protobuf payload

### 3. `test_kafka_exporter_logs_with_compression`
- Tests log export with gzip compression
- Verifies compressed messages are properly handled
- Validates log structure after decompression

### 4. `test_kafka_exporter_multiple_telemetry_types`
- Sends traces, metrics, and logs simultaneously
- Verifies all telemetry types reach their respective topics
- Tests concurrent operation

## Test Environment Management

Use the management script for common operations:

```bash
# Start Kafka environment
./scripts/kafka-test-env.sh start

# Check status
./scripts/kafka-test-env.sh status

# Consume messages from a topic (for debugging)
./scripts/kafka-test-env.sh consume otlp_traces

# View Kafka logs
./scripts/kafka-test-env.sh logs

# Stop environment
./scripts/kafka-test-env.sh stop
```

## Manual Testing

You can also manually test the Kafka exporter:

1. **Start Kafka:**
   ```bash
   ./scripts/kafka-test-env.sh start
   ```

2. **Run Rotel with Kafka exporter:**
   ```bash
   cargo run -- start --exporter kafka \
     --kafka-exporter-brokers localhost:9092 \
     --kafka-exporter-format json \
     --debug-log traces
   ```

3. **Send test data (in another terminal):**
   ```bash
   # Install telemetrygen if not already installed
   go install github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen@latest
   
   # Send traces
   telemetrygen traces --otlp-insecure --duration 5s
   ```

4. **Consume messages:**
   ```bash
   ./scripts/kafka-test-env.sh consume otlp_traces
   ```

## Troubleshooting

### Tests Fail with "Connection refused"
- Ensure Kafka is running: `./scripts/kafka-test-env.sh status`
- Check Docker containers: `docker ps`
- Restart the environment: `./scripts/kafka-test-env.sh stop && ./scripts/kafka-test-env.sh start`

### OpenSSL Build Errors
- Set environment variables as shown in the Quick Start
- On macOS, ensure OpenSSL is installed: `brew install openssl@3`

### Tests Timeout
- Increase the test timeout in the test code if needed
- Check Kafka logs: `./scripts/kafka-test-env.sh logs`
- Verify topics exist: `docker exec rotel-test-kafka kafka-topics --bootstrap-server localhost:9092 --list`

### Permission Issues
- Make sure the script is executable: `chmod +x scripts/kafka-test-env.sh`
- Check Docker permissions for your user

## Features Tested

- ✅ JSON serialization
- ✅ Protobuf serialization  
- ✅ Gzip compression
- ✅ Multiple telemetry types (traces, metrics, logs)
- ✅ Topic routing
- ✅ Producer error handling
- ✅ Concurrent message sending

## Note

These integration tests require a running Kafka instance and are not run by default with `cargo test`. They must be explicitly enabled with the `integration-tests` feature flag to ensure they don't interfere with regular unit testing workflows.