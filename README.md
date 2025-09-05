<p align="center">
  <h1 align="center">Rotel 🌶️ 🍅</h1>
</p>

<p align="center">
  High Performance, Resource Efficient OpenTelemetry Collection
</p>

<p align="center">
  <a title="Releases" target="_blank" href="https://github.com/streamfold/rotel/releases"><img src="https://img.shields.io/github/release/streamfold/rotel?style=flat-square&color=9CF"></a>
 </p>

<p align="center">
  <a title="Rotel Discord" target="_blank" href="https://rotel.dev/discord"><img alt="Chat with Rotel users and develpers on Discord" src="https://img.shields.io/discord/1349105218268168192?style=social&logo=Discord&label=Rotel%20Discord"></a>
</p>

---

## About

**Rotel** provides an efficient, high-performance solution for collecting, processing, and exporting telemetry data.
Rotel is ideal for resource-constrained environments and applications where minimizing overhead is critical.

**Features:**

- Supports metrics, logs, and traces
- OTLP receiver supporting gRPC, HTTP/Protobuf, and HTTP/JSON
- OTLP exporter supporting gRPC and HTTP/Protobuf
- Built-in batching and retry mechanisms
- Additional
  exporters: [ClickHouse](#clickhouse-exporter-configuration), [Datadog](#datadog-exporter-configuration), [AWS X-RAY](#aws-x-ray-exporter-configuration), [AWS EMF](#aws-emf-exporter-configuration),
  and [Kafka](#kafka-exporter-configuration-experimental)
- [Kafka Receiver](#kafka-receiver-configuration)

Rotel can be easily bundled with popular runtimes as packages. Its Rust implementation ensures minimal resource usage
and a compact binary size, simplifying deployment without the need for a sidecar container.

**Runtime integrations:**

- **Python:** [streamfold/pyrotel](https://github.com/streamfold/pyrotel)
- **Node.js:** [streamfold/rotel-nodejs](https://github.com/streamfold/rotel-nodejs)

We also have a custom AWS Lambda Extension layer that embeds the Rotel collector to provide OpenTelemetry collection
with minimal coldstart latency:

- **Rotel Lambda Extension**: [streamfold/rotel-lambda-extension](https://github.com/streamfold/rotel-lambda-extension)

Rotel is fully open-sourced and licensed under the Apache 2.0 license.

_Rotel is currently in early development, and we are actively looking for feedback from the community. It is not
recommended for production use at this time._

## Getting Started

To quickly get started with Rotel you can leverage the bundled [Python](https://github.com/streamfold/pyrotel) or
[NodeJS](https://github.com/streamfold/rotel-nodejs) packages. Or if you'd like to use Rotel directly from Docker,
follow these steps:

1. **Running Rotel**
    - We use the prebuilt docker image for this example, but you can also download a binary from the
      [releases](https://github.com/streamfold/rotel/releases) page.
    - Execute Rotel with the following arguments. To debug metrics or logs, add
      an additional `--debug-log metrics|logs`.

   ```bash
   docker run -ti -p 4317-4318:4317-4318 streamfold/rotel --debug-log traces --exporter blackhole
   ```

    - Rotel is now listening on localhost:4317 (gRPC) and localhost:4318 (HTTP).

2. **Verify**
    - Send OTLP traces to Rotel and verify that it is receiving data:

   ```bash
   go install github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen@latest

   telemetrygen traces --otlp-insecure --duration 5s
   ```

    - Check the output from Rotel and you should see several "Received traces" log lines.

## Configuration

Rotel is configured on the command line with multiple flags. See the table below for the full list of options. Rotel
will also output the full argument list:

```shell
rotel start --help
```

All CLI arguments can also be passed as environment variable by prefixing with `ROTEL_` and switching hyphens to
underscores. For example, `--otlp-grpc-endpoint localhost:5317` can also be specified by setting the environment
variable `ROTEL_OTLP_GRPC_ENDPOINT=localhost:5317`.

Any option above that does not contain a default is considered false or unset by default.

| Option                            | Default              | Options                                                            |
|-----------------------------------|----------------------|--------------------------------------------------------------------|
| --daemon                          |                      |                                                                    |
| --log-format                      | text                 | json                                                               |
| --pid-file                        | /tmp/rotel-agent.pid |                                                                    |
| --log-file                        | /tmp/rotel-agent.log |                                                                    |
| --debug-log                       |                      | metrics, traces, logs                                              |
| --debug-log-verbosity             | basic                | basic, detailed                                                    |
| --otlp-grpc-endpoint              | localhost:4317       |                                                                    |
| --otlp-http-endpoint              | localhost:4318       |                                                                    |
| --otlp-grpc-max-recv-msg-size-mib | 4                    |                                                                    |
| --exporter                        | otlp                 | otlp, blackhole, datadog, clickhouse, awsxray, awsemf, kafka, file |
| --otlp-receiver-traces-disabled   |                      |                                                                    |
| --otlp-receiver-metrics-disabled  |                      |                                                                    |
| --otlp-receiver-logs-disabled     |                      |                                                                    |
| --otlp-receiver-traces-http-path  | /v1/traces           |                                                                    |
| --otlp-receiver-metrics-http-path | /v1/metrics          |                                                                    |
| --otlp-receiver-logs-http-path    | /v1/logs             |                                                                    |
| --otel-resource-attributes        |                      |                                                                    |
| --enable-internal-telemetry       |                      |                                                                    |

The PID and LOG files are only used when run in `--daemon` mode.

See the section for [Multiple Exporters](#multiple-exporters) for how to configure multiple exporters

### OTLP exporter configuration

The OTLP exporter is the default, or can be explicitly selected with `--exporter otlp`.

| Option                                 | Default | Options    |
|----------------------------------------|---------|------------|
| --otlp-exporter-endpoint               |         |            |
| --otlp-exporter-protocol               | grpc    | grpc, http |
| --otlp-exporter-custom-headers         |         |            |
| --otlp-exporter-compression            | gzip    | gzip, none |
| --otlp-exporter-authenticator          |         | sigv4auth  |
| --otlp-exporter-tls-cert-file          |         |            |
| --otlp-exporter-tls-cert-pem           |         |            |
| --otlp-exporter-tls-key-file           |         |            |
| --otlp-exporter-tls-key-pem            |         |            |
| --otlp-exporter-tls-ca-file            |         |            |
| --otlp-exporter-tls-ca-pem             |         |            |
| --otlp-exporter-tls-skip-verify        |         |            |
| --otlp-exporter-request-timeout        | 5s      |            |
| --otlp-exporter-retry-initial-backoff  | 5s      |            |
| --otlp-exporter-retry-max-backoff      | 30s     |            |
| --otlp-exporter-retry-max-elapsed-time | 300s    |            |

Any of the options that start with `--otlp-exporter*` can be set per telemetry type: metrics, traces or logs. For
example, to set a custom endpoint to export traces to, set: `--otlp-exporter-traces-endpoint`. For other telemetry
types their value falls back to the top-level OTLP exporter config.

#### Cloudwatch OTLP Export

The Rotel OTLP exporter can export to the
[Cloudwatch OTLP endpoints](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-OTLPEndpoint.html)
for traces and logs. You'll need to select the HTTP protocol and enable the sigv4auth authenticator.

The sigv4auth authenticator requires the AWS authentication environment variables to be set.

**Traces**

_Tracing requires that you
enable [Transaction Search](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-Transaction-Search.html)
in the AWS console before you can send OTLP traces._

Here is the full environment variable configuration to send traces to Cloudwatch, swap the region code as needed.

```shell
ROTEL_EXPORTER=otlp
ROTEL_OTLP_EXPORTER_PROTOCOL=http
ROTEL_OTLP_EXPORTER_TRACES_ENDPOINT=https://xray.<region code>.amazonaws.com/v1/traces
ROTEL_OTLP_EXPORTER_AUTHENTICATOR=sigv4auth
```

**Logs**

_To send OTLP logs to Cloudwatch you must create a log group and log stream. Exporting will fail if these do not exist
ahead of time and they are not created by default._

Here is the full environment variable configuration to send logs to Cloudwatch, swap the region code and
log group/stream as needed.

```shell
ROTEL_EXPORTER=otlp
ROTEL_OTLP_EXPORTER_PROTOCOL=http
ROTEL_OTLP_EXPORTER_TRACES_ENDPOINT=https://logs.<region code>.amazonaws.com/v1/logs
ROTEL_OTLP_EXPORTER_LOGS_CUSTOM_HEADERS="x-aws-log-group=<log group>,x-aws-log-stream=<log stream>"
ROTEL_OTLP_EXPORTER_AUTHENTICATOR=sigv4auth
```

### Datadog exporter configuration

The Datadog exporter can be selected by passing `--exporter datadog`. The Datadog exporter only supports traces at the
moment. For more information, see the [Datadog Exporter](src/exporters/datadog/README.md) docs.

| Option                             | Default | Options                |
|------------------------------------|---------|------------------------|
| --datadog-exporter-region          | us1     | us1, us3, us5, eu, ap1 |
| --datadog-exporter-custom-endpoint |         |                        |
| --datadog-exporter-api-key         |         |                        |

Specifying a custom endpoint will override the region selection.

### ClickHouse exporter configuration

The ClickHouse exporter can be selected by passing `--exporter clickhouse`. The ClickHouse exporter supports metrics,
logs,
and traces.

| Option                                | Default | Options     |
|---------------------------------------|---------|-------------|
| --clickhouse-exporter-endpoint        |         |             |
| --clickhouse-exporter-database        | otel    |             |
| --clickhouse-exporter-table-prefix    | otel    |             |
| --clickhouse-exporter-compression     | lz4     | none, lz4   |
| --clickhouse-exporter-async-insert    | true    | true, false |
| --clickhouse-exporter-request-timeout | 5s      |             |
| --clickhouse-exporter-enable-json     |         |             |
| --clickhouse-exporter-json-underscore |         |             |
| --clickhouse-exporter-user            |         |             |
| --clickhouse-exporter-password        |         |             |

The ClickHouse endpoint must be specified while all other options can be left as defaults. The table prefix is prefixed
onto the specific telemetry table name with underscore, so a table prefix of `otel` will be combined with `_traces` to
generate the full table name of `otel_traces`.

The ClickHouse exporter will enable [async inserts](https://clickhouse.com/docs/optimize/asynchronous-inserts) by
default,
although it can be disabled server-side. Async inserts are
recommended for most workloads to avoid overloading ClickHouse with many small inserts. Async inserts can be disabled by
specifying:
`--clickhouse-exporter-async-insert false`.

The exporter will not generate the table schema if it does not exist. Use the
[clickhouse-ddl](/src/bin/clickhouse-ddl/README.md) command for generating the necessary table DDL for ClickHouse. The
DDL matches the schema used in the
OpenTelemetry [ClickHouse exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/clickhouseexporter/README.md).

Enabling JSON via the `--clickhouse-exporter-enable-json` will use the new
[JSON data type](https://clickhouse.com/docs/sql-reference/data-types/newjson) in ClickHouse. This data
type is only available on the most recent versions of ClickHouse. Make sure that you enable JSON with `--enable-json`
when creating tables with `clickhouse-ddl`. By default, any JSON key inserted with a period in it will create
a nested JSON object. You can replace periods in JSON keys with underscores by passing the option
`--clickhouse-exporter-json-underscore` which will keep the JSON keys flat. For example, the resource attribute
`service.name` will be inserted as `service_name`.

_The ClickHouse exporter is built using code from the official Rust [clickhouse-rs](https://crates.io/crates/clickhouse)
crate._

### AWS X-Ray exporter configuration

The AWS X-Ray exporter can be selected by passing `--exporter awsxray`. The X-Ray exporter only supports traces. Note:
X-Ray
limits batch sizes to 50 traces segments. If you assign a `--batch-max-size` of greater than 50, Rotel will override and
enforce the max
batch size of 50 with the warning
`INFO AWS X-Ray only supports a batch size of 50 segments, setting batch max size to 50`

AWS Credentials including `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN` for the X-Ray exporter
are
automatically sourced from Rotel's environment on startup.

| Option                             | Default   | Options          |
|------------------------------------|-----------|------------------|
| --awsxray-exporter-region          | us-east-1 | aws region codes |
| --awsxray-exporter-custom-endpoint |           |                  |

For a list of available AWS X-Ray region codes here: https://docs.aws.amazon.com/general/latest/gr/xray.html

### AWS EMF exporter configuration

The AWS EMF exporter can be selected by passing `--exporter awsemf`. The AWS EMF exporter only supports metrics. The
AWS EMF exporter will convert metrics into the AWS
Cloudwatch [Embedded metric format](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch_Embedded_Metric_Format_Specification.html)
and
send those as JSON log lines to Cloudwatch. Cloudwatch will convert the log lines into Cloudwatch Metrics.

AWS Credentials including `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN` for the EMF exporter
are automatically sourced from Rotel's environment on startup.

| Option                                                 | Default          | Options          |
|--------------------------------------------------------|------------------|------------------|
| --awsemf-exporter-region                               | us-east-1        | aws region codes |
| --awsemf-exporter-custom-endpoint                      |                  |                  |
| --awsemf-exporter-log-group-name                       | /metrics/default |                  |
| --awsemf-exporter-log-stream-name                      | otel-stream      |                  |
| --awsemf-exporter-log-retention                        | 0                |                  |
| --awsemf-exporter-namespace                            |                  |                  |
| --awsemf-exporter-retain-initial-value-of-delta-metric | false            |                  |
| --awsemf-exporter-include-dimensions                   |                  |                  |
| --awsemf-exporter-exclude-dimensions                   |                  |                  |

**DIMENSION FILTERING**:

By default all resource and metric data point attributes will be included as dimensions in the Cloudwatch Metric. You
can use the `include-dimensions` and `exclude-dimensions` options to selectively filter which dimensions are included
in the generated metric. This can be useful to include a high-cardinality dimension in the log output, but not set
it on the metric. The Cloudwatch Metric will represent the aggregation across all values without incurring the cost
of the excessive high-cardinality.
In the scenario that you want to examine the data based on the high-cardinality dimension, you can use Logs Insights
to query that dimension from the logs.

Both `include-dimensions` and `exclude-dimensions` take comma-separated wildcard patterns to match against the attribute
names from the metrics. The `*` character can be used to match zero-or-more characters. Matching is case insensitive.
By default all dimensions are included (`include-dimensions=*`), but you can also selectively filter which to include.
The `exclude-dimensions` takes precedence, so any dimension that matches an exclude pattern will be excluded.

Example:

- `--awsemf-exporter-include-dimensions service.*,http.*`
- `--awsemf-exporter-exclude-dimensions *.internal`

With these options, here's how the following attributes would be handled:

- `service.name`: included
- `http.method`: included
- `http.internal`: excluded
- `telemetry.sdk.language`: excluded

**NOTE**:

- If the log stream or log group do not exist, the exporter will attempt to create them automatically. Make sure that
  the credentials have the
  right IAM permissions.
- If `--awsemf-exporter-retain-initial-value-of-delta-metric` is true, then the initial value of a delta metric is
  retained when calculating deltas.
- If the namespace is not specified, Rotel will look for `service.namespace` and `service.name` in the resource
  attributes and use those. If those
  don't exist, it will fall back to a namespace of _default_.
- Log retention is specified in days, with zero meaning never expire. Valid values are: 1, 3, 5, 7, 14, 30, 60, 90, 120,
  150, 180, 365, 400, 545,
  731, 1827, 2192, 2557, 2922, 3288, or 3653.

### Kafka exporter configuration (Experimental)

The Kafka exporter can be selected by passing `--exporter kafka`. The Kafka exporter supports metrics,
logs, and traces.

| Option                                                    | Default           | Options                                                                     |
|-----------------------------------------------------------|-------------------|-----------------------------------------------------------------------------|
| --kafka-exporter-brokers                                  | localhost:9092    |                                                                             |
| --kafka-exporter-traces-topic                             | otlp_traces       |                                                                             |
| --kafka-exporter-metrics-topic                            | otlp_metrics      |                                                                             |
| --kafka-exporter-logs-topic                               | otlp_logs         |                                                                             |
| --kafka-exporter-format                                   | protobuf          | json, protobuf                                                              |
| --kafka-exporter-compression                              | none              | gzip, snappy, lz4, zstd, none                                               |
| --kafka-exporter-request-timeout                          | 30s               |                                                                             |
| --kafka-exporter-acks                                     | one               | none, one, all                                                              |
| --kafka-exporter-client-id                                | rotel             |                                                                             |
| --kafka-exporter-max-message-bytes                        | 1000000           |                                                                             |
| --kafka-exporter-linger-ms                                | 5                 |                                                                             |
| --kafka-exporter-retries                                  | 2147483647        |                                                                             |
| --kafka-exporter-retry-backoff-ms                         | 100               |                                                                             |
| --kafka-exporter-retry-backoff-max-ms                     | 1000              |                                                                             |
| --kafka-exporter-message-timeout-ms                       | 300000            |                                                                             |
| --kafka-exporter-request-timeout-ms                       | 30000             |                                                                             |
| --kafka-exporter-batch-size                               | 1000000           |                                                                             |
| --kafka-exporter-partitioner                              | consistent-random | consistent, consistent-random, murmur2-random, murmur2, fnv1a, fnv1a-random |
| --kafka-exporter-partition-metrics-by-resource-attributes | false             |                                                                             |
| --kafka-exporter-partition-logs-by-resource-attributes    | false             |                                                                             |
| --kafka-exporter-custom-config                            |                   |                                                                             |
| --kafka-exporter-sasl-username                            |                   |                                                                             |
| --kafka-exporter-sasl-password                            |                   |                                                                             |
| --kafka-exporter-sasl-mechanism                           |                   | plain, scram-sha256, scram-sha512                                           |
| --kafka-exporter-security-protocol                        | plaintext         | plaintext, ssl, sasl-plaintext, sasl-ssl                                    |

The Kafka broker addresses must be specified (comma-separated for multiple brokers). The exporter will create separate
topics for traces, metrics, and logs. Data can be serialized as JSON or Protobuf format.

#### Acknowledgement Modes

The `--kafka-exporter-acks` option controls the producer acknowledgement behavior, balancing between performance and
durability:

- `none` (acks=0): No acknowledgement required - fastest performance but least durable, data may be lost if the leader
  fails
- `one` (acks=1): Wait for leader acknowledgement only - balanced approach, good performance with reasonable
  durability (default)
- `all` (acks=all): Wait for all in-sync replicas to acknowledge - slowest but most durable, ensures data is not lost

For secure connections, you can configure SASL authentication:

```shell
rotel start --exporter kafka \
  --kafka-exporter-brokers "broker1:9092,broker2:9092" \
  --kafka-exporter-sasl-username "your-username" \
  --kafka-exporter-sasl-password "your-password" \
  --kafka-exporter-sasl-mechanism "SCRAM-SHA-256" \
  --kafka-exporter-security-protocol "SASL_SSL" \
  --kafka-exporter-compression "gzip" \
  --kafka-exporter-acks "all"
```

#### Producer Performance Tuning

The Kafka exporter provides several options for tuning producer performance and reliability:

- `--kafka-exporter-linger-ms`: Delay in milliseconds to wait for messages to accumulate before sending. Higher values
  improve batching efficiency but increase latency.
- `--kafka-exporter-retries`: How many times to retry sending a failing message. High values ensure delivery but may
  cause reordering.
- `--kafka-exporter-retry-backoff-ms`: Initial backoff time before retrying a failed request.
- `--kafka-exporter-retry-backoff-max-ms`: Maximum backoff time for exponentially backed-off retry requests.
- `--kafka-exporter-message-timeout-ms`: Maximum time to wait for messages to be sent successfully. Messages exceeding
  this timeout will be dropped.
- `--kafka-exporter-request-timeout-ms`: Timeout for individual requests to the Kafka brokers.
- `--kafka-exporter-batch-size`: Maximum size of message batches in bytes. Larger batches improve throughput but
  increase memory usage.
- `--kafka-exporter-partitioner`: Controls how messages are distributed across partitions. Options include consistent
  hashing and murmur2/fnv1a hash algorithms.

#### Message Partitioning Control

For improved consumer parallelism and data organization, you can enable custom partitioning based on telemetry data:

- `--kafka-exporter-partition-metrics-by-resource-attributes`: When enabled, metrics are partitioned by resource
  attributes (like service name), grouping related metrics together.
- `--kafka-exporter-partition-logs-by-resource-attributes`: When enabled, logs are partitioned by resource attributes,
  organizing logs by service or application.

These options override the global partitioner setting for specific telemetry types when enabled.

#### Advanced Configuration

For advanced use cases, you can set arbitrary Kafka producer configuration parameters using the
`--kafka-exporter-custom-config` option. This accepts comma-separated key=value pairs:

```shell
rotel start --exporter kafka \
  --kafka-exporter-custom-config "enable.idempotence=true,max.in.flight.requests.per.connection=1" \
  --kafka-exporter-brokers "broker1:9092,broker2:9092"
```

**Configuration Precedence**: Custom configuration parameters are applied _after_ all built-in options, meaning they
will override any conflicting built-in settings. For example:

```shell
# The custom config will override the built-in batch size setting
rotel start --exporter kafka \
  --kafka-exporter-batch-size 500000 \
  --kafka-exporter-custom-config "batch.size=2000000"
  # Final batch.size will be 2000000, not 500000
```

This allows you to configure any rdkafka producer parameter that isn't explicitly exposed through dedicated CLI options.
See
the [librdkafka configuration documentation](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
for all available parameters.

The Kafka exporter uses the high-performance rdkafka library and includes built-in retry logic and error handling.

#### Testing the Kafka Exporter

To run integration tests that verify actual Kafka functionality:

```shell
# Start test environment
./scripts/kafka-test-env.sh start

# Run integration tests
cargo test --test kafka_integration_tests --features integration-tests

# Stop test environment
./scripts/kafka-test-env.sh stop
```

See [KAFKA_INTEGRATION_TESTS.md](KAFKA_INTEGRATION_TESTS.md) for detailed testing instructions.

### File exporter configuration

**NOTE**: The file exporter at the moment is experimental and not enabled by default. It must be enabled by building
with the feature flag
`--features file_exporter`, like:

```shell
cargo build --features file_exporter
```

**WARNING**: The Parquet and JSON file format is evolving and subject to breaking changes between releases. There is
consolidation planned with official Arrow schemas from the OpenTelemetry Arrow project.

The File exporter can be selected with `--exporter file`. It writes telemetry
out as periodic files on the local filesystem. Currently **Parquet** and
**JSON** formats are supported.

| Option                              | Default    | Description                                                                                                  |
|-------------------------------------|------------|--------------------------------------------------------------------------------------------------------------|
| --file-exporter-format              | parquet    | `parquet` or `json`                                                                                          |
| --file-exporter-output-dir          | /tmp/rotel | Directory to place output files                                                                              |
| --file-exporter-flush-interval      | 5s         | How often to flush accumulated telemetry to a new file (accepts Go-style durations like `30s`, `2m`, `1h`)   |
| --file-exporter-parquet-compression | snappy     | Compression for Parquet files: `none`, `snappy`, `gzip`, `lz4`, `zstd` (only applies when format is parquet) |

Each flush creates a file named `<telemetry-type>-<timestamp>.<ext>` inside the
specified directory. For example, with default settings Rotel will emit files
such as `traces-20250614-120000.parquet` every five seconds. Files are saved into the `traces`, `logs`, and `metrics`
subdirectories.

_The File exporter is useful for local debugging, offline analysis, and for
feeding telemetry into batch-processing systems._

### Kafka Receiver configuration

The Kafka Receiver allows Rotel to consume telemetry data from Kafka topics. This enables you to use Kafka as a buffer
or
transport layer for your telemetry data pipeline. The receiver supports consuming metrics, logs, and traces from
separate
topics in either JSON or Protobuf format.

To enable the Kafka receiver, you must specify which telemetry types to consume using the appropriate flags:

- `--kafka-receiver-traces` to consume traces
- `--kafka-receiver-metrics` to consume metrics
- `--kafka-receiver-logs` to consume logs

| Option                                     | Default        | Options                                              |
|--------------------------------------------|----------------|------------------------------------------------------|
| --kafka-receiver-brokers                   | localhost:9092 | Kafka broker addresses (comma-separated)             |
| --kafka-receiver-traces-topic              | otlp_traces    | Topic name for traces                                |
| --kafka-receiver-metrics-topic             | otlp_metrics   | Topic name for metrics                               |
| --kafka-receiver-logs-topic                | otlp_logs      | Topic name for logs                                  |
| --kafka-receiver-traces                    | false          | Enable consuming traces                              |
| --kafka-receiver-metrics                   | false          | Enable consuming metrics                             |
| --kafka-receiver-logs                      | false          | Enable consuming logs                                |
| --kafka-receiver-format                    | protobuf       | json, protobuf                                       |
| --kafka-receiver-group-id                  | rotel-consumer | Consumer group ID for coordinated consumption        |
| --kafka-receiver-client-id                 | rotel          | Client ID for the Kafka consumer                     |
| --kafka-receiver-enable-auto-commit        | true           | Enable auto commit of offsets                        |
| --kafka-receiver-auto-commit-interval-ms   | 5000           | Auto commit interval in milliseconds                 |
| --kafka-receiver-auto-offset-reset         | latest         | earliest, latest, error                              |
| --kafka-receiver-session-timeout-ms        | 30000          | Session timeout in milliseconds                      |
| --kafka-receiver-heartbeat-interval-ms     | 3000           | Heartbeat interval in milliseconds                   |
| --kafka-receiver-max-poll-interval-ms      | 300000         | Maximum poll interval in milliseconds                |
| --kafka-receiver-max-partition-fetch-bytes | 1048576        | Maximum bytes per partition the consumer will buffer |
| --kafka-receiver-fetch-min-bytes           | 1              | Minimum number of bytes for fetch requests           |
| --kafka-receiver-fetch-max-wait-ms         | 500            | Maximum wait time for fetch requests in milliseconds |
| --kafka-receiver-socket-timeout-ms         | 60000          | Socket timeout in milliseconds                       |
| --kafka-receiver-metadata-max-age-ms       | 300000         | Maximum age of metadata in milliseconds              |
| --kafka-receiver-isolation-level           | read-committed | read-uncommitted, read-committed                     |
| --kafka-receiver-enable-partition-eof      | false          | Enable partition EOF notifications                   |
| --kafka-receiver-check-crcs                | true           | Check CRC32 of consumed messages                     |
| --kafka-receiver-custom-config             |                | Custom consumer config (comma-separated key=value)   |
| --kafka-receiver-sasl-username             |                | SASL username for authentication                     |
| --kafka-receiver-sasl-password             |                | SASL password for authentication                     |
| --kafka-receiver-sasl-mechanism            |                | plain, scram-sha256, scram-sha512                    |
| --kafka-receiver-security-protocol         |                | plaintext, ssl, sasl-plaintext, sasl-ssl             |
| --kafka-receiver-ssl-ca-location           |                | SSL CA certificate location                          |
| --kafka-receiver-ssl-certificate-location  |                | SSL certificate location                             |
| --kafka-receiver-ssl-key-location          |                | SSL key location                                     |
| --kafka-receiver-ssl-key-password          |                | SSL key password                                     |

#### Consumer Configuration

The Kafka receiver acts as a consumer and supports standard Kafka consumer configurations:

**Consumer Group Management:**

- `--kafka-receiver-group-id`: Sets the consumer group ID for coordinated consumption across multiple Rotel instances
- `--kafka-receiver-enable-auto-commit`: Controls whether offsets are automatically committed
- `--kafka-receiver-auto-commit-interval-ms`: How often to commit offsets when auto-commit is enabled

**Offset Management:**

- `--kafka-receiver-auto-offset-reset`: Controls behavior when no initial offset exists or the current offset is invalid
    - `earliest`: Start consuming from the beginning of the topic
    - `latest`: Start consuming from the end of the topic (default)
    - `error`: Throw an error if no offset is found

**Session and Heartbeat Configuration:**

- `--kafka-receiver-session-timeout-ms`: Maximum time before the consumer is considered dead and rebalancing occurs
- `--kafka-receiver-heartbeat-interval-ms`: How often to send heartbeats to the broker
- `--kafka-receiver-max-poll-interval-ms`: Maximum delay between poll() calls before consumer is considered failed

**Fetch Configuration:**

- `--kafka-receiver-fetch-min-bytes`: Minimum amount of data the server should return for a fetch request
- `--kafka-receiver-fetch-max-wait-ms`: Maximum time the server will wait to accumulate fetch-min-bytes of data
- `--kafka-receiver-max-partition-fetch-bytes`: Maximum amount of data per partition the server will return

**Data Integrity:**

- `--kafka-receiver-check-crcs`: Enables CRC32 checking of consumed messages for data integrity
- `--kafka-receiver-isolation-level`: Controls which messages are visible to the consumer
    - `read-uncommitted`: Read all messages including those from uncommitted transactions
    - `read-committed`: Only read messages from committed transactions (default)

#### Security Configuration

For secure Kafka clusters, the receiver supports both SASL and SSL authentication:

**SASL Authentication:**

```shell
rotel start \
  --kafka-receiver-traces \
  --kafka-receiver-brokers "broker1:9092,broker2:9092" \
  --kafka-receiver-sasl-username "your-username" \
  --kafka-receiver-sasl-password "your-password" \
  --kafka-receiver-sasl-mechanism "scram-sha256" \
  --kafka-receiver-security-protocol "sasl-ssl"
```

**SSL Configuration:**

```shell
rotel start \
  --kafka-receiver-traces \
  --kafka-receiver-brokers "broker1:9093,broker2:9093" \
  --kafka-receiver-security-protocol "ssl" \
  --kafka-receiver-ssl-ca-location "/path/to/ca-cert" \
  --kafka-receiver-ssl-certificate-location "/path/to/client-cert" \
  --kafka-receiver-ssl-key-location "/path/to/client-key"
```

#### Advanced Configuration

The `--kafka-receiver-custom-config` option allows setting arbitrary Kafka consumer configuration parameters using
comma-separated key=value pairs:

```shell
rotel start \
  --kafka-receiver-traces \
  --kafka-receiver-custom-config "max.poll.records=100,fetch.max.bytes=52428800"
```

Custom configuration parameters are applied after all built-in options, allowing them to override any conflicting
settings.
See
the [librdkafka configuration documentation](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
for all available consumer parameters.

#### Example Usage

Basic example consuming traces from Kafka:

```shell
rotel start \
  --kafka-receiver-traces \
  --kafka-receiver-brokers "localhost:9092" \
  --kafka-receiver-traces-topic "my-traces" \
  --kafka-receiver-format "json" \
  --exporter otlp \
  --otlp-exporter-endpoint "localhost:4317"
```

Consuming multiple telemetry types with custom configuration:

```shell
rotel start \
  --kafka-receiver-traces \
  --kafka-receiver-metrics \
  --kafka-receiver-logs \
  --kafka-receiver-brokers "kafka1:9092,kafka2:9092,kafka3:9092" \
  --kafka-receiver-group-id "rotel-production" \
  --kafka-receiver-auto-offset-reset "earliest" \
  --kafka-receiver-format "protobuf" \
  --exporter clickhouse \
  --clickhouse-exporter-endpoint "https://clickhouse.example.com:8443"
```

### Batch configuration

You can configure the properties of the batch processor, controlling both the size limit of the batch and how long the
batch
is kept before flushing. The batch properties behave the same regardless of which exporter you use. You can override the
batch settings specifically for a telemetry type by prefixing any of the options below with the telemetry type (metrics,
logs,
or traces). For example, `--traces-batch-max-size` will override the batch max size for traces only.

| Option           | Default | Options |
|------------------|---------|---------|
| --batch-max-size | 8192    |         |
| --batch-timeout  | 200ms   |         |

### Setting resource attributes

Rotel also supports setting or overwriting resource attributes on OpenTelemetry logs, metrics, and traces via the
command line or environment. The `--otel-resource-attributes` flag accepts a comma-separated list of key-value pairs to
upsert on the
the resource
attributes of ResourceLogs, ResourceMetrics, and ResourceSpans.

For example starting rotel with the following command line argument will append or overwrite the `service.name`
and `environment` attributes. `--otel-resource-attributes "service.name=my-service,environment=production"`

Alternatively you can use the `ROTEL_OTEL_RESOURCE_ATTRIBUTES` environment variable to achieve the same outcome.

`ROTEL_OTEL_RESOURCE_ATTRIBUTES=service.name=my-service,environment=production rotel start --otlp-exporter-endpoint <endpoint url>`

### Retries and timeouts

You can override the default request timeout of 5 seconds for the OTLP Exporter with the exporter setting:

- `--otlp-exporter-request-timeout`: Takes a string time duration, so `"250ms"` for 250 milliseconds, `"3s"` for 3
  seconds, etc.

Requests will be retried if they match retryable error codes like 429 (Too Many Requests) or timeout. You can control
the behavior with the following exporter options:

- `--otlp-exporter-retry-initial-backoff`: Initial backoff duration
- `--otlp-exporter-retry-max-backoff`: Maximum backoff interval
- `--otlp-exporter-retry-max-elapsed-time`: Maximum wall time a request will be retried for until it is marked as
  permanent failure

All options should be represented as string time durations.

### Internal telemetry

Rotel records a number of internal metrics that can help observe Rotel behavior during runtime. This telemetry is
opt-in and must be enabled with `--enable-internal-telemetry`. Telemetry is sent to the OTLP exporter metric endpoint
that you have configured.

**NOTE**: Internal telemetry is not sent to any outside sources and you are in full control of where this data is
exported to.

### Multiple receivers

Rotel supports configuring multiple receivers to ingest telemetry data from different sources simultaneously. This
allows you
to receive data via OTLP and consume from Kafka topics at the same time.

The following configuration parameters enable multiple receivers:

| Option      | Default | Options                           |
|-------------|---------|-----------------------------------|
| --receiver  | otlp    | otlp, kafka                       |
| --receivers |         | comma-separated list (otlp,kafka) |

**Important Notes:**

- You cannot use `--receiver` and `--receivers` at the same time
- If neither flag is specified, Rotel defaults to using the OTLP receiver
- When using multiple receivers, each receiver type can only be specified once

#### Basic Usage

**Single receiver (default OTLP):**

```shell
rotel start --exporter otlp --otlp-exporter-endpoint localhost:4317
```

**Single Kafka receiver:**

```shell
rotel start --receiver kafka \
  --kafka-receiver-traces \
  --kafka-receiver-brokers "localhost:9092" \
  --exporter otlp \
  --otlp-exporter-endpoint localhost:4317
```

**Multiple receivers (OTLP and Kafka):**

```shell
rotel start --receivers otlp,kafka \
  --kafka-receiver-traces \
  --kafka-receiver-brokers "localhost:9092" \
  --exporter otlp \
  --otlp-exporter-endpoint localhost:4317
```

#### Receiver Configuration

When using multiple receivers, each receiver maintains its own configuration:

- **OTLP Receiver**: Configure using `--otlp-*` flags (see [Configuration](#configuration) section)
- **Kafka Receiver**: Configure using `--kafka-receiver-*` flags (
  see [Kafka Receiver configuration](#kafka-receiver-configuration) section)

Each receiver can be independently configured to accept different telemetry types. For example, you might receive traces
via
OTLP while consuming logs and metrics from Kafka:

```shell
rotel start --receivers otlp,kafka \
  --otlp-receiver-metrics-disabled \
  --otlp-receiver-logs-disabled \
  --kafka-receiver-metrics \
  --kafka-receiver-logs \
  --kafka-receiver-brokers "kafka:9092" \
  --exporter clickhouse \
  --clickhouse-exporter-endpoint "https://clickhouse.example.com:8443"
```

#### Environment Variables

Both receiver flags can be set via environment variables:

- `ROTEL_RECEIVER=kafka` - Sets a single receiver
- `ROTEL_RECEIVERS=otlp,kafka` - Sets multiple receivers

Example with environment variables:

```shell
export ROTEL_RECEIVERS=otlp,kafka
export ROTEL_KAFKA_RECEIVER_TRACES=true
export ROTEL_KAFKA_RECEIVER_BROKERS=kafka1:9092,kafka2:9092
rotel start --exporter otlp --otlp-exporter-endpoint localhost:4317
```

### Multiple exporters

Rotel can be configured to support exporting to multiple destinations across multiple exporter types.

The following additional configuration parameters set up support for multiple exporters. Similar to the options above,
all
CLI arguments can be passed as environment variables as well. It is not possible to set `--exporter` and `--exporters`
at the same time.

| Option              | Default | Options                          |
|---------------------|---------|----------------------------------|
| --exporters         |         | name:type pairs, comma-separated |
| --exporters-traces  |         | exporter name                    |
| --exporters-metrics |         | exporter name                    |
| --exporters-logs    |         | exporter name                    |

First start by defining the set of exporters that you would like to use, optionally specifying a custom name for them
to differentiate their configuration options. For example, to export logs and metrics to two separate ClickHouse nodes
while exporting traces to Datadog, we'll use the following `--exporters` argument (or `ROTEL_EXPORTERS` envvar):

```shell
--exporters logging:clickhouse,stats:clickhouse,datadog
```

The argument form of `--exporters` takes `name:type` pairs separated by commas, where the first part is a custom name
and
the second part is the type of exporter. You can exclude the name if there is a single exporter by that name, which
means
the name is the same as the exporter type.

Second, you then must set environment variables of the form `ROTEL_EXPORTER_{NAME}_{PARAMETER}` to configure the
multiple
exporters. These variable names are dynamic and use the custom name to differentiate settings for similar exporter
types.
Therefore, there are no CLI argument alternatives for them at the moment. The `{PARAMETER}` fields match the
configuration
options for the given exporter type.

Using our example above, the user must set, at a minimum, the following environment variables. (For ClickHouse Cloud you
would need to include a username/password, but we are skipping those for brevity.)

- `ROTEL_EXPORTER_LOGGING_ENDPOINT=https://xxxxxxx.us-east-1.aws.clickhouse.cloud:8443`
- `ROTEL_EXPORTER_STATS_ENDPOINT=https://xxxxxxx.us-west-1.aws.clickhouse.cloud:8443`
- `ROTEL_EXPORTER_DATADOG_API_KEY=dd-abcd1234`

Lastly, the user would need to connect these exporters to the telemetry types. Using the requirements above, the user
would specify the following:

```shell
--exporters-traces datadog --exporters-metrics stats --exporters-logs logging
```

Alternatively, the following environment variables would do the same:

- `ROTEL_EXPORTERS_TRACES=datadog`
- `ROTEL_EXPORTERS_METRICS=stats`
- `ROTEL_EXPORTERS_LOGS=logging`

_NOTE: At the moment, only a single exporter can be set for any telemetry type. This constraint will be relaxed in the
future._

### Full example

The following example demonstrates how to send OTLP data to [Axiom](https://axiom.co/). Set your Axiom API key in the
envvar `AXIOM_API_KEY` and the dataset in `AXIOM_DATASET`:

```shell
ROTEL_OTLP_EXPORTER_CUSTOM_HEADERS="Authorization=Bearer ${AXIOM_API_KEY},X-Axiom-Dataset=${AXIOM_DATASET}" \
 ./rotel start --otlp-exporter-endpoint https://api.axiom.co --otlp-exporter-protocol http
```

In another window run the telemetry generator again:

```shell
telemetrygen traces --otlp-insecure --duration 1s
```

You should see demo trace data show up in Axiom.

## Processors

### Python Processor SDK

Rotel includes a Python processor SDK that allows you to write custom processors in Python to modify OpenTelemetry data
in-flight. The SDK provides interfaces for processing both traces and logs data (metrics coming soon!) through a simple
Python API.

The processor SDK enables you to:

- Access and modify trace spans, including span data, attributes, events, links and status
- Process log records, including severity, body content, and associated attributes
- Modify resource attributes across traces and logs
- Transform data using custom Python logic before it is exported

The processor SDK also includes LSP support for code completion, syntax highlighting and marking of warnings for use in
your preferred editor such as VSCode, Nvim, and Pycharm. The LSP integration is hosted on pypi and can be found
at [https://pypi.org/project/rotel-sdk/](https://pypi.org/project/rotel-sdk/).

To install the sdk simply type `pip install rotel-sdk`

Example of a simple trace processor:

```python
from rotel_sdk.open_telemetry.common.v1 import KeyValue, AnyValue
from rotel_sdk.open_telemetry.trace.v1 import ResourceSpans


def process_spans(resource_spans):
    for scope_spans in resource_spans.scope_spans:
        for span in scope_spans.spans:
            # Add custom attribute to all spans
            span.attributes.append(KeyValue("processed.by", AnyValue("my_processor")))
```

### Prebuilt Processors

Rotel also ships with prebuilt processors written in Python that you can use right out of the box or modify.
Prebuilt processors are found in the [processors](/rotel_python_processor_sdk/processors) folder under the
rotel_python_processor_sdk directory.

Current prebuilt processors include...

| Name                 | Supported telemetry types |
|----------------------|---------------------------|
| Attributes Processor | logs, metrics, traces,    |
| Redaction Processor  | logs, metrics, traces     |

#### Technical Implementation

The SDK is built using [PyO3](https://pyo3.rs), a robust Rust binding for Python that enables seamless interoperability
between Rust and
Python code. This architecture provides several benefits:

- **High Performance**: The core data structures remain in Rust memory while exposing a Python-friendly interface,
  minimizing overhead from data copying and conversions
- **Memory Safety**: Rust's ownership model and thread safety guarantees are preserved while allowing safe Python access
  to the data
- **Type Safety**: PyO3's type system ensures reliable conversions between Rust and Python types
- **GIL Management**: Automatic handling of Python's Global Interpreter Lock (GIL) for optimal performance in threaded
  environments

The SDK handles all the necessary conversions between Rust and Python types, making it easy to integrate Python
processing logic into your Rotel collector pipeline.
This allows for flexible data transformation and enrichment without modifying the core collector code.

For detailed documentation, examples, and a complete guide to writing processors, see
the [Python Processor SDK Documentation](rotel_python_processor_sdk/rotel_sdk/README.md).

## Benchmarks

We have taken the OpenTelemetry Collector benchmark suite and adapted it to run against Rotel. You can find
the testing framework at [rotel-otel-loadtests](https://github.com/streamfold/rotel-otel-loadtests) and the benchmark
results
[here](https://streamfold.github.io/rotel-otel-loadtests/benchmarks/). The benchmarks are
run nightly comparing the latest OTEL version against the latest Rotel release.

## Debugging

If you set the option `--debug-log` to `["traces"]`, or the environment variable `ROTEL_DEBUG_LOG=traces`, then
rotel will log a summary to the log file `/tmp/rotel-agent.log` each time it processes trace spans. You can add also
specify _metrics_ to debug metrics and _logs_ to debug logs. By default the debug logging will output a single line
summary of the telemetry. You can increase the verbosity by specifying `--debug-log-verbosity detailed`, which will
include verbose multi-line output.

Separate from the telemetry logging, Rotel's default log level is set to INFO and can be changed with the environment
variable `RUST_LOG`. For example, setting `RUST_LOG=debug` will increase the verbosity of all logging to debug level.
This
may include logging from third-party crates used in Rotel.

## Docker images

On release, Rotel images are published to [Dockerhub](https://hub.docker.com/r/streamfold/rotel) with the following
tags:

- `streamfold/rotel:<release name>`
- `streamfold/rotel:latest`
- `streamfold/rotel:sha-<sha>`

When running an image, map the OTLP receiver ports to their local values with the flag `-p 4317-4318:4317-4318`.

Rotel releases with built-in Python Processor support and Python 3.13 are also available
on [Dockerhub](https://hub.docker.com/repository/docker/streamfold/rotel-python-processors/general)
with the following tags:

- `streamfold/rotel-python-processors:<release name>`
- `streamfold/rotel-python-processors:latest`
- `streamfold/rotel-python-processors:sha-<sha>`

When running an image, you can mount directories from your local filesystem as volumes to provide processor code
to the container with `-v` flag, for example: `-v ~/my_processor_directory:/processors`. You can then start rotel and
pass in processors like the example below.

```
docker run -ti -p 4317-4318:4317-4318  -v ~/my_processor_director:/processors streamfold/rotel-python-processors:latest
--exporter blackhole --debug-log traces --debug-log-verbosity detailed --otlp-with-trace-processor /processors/my_processor.py`
```

## Community

Want to chat about this project, share feedback, or suggest improvements? Join
our [Discord server](https://discord.gg/reUqNWTSGC)! Whether you're a user of this project or not, we'd love to hear
your thoughts and ideas. See you there! 🚀

## Developing

See the [DEVELOPING.md](DEVELOPING.md) doc for building and development instructions.
