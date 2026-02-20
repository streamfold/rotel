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
  and [Kafka](#kafka-exporter-configuration)
- Additional
  receivers: [File](#file-receiver-configuration), [Fluent](#fluent-receiver-configuration), [Kafka](#kafka-receiver-configuration),
  [Kmsg](#kmsg-receiver-configuration-linux-only) (Linux kernel messages), and
  [Node Metrics](#node-metrics-receiver-configuration) (host CPU, memory, disk, network; Linux-oriented)
- [Python](#python-processor-sdk) and [Rust](#rust-processor-sdk) processor SDKs

Rotel can be easily bundled with popular runtimes as packages. Its Rust implementation ensures minimal resource usage
and a compact binary size, simplifying deployment without the need for a sidecar container.

**Runtime integrations:**

- **Python:** [streamfold/pyrotel](https://github.com/streamfold/pyrotel)
- **Node.js:** [streamfold/rotel-nodejs](https://github.com/streamfold/rotel-nodejs)

Rotel provides a unified data plane framework for high-performance OpenTelemetry processing. It can be used as a library and deployed in many different form factors:

- [**Lambda Extension**](https://github.com/streamfold/rotel-lambda-extension): AWS Lambda Extension for OpenTelemetry collection with minimal coldstart latency
- [**Lambda Forwarder**](https://github.com/streamfold/rotel-lambda-forwarder): Convert and forward CloudWatch and S3-stored stored logs to OpenTelemetry compatible backends
- [**AWS Firelens**](https://github.com/streamfold/aws-firelens-rotel): AWS Firelens log router for easy Amazon ECS container logging

Rotel is fully open-sourced and licensed under the Apache 2.0 license.

## Getting Started

### Running Rotel
```bash
docker run -ti -p 4317-4318:4317-4318 public.ecr.aws/rotel-dev/rotel --debug-log traces --exporter blackhole
```

Rotel is now listening on localhost:4317 (gRPC) and localhost:4318 (HTTP).

_We use the [prebuilt Docker image](#docker-images) for this example, but you can also download a binary from the [releases](https://github.com/streamfold/rotel/releases) page._

### Verify

Send OTLP traces to Rotel and verify that it is receiving data:

```bash
go install github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen@latest

telemetrygen traces --otlp-insecure --duration 5s
```

Check the output from Rotel and you should see several "Received traces" log lines.

<details>
<summary>Alternatively, generate traces with the built-in `generate-otlp` tool</summary>
<br>

```bash
# Generate and send traces directly to Rotel
cargo run --bin generate-otlp -- traces --http-endpoint localhost:4318

# Or generate a trace file for testing
cargo run --bin generate-otlp -- traces --file trace.pb

# Then send it with curl
curl -X POST http://localhost:4318/v1/traces \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @trace.pb
```

</details>

## Configuration

- [Base options](#base-options)
- [OTLP exporter](#otlp-exporter-configuration)
  - [Cloudwatch OTLP Export](#cloudwatch-otlp-export)
  - [Basic Authentication](#basic-authentication)
- [Datadog exporter](#datadog-exporter-configuration)
- [ClickHouse exporter](#clickhouse-exporter-configuration)
- [AWS X-Ray exporter](#aws-x-ray-exporter-configuration)
- [AWS EMF exporter](#aws-emf-exporter-configuration)
- [Kafka exporter](#kafka-exporter-configuration)
  - [Acknowledgement Modes](#acknowledgement-modes)
  - [Producer Performance Tuning](#producer-performance-tuning)
  - [Message Partitioning Control](#message-partitioning-control)
  - [Advanced Configuration](#advanced-configuration)
  - [Testing the Kafka Exporter](#testing-the-kafka-exporter)
- [File exporter](#file-exporter-configuration)
- [Kafka Receiver](#kafka-receiver-configuration)
  - [Offset Tracking and Data Reliability](#offset-tracking-and-data-reliability)
  - [Consumer Configuration](#consumer-configuration)
  - [Security Configuration](#security-configuration)
  - [Advanced Configuration](#advanced-configuration-1)
  - [Example Usage](#example-usage)
- [Fluent Receiver](#fluent-receiver-configuration)
- [File Receiver](#file-receiver-configuration)
  - [Watch Modes](#watch-modes)
  - [Parsers](#parsers)
  - [Offset Persistence](#offset-persistence)
  - [Example Usage](#example-usage-1)
- [Node Metrics Receiver](#node-metrics-receiver-configuration)
  - [Available Metrics](#available-metrics)
  - [Example Usage](#example-usage-2)
  - [Testing the Node Metrics Receiver](#testing-the-node-metrics-receiver)
- [Kmsg Receiver (Linux-only)](#kmsg-receiver-configuration-linux-only)
  - [Priority Levels](#priority-levels)
  - [Example Usage](#example-usage-3)
  - [Testing the Kmsg Receiver](#testing-the-kmsg-receiver)
  - [Log Record Format](#log-record-format)
- [Batch configuration](#batch-configuration)
- [Setting resource attributes](#setting-resource-attributes)
- [Retries and timeouts](#retries-and-timeouts)
- [Internal telemetry](#internal-telemetry)
- [Multiple receivers](#multiple-receivers)
  - [Basic Usage](#basic-usage)
  - [Receiver Configuration](#receiver-configuration)
  - [Environment Variables](#environment-variables)
- [Multiple exporters](#multiple-exporters)
- [AWS Authentication](#aws-authentication)
- [Full example](#full-example)

### Base options

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

| Option                                 | Default                        | Options          |
|----------------------------------------|--------------------------------|------------------|
| --otlp-exporter-endpoint               |                                |                  |
| --otlp-exporter-protocol               | grpc                           | grpc, http       |
| --otlp-exporter-custom-headers         |                                |                  |
| --otlp-exporter-compression            | gzip                           | gzip, none       |
| --otlp-exporter-authenticator          |                                | sigv4auth, basic |
| --otlp-exporter-tls-cert-file          |                                |                  |
| --otlp-exporter-tls-cert-pem           |                                |                  |
| --otlp-exporter-tls-key-file           |                                |                  |
| --otlp-exporter-tls-key-pem            |                                |                  |
| --otlp-exporter-tls-ca-file            |                                |                  |
| --otlp-exporter-tls-ca-pem             |                                |                  |
| --otlp-exporter-tls-skip-verify        |                                |                  |
| --otlp-exporter-request-timeout        | 5s                             |                  |
| --otlp-exporter-pool-idle-timeout      | 30s                            |                  |
| --otlp-exporter-pool-max-idle-per-host | 100                            |                  |
| --otlp-exporter-retry-initial-backoff  | (uses global exporter default) |                  |
| --otlp-exporter-retry-max-backoff      | (uses global exporter default) |                  |
| --otlp-exporter-retry-max-elapsed-time | (uses global exporter default) |                  |

Any of the options that start with `--otlp-exporter*` can be set per telemetry type: metrics, traces or logs. For
example, to set a custom endpoint to export traces to, set: `--otlp-exporter-traces-endpoint`. For other telemetry
types their value falls back to the top-level OTLP exporter config.

#### Cloudwatch OTLP Export

The Rotel OTLP exporter can export to the
[Cloudwatch OTLP endpoints](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-OTLPEndpoint.html)
for traces and logs. You'll need to select the HTTP protocol and enable the sigv4auth authenticator.

The sigv4auth authenticator requires the AWS authentication credentials. See
the [AWS Authentication](#aws-authentication) docs
for supported methods.

**Traces**

_Tracing requires that you
enable [Transaction Search](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-Transaction-Search.html)
in the AWS console before you can send OTLP traces._

Here is the full environment variable configuration to send traces to Cloudwatch, swap the region code as needed.

```shell
ROTEL_EXPORTERS=traces:otlp
ROTEL_EXPORTER_TRACES_PROTOCOL=http
ROTEL_EXPORTER_TRACES_ENDPOINT=https://xray.<region code>.amazonaws.com
ROTEL_EXPORTER_TRACES_AUTHENTICATOR=sigv4auth
ROTEL_EXPORTERS_TRACES=traces
```

**Logs**

_To send OTLP logs to Cloudwatch you must create a log group and log stream. Exporting will fail if these do not exist
ahead of time and they are not created by default._

Here is the full environment variable configuration to send logs to Cloudwatch, swap the region code and
log group/stream as needed.

```shell
ROTEL_EXPORTERS=logs:otlp
ROTEL_EXPORTER_LOGS_PROTOCOL=http
ROTEL_EXPORTER_LOGS_ENDPOINT=https://logs.<region code>.amazonaws.com
ROTEL_EXPORTER_LOGS_CUSTOM_HEADERS="x-aws-log-group=<log group>,x-aws-log-stream=<log stream>"
ROTEL_EXPORTER_LOGS_AUTHENTICATOR=sigv4auth
ROTEL_EXPORTERS_LOGS=logs
```

#### Basic Authentication

For OTLP endpoints that require HTTP Basic Authentication, you can use the `basic` authenticator:

| Option                              | Default | Options |
|-------------------------------------|---------|---------|
| --otlp-exporter-basic-auth-username |         |         |
| --otlp-exporter-basic-auth-password |         |         |

Example configuration:

```shell
ROTEL_OTLP_EXPORTER_ENDPOINT=https://collector.example.com:443
ROTEL_OTLP_EXPORTER_AUTHENTICATOR=basic
ROTEL_OTLP_EXPORTER_BASIC_AUTH_USERNAME=myuser
ROTEL_OTLP_EXPORTER_BASIC_AUTH_PASSWORD=mypassword
```

### Datadog exporter configuration

The Datadog exporter can be selected by passing `--exporter datadog`. The Datadog exporter only supports traces at the
moment. For more information, see the [Datadog Exporter](src/exporters/datadog/README.md) docs.

| Option                                    | Default                        | Options                |
|-------------------------------------------|--------------------------------|------------------------|
| --datadog-exporter-region                 | us1                            | us1, us3, us5, eu, ap1 |
| --datadog-exporter-custom-endpoint        |                                |                        |
| --datadog-exporter-api-key                |                                |                        |
| --datadog-exporter-retry-initial-backoff  | (uses global exporter default) |                        |
| --datadog-exporter-retry-max-backoff      | (uses global exporter default) |                        |
| --datadog-exporter-retry-max-elapsed-time | (uses global exporter default) |                        |

Specifying a custom endpoint will override the region selection.

### ClickHouse exporter configuration

The ClickHouse exporter can be selected by passing `--exporter clickhouse`. The ClickHouse exporter supports metrics,
logs,
and traces.

| Option                                       | Default                        | Options     |
|----------------------------------------------|--------------------------------|-------------|
| --clickhouse-exporter-endpoint               |                                |             |
| --clickhouse-exporter-database               | otel                           |             |
| --clickhouse-exporter-table-prefix           | otel                           |             |
| --clickhouse-exporter-compression            | lz4                            | none, lz4   |
| --clickhouse-exporter-async-insert           | true                           | true, false |
| --clickhouse-exporter-request-timeout        | 5s                             |             |
| --clickhouse-exporter-enable-json            |                                |             |
| --clickhouse-exporter-nested-kv-max-depth    | 3                              |             |
| --clickhouse-exporter-json-underscore        |                                |             |
| --clickhouse-exporter-user                   |                                |             |
| --clickhouse-exporter-password               |                                |             |
| --clickhouse-exporter-retry-initial-backoff  | (uses global exporter default) |             |
| --clickhouse-exporter-retry-max-backoff      | (uses global exporter default) |             |
| --clickhouse-exporter-retry-max-elapsed-time | (uses global exporter default) |             |

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

When exporting OpenTelemetry attributes that contain nested `KeyValueList` structures (such as GenAI message
attributes like `gen_ai.input.messages`), use `--clickhouse-exporter-nested-kv-max-depth` to control the
depth of nested objects that are supported. By default this will support nested attributes to a depth of
three, to increase this depth set the parameter higher. Set to zero to disable any nesting, complex attributes
will be stored as their JSON string representation.

For example, increasing the depth to 10:

```shell
rotel start --exporter clickhouse \
  --clickhouse-exporter-endpoint "http://localhost:8123" \
  --clickhouse-exporter-enable-json \
  --clickhouse-exporter-nested-kv-max-depth 10
```

_The ClickHouse exporter is built using code from the official Rust [clickhouse-rs](https://crates.io/crates/clickhouse)
crate._

### AWS X-Ray exporter configuration

The AWS X-Ray exporter can be selected by passing `--exporter awsxray`. The X-Ray exporter only supports traces.

See the [AWS Authentication](#aws-authentication) section for how to configure AWS credentials required for the AWS
X-Ray exporter.

| Option                                    | Default                                              | Options          |
|-------------------------------------------|------------------------------------------------------|------------------|
| --awsxray-exporter-region                 | `$AWS_REGION`, `$AWS_DEFAULT_REGION`, or `us-east-1` | aws region codes |
| --awsxray-exporter-custom-endpoint        |                                                      |                  |
| --awsxray-exporter-retry-initial-backoff  | (uses global exporter default)                       |                  |
| --awsxray-exporter-retry-max-backoff      | (uses global exporter default)                       |                  |
| --awsxray-exporter-retry-max-elapsed-time | (uses global exporter default)                       |                  |

For a list of available AWS X-Ray region codes here: https://docs.aws.amazon.com/general/latest/gr/xray.html

### AWS EMF exporter configuration

The AWS EMF exporter can be selected by passing `--exporter awsemf`. The AWS EMF exporter only supports metrics. The
AWS EMF exporter will convert metrics into the AWS
Cloudwatch [Embedded metric format](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch_Embedded_Metric_Format_Specification.html)
and
send those as JSON log lines to Cloudwatch. Cloudwatch will convert the log lines into Cloudwatch Metrics.

See the [AWS Authentication](#aws-authentication) section for how to configure AWS credentials required for the AWS EMF
exporter.

| Option                                                 | Default                                              | Options          |
|--------------------------------------------------------|------------------------------------------------------|------------------|
| --awsemf-exporter-region                               | `$AWS_REGION`, `$AWS_DEFAULT_REGION`, or `us-east-1` | aws region codes |
| --awsemf-exporter-custom-endpoint                      |                                                      |                  |
| --awsemf-exporter-log-group-name                       | /metrics/default                                     |                  |
| --awsemf-exporter-log-stream-name                      | otel-stream                                          |                  |
| --awsemf-exporter-log-retention                        | 0                                                    |                  |
| --awsemf-exporter-namespace                            |                                                      |                  |
| --awsemf-exporter-retain-initial-value-of-delta-metric | false                                                |                  |
| --awsemf-exporter-include-dimensions                   |                                                      |                  |
| --awsemf-exporter-exclude-dimensions                   |                                                      |                  |
| --awsemf-exporter-retry-initial-backoff                | (uses global exporter default)                       |                  |
| --awsemf-exporter-retry-max-backoff                    | (uses global exporter default)                       |                  |
| --awsemf-exporter-retry-max-elapsed-time               | (uses global exporter default)                       |                  |

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

### Kafka exporter configuration

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

The Kafka Receiver allows Rotel to consume telemetry data from Kafka topics. The receiver supports consuming metrics,
logs, and traces from
separate topics in either JSON or Protobuf format.

To enable the Kafka receiver, you must specify which telemetry types to consume using the appropriate flags:

- `--kafka-receiver-traces` to consume traces
- `--kafka-receiver-metrics` to consume metrics
- `--kafka-receiver-logs` to consume logs

| Option                                             | Default        | Options                                                                |
|----------------------------------------------------|----------------|------------------------------------------------------------------------|
| --kafka-receiver-brokers                           | localhost:9092 | Kafka broker addresses (comma-separated)                               |
| --kafka-receiver-traces-topic                      | otlp_traces    | Topic name for traces                                                  |
| --kafka-receiver-metrics-topic                     | otlp_metrics   | Topic name for metrics                                                 |
| --kafka-receiver-logs-topic                        | otlp_logs      | Topic name for logs                                                    |
| --kafka-receiver-traces                            | false          | Enable consuming traces                                                |
| --kafka-receiver-metrics                           | false          | Enable consuming metrics                                               |
| --kafka-receiver-logs                              | false          | Enable consuming logs                                                  |
| --kafka-receiver-format                            | protobuf       | json, protobuf                                                         |
| --kafka-receiver-group-id                          | rotel-consumer | Consumer group ID for coordinated consumption                          |
| --kafka-receiver-client-id                         | rotel          | Client ID for the Kafka consumer                                       |
| --kafka-receiver-enable-auto-commit                | false          | Enable auto commit of offsets                                          |
| --kafka-receiver-auto-commit-interval-ms           | 5000           | Auto commit interval in milliseconds                                   |
| --kafka-receiver-auto-offset-reset                 | latest         | earliest, latest, error                                                |
| --kafka-receiver-session-timeout-ms                | 30000          | Session timeout in milliseconds                                        |
| --kafka-receiver-heartbeat-interval-ms             | 3000           | Heartbeat interval in milliseconds                                     |
| --kafka-receiver-max-poll-interval-ms              | 300000         | Maximum poll interval in milliseconds                                  |
| --kafka-receiver-max-partition-fetch-bytes         | 1048576        | Maximum bytes per partition the consumer will buffer                   |
| --kafka-receiver-fetch-min-bytes                   | 1              | Minimum number of bytes for fetch requests                             |
| --kafka-receiver-fetch-max-wait-ms                 | 500            | Maximum wait time for fetch requests in milliseconds                   |
| --kafka-receiver-socket-timeout-ms                 | 60000          | Socket timeout in milliseconds                                         |
| --kafka-receiver-metadata-max-age-ms               | 300000         | Maximum age of metadata in milliseconds                                |
| --kafka-receiver-isolation-level                   | read-committed | read-uncommitted, read-committed                                       |
| --kafka-receiver-enable-partition-eof              | false          | Enable partition EOF notifications                                     |
| --kafka-receiver-check-crcs                        | true           | Check CRC32 of consumed messages                                       |
| --kafka-receiver-disable-exporter-indefinite-retry |                | Disable indefinite retry for exporters when offset tracking is enabled |
| --kafka-receiver-custom-config                     |                | Custom consumer config (comma-separated key=value)                     |
| --kafka-receiver-sasl-username                     |                | SASL username for authentication                                       |
| --kafka-receiver-sasl-password                     |                | SASL password for authentication                                       |
| --kafka-receiver-sasl-mechanism                    |                | plain, scram-sha256, scram-sha512                                      |
| --kafka-receiver-security-protocol                 |                | plaintext, ssl, sasl-plaintext, sasl-ssl                               |
| --kafka-receiver-ssl-ca-location                   |                | SSL CA certificate location                                            |
| --kafka-receiver-ssl-certificate-location          |                | SSL certificate location                                               |
| --kafka-receiver-ssl-key-location                  |                | SSL key location                                                       |
| --kafka-receiver-ssl-key-password                  |                | SSL key password                                                       |

#### Offset Tracking and Data Reliability

By default, the Kafka receiver uses **manual offset tracking** to ensure data reliability. With offset tracking enabled:

- **At Least Once Guaranteed Delivery**: Kafka offsets are only committed after telemetry data is successfully exported
- **Indefinite Retry**: Exporters retry indefinitely by default to prevent data loss. If an export fails, the exporter
  will keep retrying until it succeeds.
- **Backpressure Handling**: The Kafka receiver will pause consuming when the pipeline reaches its maximum
  in-memory capacity

**Disabling Indefinite Retry:**

If you prefer to revert to timeout-based retry behavior (which may result in data loss on persistent export failures),
use:

```shell
--kafka-receiver-disable-exporter-indefinite-retry
```

With this flag, failed exports that exceed the retry timeout will be negatively acknowledged (NACK'd), allowing the
receiver to continue processing new messages.

**Using Auto-Commit (Legacy Behavior):**

To revert to the legacy auto-commit behavior where offsets are committed immediately regardless of export success:

```shell
--kafka-receiver-enable-auto-commit
```

**Warning**: Auto-commit mode may result in data loss if exports fail, as Kafka will mark messages as consumed even if
they weren't successfully exported.

#### Consumer Configuration

The Kafka receiver acts as a consumer and supports standard Kafka consumer configurations:

**Consumer Group Management:**

- `--kafka-receiver-group-id`: Sets the consumer group ID for coordinated consumption across multiple Rotel instances
- `--kafka-receiver-enable-auto-commit`: Controls whether offsets are automatically committed by Kafka (default: false).
  When disabled, Rotel uses manual offset tracking to ensure data reliability.
- `--kafka-receiver-auto-commit-interval-ms`: How often to commit offsets when auto-commit is enabled (only applies when
  auto-commit is true)
- `--kafka-receiver-disable-exporter-indefinite-retry`: When using manual offset tracking, exporters retry indefinitely
  by default. Set this flag to revert to timeout-based retries.

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
settings. See
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

### Fluent Receiver configuration

_The Fluent Receiver is currently only included when built with the opt-in feature `--features fluent_receiver`._

The Fluent Receiver allows Rotel to receive telemetry data in Fluentd/Fluent
Bit [forward protocol format](https://chronosphere.io/learn/forward-protocol-fluentd-fluent-bit/). This enables
compatibility with existing Fluentd and Fluent Bit deployments, allowing them to send logs directly to Rotel for
processing and export. Select the Fluent receiver with the option `--receiver fluent`.

The receiver supports both UNIX domain sockets and TCP endpoints, converting incoming Fluent messages to OpenTelemetry
logs format.

| Option                        | Default | Description                                            |
|-------------------------------|---------|--------------------------------------------------------|
| --fluent-receiver-socket-path |         | Path to UNIX socket file for receiving Fluent messages |
| --fluent-receiver-endpoint    |         | TCP endpoint to bind (e.g., 127.0.0.1:24224)           |

**Note**: At least one of `--fluent-receiver-socket-path` or `--fluent-receiver-endpoint` must be specified when using
the Fluent
receiver.

**Example Usage**:

```bash
# Using UNIX socket
rotel start \
  --receiver fluent \
  --fluent-receiver-socket-path /var/run/rotel-fluent.sock \
  [...exporter args]

# Using TCP endpoint
rotel start \
  --receiver fluent \
  --fluent-receiver-endpoint 127.0.0.1:24224 \
  [...exporter args]

# Using both socket and TCP
rotel start \
  --receiver fluent \
  --fluent-receiver-socket-path /var/run/rotel-fluent.sock \
  --fluent-receiver-endpoint 0.0.0.0:24224 \
  [...exporter args]
```

_Compression and message acknowledgement are not supported at the moment._

### File Receiver configuration

**NOTE**: The File Receiver is currently experimental and under development. Users should expect potential breaking
changes in future releases.

The File Receiver allows Rotel to tail log files and convert them to OpenTelemetry logs. It supports glob patterns for
file discovery, multiple parsing formats, and efficient file watching using native OS mechanisms (inotify on Linux,
FSEvents on macOS) with fallback to polling.

To enable the File Receiver, specify it with `--receiver file` and provide at least one include pattern.

| Option                                             | Default                          | Description                                                                  |
|----------------------------------------------------|----------------------------------|------------------------------------------------------------------------------|
| --file-receiver-include                            |                                  | Comma-separated glob patterns for files to include (e.g., "/var/log/\*.log") |
| --file-receiver-exclude                            |                                  | Comma-separated glob patterns for files to exclude                           |
| --file-receiver-parser                             | none                             | Parser type: none, json, regex, nginx_access, nginx_error                    |
| --file-receiver-nginx-access-format                | auto                             | Nginx access log format: auto, combined, json (when parser=nginx_access)     |
| --file-receiver-regex-pattern                      |                                  | Regex pattern with named capture groups (required when parser=regex)         |
| --file-receiver-start-at                           | end                              | Where to start reading: beginning or end                                     |
| --file-receiver-watch-mode                         | auto                             | Watch mode: auto, native, poll                                               |
| --file-receiver-poll-interval-ms                   | 250                              | Poll interval in milliseconds for file changes                               |
| --file-receiver-debounce-interval-ms               | 200                              | Debounce interval in milliseconds for native watcher events                  |
| --file-receiver-offsets-path                       | /var/lib/rotel/file_offsets.json | Path to store file offsets for persistence across restarts                   |
| --file-receiver-max-log-size                       | 65536                            | Maximum log line size in bytes (lines exceeding this will be truncated)      |
| --file-receiver-include-file-name                  | true                             | Include file name as a log attribute                                         |
| --file-receiver-include-file-path                  | false                            | Include full file path as a log attribute                                    |
| --file-receiver-max-concurrent-files               | 4                                | Maximum number of concurrent file processing threads                         |
| --file-receiver-rotate-wait-ms                     | 1000                             | Time in ms to wait after EOF on a rotated file before closing                |
| --file-receiver-shutdown-worker-drain-timeout-ms   | 250                              | Max time in ms to wait for workers to complete during shutdown               |
| --file-receiver-shutdown-records-drain-timeout-ms  | 100                              | Max time in ms to wait for records to be sent during shutdown                |
| --file-receiver-max-checkpoint-failure-duration-ms | 60000                            | Max duration in ms of consecutive checkpoint failures before exiting         |
| --file-receiver-max-poll-failure-duration-ms       | 60000                            | Max duration in ms of consecutive poll failures before exiting               |
| --file-receiver-max-watcher-error-duration-ms      | 60000                            | Max duration in ms of consecutive watcher errors before falling back to poll |
| --file-receiver-max-batch-size                     | 100                              | Maximum number of log records to batch before sending to pipeline            |
| --file-receiver-disable-exporter-indefinite-retry  | false                            | Disable indefinite retry for exporters (may result in data loss)             |

#### Watch Modes

The File Receiver supports three watch modes:

- **auto** (default): Uses native file system watching (inotify/kqueue/FSEvents) with automatic fallback to polling if
  native watching fails
- **native**: Forces native file system watching only
- **poll**: Forces polling mode, useful for NFS or network file systems where native watching is unreliable

#### Parsers

The receiver includes several built-in parsers:

- **none**: Raw log lines are passed through as-is in the log body
- **json**: Parses JSON log lines and extracts fields as attributes
- **regex**: Uses a custom regex pattern with named capture groups to extract attributes
- **nginx_access**: Parses nginx access logs. Supports multiple formats via `--file-receiver-nginx-access-format`:
  - `auto` (default): Auto-detects format per line (JSON vs combined)
  - `combined`: Standard nginx combined log format (regex-based)
  - `json`: JSON-formatted nginx logs (for nginx configured with `log_format json`)
- **nginx_error**: Parses nginx error log format

When using the regex parser, provide a pattern with named capture groups:

```shell
--file-receiver-parser regex \
--file-receiver-regex-pattern '^(?P<timestamp>\S+) (?P<level>\S+) (?P<message>.*)$'
```

#### Offset Persistence

The File Receiver tracks file offsets to resume reading from where it left off after restarts. Offsets are persisted to
the path specified by `--file-receiver-offsets-path`. The receiver uses file device ID and inode number to identify
files, allowing it to handle log rotation correctly.

#### Example Usage

Parsing nginx access logs and exporting to ClickHouse:

```shell
rotel start \
  --receiver file \
  --file-receiver-include "/var/log/nginx/access.log" \
  --file-receiver-parser nginx_access \
  --file-receiver-start-at beginning \
  --exporter clickhouse \
  --clickhouse-exporter-endpoint "http://localhost:8123" \
  --file-receiver-offsets-path "/tmp/rotel-offsets.json"
```

Basic example tailing nginx access logs to OTLP:

```shell
rotel start \
  --receiver file \
  --file-receiver-include "/var/log/nginx/access.log" \
  --file-receiver-parser nginx_access \
  --file-receiver-start-at end \
  --exporter otlp \
  --otlp-exporter-endpoint "localhost:4317"
```

Tailing multiple log files with glob patterns:

```shell
rotel start \
  --receiver file \
  --file-receiver-include "/var/log/*.log,/var/log/apps/**/*.log" \
  --file-receiver-exclude "/var/log/debug.log" \
  --file-receiver-start-at beginning \
  --file-receiver-offsets-path "/tmp/rotel-offsets.json" \
  --exporter clickhouse \
  --clickhouse-exporter-endpoint "http://localhost:8123"
```

Using regex parser for custom log format:

```shell
rotel start \
  --receiver file \
  --file-receiver-include "/var/log/myapp/*.log" \
  --file-receiver-parser regex \
  --file-receiver-regex-pattern '^(?P<timestamp>[^ ]+) \[(?P<level>\w+)\] (?P<message>.*)$' \
  --exporter otlp \
  --otlp-exporter-endpoint "localhost:4317"
```

### Node Metrics Receiver configuration

**NOTE**: The Node Metrics Receiver is currently only included when built with the opt-in feature
`--features node_metrics_receiver`.

The Node Metrics Receiver periodically scrapes system metrics from the host machine and converts them to OpenTelemetry
metrics format. It reads directly from Linux kernel interfaces (`/proc`, `/sys`, syscalls), emitting raw counter values
compatible with Prometheus node_exporter conventions.

**NOTE**: This receiver targets Linux and requires access to the `/proc` and `/sys` filesystems. Elsewhere only the
`uname`, `time` and `textfile` collectors return data — the collectors that read `/proc` or `/sys` yield nothing — and a
warning is logged at startup.

Metric names, help strings and the default filesystem-type exclusion list follow Prometheus
[node_exporter](https://github.com/prometheus/node_exporter) (Apache-2.0), so existing dashboards and alerting rules
largely carry over. The collector split differs in one place: `node_boot_time_seconds` is produced by the `time`
collector here, while node_exporter exposes it from its `stat` collector.

Select the Node Metrics receiver with the option `--receiver node_metrics`.

| Option                                           | Default      | Description                                                                              |
|--------------------------------------------------|--------------|------------------------------------------------------------------------------------------|
| --node-metrics-receiver-scrape-interval          | 60s          | Scrape interval as a string time duration (minimum 1 second)                             |
| --node-metrics-receiver-cpu                      | true         | Enable CPU metrics collection                                                            |
| --node-metrics-receiver-loadavg                  | true         | Enable load average metrics collection                                                   |
| --node-metrics-receiver-memory                   | true         | Enable memory metrics collection                                                         |
| --node-metrics-receiver-network                  | true         | Enable network metrics collection                                                        |
| --node-metrics-receiver-filesystem               | true         | Enable filesystem metrics collection                                                     |
| --node-metrics-receiver-uname                    | true         | Enable system info metrics (kernel, hostname)                                            |
| --node-metrics-receiver-stat                     | true         | Enable kernel stat counters (forks, context switches, interrupts, procs running/blocked) |
| --node-metrics-receiver-processes                | true         | Enable process metrics (kernel limits: threads-max, pid_max)                             |
| --node-metrics-receiver-diskstats                | true         | Enable disk I/O statistics (Linux only, /proc/diskstats)                                 |
| --node-metrics-receiver-vmstat                   | true         | Enable virtual memory statistics (/proc/vmstat)                                          |
| --node-metrics-receiver-netstat                  | true         | Enable network statistics (/proc/net/netstat and /proc/net/snmp)                         |
| --node-metrics-receiver-sockstat                 | true         | Enable socket statistics (/proc/net/sockstat and /proc/net/sockstat6)                    |
| --node-metrics-receiver-filefd                   | true         | Enable file descriptor statistics (/proc/sys/fs/file-nr)                                 |
| --node-metrics-receiver-cpufreq                  | true         | Enable CPU frequency metrics (/sys/devices/system/cpu/cpu*/cpufreq/)                     |
| --node-metrics-receiver-thermal-zone             | true         | Enable thermal zone and cooling device metrics (/sys/class/thermal/)                     |
| --node-metrics-receiver-nvme                     | true         | Enable NVMe device info metrics (/sys/class/nvme/)                                       |
| --node-metrics-receiver-hwmon                    | true         | Enable hardware monitoring sensor metrics (/sys/class/hwmon/)                            |
| --node-metrics-receiver-time                     | true         | Enable time metrics (node_time_seconds, node_boot_time_seconds)                          |
| --node-metrics-receiver-textfile                 | false        | Enable textfile collector for custom Prometheus metrics                                  |
| --node-metrics-receiver-textfile-directory       |              | Directory of Prometheus-format textfiles (*.prom), or a single .prom file                |
| --node-metrics-receiver-filesystem-mount-exclude |              | Regex pattern to exclude filesystem mount points, in addition to the built-in excludes   |
| --node-metrics-receiver-include-filter           |              | Regex pattern to include only matching metric names                                      |
| --node-metrics-receiver-exclude-filter           |              | Regex pattern to exclude matching metric names                                           |
| --node-metrics-receiver-rootfs-path              | /            | Prefix under which the host root is mounted (for containerized monitoring)               |
| --node-metrics-receiver-procfs-path              | /proc        | Path to procfs mount point (for containerized monitoring)                                |
| --node-metrics-receiver-sysfs-path               | /sys         | Path to sysfs mount point (for containerized monitoring)                                 |
| --node-metrics-receiver-service-name             | node_metrics | Service name for the OTLP Resource attribute                                             |

The collector toggles are booleans that take an explicit value, for example `--node-metrics-receiver-cpu false` or
`--node-metrics-receiver-cpu=true`. Passing one of them without a value is an error. The same applies to the
environment variable form, `ROTEL_NODE_METRICS_RECEIVER_CPU=false`.

`--node-metrics-receiver-rootfs-path` is prefixed onto the procfs and sysfs paths, so `/host` alone yields
`/host/proc` and `/host/sys`. A procfs or sysfs path that is already *under* the rootfs prefix is rejected at startup
with an error naming both flags, rather than silently producing `/host/host/proc` — so pass either the rootfs path or
the already-prefixed individual paths, not both. The comparison is path-component aware, so only a genuine prefix is
rejected: `/host` with `/host/proc` is an error, while `/host` with `/hostile/proc` or `/proc-alt` is accepted and
composes normally into `/host/hostile/proc` and `/host/proc-alt`. The rootfs path applies to the procfs and sysfs paths
only, so `--node-metrics-receiver-textfile-directory` is always used exactly as given.

A few more startup and runtime behaviours:

- The scrape interval is a string time duration, so `30s`, `90s` or `2m`, with a 1 second minimum enforced at startup.
- The first scrape runs immediately at startup rather than after one interval. Ticks stay aligned to the original
  schedule, so a tick missed while a slow scrape was in progress is skipped rather than delayed or fired back-to-back
  as a catch-up burst.
- A scrape that runs longer than the scrape interval, or 30 seconds if the interval is shorter than that, is timed out,
  logged, and skipped. (The floor matters: with a 1 second interval, a merely slow host would otherwise time out on
  every cycle and the receiver would report nothing at all.) Because a blocking read cannot be aborted, scraping resumes
  only once the timed-out scrape actually finishes; each interval until then is skipped and counted as a scrape failure.
  An unresponsive filesystem is the usual cause — exclude it with
  `--node-metrics-receiver-filesystem-mount-exclude`.
- A collector that panics does not take the receiver down: the panic is logged, counted as a scrape failure, and
  scraping continues on the next tick.
- A slow or full pipeline does not block scraping. If the pipeline has not accepted a batch within 10 seconds (or the
  scrape interval, whichever is shorter), the batch is abandoned with a warning and counted as refused, and the next
  tick scrapes as usual. The warning notes that the batch "may still be delivered", because the hand-off can complete
  after the timeout has fired. Without this bound a backed-up exporter would park the receiver indefinitely.
- If no configured exporter accepts metrics, the receiver logs a warning and stays idle without scraping.
- Enabling `--node-metrics-receiver-textfile` without `--node-metrics-receiver-textfile-directory` is a startup error.
  Setting the directory without enabling the collector only logs a warning.
- Duplicate entries in the collector list are removed, keeping the first occurrence. This matters only when the config
  is built programmatically; the CLI toggles cannot name a collector twice.
- Within one metric, data points that repeat an already seen label set are dropped with a warning and the first
  occurrence is kept, since duplicates would break the OTLP single-writer principle.
- Every batch carries the resource attributes `service.name` (from `--node-metrics-receiver-service-name`) and, on
  Linux, `os.type`. `host.name` (the local hostname) is added too, and is what keeps otherwise identical series from
  different hosts distinct at the backend — it is omitted only if the hostname is not valid UTF-8.
- On shutdown, a batch already in flight is given a short grace window to reach the pipeline; if the window expires, or
  the pipeline has already closed, the batch is reported as refused. A scrape still running when cancellation arrives is
  discarded without being sent.
- With [internal telemetry](#internal-telemetry) enabled, the receiver reports
  `rotel_receiver_accepted_metric_points`, `rotel_receiver_refused_metric_points`, `rotel_receiver_scrape_failures` and
  `rotel_receiver_empty_scrapes`, all tagged `receiver="node_metrics"`. Refused metric points are the points of a batch
  that could not be handed to the pipeline: the pipeline was closed, the shutdown grace window expired, or the send
  timed out. A scrape failure is counted when a scrape exceeds the interval and is timed out, when a collector panics,
  and for every cycle skipped while a previous scrape is still running. A scrape that collects nothing at all
  increments `rotel_receiver_empty_scrapes` and is logged as a warning; it is not counted as a scrape failure, since an
  empty result is a configuration problem rather than a failed scrape.

#### Available Metrics

The receiver collects the following metrics (names follow Prometheus node_exporter conventions):

**CPU** (when `--node-metrics-receiver-cpu=true`):
- `node_cpu_seconds_total` - Seconds the CPUs spent in each mode (with `cpu` and `mode` labels: user, nice, system, idle, iowait, irq, softirq, steal)
- `node_cpu_guest_seconds_total` - Seconds the CPUs spent in guest mode (with `cpu` and `mode` labels: user, nice)

**NOTE**: Only per-core lines from `/proc/stat` are reported. The aggregate `cpu` line is deliberately skipped, as
Prometheus node_exporter does, so there is no all-core total metric; sum across the `cpu` label instead.

**Load Average** (when `--node-metrics-receiver-loadavg=true`):
- `node_load1` - 1-minute load average
- `node_load5` - 5-minute load average
- `node_load15` - 15-minute load average

**Memory** (when `--node-metrics-receiver-memory=true`):
All fields from `/proc/meminfo` are exposed dynamically, including:
- `node_memory_MemTotal_bytes` - Total memory in bytes
- `node_memory_MemFree_bytes` - Free memory in bytes
- `node_memory_MemAvailable_bytes` - Available memory in bytes
- `node_memory_Buffers_bytes` - Buffer memory in bytes
- `node_memory_Cached_bytes` - Cached memory in bytes
- `node_memory_SwapTotal_bytes` - Total swap in bytes
- `node_memory_SwapFree_bytes` - Free swap in bytes
- ... and 30+ additional memory metrics

**NOTE**: Only fields carrying the `kB` suffix are converted to bytes; those get the `_bytes` name suffix and the unit
`By`. Unitless fields keep their kernel name and carry no unit, for example `HugePages_Total` is exposed as
`node_memory_HugePages_Total`. Parenthesised field names are normalised, so `Active(anon)` becomes
`node_memory_Active_anon_bytes`.

**Network** (when `--node-metrics-receiver-network=true`, per interface with `device` label):
- `node_network_receive_bytes_total` - Bytes received
- `node_network_receive_packets_total` - Packets received
- `node_network_receive_errs_total` - Receive errors
- `node_network_receive_drop_total` - Receive drops
- `node_network_receive_fifo_total` - Receive FIFO errors
- `node_network_receive_frame_total` - Receive frame errors
- `node_network_receive_compressed_total` - Compressed packets received
- `node_network_receive_multicast_total` - Multicast packets received
- `node_network_transmit_bytes_total` - Bytes transmitted
- `node_network_transmit_packets_total` - Packets transmitted
- `node_network_transmit_errs_total` - Transmit errors
- `node_network_transmit_drop_total` - Transmit drops
- `node_network_transmit_fifo_total` - Transmit FIFO errors
- `node_network_transmit_colls_total` - Transmit collisions
- `node_network_transmit_carrier_total` - Transmit carrier errors
- `node_network_transmit_compressed_total` - Compressed packets transmitted

**Filesystem** (when `--node-metrics-receiver-filesystem=true`, per mount with `device`, `fstype`, `mountpoint` labels):
- `node_filesystem_size_bytes` - Total filesystem size
- `node_filesystem_free_bytes` - Free space (total free blocks)
- `node_filesystem_avail_bytes` - Available space (free blocks for non-root users)
- `node_filesystem_files` - Total file nodes (inodes)
- `node_filesystem_files_free` - Free file nodes
- `node_filesystem_readonly` - Whether the filesystem is mounted read-only (1 = readonly)

**NOTE**:
- Virtual filesystems are automatically filtered out. Besides the obvious ones (proc, sysfs, cgroup, overlay), the list
  also covers `squashfs`, `iso9660` and `autofs` — so snap and other squashfs-backed mounts, and CD/DVD images, are not
  reported.
- Network filesystem types are also excluded by default, because a `statfs` call on an unreachable server can block or
  hang. Examples include NFS, SMB/CIFS, Ceph, GlusterFS, Lustre, BeeGFS, GPFS, 9p, AFS, NCP, WebDAV, and remote FUSE
  mounts such as sshfs and s3fs; the authoritative list is `collect_filesystem` in
  `src/receivers/node_metrics/collector/procfs.rs`. Local FUSE filesystems, such as mergerfs or gocryptfs, are reported
  normally. This list of excluded types is not currently overridable:
  `--node-metrics-receiver-filesystem-mount-exclude` can only remove further mount points, it cannot bring an excluded
  type back.
- Mount points are excluded by prefix, with the match respecting path boundaries, so `/dev` does not exclude
  `/developer`. `/proc`, `/sys` and `/dev` are excluded including the mount point itself. `/run/credentials`,
  `/run/user`, `/var/lib/docker` and `/var/lib/containers` are excluded only *below* the path: a filesystem mounted
  exactly at one of them is a real volume and is reported, which matters most for a dedicated `/var/lib/docker` disk.
- Duplicate mounts (same device + mountpoint) are deduplicated.
- Mount paths containing octal escapes (e.g. `\040` for spaces) are decoded automatically.

**System/Uname** (when `--node-metrics-receiver-uname=true`):
- `node_uname_info` - System info gauge (labels: sysname, release, version, nodename, machine, domainname [Linux only])

**Time** (when `--node-metrics-receiver-time=true`):
- `node_boot_time_seconds` - System boot time in seconds since epoch (from the `btime` line of `/proc/stat`, read at
  startup and re-read on every scrape that reads `/proc/stat` — that is, whenever the `cpu`, `stat` or `time` collector
  is enabled; the metric is omitted, rather than reported as zero, while the boot time is unknown, and a value that is
  not plausible — zero, negative, or in the future — is rejected rather than published)
- `node_time_seconds` - Current system time in seconds since epoch

**NOTE**: The boot time is not latched, because the kernel's `btime` moves whenever the wall clock is stepped — a device
with no RTC boots near the epoch and then jumps once NTP syncs. `node_boot_time_seconds` therefore tracks the current
kernel value, while the start time stamped on cumulative counters stays latched at the first known value, so a clock
step is not reported downstream as a counter reset.

**Stat** (when `--node-metrics-receiver-stat=true`):
- `node_forks_total` - Total number of forks since boot
- `node_context_switches_total` - Total number of context switches
- `node_intr_total` - Total number of interrupts serviced (unit `{interrupts}`)
- `node_procs_running` - Number of processes in runnable state
- `node_procs_blocked` - Number of processes blocked waiting for I/O

**Processes** (when `--node-metrics-receiver-processes=true`):
- `node_processes_max_threads` - Kernel thread limit (`/proc/sys/kernel/threads-max`)
- `node_processes_max_processes` - Kernel PID limit (`/proc/sys/kernel/pid_max`)

**Disk I/O** (when `--node-metrics-receiver-diskstats=true`, per device with `device` label):
- `node_disk_reads_completed_total` - Total number of reads completed
- `node_disk_reads_merged_total` - Total number of reads merged
- `node_disk_read_bytes_total` - Total bytes read
- `node_disk_read_time_seconds_total` - Total time spent reading
- `node_disk_writes_completed_total` - Total number of writes completed
- `node_disk_writes_merged_total` - Total number of writes merged
- `node_disk_written_bytes_total` - Total bytes written
- `node_disk_write_time_seconds_total` - Total time spent writing
- `node_disk_io_now` - Number of I/Os currently in progress
- `node_disk_io_time_seconds_total` - Total time spent doing I/Os
- `node_disk_io_time_weighted_seconds_total` - Weighted time spent doing I/Os
- `node_disk_discards_completed_total` - Total discards completed (kernel 4.18+)
- `node_disk_discards_merged_total` - Total discards merged (kernel 4.18+)
- `node_disk_discarded_sectors_total` - Total sectors discarded (kernel 4.18+)
- `node_disk_discard_time_seconds_total` - Total time spent discarding (kernel 4.18+)
- `node_disk_flush_requests_total` - Total flush requests completed (kernel 5.5+)
- `node_disk_flush_requests_time_seconds_total` - Total time spent flushing (kernel 5.5+)

**NOTE**: Virtual devices (ram, zram, loop, fd) and partitions are automatically filtered out.

**Vmstat** (when `--node-metrics-receiver-vmstat=true`):
All fields from `/proc/vmstat` are exposed dynamically as `node_vmstat_{field}`, including:
- `node_vmstat_pgfault` - Page faults
- `node_vmstat_pgmajfault` - Major page faults
- `node_vmstat_pgpgin` - Pages paged in
- `node_vmstat_pgpgout` - Pages paged out
- `node_vmstat_pswpin` - Pages swapped in
- `node_vmstat_pswpout` - Pages swapped out
- ... and 100+ additional vmstat metrics

**NOTE**: The vmstat collector emits all fields from `/proc/vmstat`, which can be 100+ metrics on modern kernels.
To reduce cardinality, either disable the collector entirely (`--node-metrics-receiver-vmstat false`)
or use an include filter that lists all desired metric prefixes across all collectors, e.g.:
`--node-metrics-receiver-include-filter "^node_(vmstat_(oom_kill|pgpg|pswp|pg.*fault)|load|memory_|cpu_)"`

**Netstat** (when `--node-metrics-receiver-netstat=true`):
TCP/IP statistics from `/proc/net/netstat` and `/proc/net/snmp` as `node_netstat_{Protocol}_{Field}`:
- `node_netstat_TcpExt_TCPTimeouts` - TCP timeouts
- `node_netstat_TcpExt_TCPRetransFail` - TCP retransmit failures
- `node_netstat_TcpExt_SyncookiesSent` - SYN cookies sent
- `node_netstat_IpExt_InOctets` - IP octets received
- `node_netstat_IpExt_OutOctets` - IP octets sent
- `node_netstat_Tcp_CurrEstab` - TCP connections currently established (from /proc/net/snmp)
- ... and many more TCP/IP statistics

**NOTE**: Like vmstat, the netstat collector emits every field of both files, which is 200+ metrics — considerably more
than node_exporter, which whitelists a subset by default. On a backend billed per series, either disable the collector
or narrow it with an include filter such as
`--node-metrics-receiver-include-filter "^node_netstat_(Tcp|Udp)_"`.

**Sockstat** (when `--node-metrics-receiver-sockstat=true`):
Socket allocation statistics from `/proc/net/sockstat` and `/proc/net/sockstat6`:
- `node_sockstat_sockets_used` - Total sockets in use
- `node_sockstat_TCP_inuse` - TCP sockets in use
- `node_sockstat_TCP_orphan` - TCP orphaned sockets
- `node_sockstat_TCP_tw` - TCP TIME_WAIT sockets
- `node_sockstat_TCP_alloc` - TCP sockets allocated
- `node_sockstat_TCP_mem` - TCP memory pages
- `node_sockstat_TCP_mem_bytes` - TCP memory in bytes
- `node_sockstat_UDP_inuse` - UDP sockets in use
- `node_sockstat_UDP_mem` - UDP memory pages
- `node_sockstat_UDP_mem_bytes` - UDP memory in bytes
- `node_sockstat_RAW_inuse` - RAW sockets in use
- `node_sockstat_TCP6_inuse` - TCP6 sockets in use (from sockstat6)
- ... and additional IPv6 socket statistics

**Filefd** (when `--node-metrics-receiver-filefd=true`):
- `node_filefd_allocated` - Number of allocated file descriptors (unit `{file_descriptors}`)
- `node_filefd_maximum` - Maximum number of file descriptors allowed (unit `{file_descriptors}`)

**CPU Frequency** (when `--node-metrics-receiver-cpufreq=true`, read from
`/sys/devices/system/cpu/cpu*/cpufreq/`, per CPU with `cpu` label):
- `node_cpu_scaling_frequency_hertz` - Current CPU scaling frequency
- `node_cpu_scaling_frequency_min_hertz` - Minimum CPU scaling frequency
- `node_cpu_scaling_frequency_max_hertz` - Maximum CPU scaling frequency
- `node_cpu_frequency_hertz` - Current CPU frequency (from cpuinfo)
- `node_cpu_frequency_min_hertz` - Minimum CPU frequency (from cpuinfo)
- `node_cpu_frequency_max_hertz` - Maximum CPU frequency (from cpuinfo)

**NOTE**: Only directories named `cpu` followed by digits (`cpu0`, `cpu1`, ...) are scanned, so entries such as
`cpuidle` and `cpufreq` under the same parent are ignored.

**Thermal Zone** (when `--node-metrics-receiver-thermal-zone=true`):
- `node_thermal_zone_temp` - Thermal zone temperature in Celsius (with `zone` and `type` labels)
- `node_cooling_device_cur_state` - Current cooling device state (with `name` and `type` labels)
- `node_cooling_device_max_state` - Maximum cooling device state (with `name` and `type` labels)

**NVMe** (when `--node-metrics-receiver-nvme=true`):
- `node_nvme_info` - NVMe device info gauge (with `device`, `firmware_revision`, `model`, `serial`, `state` labels)

**NOTE**: Only entries named `nvme` followed by digits (`nvme0`, `nvme1`, ...) are scanned, so any other entry under
`/sys/class/nvme` is ignored.

**Hwmon** (when `--node-metrics-receiver-hwmon=true`, sensor readings carry `chip` and `sensor` labels):
- `node_hwmon_temp_celsius` - Temperature sensor reading
- `node_hwmon_temp_max_celsius` - Temperature max threshold
- `node_hwmon_temp_crit_celsius` - Temperature critical threshold
- `node_hwmon_in_volts` - Voltage sensor reading
- `node_hwmon_fan_rpm` - Fan speed, in revolutions per minute (unit `{rev}/min`)
- `node_hwmon_power_watts` - Power consumption in watts
- `node_hwmon_curr_amps` - Current in amps (converted from milliamps)
- `node_hwmon_chip_names` - Annotation gauge carrying the human-readable chip model (with `chip` and `chip_name` labels)
- `node_hwmon_sensor_label` - Annotation gauge carrying the human-readable sensor label, when the chip provides one (with `chip`, `sensor` and `label` labels)

**NOTE**: The `sensor` label is the raw sysfs name (`temp1`, `in0`), matching Prometheus node_exporter — it is unique
within a chip, so two sensors can never collapse onto one series, and it does not move when a driver gains or changes a
label file. Where the chip provides human-readable text, that is published separately by `node_hwmon_sensor_label` and
can be joined on `chip` and `sensor`.

**NOTE**: The `chip` label is derived from the backing device of the hwmon instance, so two chips reporting the same
model name (for example two NVMe drives, both named `nvme`) do not collapse onto the same label set. The model name
itself is published separately by `node_hwmon_chip_names`, which can be joined on `chip`. When an hwmon instance has no
backing `device`, the identity falls back to the chip model name combined with the `hwmonN` directory, which is still
unique within a host but is not stable across reboots, since the hwmon index depends on module load order.

**NOTE**: The `sensor` label uses the human-readable name from the sysfs `*_label` file when available
(e.g., "Core 0", "Package id 0"), falling back to the raw sensor name (e.g., "temp1").

**NOTE**: Only the temperature, voltage, fan, power and current sensor kinds are read. Any other kind exposed by a chip
is skipped with a debug log.

**Textfile** (when `--node-metrics-receiver-textfile=true`):
Custom metrics from Prometheus-format text files. `--node-metrics-receiver-textfile-directory` accepts either a
directory, in which case every `*.prom` file in it is read in sorted filename order, or the path of a single `.prom`
file. The directory scan is not recursive, so subdirectories are never descended into, and an entry that is itself a
directory named `something.prom` is skipped silently. Symlinks are followed, so a symlinked `.prom` file is collected
like any other.
The `--node-metrics-receiver-include-filter` and `--node-metrics-receiver-exclude-filter` patterns apply to textfile
metrics just as they do to the built-in ones. Accepted input:
- Metric lines: `metric_name{label="value"} 42.5`
- TYPE comments: `# TYPE metric_name counter|gauge` (used to set the metric type)
- HELP comments: `# HELP metric_name Description` (propagated as the OTLP metric description)

Support for the exposition format is deliberately partial:
- Only the `counter` and `gauge` types are supported. `histogram`, `summary` and `untyped` are treated as gauges, as is
  any metric with no `# TYPE` line.
- Trailing per-sample timestamps are accepted but ignored; every data point is stamped with the scrape time.
- `# TYPE` and `# HELP` are scoped to the file they appear in, so the same metric name can be declared differently in
  two files. Conflicting `# TYPE` declarations produce two separate OTLP metrics sharing that name, one gauge and one
  sum, because metrics are grouped by name *and* type. Within each of those groups the unit and description of the first
  sample seen win.
- A textfile larger than 10 MiB is skipped, guarding against a runaway writer script, and its
  `node_textfile_scrape_error` is set to 1. The limit is enforced while reading, not from the file's reported size, so a
  file that grows between the two cannot slip past it. Its `node_textfile_mtime_seconds` is still emitted even though the
  file itself is skipped, so a writer that has stalled stays observable.
- At most 100,000 samples are taken from the textfile directory per scrape. Beyond that the remainder of the file is
  dropped, a warning names how many samples were kept, and no further files are read — a bound on memory that the
  per-file size limit alone does not give, since a small file can expand into a great many samples.
- Files that are not regular files are skipped silently: a FIFO named `*.prom` would otherwise block the scrape
  indefinitely when opened. A symlink to a regular file is followed and collected; a `.prom` entry that cannot be read
  at all, such as a dangling symlink, sets `node_textfile_scrape_error` to 1.
- A line with a malformed label — an invalid or empty label name, or a value that is not `key="value"` — is rejected in
  full and counted as a malformed line, rather than being emitted without that label. Dropping a label would change the
  series identity and silently merge samples that are meant to be distinct. A label name repeated within one line keeps
  its first value.

Additionally emits:
- `node_textfile_scrape_error` - Per-file error indicator (0 = success, 1 = the file could not be read, exceeded the
  10 MiB size limit, or contained a malformed line; with `filename` label). A textfile directory that is missing or
  cannot be read yields a single error point whose `filename` label holds the configured path rather than a basename.
- `node_textfile_mtime_seconds` - Modification time of the file in seconds since epoch (with `filename` label), useful for spotting a writer script that has stopped updating

Example textfile (`/var/lib/node_exporter/textfile/custom.prom`):
```
# HELP my_app_version Application version info
# TYPE my_app_version gauge
my_app_version{version="1.2.3"} 1
# TYPE my_app_requests_total counter
my_app_requests_total{endpoint="/api"} 12345
```

#### Example Usage

Basic usage collecting all metrics and exporting to OTLP:

```shell
rotel start \
  --receiver node_metrics \
  --node-metrics-receiver-scrape-interval 30s \
  --exporter otlp \
  --otlp-exporter-endpoint "localhost:4317"
```

Collecting only CPU and memory metrics:

```shell
rotel start \
  --receiver node_metrics \
  --node-metrics-receiver-loadavg false \
  --node-metrics-receiver-network false \
  --node-metrics-receiver-filesystem false \
  --node-metrics-receiver-uname false \
  --node-metrics-receiver-stat false \
  --node-metrics-receiver-processes false \
  --node-metrics-receiver-diskstats false \
  --node-metrics-receiver-vmstat false \
  --node-metrics-receiver-netstat false \
  --node-metrics-receiver-sockstat false \
  --node-metrics-receiver-filefd false \
  --node-metrics-receiver-cpufreq false \
  --node-metrics-receiver-thermal-zone false \
  --node-metrics-receiver-nvme false \
  --node-metrics-receiver-hwmon false \
  --node-metrics-receiver-time false \
  --exporter otlp \
  --otlp-exporter-endpoint "localhost:4317"
```

Using filters to only collect memory-related metrics:

```shell
rotel start \
  --receiver node_metrics \
  --node-metrics-receiver-include-filter "^node_memory_" \
  --exporter otlp \
  --otlp-exporter-endpoint "localhost:4317"
```

Monitoring the host system from inside a container. Mount the host root, and point the receiver at it with a single
rootfs path:

```shell
# docker run -v /:/host:ro,rslave ...
rotel start \
  --receiver node_metrics \
  --node-metrics-receiver-rootfs-path /host \
  --exporter otlp \
  --otlp-exporter-endpoint "localhost:4317"
```

**NOTE**: mounting only `/proc` and `/sys` is enough for every collector *except* `filesystem`. Filesystem metrics need
the host root as well, because the mount table names host paths that `statfs` has to be able to resolve — the rootfs
path is what makes that work. Without it the receiver would report the container's own mounts labelled as though they
were the host's, so prefer the form above over setting `--node-metrics-receiver-procfs-path` and
`--node-metrics-receiver-sysfs-path` individually. The mount table is read from PID 1 (`/proc/1/mounts`) rather than
`/proc/self/mounts` for the same reason: the latter is always the reading process's own mount namespace, whatever
procfs it is read from.

**NOTE**: The same reasoning applies to a hardened systemd unit on the host. With `PrivateMounts=`, `PrivateTmp=` or
`ProtectSystem=`, rotel's mount namespace differs from PID 1's: the mount table names the host's paths while `statfs`
resolves inside the unit's namespace, so a metric labelled `mountpoint="/tmp"` would describe the unit's private tmpfs.
Prometheus `node_exporter` behaves the same way. Run the receiver without mount-namespace isolation if filesystem
metrics matter.

Using the textfile collector for custom Prometheus metrics:

```shell
rotel start \
  --receiver node_metrics \
  --node-metrics-receiver-textfile true \
  --node-metrics-receiver-textfile-directory /var/lib/node_exporter/textfile \
  --exporter otlp \
  --otlp-exporter-endpoint "localhost:4317"
```

#### Testing the Node Metrics Receiver

To run integration tests that read the real `/proc` and `/sys` of a Linux host:

```shell
NODE_METRICS_INTEGRATION_TESTS=true cargo test --test node_metrics_integration_tests --features node_metrics_receiver
```

The tests are skipped unless `NODE_METRICS_INTEGRATION_TESTS=true` is set and the `node_metrics_receiver` feature is
enabled. Collectors for optional hardware (thermal zones, hwmon sensors, NVMe devices, cpufreq, diskstats) are absent in
many containers and virtual machines, so the tests for those accept an empty result and require only that any value
produced is finite — asserting a particular piece of hardware is present would make them flaky by design.

### Kmsg Receiver configuration (Linux-only)

**NOTE**: The Kmsg Receiver is Linux-only and must be enabled with the feature flag `--features kmsg_receiver`.

The Kmsg Receiver reads kernel log messages from `/dev/kmsg` and converts them to OpenTelemetry logs. This receiver is
ideal for IoT devices, embedded systems, and any Linux environment where kernel-level logging is important.

To enable the Kmsg receiver, specify it with `--receiver kmsg`.

| Option                                            | Default                            | Description                                                                                               |
|---------------------------------------------------|------------------------------------|-----------------------------------------------------------------------------------------------------------|
| --kmsg-receiver-priority-level                    | 6                                  | Maximum priority level to include (0-7, lower = more severe). Messages with priority <= this are included |
| --kmsg-receiver-read-existing                     | false                              | Read existing messages from the kernel ring buffer on startup                                             |
| --kmsg-receiver-batch-size                        | 100                                | Maximum number of log records to batch before sending                                                     |
| --kmsg-receiver-batch-timeout-ms                  | 250                                | Maximum time to wait before flushing a batch (milliseconds)                                               |
| --kmsg-receiver-offsets-path                      | /var/lib/rotel/kmsg_offsets.json   | Path to persist read offset for resume across restarts                                                    |
| --kmsg-receiver-no-offsets-persistence            | false                              | Disable offset persistence (no resume across restarts)                                                    |
| --kmsg-receiver-offsets-checkpoint-interval-ms    | 5000                               | How often to checkpoint the current offset to disk (milliseconds, minimum 100)                            |

**Note:** When offset persistence is enabled, the receiver implements **at-least-once delivery** semantics. Offsets
are only persisted after messages have been successfully exported by downstream exporters, ensuring no messages are
lost on crash or restart. This may result in some messages being reprocessed after a crash (duplicates are possible,
but data loss is not).

When a valid checkpoint exists with a matching boot ID, the receiver automatically reads from the beginning of the
ring buffer and skips already-processed messages to resume where it left off. This effectively overrides
`--kmsg-receiver-read-existing` on subsequent restarts. After a system reboot, the checkpoint is ignored since
kernel message sequences reset.

#### Priority Levels

The kmsg receiver uses standard syslog priority levels (lower number = more severe):

| Level | Name      | Description                          |
|-------|-----------|--------------------------------------|
| 0     | Emergency | System is unusable                   |
| 1     | Alert     | Action must be taken immediately     |
| 2     | Critical  | Critical conditions                  |
| 3     | Error     | Error conditions                     |
| 4     | Warning   | Warning conditions                   |
| 5     | Notice    | Normal but significant condition     |
| 6     | Info      | Informational (default filter level) |
| 7     | Debug     | Debug-level messages                 |

The default level of 6 (Info) excludes debug messages, which is typically appropriate for production. Set to 7 to include all messages.

#### Example Usage

Basic example reading kernel logs and exporting to OTLP:

```shell
rotel start \
  --receiver kmsg \
  --kmsg-receiver-priority-level 4 \
  --exporter otlp \
  --otlp-exporter-endpoint "localhost:4317"
```

Reading all messages including existing buffer:

```shell
rotel start \
  --receiver kmsg \
  --kmsg-receiver-read-existing \
  --kmsg-receiver-priority-level 7 \
  --exporter clickhouse \
  --clickhouse-exporter-endpoint "http://localhost:8123"
```

Custom offset persistence path and checkpoint interval (useful for embedded devices with specific storage requirements):

```shell
rotel start \
  --receiver kmsg \
  --kmsg-receiver-offsets-path /data/rotel/kmsg_state.json \
  --kmsg-receiver-offsets-checkpoint-interval-ms 10000 \
  --exporter otlp \
  --otlp-exporter-endpoint "localhost:4317"
```

Disable offset persistence (always start fresh, no resume):

```shell
rotel start \
  --receiver kmsg \
  --kmsg-receiver-no-offsets-persistence \
  --exporter otlp \
  --otlp-exporter-endpoint "localhost:4317"
```

#### Testing the Kmsg Receiver

To run integration tests that verify actual `/dev/kmsg` functionality:

```shell
# On Linux with appropriate permissions (root or CAP_SYSLOG)
cargo test --test kmsg_integration_tests --features "integration-tests,kmsg_receiver"

# In Docker (works on Linux, macOS, and Windows with Docker Desktop)
./scripts/kmsg-test-env.sh build   # One-time setup
./scripts/kmsg-test-env.sh test    # Run tests
./scripts/kmsg-test-env.sh help    # See all commands
```

Note: On macOS/Windows, Docker Desktop runs containers in a Linux VM, so the tests will
read kernel messages from that VM's kernel (not your host OS). This is sufficient for
verifying the receiver works correctly.

#### Log Record Format

Each kernel message is converted to an OTLP LogRecord with:

- **Body**: The kernel log message content
- **Timestamp**: Converted from kernel timestamp (microseconds since boot) to absolute time
- **Severity**: Mapped from syslog priority to OpenTelemetry severity levels
- **Attributes**:
  - `kmsg.priority`: Priority level (0-7)
  - `kmsg.priority_name`: Human-readable priority name (e.g., "INFO", "ERROR")
  - `kmsg.facility`: Syslog facility code (0-23)
  - `kmsg.facility_name`: Human-readable facility name (e.g., "kern", "user", "daemon")
  - `kmsg.sequence`: Kernel message sequence number
  - `kmsg.continuation`: Boolean flag if this is a continuation message

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

Requests will be retried if they match retryable error codes like 429 (Too Many Requests) or timeout. You can control
the retry behavior globally for all exporters with the following options:

- `--exporter-retry-initial-backoff`: Initial backoff duration (default: 5s)
- `--exporter-retry-max-backoff`: Maximum backoff interval (default: 30s)
- `--exporter-retry-max-elapsed-time`: Maximum wall time a request will be retried for until it is marked as
  permanent failure (default: 300s)

These global retry settings apply to all exporters unless overridden by exporter-specific retry options (see individual
exporter configuration sections below).

Each exporter can also override the default request timeout. For example, the OTLP Exporter default timeout of 5 seconds
can be overridden with:

- `--otlp-exporter-request-timeout`: Takes a string time duration, so `"250ms"` for 250 milliseconds, `"3s"` for 3
  seconds, etc.

All time options should be represented as string time durations.

### Internal telemetry

Rotel records a number of internal metrics that can help observe Rotel behavior during runtime. This telemetry is
opt-in and must be enabled with `--enable-internal-telemetry`. Telemetry is sent to the exporters configured
with the `--exporters-internal-metrics` option.

**NOTE**: Internal telemetry is not sent to any outside sources and you are in full control of where this data is
exported to.

### Multiple receivers

Rotel supports configuring multiple receivers to ingest telemetry data from different sources simultaneously. This
allows you
to receive data via OTLP and consume from Kafka topics at the same time.

The following configuration parameters enable multiple receivers:

| Option      | Default | Options                                                  |
|-------------|---------|----------------------------------------------------------|
| --receiver  | otlp    | otlp, kafka, fluent, file, kmsg, node_metrics            |
| --receivers |         | comma-separated list (otlp,kafka,file,kmsg,node_metrics) |

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

The following additional configuration parameters set up support for multiple
exporters. Similar to the options above, all CLI arguments can be passed as
environment variables as well. It is not possible to set `--exporter` and
`--exporters` at the same time.

| Option                       | Default | Options                          |
|------------------------------|---------|----------------------------------|
| --exporters                  |         | name:type pairs, comma-separated |
| --exporters-traces           |         | exporter name                    |
| --exporters-metrics          |         | exporter name                    |
| --exporters-logs             |         | exporter name                    |
| --exporters-internal-metrics |         | exporter name                    |

First start by defining the set of exporters that you would like to use, optionally specifying a custom name for them
to differentiate their configuration options. For example, to export logs and metrics to two separate ClickHouse nodes
while exporting traces to Datadog, we'll use the following `--exporters` argument (or `ROTEL_EXPORTERS` envvar):

```shell
--exporters logging:clickhouse,stats:clickhouse,datadog
```

The argument form of `--exporters` takes `name:type` pairs separated by commas,
where the first part is a custom name and the second part is the type of
exporter. You can exclude the name if there is a single exporter by that name,
which means the name is the same as the exporter type.

Second, you then must set environment variables of the form
`ROTEL_EXPORTER_{NAME}_{PARAMETER}` to configure the multiple exporters. These
variable names are dynamic and use the custom name to differentiate settings for
similar exporter types. Therefore, there are no CLI argument alternatives for
them at the moment. The `{PARAMETER}` fields match the configuration options for
the given exporter type.

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

You can send telemetry to multiple exporters by listing multiple comma-separated in the exporters configuration.
Telemetry
is sent sequentially to the sending queues for each exporter in-order. That means if one exporter is generating back
pressure
it may impact the other exporters.

For example, to send logs to both the stats and logging clickhouse exporters,
you would instead set the `ROTEL_EXPORTERS_LOGS` environment variable to:

- `ROTEL_EXPORTERS_LOGS=stats,logging`

### AWS Authentication

For exporters that rely on AWS authentication, Rotel supports several methods of configuring AWS credentials.
Rotel relies on the standard AWS SDK credential provider methods for locating the right credentials. See the
AWS [documentation](https://docs.aws.amazon.com/sdkref/latest/guide/creds-config-files.html) for how to
configure and acquire credentials when running Rotel.

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

### Rust Processor SDK

Rotel supports custom processors written in Rust, compiled as dynamic libraries and loaded at runtime. Rust processors
provide native performance with no FFI overhead beyond the initial type conversion, making them ideal for
high-throughput pipelines.

The SDK supports both synchronous and asynchronous processors:

- **Sync processors** (`RotelProcessor`) modify telemetry data in place — best for simple, CPU-bound transformations
- **Async processors** (`AsyncProcessor`) support `async fn` methods with automatic tokio runtime management — best for processors that need I/O such as external API calls or database lookups

Example of a simple sync trace processor:

```rust
use rotel_rust_processor_sdk::prelude::*;

#[derive(Default)]
pub struct MyProcessor;

impl RotelProcessor for MyProcessor {
    fn process_spans(
        &self,
        spans: &mut RResourceSpans,
        _context: &ROption<RRequestContext>,
    ) {
        for scope_spans in spans.scope_spans.iter_mut() {
            for span in scope_spans.spans.iter_mut() {
                span.attributes.push(RKeyValue::string("processed_by", "my_processor"));
            }
        }
    }
}

export_processor!(MyProcessor);
```

Run with Rotel:

```bash
rotel start \
    --rust-trace-processor ./target/release/libmy_processor.dylib \
    --exporter blackhole
```

Multiple processors can be chained and will execute in order. Rotel must be built with `--features rust_processor`
to enable Rust processor support.

For the full guide including async processors, available data types, request context access, and end-to-end examples,
see the [Rust Processor SDK Documentation](rotel_rust_processor_sdk/README.md).

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

On release, Rotel images are published to
[Amazon ECR Public](https://gallery.ecr.aws/rotel-dev/rotel) with the following tags:

- `public.ecr.aws/rotel-dev/rotel:<release name>`
- `public.ecr.aws/rotel-dev/rotel:latest`
- `public.ecr.aws/rotel-dev/rotel:sha-<sha>`

When running an image, map the OTLP receiver ports to their local values with the flag `-p 4317-4318:4317-4318`.

Rotel releases with built-in Python Processor support and Python 3.13 are also available
on [Amazon ECR Public](https://gallery.ecr.aws/rotel-dev/python-processors) with the following tags:

- `public.ecr.aws/rotel-dev/python-processors:<release name>`
- `public.ecr.aws/rotel-dev/python-processors:latest`
- `public.ecr.aws/rotel-dev/python-processors:sha-<sha>`

When running an image, you can mount directories from your local filesystem as volumes to provide processor code
to the container with `-v` flag, for example: `-v ~/my_processor_directory:/processors`. You can then start rotel and
pass in processors like the example below.

```
docker run -ti -p 4317-4318:4317-4318  -v ~/my_processor_director:/processors public.ecr.aws/rotel-dev/python-processors:latest
--exporter blackhole --debug-log traces --debug-log-verbosity detailed --otlp-with-trace-processor /processors/my_processor.py`
```

## Community

Want to chat about this project, share feedback, or suggest improvements? Join
our [Discord server](https://discord.gg/reUqNWTSGC)! Whether you're a user of this project or not, we'd love to hear
your thoughts and ideas. See you there! 🚀

## Developing

See the [DEVELOPING.md](DEVELOPING.md) doc for building and development instructions.

## Releasing

For information about creating releases, see [RELEASING.md](RELEASING.md).
