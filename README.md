# rotel 🌶️ 🍅

Rotel is a lightweight OpenTelemetry collector implemented in Rust.

## Description

**Rotel** provides an efficient, high-performance solution for collecting, processing, and exporting telemetry data. Rotel is ideal for resource-constrained environments and applications where minimizing overhead is critical.

**Features:**
- Supports metrics, logs, and traces
- OTLP receiver and exporters with gRPC and HTTP/Protobuf support
- Built-in batching and retry mechanisms
- Experimental Datadog Trace exporter (based on port from Go)

Rotel can be easily bundled with popular runtimes as packages. Its Rust implementation ensures minimal resource usage and a compact binary size, simplifying deployment without the need for a sidecar container.

**Runtime integrations:**
- **Python:** [streamfold/pyrotel](https://github.com/streamfold/pyrotel)
- **Node.js:** [streamfold/rotel-nodejs](https://github.com/streamfold/rotel-nodejs)

Rotel is fully open-sourced and licensed under the Apache 2.0 license. 

_Rotel is currently in early development, and we are actively looking for feedback from the community. It is not recommended for production use at this time._

## Getting Started

To quickly get started with Rotel you can leverage the bundled [Python](https://github.com/streamfold/pyrotel) or
[NodeJS](https://github.com/streamfold/rotel-nodejs) packages. Or if you'd like to use
Rotel as a standalone executable, follow these steps:

1. **Download Binary**
    - Download the latest binary from the rotel [releases](https://github.com/streamfold/rotel/releases) page. If you don't see a build for your platform, let us know!

2. **Installation**
    - Unpack the binary
   ```bash
   tar -xzvf rotel_v{version}_{arch}.tar.gz
   ```

3. **Running Rotel**
    - Execute Rotel with the following arguments. To debug metrics or logs, add
      an additional `--debug-log metrics|logs`.
   ```bash
   ./rotel start  --debug-log traces --exporter blackhole
   ```
   - Rotel is now listening on localhost:4317 (gRPC) and localhost:4318 (HTTP).

4. **Verify**
    - Send OTLP traces to Rotel and verify that it is receiving data: 
   ```bash
   go install github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen@latest
   
   telemetrygen traces --otlp-insecure --duration 5s
   ```
   - Check the output from Rotel and you should see several "Received traces" log lines.

## Configuration

Rotel is configured on the command line with multiple flags. See the table below for the full list of options. Rotel will also output the full argument list:
```shell
rotel start --help
```

All CLI arguments can also be passed as environment variable by prefixing with `ROTEL_` and switching hyphens to underscores. For example, `--log-level info` can also be specified by setting the environment variable `ROTEL_LOG_LEVEL=info`.


| Option                                 | Default              | Options                  |
|----------------------------------------|----------------------|--------------------------|
| --daemon                               |                      |                          |
| --log-level                            | info                 | debug, info, warn, error |
| --log-format                           | text                 | json                     |
| --pid-file                             | /tmp/rotel-agent.pid |                          |
| --log-file                             | /tmp/rotel-agent.log |                          |
| --debug-log                            |                      | metrics, traces, logs    |
| --otlp-grpc-endpoint                   | localhost:4317       |                          |
| --otlp-http-endpoint                   | localhost:4318       |                          |
| --otlp-grpc-max-recv-msg-size-mib      | 4                    |                          |
| --exporter                             | otlp                 | otlp, blackhole          |
| --otlp-receiver-traces-disabled        |                      |                          |
| --otlp-receiver-metrics-disabled       |                      |                          |
| --otlp-receiver-logs-disabled          |                      |                          |
| --otlp-receiver-traces-http-path       | /v1/traces           |                          |
| --otlp-receiver-metrics-http-path      | /v1/metrics          |                          |
| --otlp-receiver-logs-http-path         | /v1/logs             |                          |
| --otlp-exporter-endpoint               |                      |                          |
| --otlp-exporter-protocol               | grpc                 | grpc, http               |
| --otlp-exporter-custom-headers         |                      |                          |
| --otlp-exporter-compression            | gzip                 | gzip, none               |
| --otlp-exporter-tls-cert-file          |                      |                          |
| --otlp-exporter-tls-cert-pem           |                      |                          |
| --otlp-exporter-tls-key-file           |                      |                          |
| --otlp-exporter-tls-key-pem            |                      |                          |
| --otlp-exporter-tls-ca-file            |                      |                          |
| --otlp-exporter-tls-ca-pem             |                      |                          |
| --otlp-exporter-tls-skip-verify        |                      |                          |
| --otlp-exporter-request-timeout        | 5s                   |                          |
| --otlp-exporter-retry-initial-backoff  | 5s                   |                          |
| --otlp-exporter-retry-max-backoff      | 30s                  |                          |
| --otlp-exporter-retry-max-elapsed-time | 300s                 |                          |
| --otlp-exporter-batch-max-size         | 8192                 |                          |
| --otlp-exporter-batch-timeout          | 200ms                |                          |


**Notes**:

* The PID and LOG files are only used when run in `--daemon` mode.
* Any of the options that start with `--otlp-exporter*` can be set per telemetry type: metrics, traces or logs. For example, to set a custom endpoint to export traces to, set: `--otlp-exporter-traces-endpoint`. For other telemetry types their value falls back to the top-level OTLP exporter config.
* Any option above that does not contain a default is considered false or unset by default.

### Retries and timeouts

You can override the default request timeout of 5 seconds for the OTLP Exporter with the exporter setting:

* `--otlp-exporter-request-timeout`: Takes a string time duration, so `"250ms"` for 250 milliseconds, `"3s"` for 3 seconds, etc.

Requests will be retried if they match retryable error codes like 429 (Too Many Requests) or timeout. You can control the behavior with the following exporter options:

* `--otlp-exporter-retry-initial-backoff`: Initial backoff duration
* `--otlp-exporter-retry-max-backoff`: Maximum backoff interval
* `--otlp-exporter-retry-max-elapsed-time`: Maximum wall time a request will be retried for until it is marked as permanent failure

All options should be represented as string time durations.

### Full example

The following example demonstrates how to send OTLP data to [Axiom](https://axiom.co/). Set your Axiom API key in the envvar `AXIOM_API_KEY` and the dataset in `AXIOM_DATASET`:

```shell
ROTEL_OTLP_EXPORTER_CUSTOM_HEADERS="Authorization=Bearer ${AXIOM_API_KEY},X-Axiom-Dataset=${AXIOM_DATASET}" \
 ./rotel start --otlp-exporter-endpoint https://api.axiom.co --otlp-exporter-protocol http

```

In another window run the telemetry generator again:
```shell
telemetrygen traces --otlp-insecure --duration 1s
```
You should see demo trace data show up in Axiom.

## Debugging

If you set the option `--otlp-debug-log` to `["traces"]`, or the environment variable `ROTEL_DEBUG_LOG=traces`, then rotel will log a summary to the log file `/tmp/rotel-agent.log` each time it processes trace spans. You can add also specify *metrics* to debug metrics and *logs* to debug logs.   

## Community

Want to chat about this project, share feedback, or suggest improvements? Join our [Discord server](https://discord.gg/reUqNWTSGC)! Whether you're a user of this project or not, we'd love to hear your thoughts and ideas. See you there! 🚀

## Developing

See the [DEVELOPING.md](DEVELOPING.md) doc for building and development instructions.
