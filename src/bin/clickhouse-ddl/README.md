# clickhouse-ddl

Use this CLI tool to generate and execute the Clickhouse DDL statements necessary for OpenTelemetry data.

## Usage

Examples:

```shell
# generate DDL for logs
docker run -ti streamfold/rotel-clickhouse-ddl create --endpoint https://abcd1234.us-east-1.aws.clickhouse.cloud:8443 --logs

# generate DDL for traces, use the JSON type
docker run -ti streamfold/rotel-clickhouse-ddl create --endpoint https://abcd1234.us-east-1.aws.clickhouse.cloud:8443 --traces --enable-json

# generate DDL for traces with GenAI materialized fields and multi-tenant partitioning
docker run -ti streamfold/rotel-clickhouse-ddl create --endpoint https://abcd1234.us-east-1.aws.clickhouse.cloud:8443 \
  --traces --materialize-genai-fields --partition-by-service-name
```

If you are executing against a localhost Clickhouse server, add `--network="host"`:

```shell
docker run -ti --network="host" streamfold/rotel-clickhouse-ddl create --endpoint http://localhost:8123 --logs

# For GenAI/LLM telemetry with optimized schema
docker run -ti --network="host" streamfold/rotel-clickhouse-ddl create --endpoint http://localhost:8123 \
  --traces --materialize-genai-fields --partition-by-service-name
```

Full usage:

```shell
Usage: clickhouse-ddl create [OPTIONS] --endpoint <ENDPOINT>

Options:
      --endpoint <ENDPOINT>                Clickhouse endpoint
      --database <DATABASE>                Database [default: otel]
      --table-prefix <TABLE_PREFIX>        Table prefix [default: otel]
      --user <USER>                        User
      --password <PASSWORD>                Password
      --engine <ENGINE>                    DB engine [default: MergeTree]
      --cluster <CLUSTER>                  Cluster name
      --traces                             Create trace spans tables
      --logs                               Create logs tables
      --metrics                            Create metrics tables
      --ttl <TTL>                          TTL [default: 0s]
      --enable-json                        Enable JSON column type
      --materialize-genai-fields           Materialize GenAI fields from SpanAttributes (for traces)
      --partition-by-service-name          Partition by ServiceName for multi-tenant optimization (for traces)
  -h, --help                               Print help
```

## GenAI/LLM Telemetry

For GenAI/LLM observability workloads, use the `--materialize-genai-fields` flag to create MATERIALIZED columns
that extract GenAI semantic convention attributes from `SpanAttributes`. This enables efficient querying of:

- `GenAIOperationName` - The operation type (e.g., "chat", "embeddings")
- `GenAIProviderName` - The LLM provider (e.g., "openai", "anthropic")
- `GenAIRequestModel` / `GenAIResponseModel` - Model names
- `GenAIInputTokens` / `GenAIOutputTokens` - Token usage
- `GenAILastInputMessage` / `GenAILastOutputMessage` - Last messages in conversation
- And more...

The `--partition-by-service-name` flag optimizes multi-tenant deployments by partitioning data by service name,
enabling efficient per-tenant queries.
