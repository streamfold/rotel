# clickhouse-ddl

Use this CLI tool to generate and execute the Clickhouse DDL statements necessary for OpenTelemetry data.

## Usage

Examples:

```shell
# generate DDL for logs
docker run -ti streamfold/rotel-clickhouse-ddl create --endpoint https://abcd1234.us-east-1.aws.clickhouse.cloud:8443 --logs

# generate DDL for traces, use the JSON type
docker run -ti streamfold/rotel-clickhouse-ddl create --endpoint https://abcd1234.us-east-1.aws.clickhouse.cloud:8443 --traces --enable-json
```

If you are executing against a localhost Clickhouse server, add `--network="host"`:

```shell
docker run -ti --network="host" streamfold/rotel-clickhouse-ddl create --endpoint http://localhost:8123 --logs
```

Full usage:

```shell
Usage: clickhouse-ddl create [OPTIONS] --endpoint <ENDPOINT>

Options:
      --endpoint <ENDPOINT>          Clickhouse endpoint
      --database <DATABASE>          Database [default: otel]
      --table-prefix <TABLE_PREFIX>  Table prefix [default: otel]
      --user <USER>                  User
      --password <PASSWORD>          Password
      --engine <ENGINE>              DB engine [default: MergeTree]
      --cluster <CLUSTER>            Cluster name
      --traces                       Create trace spans tables
      --logs                         Create logs tables
      --metrics                      Create metrics tables
      --ttl <TTL>                    TTL [default: 0s]
      --enable-json                  Enable JSON column type
  -h, --help                         Print help
```
