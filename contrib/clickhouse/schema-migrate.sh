#!/bin/bash

DATABASE=${DATABASE:-"otel"}
TABLE_PREFIX=${TABLE_PREFIX:-"otel_"}
TABLE_ENGINE=${TABLE_ENGINE:-"MergeTree"}

CLUSTER_NAME=${CLUSTER_NAME:-""}

TTL_TODO="TTL toDateTime(Timestamp) + toIntervalDay(3)"

#
# Trace spans
#
echo "# TRACES"
cat <<EOF
CREATE TABLE ${DATABASE}.${TABLE_PREFIX}traces
(
        \`Timestamp\` DateTime64(9) CODEC(Delta(8), ZSTD(1)),
        \`TraceId\` String CODEC(ZSTD(1)),
        \`SpanId\` String CODEC(ZSTD(1)),
        \`ParentSpanId\` String CODEC(ZSTD(1)),
        \`TraceState\` String CODEC(ZSTD(1)),
        \`SpanName\` LowCardinality(String) CODEC(ZSTD(1)),
        \`SpanKind\` LowCardinality(String) CODEC(ZSTD(1)),
        \`ServiceName\` LowCardinality(String) CODEC(ZSTD(1)),
        \`ResourceAttributes\` JSON CODEC(ZSTD(1)),
        \`ScopeName\` String CODEC(ZSTD(1)),
        \`ScopeVersion\` String CODEC(ZSTD(1)),
        \`SpanAttributes\` JSON CODEC(ZSTD(1)),
        \`Duration\` Int64 CODEC(ZSTD(1)),
        \`StatusCode\` LowCardinality(String) CODEC(ZSTD(1)),
        \`StatusMessage\` String CODEC(ZSTD(1)),
        \`Events.Timestamp\` Array(DateTime64(9)) CODEC(ZSTD(1)),
        \`Events.Name\` Array(LowCardinality(String)) CODEC(ZSTD(1)),
        \`Events.Attributes\` Array(JSON) CODEC(ZSTD(1)),
        \`Links.TraceId\` Array(String) CODEC(ZSTD(1)),
        \`Links.SpanId\` Array(String) CODEC(ZSTD(1)),
        \`Links.TraceState\` Array(String) CODEC(ZSTD(1)),
        \`Links.Attributes\` Array(JSON) CODEC(ZSTD(1)),
        INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
        INDEX idx_duration Duration TYPE minmax GRANULARITY 1
)
ENGINE = ${TABLE_ENGINE}
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SpanName, toUnixTimestamp(Timestamp), TraceId)
${TTL_TODO}
SETTINGS ttl_only_drop_parts = 1
EOF

echo
echo
echo

#
# Log records
#
echo "# LOGS"
cat <<EOF
CREATE TABLE IF NOT EXISTS ${DATABASE}.${TABLE_PREFIX}logs (
	Timestamp DateTime64(9) CODEC(Delta(8), ZSTD(1)),
	TimestampTime DateTime DEFAULT toDateTime(Timestamp),
	TraceId String CODEC(ZSTD(1)),
	SpanId String CODEC(ZSTD(1)),
	TraceFlags UInt8,
	SeverityText LowCardinality(String) CODEC(ZSTD(1)),
	SeverityNumber UInt8,
	ServiceName LowCardinality(String) CODEC(ZSTD(1)),
	Body String CODEC(ZSTD(1)),
	ResourceSchemaUrl LowCardinality(String) CODEC(ZSTD(1)),
	ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
	ScopeSchemaUrl LowCardinality(String) CODEC(ZSTD(1)),
	ScopeName String CODEC(ZSTD(1)),
	ScopeVersion LowCardinality(String) CODEC(ZSTD(1)),
	ScopeAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
	LogAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),

	INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
	INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_scope_attr_value mapValues(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_log_attr_key mapKeys(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_log_attr_value mapValues(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_body Body TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 8
) ENGINE = ${TABLE_ENGINE}
PARTITION BY toDate(TimestampTime)
PRIMARY KEY (ServiceName, TimestampTime)
ORDER BY (ServiceName, TimestampTime, Timestamp)
${TTL_TODO}
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
EOF