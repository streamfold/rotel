use crate::ddl::{
    build_cluster_string, build_table_name, build_ttl_string, get_json_col_type,
    replace_placeholders,
};
use std::collections::HashMap;
use std::time::Duration;

pub(crate) fn get_traces_ddl(
    cluster: &Option<String>,
    database: &String,
    table_prefix: &String,
    engine: &str,
    ttl: &Duration,
    use_json: bool,
) -> Vec<String> {
    let map_or_json = get_json_col_type(use_json);
    let mut map_indices = "";
    let mut json_setting = "";
    if !use_json {
        map_indices = TRACES_TABLE_MAP_INDICES_SQL;
    } else {
        json_setting = ", allow_experimental_json_type = 1";
    }

    let table_sql = replace_placeholders(
        TRACES_TABLE_SQL,
        &HashMap::from([
            (
                "TABLE",
                build_table_name(database, table_prefix, "traces").as_str(),
            ),
            ("CLUSTER", build_cluster_string(cluster).as_str()),
            ("MAP_OR_JSON", map_or_json),
            ("ENGINE", engine),
            ("MAP_INDICES", map_indices),
            (
                "TTL_EXPR",
                build_ttl_string(ttl, "Timestamp").as_str(),
            ),
            ("JSON_SETTING", json_setting),
        ]),
    );

    let table_id_ts_sql = replace_placeholders(
        TRACES_TABLE_ID_TS_SQL,
        &HashMap::from([
            (
                "TABLE",
                build_table_name(database, table_prefix, "traces_trace_id_ts").as_str(),
            ),
            ("CLUSTER", build_cluster_string(cluster).as_str()),
            ("ENGINE", engine),
            (
                "TTL_EXPR",
                build_ttl_string(ttl, "Start").as_str(),
            ),
        ]),
    );

    let table_id_ts_mv_sql = replace_placeholders(
        TRACES_TABLE_ID_TS_MV_SQL,
        &HashMap::from([
            (
                "TABLE",
                build_table_name(database, table_prefix, "traces_trace_id_ts_mv").as_str(),
            ),
            ("CLUSTER", build_cluster_string(cluster).as_str()),
            (
                "TABLE_ID_TS",
                build_table_name(database, table_prefix, "traces_trace_id_ts").as_str(),
            ),
            (
                "TABLE_TRACES",
                build_table_name(database, table_prefix, "traces").as_str(),
            ),
        ]),
    );

    vec![table_sql, table_id_ts_sql, table_id_ts_mv_sql]
}

const TRACES_TABLE_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS %%TABLE%% %%CLUSTER%% (
	Timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),
	TraceId String CODEC(ZSTD(1)),
	SpanId String CODEC(ZSTD(1)),
	ParentSpanId String CODEC(ZSTD(1)),
	TraceState String CODEC(ZSTD(1)),
	SpanName LowCardinality(String) CODEC(ZSTD(1)),
	SpanKind LowCardinality(String) CODEC(ZSTD(1)),
	ServiceName LowCardinality(String) CODEC(ZSTD(1)),
	ResourceAttributes %%MAP_OR_JSON%% CODEC(ZSTD(1)),
	ScopeName String CODEC(ZSTD(1)),
	ScopeVersion String CODEC(ZSTD(1)),
	SpanAttributes %%MAP_OR_JSON%% CODEC(ZSTD(1)),
	Duration UInt64 CODEC(ZSTD(1)),
	StatusCode LowCardinality(String) CODEC(ZSTD(1)),
	StatusMessage String CODEC(ZSTD(1)),
	Events Nested (
		Timestamp DateTime64(9),
		Name LowCardinality(String),
		Attributes %%MAP_OR_JSON%%
	) CODEC(ZSTD(1)),
	Links Nested (
		TraceId String,
		SpanId String,
		TraceState String,
		Attributes %%MAP_OR_JSON%%
	) CODEC(ZSTD(1)),
	INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,

    %%MAP_INDICES%%

	INDEX idx_duration Duration TYPE minmax GRANULARITY 1
) ENGINE = %%ENGINE%%
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SpanName, Timestamp)
%%TTL_EXPR%%
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1 %%JSON_SETTING%%
;
"#;

const TRACES_TABLE_MAP_INDICES_SQL: &str = r#"
	INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_span_attr_key mapKeys(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_span_attr_value mapValues(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
"#;

const TRACES_TABLE_ID_TS_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS %%TABLE%% %%CLUSTER%% (
     TraceId String CODEC(ZSTD(1)),
     Start DateTime64(9) CODEC(Delta, ZSTD(1)),
     End DateTime64(9) CODEC(Delta, ZSTD(1)),
     INDEX idx_trace_id TraceId TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = %%ENGINE%%
PARTITION BY toDate(Start)
ORDER BY (TraceId, Start)
%%TTL_EXPR%%
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
"#;

const TRACES_TABLE_ID_TS_MV_SQL: &str = r#"
CREATE MATERIALIZED VIEW IF NOT EXISTS %%TABLE%% %%CLUSTER%%
TO %%TABLE_ID_TS%%
(
	TraceId String,
	Start DateTime64(9),
	End DateTime64(9)
)
AS SELECT
	TraceId,
	min(Timestamp) as Start,
	max(Timestamp) as End
FROM
%%TABLE_TRACES%%
WHERE TraceId != ''
GROUP BY TraceId;
"#;
