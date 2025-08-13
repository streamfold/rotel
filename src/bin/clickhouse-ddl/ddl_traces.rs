use crate::Engine;
use crate::ddl::{
    build_cluster_string, build_table_name, build_ttl_string, get_json_col_type, get_order_by,
    get_partition_by, get_settings, replace_placeholders,
};
use std::collections::HashMap;
use std::time::Duration;

pub(crate) fn get_traces_ddl(
    cluster: &Option<String>,
    database: &String,
    table_prefix: &String,
    engine: Engine,
    ttl: &Duration,
    use_json: bool,
) -> Vec<String> {
    let map_or_json = get_json_col_type(use_json);

    let map_indices = if !use_json && engine != Engine::Null {
        TRACES_TABLE_MAP_INDICES_SQL
    } else {
        ""
    };

    let indices = if engine != Engine::Null {
        TRACES_TABLE_INDICES_SQL
    } else {
        ""
    };

    let settings_str = get_settings(use_json, engine);

    let table_sql = replace_placeholders(
        TRACES_TABLE_SQL,
        &HashMap::from([
            (
                "TABLE",
                build_table_name(database, table_prefix, "traces").as_str(),
            ),
            ("CLUSTER", &build_cluster_string(cluster)),
            ("MAP_OR_JSON", map_or_json),
            ("ENGINE", &engine.to_string()),
            ("MAP_INDICES", map_indices),
            ("INDICES", indices),
            ("TTL_EXPR", &build_ttl_string(ttl, "toDateTime(Timestamp)")),
            ("SETTINGS", &settings_str),
            (
                "PARTITION_BY",
                &get_partition_by("toDate(Timestamp)", engine),
            ),
            (
                "ORDER_BY",
                &get_order_by("(ServiceName, SpanName, toDateTime(Timestamp))", engine),
            ),
        ]),
    );

    let table_id_ts_sql = replace_placeholders(
        TRACES_TABLE_ID_TS_SQL,
        &HashMap::from([
            (
                "TABLE",
                build_table_name(database, table_prefix, "traces_trace_id_ts").as_str(),
            ),
            ("CLUSTER", &build_cluster_string(cluster)),
            ("ENGINE", &engine.to_string()),
            ("TTL_EXPR", &build_ttl_string(ttl, "toDateTime(Start)")),
            ("SETTINGS", &settings_str),
            ("PARTITION_BY", &get_partition_by("toDate(Start)", engine)),
            ("ORDER_BY", &get_order_by("(TraceId, Start)", engine)),
        ]),
    );

    let table_id_ts_mv_sql = replace_placeholders(
        TRACES_TABLE_ID_TS_MV_SQL,
        &HashMap::from([
            (
                "TABLE",
                build_table_name(database, table_prefix, "traces_trace_id_ts_mv").as_str(),
            ),
            ("CLUSTER", &build_cluster_string(cluster)),
            (
                "TABLE_ID_TS",
                &build_table_name(database, table_prefix, "traces_trace_id_ts"),
            ),
            (
                "TABLE_TRACES",
                &build_table_name(database, table_prefix, "traces"),
            ),
        ]),
    );

    match engine {
        Engine::Null => vec![table_sql],
        _ => vec![table_sql, table_id_ts_sql, table_id_ts_mv_sql],
    }
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

    %%MAP_INDICES%%

    %%INDICES%%
) ENGINE = %%ENGINE%%
%%PARTITION_BY%%
%%ORDER_BY%%
%%TTL_EXPR%%
%%SETTINGS%%
;
"#;

const TRACES_TABLE_INDICES_SQL: &str = r#"
	INDEX idx_duration Duration TYPE minmax GRANULARITY 1,
	INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
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
     Start DateTime CODEC(Delta, ZSTD(1)),
     End DateTime CODEC(Delta, ZSTD(1)),
     INDEX idx_trace_id TraceId TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = %%ENGINE%%
%%PARTITION_BY%%
%%ORDER_BY%%
%%TTL_EXPR%%
%%SETTINGS%%
;
"#;

const TRACES_TABLE_ID_TS_MV_SQL: &str = r#"
CREATE MATERIALIZED VIEW IF NOT EXISTS %%TABLE%% %%CLUSTER%%
TO %%TABLE_ID_TS%%
AS SELECT
	TraceId,
	min(Timestamp) as Start,
	max(Timestamp) as End
FROM
%%TABLE_TRACES%%
WHERE TraceId != ''
GROUP BY TraceId;
"#;
