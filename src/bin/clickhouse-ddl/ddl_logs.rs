use crate::Engine;
use crate::ddl::{
    build_cluster_string, build_table_name, build_ttl_string, get_json_col_type, get_order_by,
    get_partition_by, get_primary_key, get_settings, replace_placeholders,
};
use std::collections::HashMap;
use std::time::Duration;

pub(crate) fn get_logs_ddl(
    cluster: &Option<String>,
    database: &String,
    table_prefix: &String,
    engine: Engine,
    ttl: &Duration,
    use_json: bool,
) -> Vec<String> {
    let map_or_json = get_json_col_type(use_json);

    let map_indices = if !use_json && engine != Engine::Null {
        LOGS_TABLE_MAP_INDICES_SQL
    } else {
        ""
    };

    let indices = if engine != Engine::Null {
        LOGS_TABLE_INDICES_SQL
    } else {
        ""
    };

    let settings_str = get_settings(use_json, engine);

    let table_sql = replace_placeholders(
        LOGS_TABLE_SQL,
        &HashMap::from([
            (
                "TABLE",
                build_table_name(database, table_prefix, "logs").as_str(),
            ),
            ("CLUSTER", &build_cluster_string(cluster)),
            ("MAP_OR_JSON", map_or_json),
            ("ENGINE", &engine.to_string()),
            ("MAP_INDICES", map_indices),
            ("INDICES", indices),
            (
                "PARTITION_BY",
                &get_partition_by("toDate(TimestampTime)", engine),
            ),
            (
                "ORDER_BY",
                &get_order_by("(ServiceName, TimestampTime, Timestamp)", engine),
            ),
            (
                "PRIMARY_KEY",
                &get_primary_key("(ServiceName, TimestampTime)", engine),
            ),
            ("TTL_EXPR", &build_ttl_string(ttl, "TimestampTime")),
            ("SETTINGS", &settings_str),
        ]),
    );

    vec![table_sql]
}

const LOGS_TABLE_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS %%TABLE%% %%CLUSTER%% (
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
	ResourceAttributes %%MAP_OR_JSON%% CODEC(ZSTD(1)),
	ScopeSchemaUrl LowCardinality(String) CODEC(ZSTD(1)),
	ScopeName String CODEC(ZSTD(1)),
	ScopeVersion LowCardinality(String) CODEC(ZSTD(1)),
	ScopeAttributes %%MAP_OR_JSON%% CODEC(ZSTD(1)),
	LogAttributes %%MAP_OR_JSON%% CODEC(ZSTD(1)),

    %%MAP_INDICES%%

    %%INDICES%%
) ENGINE = %%ENGINE%%
%%PARTITION_BY%%
%%PRIMARY_KEY%%
%%ORDER_BY%%
%%TTL_EXPR%%
%%SETTINGS%%
;
"#;

const LOGS_TABLE_INDICES_SQL: &str = r#"
	INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
	INDEX idx_body Body TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 8,
"#;

const LOGS_TABLE_MAP_INDICES_SQL: &str = r#"
	INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_scope_attr_value mapValues(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_log_attr_key mapKeys(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_log_attr_value mapValues(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
"#;
