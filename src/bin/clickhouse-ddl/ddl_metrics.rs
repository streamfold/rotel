use crate::Engine;
use crate::ddl::{
    build_cluster_string, build_table_name, build_ttl_string, get_json_col_type, get_order_by,
    get_partition_by, get_settings, replace_placeholders,
};
use std::collections::HashMap;
use std::time::Duration;

pub(crate) fn get_metrics_ddl(
    cluster: &Option<String>,
    database: &String,
    table_prefix: &String,
    engine: Engine,
    ttl: &Duration,
    use_json: bool,
) -> Vec<String> {
    let map_or_json = get_json_col_type(use_json);

    let order_by = if !use_json {
        "(ServiceName, MetricName, Attributes, toUnixTimestamp64Nano(TimeUnix))"
    } else {
        "(ServiceName, MetricName, toUnixTimestamp64Nano(TimeUnix))"
    };

    let map_indices = if !use_json && engine != Engine::Null {
        METRICS_TABLE_MAP_INDICES_SQL
    } else {
        ""
    };

    let settings_str = get_settings(use_json, engine);

    let sqls = [
        (METRICS_SUM_TABLE_SQL, "metrics_sum"),
        (METRICS_GAUGE_TABLE_SQL, "metrics_gauge"),
        (METRICS_HISTOGRAM_TABLE_SQL, "metrics_histogram"),
        (
            METRICS_EXP_HISTOGRAM_TABLE_SQL,
            "metrics_exponential_histogram",
        ),
        (METRICS_SUMMARY_TABLE_SQL, "metrics_summary"),
    ];

    sqls.iter()
        .map(|(table_sql, table_name)| {
            replace_placeholders(
                table_sql,
                &HashMap::from([
                    (
                        "TABLE",
                        build_table_name(database, table_prefix, table_name).as_str(),
                    ),
                    ("CLUSTER", &build_cluster_string(cluster)),
                    ("MAP_OR_JSON", map_or_json),
                    ("ENGINE", &engine.to_string()),
                    ("MAP_INDICES", map_indices),
                    (
                        "PARTITION_BY",
                        &get_partition_by("toDate(TimeUnix)", engine),
                    ),
                    ("ORDER_BY", &get_order_by(order_by, engine)),
                    ("TTL_EXPR", &build_ttl_string(ttl, "toDateTime(TimeUnix)")),
                    ("SETTINGS", &settings_str),
                ]),
            )
        })
        .collect()
}

const METRICS_SUM_TABLE_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS %%TABLE%% %%CLUSTER%% (
    ResourceAttributes %%MAP_OR_JSON%% CODEC(ZSTD(1)),
    ResourceSchemaUrl String CODEC(ZSTD(1)),
    ScopeName String CODEC(ZSTD(1)),
    ScopeVersion String CODEC(ZSTD(1)),
    ScopeAttributes %%MAP_OR_JSON%% CODEC(ZSTD(1)),
    ScopeDroppedAttrCount UInt32 CODEC(ZSTD(1)),
    ScopeSchemaUrl String CODEC(ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    MetricName String CODEC(ZSTD(1)),
    MetricDescription String CODEC(ZSTD(1)),
    MetricUnit String CODEC(ZSTD(1)),
    Attributes %%MAP_OR_JSON%% CODEC(ZSTD(1)),
	StartTimeUnix DateTime64(9) CODEC(Delta, ZSTD(1)),
	TimeUnix DateTime64(9) CODEC(Delta, ZSTD(1)),
	Value Float64 CODEC(ZSTD(1)),
	Flags UInt32  CODEC(ZSTD(1)),
    Exemplars Nested (
		FilteredAttributes %%MAP_OR_JSON%%,
		TimeUnix DateTime64(9),
		Value Float64,
		SpanId String,
		TraceId String
    ) CODEC(ZSTD(1)),
    AggregationTemporality Int32 CODEC(ZSTD(1)),
	IsMonotonic Boolean CODEC(Delta, ZSTD(1)),

    %%MAP_INDICES%%

) ENGINE = %%ENGINE%%
%%TTL_EXPR%%
%%PARTITION_BY%%
%%ORDER_BY%%
%%SETTINGS%%
;
"#;

const METRICS_GAUGE_TABLE_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS %%TABLE%% %%CLUSTER%% (
    ResourceAttributes %%MAP_OR_JSON%% CODEC(ZSTD(1)),
    ResourceSchemaUrl String CODEC(ZSTD(1)),
    ScopeName String CODEC(ZSTD(1)),
    ScopeVersion String CODEC(ZSTD(1)),
    ScopeAttributes %%MAP_OR_JSON%% CODEC(ZSTD(1)),
    ScopeDroppedAttrCount UInt32 CODEC(ZSTD(1)),
    ScopeSchemaUrl String CODEC(ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    MetricName String CODEC(ZSTD(1)),
    MetricDescription String CODEC(ZSTD(1)),
    MetricUnit String CODEC(ZSTD(1)),
    Attributes %%MAP_OR_JSON%% CODEC(ZSTD(1)),
    StartTimeUnix DateTime64(9) CODEC(Delta, ZSTD(1)),
    TimeUnix DateTime64(9) CODEC(Delta, ZSTD(1)),
    Value Float64 CODEC(ZSTD(1)),
    Flags UInt32 CODEC(ZSTD(1)),
    Exemplars Nested (
		FilteredAttributes %%MAP_OR_JSON%%,
		TimeUnix DateTime64(9),
		Value Float64,
		SpanId String,
		TraceId String
    ) CODEC(ZSTD(1)),

    %%MAP_INDICES%%

) ENGINE = %%ENGINE%%
%%TTL_EXPR%%
%%PARTITION_BY%%
%%ORDER_BY%%
%%SETTINGS%%
;
"#;

const METRICS_HISTOGRAM_TABLE_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS %%TABLE%% %%CLUSTER%% (
    ResourceAttributes %%MAP_OR_JSON%% CODEC(ZSTD(1)),
    ResourceSchemaUrl String CODEC(ZSTD(1)),
    ScopeName String CODEC(ZSTD(1)),
    ScopeVersion String CODEC(ZSTD(1)),
    ScopeAttributes %%MAP_OR_JSON%% CODEC(ZSTD(1)),
    ScopeDroppedAttrCount UInt32 CODEC(ZSTD(1)),
    ScopeSchemaUrl String CODEC(ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    MetricName String CODEC(ZSTD(1)),
    MetricDescription String CODEC(ZSTD(1)),
    MetricUnit String CODEC(ZSTD(1)),
    Attributes %%MAP_OR_JSON%% CODEC(ZSTD(1)),
	StartTimeUnix DateTime64(9) CODEC(Delta, ZSTD(1)),
	TimeUnix DateTime64(9) CODEC(Delta, ZSTD(1)),
    Count UInt64 CODEC(Delta, ZSTD(1)),
    Sum Float64 CODEC(ZSTD(1)),
    BucketCounts Array(UInt64) CODEC(ZSTD(1)),
    ExplicitBounds Array(Float64) CODEC(ZSTD(1)),
	Exemplars Nested (
		FilteredAttributes %%MAP_OR_JSON%%,
		TimeUnix DateTime64(9),
		Value Float64,
		SpanId String,
		TraceId String
    ) CODEC(ZSTD(1)),
    Flags UInt32 CODEC(ZSTD(1)),
    Min Float64 CODEC(ZSTD(1)),
    Max Float64 CODEC(ZSTD(1)),
		AggregationTemporality Int32 CODEC(ZSTD(1)),

    %%MAP_INDICES%%

) ENGINE = %%ENGINE%%
%%TTL_EXPR%%
%%PARTITION_BY%%
%%ORDER_BY%%
%%SETTINGS%%
;
"#;

const METRICS_EXP_HISTOGRAM_TABLE_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS %%TABLE%% %%CLUSTER%% (
    ResourceAttributes %%MAP_OR_JSON%% CODEC(ZSTD(1)),
    ResourceSchemaUrl String CODEC(ZSTD(1)),
    ScopeName String CODEC(ZSTD(1)),
    ScopeVersion String CODEC(ZSTD(1)),
    ScopeAttributes %%MAP_OR_JSON%% CODEC(ZSTD(1)),
    ScopeDroppedAttrCount UInt32 CODEC(ZSTD(1)),
    ScopeSchemaUrl String CODEC(ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    MetricName String CODEC(ZSTD(1)),
    MetricDescription String CODEC(ZSTD(1)),
    MetricUnit String CODEC(ZSTD(1)),
    Attributes %%MAP_OR_JSON%% CODEC(ZSTD(1)),
	StartTimeUnix DateTime64(9) CODEC(Delta, ZSTD(1)),
	TimeUnix DateTime64(9) CODEC(Delta, ZSTD(1)),
    Count UInt64 CODEC(Delta, ZSTD(1)),
    Sum Float64 CODEC(ZSTD(1)),
    Scale Int32 CODEC(ZSTD(1)),
    ZeroCount UInt64 CODEC(ZSTD(1)),
	PositiveOffset Int32 CODEC(ZSTD(1)),
	PositiveBucketCounts Array(UInt64) CODEC(ZSTD(1)),
	NegativeOffset Int32 CODEC(ZSTD(1)),
	NegativeBucketCounts Array(UInt64) CODEC(ZSTD(1)),
	Exemplars Nested (
		FilteredAttributes %%MAP_OR_JSON%%,
		TimeUnix DateTime64(9),
		Value Float64,
		SpanId String,
		TraceId String
    ) CODEC(ZSTD(1)),
    Flags UInt32  CODEC(ZSTD(1)),
    Min Float64 CODEC(ZSTD(1)),
    Max Float64 CODEC(ZSTD(1)),
		AggregationTemporality Int32 CODEC(ZSTD(1)),

    %%MAP_INDICES%%

) ENGINE = %%ENGINE%%
%%TTL_EXPR%%
%%PARTITION_BY%%
%%ORDER_BY%%
%%SETTINGS%%
;
"#;

const METRICS_SUMMARY_TABLE_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS %%TABLE%% %%CLUSTER%% (
    ResourceAttributes %%MAP_OR_JSON%% CODEC(ZSTD(1)),
    ResourceSchemaUrl String CODEC(ZSTD(1)),
    ScopeName String CODEC(ZSTD(1)),
    ScopeVersion String CODEC(ZSTD(1)),
    ScopeAttributes %%MAP_OR_JSON%% CODEC(ZSTD(1)),
    ScopeDroppedAttrCount UInt32 CODEC(ZSTD(1)),
    ScopeSchemaUrl String CODEC(ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    MetricName String CODEC(ZSTD(1)),
    MetricDescription String CODEC(ZSTD(1)),
    MetricUnit String CODEC(ZSTD(1)),
    Attributes %%MAP_OR_JSON%% CODEC(ZSTD(1)),
	StartTimeUnix DateTime64(9) CODEC(Delta, ZSTD(1)),
	TimeUnix DateTime64(9) CODEC(Delta, ZSTD(1)),
    Count UInt64 CODEC(Delta, ZSTD(1)),
    Sum Float64 CODEC(ZSTD(1)),
    ValueAtQuantiles Nested(
		Quantile Float64,
		Value Float64
	) CODEC(ZSTD(1)),
    Flags UInt32  CODEC(ZSTD(1)),

    %%MAP_INDICES%%

) ENGINE = %%ENGINE%%
%%TTL_EXPR%%
%%PARTITION_BY%%
%%ORDER_BY%%
%%SETTINGS%%
;
"#;

const METRICS_TABLE_MAP_INDICES_SQL: &str = r#"
	INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_scope_attr_value mapValues(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_attr_key mapKeys(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_attr_value mapValues(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1
"#;
