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
    materialize_genai_fields: bool,
    partition_by_service_name: bool,
) -> Vec<String> {
    let map_or_json = get_json_col_type(use_json);

    let map_indices = if !use_json && engine != Engine::Null {
        TRACES_TABLE_MAP_INDICES_SQL
    } else {
        ""
    };

    // Build indices - add service name index if partitioning by service
    let mut indices = String::new();
    if engine != Engine::Null {
        indices.push_str(TRACES_TABLE_INDICES_SQL);
        if partition_by_service_name {
            indices.push_str(TRACES_SERVICE_INDEX_SQL);
        }
    }

    // Add GenAI materialized columns if requested
    let genai_materialized = if materialize_genai_fields {
        get_genai_materialized_columns(use_json)
    } else {
        ""
    };

    let settings_str = get_settings(use_json, engine);

    // Determine partition and order by based on options
    let (partition_expr, order_expr) = if partition_by_service_name {
        (
            "(ServiceName, toDate(Timestamp))",
            "(ServiceName, Timestamp)",
        )
    } else {
        (
            "toDate(Timestamp)",
            "(ServiceName, SpanName, toDateTime(Timestamp))",
        )
    };

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
            ("INDICES", indices.as_str()),
            ("GENAI_MATERIALIZED", genai_materialized),
            ("TTL_EXPR", &build_ttl_string(ttl, "toDateTime(Timestamp)")),
            ("SETTINGS", &settings_str),
            ("PARTITION_BY", &get_partition_by(partition_expr, engine)),
            ("ORDER_BY", &get_order_by(order_expr, engine)),
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

/// Returns the appropriate GenAI MATERIALIZED columns based on whether JSON or Map type is used
fn get_genai_materialized_columns(use_json: bool) -> &'static str {
    if use_json {
        GENAI_MATERIALIZED_JSON_SQL
    } else {
        GENAI_MATERIALIZED_MAP_SQL
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

    %%GENAI_MATERIALIZED%%

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

const TRACES_SERVICE_INDEX_SQL: &str = r#"
	INDEX idx_service_date (ServiceName, toDate(Timestamp)) TYPE minmax GRANULARITY 1,
"#;

const TRACES_TABLE_MAP_INDICES_SQL: &str = r#"
	INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_span_attr_key mapKeys(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_span_attr_value mapValues(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
"#;

// GenAI MATERIALIZED columns for Map type (default)
// Uses SpanAttributes['key'] syntax and casts for numeric values
const GENAI_MATERIALIZED_MAP_SQL: &str = r#"
    -- GenAI MATERIALIZED columns (extracted from SpanAttributes Map)
    GenAIConversationId String MATERIALIZED SpanAttributes['gen_ai.conversation.id'],
    GenAIOperationName LowCardinality(String) MATERIALIZED SpanAttributes['gen_ai.operation.name'],
    GenAIProviderName LowCardinality(String) MATERIALIZED SpanAttributes['gen_ai.provider.name'],
    GenAIRequestModel LowCardinality(String) MATERIALIZED SpanAttributes['gen_ai.request.model'],
    GenAIResponseModel LowCardinality(String) MATERIALIZED SpanAttributes['gen_ai.response.model'],
    GenAIInputTokens UInt32 MATERIALIZED toUInt32OrZero(SpanAttributes['gen_ai.usage.input_tokens']),
    GenAIOutputTokens UInt32 MATERIALIZED toUInt32OrZero(SpanAttributes['gen_ai.usage.output_tokens']),
    GenAITemperature Float32 MATERIALIZED toFloat32OrZero(SpanAttributes['gen_ai.request.temperature']),
    GenAIMaxTokens UInt32 MATERIALIZED toUInt32OrZero(SpanAttributes['gen_ai.request.max_tokens']),
    GenAIResponseId String MATERIALIZED SpanAttributes['gen_ai.response.id'],
    GenAIErrorType LowCardinality(String) MATERIALIZED SpanAttributes['error.type'],
    -- Message content (stored as JSON strings in Map values)
    GenAILastInputMessage String MATERIALIZED JSONExtractRaw(SpanAttributes['gen_ai.input.messages'], -1),
    GenAILastOutputMessage String MATERIALIZED JSONExtractRaw(SpanAttributes['gen_ai.output.messages'], -1),
    GenAISystemInstructions String MATERIALIZED SpanAttributes['gen_ai.system.instructions'],
    GenAIToolDefinitions String MATERIALIZED SpanAttributes['gen_ai.tool.definitions'],
"#;

// GenAI MATERIALIZED columns for JSON type (--enable-json)
// Uses JSON path syntax
const GENAI_MATERIALIZED_JSON_SQL: &str = r#"
    -- GenAI MATERIALIZED columns (extracted from SpanAttributes JSON)
    GenAIConversationId String MATERIALIZED SpanAttributes.'gen_ai.conversation.id',
    GenAIOperationName LowCardinality(String) MATERIALIZED SpanAttributes.'gen_ai.operation.name',
    GenAIProviderName LowCardinality(String) MATERIALIZED SpanAttributes.'gen_ai.provider.name',
    GenAIRequestModel LowCardinality(String) MATERIALIZED SpanAttributes.'gen_ai.request.model',
    GenAIResponseModel LowCardinality(String) MATERIALIZED SpanAttributes.'gen_ai.response.model',
    GenAIInputTokens UInt32 MATERIALIZED SpanAttributes.'gen_ai.usage.input_tokens',
    GenAIOutputTokens UInt32 MATERIALIZED SpanAttributes.'gen_ai.usage.output_tokens',
    GenAITemperature Float32 MATERIALIZED SpanAttributes.'gen_ai.request.temperature',
    GenAIMaxTokens UInt32 MATERIALIZED SpanAttributes.'gen_ai.request.max_tokens',
    GenAIResponseId String MATERIALIZED SpanAttributes.'gen_ai.response.id',
    GenAIErrorType LowCardinality(String) MATERIALIZED SpanAttributes.'error.type',
    -- Message content
    GenAILastInputMessage String MATERIALIZED toString(SpanAttributes.'gen_ai.input.messages'[-1]),
    GenAILastOutputMessage String MATERIALIZED toString(SpanAttributes.'gen_ai.output.messages'[-1]),
    GenAISystemInstructions String MATERIALIZED toString(SpanAttributes.'gen_ai.system.instructions'),
    GenAIToolDefinitions String MATERIALIZED toString(SpanAttributes.'gen_ai.tool.definitions'),
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
