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
                build_ttl_string(ttl, "toDateTime(Timestamp)").as_str(),
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
                build_ttl_string(ttl, "toDateTime(Start)").as_str(),
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

pub(crate) fn get_logs_ddl(
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
        map_indices = LOGS_TABLE_MAP_INDICES_SQL;
    } else {
        json_setting = ", allow_experimental_json_type = 1";
    }

    let table_sql = replace_placeholders(
        LOGS_TABLE_SQL,
        &HashMap::from([
            (
                "TABLE",
                build_table_name(database, table_prefix, "logs").as_str(),
            ),
            ("CLUSTER", build_cluster_string(cluster).as_str()),
            ("MAP_OR_JSON", map_or_json),
            ("ENGINE", engine),
            ("MAP_INDICES", map_indices),
            ("TTL_EXPR", build_ttl_string(ttl, "TimestampTime").as_str()),
            ("JSON_SETTING", json_setting),
        ]),
    );

    vec![table_sql]
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
ORDER BY (ServiceName, SpanName, toDateTime(Timestamp))
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
     Start DateTime CODEC(Delta, ZSTD(1)),
     End DateTime CODEC(Delta, ZSTD(1)),
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
AS SELECT
	TraceId,
	min(Timestamp) as Start,
	max(Timestamp) as End
FROM
%%TABLE_TRACES%%
WHERE TraceId != ''
GROUP BY TraceId;
"#;

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

	INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
	
    %%MAP_INDICES%%
	
	INDEX idx_body Body TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 8
) ENGINE = %%ENGINE%%
PARTITION BY toDate(TimestampTime)
PRIMARY KEY (ServiceName, TimestampTime)
ORDER BY (ServiceName, TimestampTime, Timestamp)
%%TTL_EXPR%%
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1 %%JSON_SETTING%%
;
"#;

const LOGS_TABLE_MAP_INDICES_SQL: &str = r#"
	INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_scope_attr_value mapValues(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_log_attr_key mapKeys(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
	INDEX idx_log_attr_value mapValues(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
"#;

fn build_table_name(database: &String, table_prefix: &String, table_name: &str) -> String {
    format!("{}.{}_{}", database, table_prefix, table_name)
}

fn build_cluster_string(cluster: &Option<String>) -> String {
    match cluster {
        None => "".to_string(),
        Some(name) => format!("ON CLUSTER {}", name),
    }
}

fn build_ttl_string(ttl: &Duration, time_field: &str) -> String {
    let ttl_secs = ttl.as_secs();
    if ttl_secs == 0 {
        return "".to_string();
    }

    if ttl_secs % 86400 == 0 {
        return format!("TTL {} + toIntervalDay({})", time_field, ttl_secs / 86400);
    }
    if ttl_secs % 3600 == 0 {
        return format!("TTL {} + toIntervalHour({})", time_field, ttl_secs / 3600);
    }
    if ttl_secs % 60 == 0 {
        return format!("TTL {} + toIntervalMinute({})", time_field, ttl_secs / 60);
    }

    format!("TTL {} + toIntervalSecond({})", time_field, ttl_secs)
}

fn replace_placeholders(template: &str, replacements: &HashMap<&str, &str>) -> String {
    let mut result = template.to_string();

    for (key, value) in replacements {
        let placeholder = format!("%%{}%%", key);
        result = result.replace(&placeholder, value);
    }

    result
}

fn get_json_col_type<'a>(use_json: bool) -> &'a str {
    match use_json {
        true => "JSON",
        false => "Map(LowCardinality(String), String)",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_zero_ttl() {
        let duration = Duration::from_secs(0);
        let result = build_ttl_string(&duration, "timestamp");
        assert_eq!(result, "");
    }

    #[test]
    fn test_day_interval() {
        // One day (86400 seconds)
        let duration = Duration::from_secs(86400);
        let result = build_ttl_string(&duration, "timestamp");
        assert_eq!(result, "TTL timestamp + toIntervalDay(1)");

        // Multiple days
        let duration = Duration::from_secs(86400 * 7);
        let result = build_ttl_string(&duration, "timestamp");
        assert_eq!(result, "TTL timestamp + toIntervalDay(7)");
    }

    #[test]
    fn test_hour_interval() {
        // One hour (3600 seconds)
        let duration = Duration::from_secs(3600);
        let result = build_ttl_string(&duration, "timestamp");
        assert_eq!(result, "TTL timestamp + toIntervalHour(1)");

        // Multiple hours (but not a full day)
        let duration = Duration::from_secs(3600 * 23);
        let result = build_ttl_string(&duration, "timestamp");
        assert_eq!(result, "TTL timestamp + toIntervalHour(23)");
    }

    #[test]
    fn test_minute_interval() {
        // One minute (60 seconds)
        let duration = Duration::from_secs(60);
        let result = build_ttl_string(&duration, "timestamp");
        assert_eq!(result, "TTL timestamp + toIntervalMinute(1)");

        // Multiple minutes (but not a full hour)
        let duration = Duration::from_secs(60 * 59);
        let result = build_ttl_string(&duration, "timestamp");
        assert_eq!(result, "TTL timestamp + toIntervalMinute(59)");
    }

    #[test]
    fn test_second_interval() {
        // A few seconds (not a full minute)
        let duration = Duration::from_secs(45);
        let result = build_ttl_string(&duration, "timestamp");
        assert_eq!(result, "TTL timestamp + toIntervalSecond(45)");

        // A large number of seconds (not divisible by 60)
        let duration = Duration::from_secs(3601); // 1 hour and 1 second
        let result = build_ttl_string(&duration, "timestamp");
        assert_eq!(result, "TTL timestamp + toIntervalSecond(3601)");
    }

    #[test]
    fn test_different_time_field() {
        // Test with a different time field name
        let duration = Duration::from_secs(86400);
        let result = build_ttl_string(&duration, "created_at");
        assert_eq!(result, "TTL created_at + toIntervalDay(1)");
    }

    #[test]
    fn test_edge_cases() {
        // Test with very large duration
        let duration = Duration::from_secs(86400 * 365 * 10); // ~10 years
        let result = build_ttl_string(&duration, "timestamp");
        assert_eq!(result, "TTL timestamp + toIntervalDay(3650)");

        // Test with one second
        let duration = Duration::from_secs(1);
        let result = build_ttl_string(&duration, "timestamp");
        assert_eq!(result, "TTL timestamp + toIntervalSecond(1)");
    }
}
