use serde::{Serialize, Serializer};

//
// Trace spans
//

//
// *** NOTE ***
//
// The serde configuration of the column names here currently does not do anything. The column
// names are statically configured in the list below and must match both the ordering of this
// struct and the names created in the DB schema. If they ever get out of alignment, things
// will break. We'll look at ways to fix this in the future.
// *************

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct SpanRow {
    pub(crate) timestamp: u64,
    pub(crate) trace_id: String,
    pub(crate) span_id: String,
    pub(crate) parent_span_id: String,
    pub(crate) trace_state: String,
    pub(crate) span_name: String,
    pub(crate) span_kind: String,
    pub(crate) service_name: String,
    pub(crate) resource_attributes: MapOrJson,
    pub(crate) scope_name: String,
    pub(crate) scope_version: String,
    pub(crate) span_attributes: MapOrJson,
    pub(crate) duration: i64,
    pub(crate) status_code: String,
    pub(crate) status_message: String,

    #[serde(rename = "Events.Timestamp")]
    pub(crate) events_timestamp: Vec<u64>,
    #[serde(rename = "Events.Name")]
    pub(crate) events_name: Vec<String>,
    #[serde(rename = "Events.Attributes")]
    pub(crate) events_attributes: Vec<MapOrJson>,

    #[serde(rename = "Links.TraceId")]
    pub(crate) links_trace_id: Vec<String>,
    #[serde(rename = "Links.SpanId")]
    pub(crate) links_span_id: Vec<String>,
    #[serde(rename = "Links.TraceState")]
    pub(crate) links_trace_state: Vec<String>,
    #[serde(rename = "Links.Attributes")]
    pub(crate) links_attributes: Vec<MapOrJson>,
}

pub fn get_span_row_col_keys() -> String {
    let fields = vec![
        "Timestamp",
        "TraceId",
        "SpanId",
        "ParentSpanId",
        "TraceState",
        "SpanName",
        "SpanKind",
        "ServiceName",
        "ResourceAttributes",
        "ScopeName",
        "ScopeVersion",
        "SpanAttributes",
        "Duration",
        "StatusCode",
        "StatusMessage",
        "Events.Timestamp",
        "Events.Name",
        "Events.Attributes",
        "Links.TraceId",
        "Links.SpanId",
        "Links.TraceState",
        "Links.Attributes",
    ];

    fields.join(",")
}

//
// Log records
//

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct LogRecordRow {
    pub(crate) timestamp: u64,
    pub(crate) trace_id: String,
    pub(crate) span_id: String,
    pub(crate) trace_flags: u8,
    pub(crate) severity_text: String,
    pub(crate) severity_number: u8,
    pub(crate) service_name: String,
    pub(crate) body: String,
    pub(crate) resource_schema_url: String,
    pub(crate) resource_attributes: MapOrJson,
    pub(crate) scope_schema_url: String,
    pub(crate) scope_name: String,
    pub(crate) scope_version: String,
    pub(crate) scope_attributes: MapOrJson,
    pub(crate) log_attributes: MapOrJson,
}

pub fn get_log_row_col_keys() -> String {
    let fields = vec![
        "Timestamp",
        "TraceId",
        "SpanId",
        "TraceFlags",
        "SeverityText",
        "SeverityNumber",
        "ServiceName",
        "Body",
        "ResourceSchemaUrl",
        "ResourceAttributes",
        "ScopeSchemaUrl",
        "ScopeName",
        "ScopeVersion",
        "ScopeAttributes",
        "LogAttributes",
    ];

    fields.join(",")
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct MetricsMeta {
    pub(crate) resource_attributes: MapOrJson,
    pub(crate) resource_schema_url: String,
    pub(crate) scope_name: String,
    pub(crate) scope_version: String,
    pub(crate) scope_attributes: MapOrJson,
    pub(crate) scope_dropped_attr_count: u32,
    pub(crate) scope_schema_url: String,
    pub(crate) service_name: String,
    pub(crate) metric_name: String,
    pub(crate) metric_description: String,
    pub(crate) metric_unit: String,
    pub(crate) attributes: MapOrJson,
    pub(crate) start_time_unix: u64,
    pub(crate) time_unix: u64,
}

pub fn get_metrics_meta_col_keys<'a>() -> Vec<&'a str> {
    vec![
        "ResourceAttributes",
        "ResourceSchemaUrl",
        "ScopeName",
        "ScopeVersion",
        "ScopeAttributes",
        "ScopeDroppedAttrCount",
        "ScopeSchemaUrl",
        "ServiceName",
        "MetricName",
        "MetricDescription",
        "MetricUnit",
        "Attributes",
        "StartTimeUnix",
        "TimeUnix",
    ]
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct MetricsSumRow {
    #[serde(flatten)]
    pub(crate) meta: MetricsMeta,

    pub(crate) value: f64,
    pub(crate) flags: u32,

    #[serde(rename = "Exemplars.FilteredAttributes")]
    pub(crate) exemplars_filtered_attributes: MapOrJson,
    #[serde(rename = "Exemplars.TimeUnix")]
    pub(crate) exemplars_time_unix: u64,
    #[serde(rename = "Exemplars.Value")]
    pub(crate) exemplars_value: f64,
    #[serde(rename = "Exemplars.SpanId")]
    pub(crate) exemplars_span_id: String,
    #[serde(rename = "Exemplars.TraceId")]
    pub(crate) exemplars_trace_id: String,

    pub(crate) aggregation_temporality: i32,
    pub(crate) is_monotonic: bool,
}

pub fn get_metrics_sum_row_col_keys() -> String {
    let fields = [
        get_metrics_meta_col_keys(),
        vec![
            "Value",
            "Flags",
            "Exemplars.FilteredAttributes",
            "Exemplars.TimeUnix",
            "Exemplars.Value",
            "Exemplars.SpanId",
            "Exemplars.TraceId",
            "AggregationTemporality",
            "IsMonotonic",
        ],
    ]
    .concat();

    fields.join(",")
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct MetricsGaugeRow {
    #[serde(flatten)]
    pub(crate) meta: MetricsMeta,

    pub(crate) value: f64,
    pub(crate) flags: u32,

    #[serde(rename = "Exemplars.FilteredAttributes")]
    pub(crate) exemplars_filtered_attributes: MapOrJson,
    #[serde(rename = "Exemplars.TimeUnix")]
    pub(crate) exemplars_time_unix: u64,
    #[serde(rename = "Exemplars.Value")]
    pub(crate) exemplars_value: f64,
    #[serde(rename = "Exemplars.SpanId")]
    pub(crate) exemplars_span_id: String,
    #[serde(rename = "Exemplars.TraceId")]
    pub(crate) exemplars_trace_id: String,
}

pub fn get_metrics_gauge_row_col_keys() -> String {
    let fields = [
        get_metrics_meta_col_keys(),
        vec![
            "Value",
            "Flags",
            "Exemplars.FilteredAttributes",
            "Exemplars.TimeUnix",
            "Exemplars.Value",
            "Exemplars.SpanId",
            "Exemplars.TraceId",
        ],
    ]
    .concat();

    fields.join(",")
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct MetricsHistogramRow {
    #[serde(flatten)]
    pub(crate) meta: MetricsMeta,

    pub(crate) count: u64,
    pub(crate) sum: f64,
    pub(crate) bucket_counts: Vec<u64>,
    pub(crate) explicit_bounds: Vec<f64>,

    #[serde(rename = "Exemplars.FilteredAttributes")]
    pub(crate) exemplars_filtered_attributes: MapOrJson,
    #[serde(rename = "Exemplars.TimeUnix")]
    pub(crate) exemplars_time_unix: u64,
    #[serde(rename = "Exemplars.Value")]
    pub(crate) exemplars_value: f64,
    #[serde(rename = "Exemplars.SpanId")]
    pub(crate) exemplars_span_id: String,
    #[serde(rename = "Exemplars.TraceId")]
    pub(crate) exemplars_trace_id: String,

    pub(crate) flags: u32,
    pub(crate) min: f64,
    pub(crate) max: f64,
    pub(crate) aggregation_temporality: i32,
}

pub fn get_metrics_histogram_row_col_keys() -> String {
    let fields = [
        get_metrics_meta_col_keys(),
        vec![
            "Count",
            "Sum",
            "BucketCounts",
            "ExplicitBounds",
            "Exemplars.FilteredAttributes",
            "Exemplars.TimeUnix",
            "Exemplars.Value",
            "Exemplars.SpanId",
            "Exemplars.TraceId",
            "Flags",
            "Min",
            "Max",
            "AggregationTemporality",
        ],
    ]
    .concat();

    fields.join(",")
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct MetricsExpHistogramRow {
    #[serde(flatten)]
    pub(crate) meta: MetricsMeta,

    pub(crate) count: u64,
    pub(crate) sum: f64,
    pub(crate) scale: i32,
    pub(crate) zero_count: u64,

    pub(crate) positive_offset: i32,
    pub(crate) positive_bucket_counts: Vec<u64>,
    pub(crate) negative_offset: i32,
    pub(crate) negative_bucket_counts: Vec<u64>,

    #[serde(rename = "Exemplars.FilteredAttributes")]
    pub(crate) exemplars_filtered_attributes: MapOrJson,
    #[serde(rename = "Exemplars.TimeUnix")]
    pub(crate) exemplars_time_unix: u64,
    #[serde(rename = "Exemplars.Value")]
    pub(crate) exemplars_value: f64,
    #[serde(rename = "Exemplars.SpanId")]
    pub(crate) exemplars_span_id: String,
    #[serde(rename = "Exemplars.TraceId")]
    pub(crate) exemplars_trace_id: String,

    pub(crate) flags: u32,
    pub(crate) min: f64,
    pub(crate) max: f64,
    pub(crate) aggregation_temporality: i32,
}

pub fn get_metrics_exp_histogram_row_col_keys() -> String {
    let fields = [
        get_metrics_meta_col_keys(),
        vec![
            "Count",
            "Sum",
            "Scale",
            "ZeroCount",
            "PositiveOffset",
            "PositiveBucketCounts",
            "NegativeOffset",
            "NegativeBucketCounts",
            "Exemplars.FilteredAttributes",
            "Exemplars.TimeUnix",
            "Exemplars.Value",
            "Exemplars.SpanId",
            "Exemplars.TraceId",
            "Flags",
            "Min",
            "Max",
            "AggregationTemporality",
        ],
    ]
    .concat();

    fields.join(",")
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct MetricsSummaryRow {
    #[serde(flatten)]
    pub(crate) meta: MetricsMeta,

    pub(crate) count: u64,
    pub(crate) sum: f64,

    #[serde(rename = "ValueAtQuantiles.Quantile")]
    pub(crate) value_at_quantiles_quantile: f64,
    #[serde(rename = "ValueAtQuantiles.Value")]
    pub(crate) value_at_quantiles_value: f64,
    //
    // #[serde(rename = "Exemplars.FilteredAttributes")]
    // pub(crate) exemplars_filtered_attributes: MapOrJson,
    // #[serde(rename = "Exemplars.TimeUnix")]
    // pub(crate) exemplars_time_unix: u64,
    // #[serde(rename = "Exemplars.Value")]
    // pub(crate) exemplars_value: f64,
    // #[serde(rename = "Exemplars.SpanId")]
    // pub(crate) exemplars_span_id: String,
    // #[serde(rename = "Exemplars.TraceId")]
    // pub(crate) exemplars_trace_id: String,
    pub(crate) flags: u32,
}

pub fn get_metrics_summary_row_col_keys() -> String {
    let fields = [
        get_metrics_meta_col_keys(),
        vec![
            "Count",
            "Sum",
            "ValueAtQuantiles.Quantile",
            "ValueAtQauntiles.Value",
            // "Exemplars.FilteredAttributes",
            // "Exemplars.TimeUnix",
            // "Exemplars.Value",
            // "Exemplars.SpanId",
            // "Exemplars.TraceId",
            "Flags",
        ],
    ]
    .concat();

    fields.join(",")
}

#[derive(Debug)]
pub enum MapOrJson {
    Map(Vec<(String, String)>),
    Json(String),
}

impl Serialize for MapOrJson {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            MapOrJson::Map(map) => map.serialize(serializer),
            MapOrJson::Json(str) => str.serialize(serializer),
        }
    }
}
