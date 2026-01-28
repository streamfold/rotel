use std::{borrow::Cow, collections::HashMap};

use serde::{Serialize, Serializer, ser::SerializeMap};

use crate::exporters::clickhouse::rowbinary::json::JsonType;

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
pub struct SpanRow<'a> {
    pub(crate) timestamp: u64,
    pub(crate) trace_id: &'a str,
    pub(crate) span_id: &'a str,
    pub(crate) parent_span_id: &'a str,
    pub(crate) trace_state: String,
    pub(crate) span_name: String,
    pub(crate) span_kind: &'a str,
    pub(crate) service_name: &'a str,
    pub(crate) resource_attributes: &'a MapOrJson<'a>,
    pub(crate) scope_name: &'a str,
    pub(crate) scope_version: &'a str,
    pub(crate) span_attributes: MapOrJson<'a>,
    pub(crate) duration: i64,
    pub(crate) status_code: &'a str,
    pub(crate) status_message: &'a str,

    #[serde(rename = "Events.Timestamp")]
    pub(crate) events_timestamp: Vec<u64>,
    #[serde(rename = "Events.Name")]
    pub(crate) events_name: Vec<String>,
    #[serde(rename = "Events.Attributes")]
    pub(crate) events_attributes: Vec<MapOrJson<'a>>,

    #[serde(rename = "Links.TraceId")]
    pub(crate) links_trace_id: Vec<String>,
    #[serde(rename = "Links.SpanId")]
    pub(crate) links_span_id: Vec<String>,
    #[serde(rename = "Links.TraceState")]
    pub(crate) links_trace_state: Vec<String>,
    #[serde(rename = "Links.Attributes")]
    pub(crate) links_attributes: Vec<MapOrJson<'a>>,
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
pub struct LogRecordRow<'a> {
    pub(crate) timestamp: u64,
    pub(crate) trace_id: &'a str,
    pub(crate) span_id: &'a str,
    pub(crate) trace_flags: u8,
    pub(crate) severity_text: String,
    pub(crate) severity_number: u8,
    pub(crate) service_name: &'a str,
    pub(crate) body: String,
    pub(crate) resource_schema_url: &'a str,
    pub(crate) resource_attributes: &'a MapOrJson<'a>,
    pub(crate) scope_schema_url: &'a str,
    pub(crate) scope_name: &'a str,
    pub(crate) scope_version: &'a str,
    pub(crate) scope_attributes: &'a MapOrJson<'a>,
    pub(crate) log_attributes: MapOrJson<'a>,
    pub(crate) event_name: &'a str,
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
        "EventName",
    ];

    fields.join(",")
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct MetricsMeta<'a> {
    pub(crate) resource_attributes: &'a MapOrJson<'a>,
    pub(crate) resource_schema_url: &'a str,
    pub(crate) scope_name: &'a str,
    pub(crate) scope_version: &'a str,
    pub(crate) scope_attributes: &'a MapOrJson<'a>,
    pub(crate) scope_dropped_attr_count: u32,
    pub(crate) scope_schema_url: &'a str,
    pub(crate) service_name: &'a str,
    pub(crate) metric_name: &'a str,
    pub(crate) metric_description: &'a str,
    pub(crate) metric_unit: &'a str,
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
    ]
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct MetricsExemplars {
    #[serde(rename = "Exemplars.FilteredAttributes")]
    pub(crate) exemplars_filtered_attributes: Vec<MapOrJson<'static>>,
    #[serde(rename = "Exemplars.TimeUnix")]
    pub(crate) exemplars_time_unix: Vec<u64>,
    #[serde(rename = "Exemplars.Value")]
    pub(crate) exemplars_value: Vec<f64>,
    #[serde(rename = "Exemplars.SpanId")]
    pub(crate) exemplars_span_id: Vec<String>,
    #[serde(rename = "Exemplars.TraceId")]
    pub(crate) exemplars_trace_id: Vec<String>,
}

pub fn get_metrics_exemplars_col_keys<'a>() -> Vec<&'a str> {
    vec![
        "Exemplars.FilteredAttributes",
        "Exemplars.TimeUnix",
        "Exemplars.Value",
        "Exemplars.SpanId",
        "Exemplars.TraceId",
    ]
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct MetricsSumRow<'a> {
    #[serde(flatten)]
    pub(crate) meta: &'a MetricsMeta<'a>,

    pub(crate) attributes: &'a MapOrJson<'a>,
    pub(crate) start_time_unix: u64,
    pub(crate) time_unix: u64,

    pub(crate) value: f64,
    pub(crate) flags: u32,

    pub(crate) aggregation_temporality: i32,
    pub(crate) is_monotonic: bool,

    #[serde(flatten)]
    pub(crate) exemplars: MetricsExemplars,
}

pub fn get_metrics_sum_row_col_keys() -> String {
    let fields = [
        get_metrics_meta_col_keys(),
        vec!["Attributes", "StartTimeUnix", "TimeUnix"],
        vec!["Value", "Flags", "AggregationTemporality", "IsMonotonic"],
        get_metrics_exemplars_col_keys(),
    ]
    .concat();

    fields.join(",")
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct MetricsGaugeRow<'a> {
    #[serde(flatten)]
    pub(crate) meta: &'a MetricsMeta<'a>,

    pub(crate) attributes: &'a MapOrJson<'a>,
    pub(crate) start_time_unix: u64,
    pub(crate) time_unix: u64,

    pub(crate) value: f64,
    pub(crate) flags: u32,

    #[serde(flatten)]
    pub(crate) exemplars: MetricsExemplars,
}

pub fn get_metrics_gauge_row_col_keys() -> String {
    let fields = [
        get_metrics_meta_col_keys(),
        vec!["Attributes", "StartTimeUnix", "TimeUnix"],
        vec!["Value", "Flags"],
        get_metrics_exemplars_col_keys(),
    ]
    .concat();

    fields.join(",")
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct MetricsHistogramRow<'a> {
    #[serde(flatten)]
    pub(crate) meta: &'a MetricsMeta<'a>,

    pub(crate) attributes: &'a MapOrJson<'a>,
    pub(crate) start_time_unix: u64,
    pub(crate) time_unix: u64,

    pub(crate) count: u64,
    pub(crate) sum: f64,
    pub(crate) bucket_counts: Vec<u64>,
    pub(crate) explicit_bounds: Vec<f64>,

    pub(crate) flags: u32,
    pub(crate) min: f64,
    pub(crate) max: f64,
    pub(crate) aggregation_temporality: i32,

    #[serde(flatten)]
    pub(crate) exemplars: MetricsExemplars,
}

pub fn get_metrics_histogram_row_col_keys() -> String {
    let fields = [
        get_metrics_meta_col_keys(),
        vec!["Attributes", "StartTimeUnix", "TimeUnix"],
        vec![
            "Count",
            "Sum",
            "BucketCounts",
            "ExplicitBounds",
            "Flags",
            "Min",
            "Max",
            "AggregationTemporality",
        ],
        get_metrics_exemplars_col_keys(),
    ]
    .concat();

    fields.join(",")
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct MetricsExpHistogramRow<'a> {
    #[serde(flatten)]
    pub(crate) meta: &'a MetricsMeta<'a>,

    pub(crate) attributes: &'a MapOrJson<'a>,
    pub(crate) start_time_unix: u64,
    pub(crate) time_unix: u64,

    pub(crate) count: u64,
    pub(crate) sum: f64,
    pub(crate) scale: i32,
    pub(crate) zero_count: u64,

    pub(crate) positive_offset: i32,
    pub(crate) positive_bucket_counts: Vec<u64>,
    pub(crate) negative_offset: i32,
    pub(crate) negative_bucket_counts: Vec<u64>,

    pub(crate) flags: u32,
    pub(crate) min: f64,
    pub(crate) max: f64,
    pub(crate) aggregation_temporality: i32,

    #[serde(flatten)]
    pub(crate) exemplars: MetricsExemplars,
}

pub fn get_metrics_exp_histogram_row_col_keys() -> String {
    let fields = [
        get_metrics_meta_col_keys(),
        vec!["Attributes", "StartTimeUnix", "TimeUnix"],
        vec![
            "Count",
            "Sum",
            "Scale",
            "ZeroCount",
            "PositiveOffset",
            "PositiveBucketCounts",
            "NegativeOffset",
            "NegativeBucketCounts",
            "Flags",
            "Min",
            "Max",
            "AggregationTemporality",
        ],
        get_metrics_exemplars_col_keys(),
    ]
    .concat();

    fields.join(",")
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct MetricsSummaryRow<'a> {
    #[serde(flatten)]
    pub(crate) meta: &'a MetricsMeta<'a>,

    pub(crate) attributes: &'a MapOrJson<'a>,
    pub(crate) start_time_unix: u64,
    pub(crate) time_unix: u64,

    pub(crate) count: u64,
    pub(crate) sum: f64,

    #[serde(rename = "ValueAtQuantiles.Quantile")]
    pub(crate) value_at_quantiles_quantile: Vec<f64>,
    #[serde(rename = "ValueAtQuantiles.Value")]
    pub(crate) value_at_quantiles_value: Vec<f64>,

    pub(crate) flags: u32,
}

pub fn get_metrics_summary_row_col_keys() -> String {
    let fields = [
        get_metrics_meta_col_keys(),
        vec!["Attributes", "StartTimeUnix", "TimeUnix"],
        vec![
            "Count",
            "Sum",
            "ValueAtQuantiles.Quantile",
            "ValueAtQauntiles.Value",
            "Flags",
        ],
    ]
    .concat();

    fields.join(",")
}

#[derive(Debug)]
pub enum MapOrJson<'a> {
    Map(Vec<(String, String)>),
    Json(HashMap<Cow<'a, str>, JsonType<'a>>),
    JsonOwned(HashMap<String, JsonType<'static>>),
}

impl<'a> Serialize for MapOrJson<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            MapOrJson::Map(vec) => vec.serialize(serializer),
            MapOrJson::Json(map) => {
                let mut m = serializer.serialize_map(Some(map.len()))?;

                for (k, v) in map {
                    m.serialize_entry(k, v)?;
                }
                m.end()
            }
            MapOrJson::JsonOwned(map) => {
                let mut m = serializer.serialize_map(Some(map.len()))?;

                for (k, v) in map {
                    m.serialize_entry(k, v)?;
                }
                m.end()
            }
        }
    }
}
