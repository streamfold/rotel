use clickhouse::Row;
use serde::Serialize;

#[derive(Debug, PartialEq, Serialize)]
pub(crate) struct Timestamp64(pub(crate) u64);

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SpanRow {
    pub(crate) timestamp: Timestamp64,
    pub(crate) trace_id: String,
    pub(crate) span_id: String,
    pub(crate) parent_span_id: String,
    pub(crate) trace_state: String,
    pub(crate) span_name: String,
    pub(crate) span_kind: String,
    pub(crate) service_name: String,
    pub(crate) resource_attributes: Vec<(String, String)>,
    pub(crate) scope_name: String,
    pub(crate) scope_version: String,
    pub(crate) span_attributes: Vec<(String, String)>,
    pub(crate) duration: i64,
    pub(crate) status_code: String,
    pub(crate) status_message: String,

    #[serde(rename = "Events.Timestamp")]
    pub(crate) events_timestamp: Vec<Timestamp64>,
    #[serde(rename = "Events.Name")]
    pub(crate) events_name: Vec<String>,
    #[serde(rename = "Events.Attributes")]
    pub(crate) events_attributes: Vec<Vec<(String, String)>>,

    #[serde(rename = "Links.TraceId")]
    pub(crate) links_trace_id: Vec<String>,
    #[serde(rename = "Links.SpanId")]
    pub(crate) links_span_id: Vec<String>,
    #[serde(rename = "Links.TraceState")]
    pub(crate) links_trace_state: Vec<String>,
    #[serde(rename = "Links.Attributes")]
    pub(crate) links_attributes: Vec<Vec<(String, String)>>,
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