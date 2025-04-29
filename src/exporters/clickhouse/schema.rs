use serde::Serialize;

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
    pub(crate) resource_attributes: Vec<(String, String)>,
    pub(crate) scope_name: String,
    pub(crate) scope_version: String,
    pub(crate) span_attributes: Vec<(String, String)>,
    pub(crate) duration: i64,
    pub(crate) status_code: String,
    pub(crate) status_message: String,

    #[serde(rename = "Events.Timestamp")]
    pub(crate) events_timestamp: Vec<u64>,
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
