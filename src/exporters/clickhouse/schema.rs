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
    pub(crate) trace_id: String,
    pub(crate) span_id: String,
    pub(crate) trace_flags: u8,
    pub(crate) severity_text: String,
    pub(crate) severity_number: u8,
    pub(crate) service_name: String,
    pub(crate) body: String,
    pub(crate) resource_schema_url: String,
    pub(crate) resource_attributes: MapOrJson<'a>,
    pub(crate) scope_schema_url: String,
    pub(crate) scope_name: String,
    pub(crate) scope_version: String,
    pub(crate) scope_attributes: MapOrJson<'a>,
    pub(crate) log_attributes: MapOrJson<'a>,
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

#[derive(Serialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct MetricsMeta {
    pub(crate) resource_attributes: MapOrJson<'static>,
    pub(crate) resource_schema_url: String,
    pub(crate) scope_name: String,
    pub(crate) scope_version: String,
    pub(crate) scope_attributes: MapOrJson<'static>,
    pub(crate) scope_dropped_attr_count: u32,
    pub(crate) scope_schema_url: String,
    pub(crate) service_name: String,
    pub(crate) metric_name: String,
    pub(crate) metric_description: String,
    pub(crate) metric_unit: String,
    pub(crate) attributes: MapOrJson<'static>,
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
    pub(crate) meta: &'a MetricsMeta,

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
    pub(crate) meta: &'a MetricsMeta,

    pub(crate) value: f64,
    pub(crate) flags: u32,

    #[serde(flatten)]
    pub(crate) exemplars: MetricsExemplars,
}

pub fn get_metrics_gauge_row_col_keys() -> String {
    let fields = [
        get_metrics_meta_col_keys(),
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
    pub(crate) meta: &'a MetricsMeta,

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
    pub(crate) meta: &'a MetricsMeta,

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
    pub(crate) meta: &'a MetricsMeta,

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

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};
    use http::Method;
    use http_body_util::Full;
    use url::Url;

    use super::*;
    use std::collections::HashMap;

    use crate::exporters::{
        clickhouse::{compression, rowbinary::serialize_into, schema::MapOrJson},
        http::{
            client::{build_hyper_client, response_bytes},
            tls::Config,
        },
    };

    #[tokio::test]
    #[ignore]
    async fn it_serializes_json() {
        let mut hm = HashMap::<Cow<'_, str>, JsonType>::new();

        hm.insert(Cow::Borrowed("a"), JsonType::Int(1));
        hm.insert(Cow::Borrowed("ba"), JsonType::Int(2));
        hm.insert(Cow::Borrowed("c"), JsonType::Str("str"));
        hm.insert(Cow::Borrowed("d"), JsonType::Str("blah"));
        hm.insert(Cow::Borrowed("e"), JsonType::Double(2.345));

        let row = JsonTest {
            timestamp: 10,
            foo: MapOrJson::Json(hm),
        };

        let mut buf_orig = BytesMut::with_capacity(1024);
        serialize_into(&mut buf_orig, &row).unwrap();

        //assert!(false);
        let expected_buf = vec![
            0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x01, 0x63, 0x15, 0x03, 0x73,
            0x74, 0x72, 0x02, 0x62, 0x61, 0x0a, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x01, 0x61, 0x0a, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];

        let expected_buf = vec![
            0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x63, 0x15, 0x03, 0x73,
            0x74, 0x72,
        ];

        //assert_eq!(buf.to_vec(), expected_buf);
        //
        //assert_eq!(buf_orig.to_vec(), expected_buf);

        //let buf = Bytes::from(expected_buf);
        let buf = buf_orig.freeze();

        print!("buf bytes: ");
        for byte in buf.iter() {
            print!("{:02x} ", byte);
        }
        println!();

        let buf = compression::lz4::compress(&buf).unwrap();

        let tls_config = Config::builder().build().unwrap();
        let client = build_hyper_client(tls_config, false).unwrap();

        let mut uri = Url::parse("http://localhost:8123").unwrap();
        {
            let mut pairs = uri.query_pairs_mut();
            pairs.clear();

            let query = "INSERT INTO jsontest (Timestamp, Foo) FORMAT RowBinary";

            pairs.append_pair("database", "otel");
            pairs.append_pair("query", query);
            pairs.append_pair("decompress", "1");
            pairs.append_pair("async_insert", "1");
            pairs.append_pair("allow_experimental_json_type", "1");
        }

        println!("uri: {}", uri.to_string());

        let builder = hyper::Request::builder()
            .method(Method::POST)
            .uri(uri.to_string());

        //let req = builder.body(Full::<Bytes>::from(buf.freeze())).unwrap();
        let req = builder.body(Full::<Bytes>::from(buf)).unwrap();

        let resp = client.request(req).await.unwrap();

        let (head, body) = resp.into_parts();

        let body = response_bytes(body).await.unwrap();

        println!("Response body: {}", String::from_utf8_lossy(&body));

        assert!(head.status.as_u16() == 200);
        assert!(false);
    }

    #[derive(Serialize)]
    #[serde(rename_all = "PascalCase")]
    struct JsonTest<'a> {
        pub(crate) timestamp: u64,
        pub(crate) foo: MapOrJson<'a>,
    }
}
