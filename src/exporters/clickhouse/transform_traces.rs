use std::borrow::Cow;

use crate::exporters::clickhouse::payload::{ClickhousePayload, ClickhousePayloadBuilder};
use crate::exporters::clickhouse::request_builder::TransformPayload;
use crate::exporters::clickhouse::request_mapper::RequestType;
use crate::exporters::clickhouse::schema::SpanRow;
use crate::exporters::clickhouse::transformer::{Transformer, find_attribute};
use crate::otlp::cvattr;
use opentelemetry_proto::tonic::trace::v1::span::SpanKind;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, Span};
use opentelemetry_semantic_conventions::resource::SERVICE_NAME;
use tower::BoxError;

impl TransformPayload<ResourceSpans> for Transformer {
    fn transform(
        &self,
        input: Vec<ResourceSpans>,
    ) -> Result<Vec<(RequestType, ClickhousePayload)>, BoxError> {
        let mut trace_id_ar = [0u8; 32];
        let mut span_id_ar = [0u8; 16];
        let mut parent_id_ar = [0u8; 16];

        let mut payload_builder = ClickhousePayloadBuilder::new(self.compression.clone());
        for rs in input {
            let res_attrs = rs.resource.unwrap_or_default().attributes;
            let res_attrs = cvattr::convert_into(res_attrs);
            let service_name = find_attribute(SERVICE_NAME, &res_attrs);
            let res_attrs_field = self.transform_attrs(&res_attrs);

            for ss in rs.scope_spans {
                let (scope_name, scope_version) = match ss.scope {
                    Some(scope) => (scope.name, scope.version),
                    None => (String::new(), String::new()),
                };

                for span in ss.spans {
                    let status_message = match &span.status {
                        None => "",
                        Some(s) => s.message.as_str(),
                    };

                    let (status_code, span_attrs) =
                        (status_code(&span), cvattr::convert_into(span.attributes));

                    //
                    // Events
                    //
                    let events_count = span.events.len();
                    let mut events_timestamp = Vec::with_capacity(events_count);
                    let mut events_name = Vec::with_capacity(events_count);
                    let mut events_attrs = Vec::with_capacity(events_count);

                    for event in span.events {
                        events_timestamp.push(event.time_unix_nano);
                        events_name.push(event.name);
                        events_attrs.push(cvattr::convert_into(event.attributes));
                    }
                    let events_attributes = events_attrs
                        .iter()
                        .map(|attr| self.transform_attrs(&attr))
                        .collect();

                    //
                    // Links
                    //
                    let links_count = span.links.len();
                    let mut links_trace_id = Vec::with_capacity(links_count);
                    let mut links_span_id = Vec::with_capacity(links_count);
                    let mut links_trace_state = Vec::with_capacity(links_count);
                    let mut links_attrs = Vec::with_capacity(links_count);

                    for link in span.links {
                        links_trace_id.push(hex::encode(&link.trace_id));
                        links_span_id.push(hex::encode(&link.span_id));
                        links_trace_state.push(link.trace_state);
                        links_attrs.push(cvattr::convert_into(link.attributes));
                    }
                    let links_attributes = links_attrs
                        .iter()
                        .map(|attr| self.transform_attrs(&attr))
                        .collect();

                    // Encode these to stack arrays to reduce memory churn
                    let trace_id = encode_id(&span.trace_id, &mut trace_id_ar);
                    let span_id = encode_id(&span.span_id, &mut span_id_ar);
                    let parent_span_id = encode_id(&span.parent_span_id, &mut parent_id_ar);

                    // avoid overflow
                    let duration = if span.end_time_unix_nano > span.start_time_unix_nano {
                        (span.end_time_unix_nano - span.start_time_unix_nano) as i64
                    } else {
                        0
                    };

                    let row = SpanRow {
                        timestamp: span.start_time_unix_nano,
                        trace_id,
                        span_id,
                        parent_span_id,
                        trace_state: span.trace_state,
                        span_name: span.name,
                        span_kind: span_kind_to_string(span.kind),
                        service_name: &service_name,
                        resource_attributes: &res_attrs_field,
                        scope_name: &scope_name,
                        scope_version: &scope_version,
                        span_attributes: self.transform_attrs(&span_attrs),
                        duration,
                        status_code,
                        status_message,
                        events_timestamp,
                        events_name,
                        events_attributes,
                        links_trace_id,
                        links_span_id,
                        links_trace_state,
                        links_attributes,
                    };

                    payload_builder.add_row(&row)?;
                }
            }
        }

        payload_builder
            .finish()
            .map(|payload| vec![(RequestType::Traces, payload)])
    }
}

fn encode_id<'a>(id: &[u8], out: &'a mut [u8]) -> &'a str {
    match hex::encode_to_slice(id, out) {
        Ok(_) => {
            // We can be pretty sure the encoded string is utf8 safe
            std::str::from_utf8(out).unwrap_or_default()
        }
        Err(_) => {
            // Trace and Span IDs are required to have a certain length (8 or 16 bytes), the only
            // case this should fail is on an empty ID, like parent_span_id on a root span.
            ""
        }
    }
}

fn span_kind_to_string<'a>(kind: i32) -> &'a str {
    if kind == SpanKind::Internal as i32 {
        "Internal"
    } else if kind == SpanKind::Server as i32 {
        "Server"
    } else if kind == SpanKind::Client as i32 {
        "Client"
    } else if kind == SpanKind::Producer as i32 {
        "Producer"
    } else if kind == SpanKind::Consumer as i32 {
        "Consumer"
    } else {
        "Unspecified"
    }
}

fn status_code<'a>(span: &Span) -> &'a str {
    match &span.status {
        None => "Unset",
        Some(s) => match s.code {
            1 => "Ok",
            2 => "Error",
            _ => "Unset",
        },
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::exporters::clickhouse::transformer::Transformer;
    use opentelemetry_proto::tonic::common::v1::AnyValue;
    use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
    use opentelemetry_proto::tonic::common::v1::KeyValue;
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ScopeSpans, Status};

    #[test]
    fn test_negative_duration_handling() {
        let transformer =
            Transformer::new(crate::exporters::clickhouse::Compression::Lz4, true, false);

        // Create a span where end_time is before start_time
        let span = Span {
            trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
            parent_span_id: vec![],
            name: "test_span".to_string(),
            kind: SpanKind::Internal as i32,
            start_time_unix_nano: 1000000000, // Later time
            end_time_unix_nano: 500000000,    // Earlier time
            attributes: vec![],
            events: vec![],
            links: vec![],
            status: Some(Status {
                code: 1,
                message: "".to_string(),
            }),
            trace_state: "".to_string(),
            ..Default::default()
        };

        let scope_spans = ScopeSpans {
            scope: Some(InstrumentationScope {
                name: "test_scope".to_string(),
                version: "1.0".to_string(),
                ..Default::default()
            }),
            spans: vec![span],
            ..Default::default()
        };

        let resource_spans = ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: SERVICE_NAME.to_string(),
                    value: Some(AnyValue {
                        value: Some(
                            opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                                "test_service".to_string(),
                            ),
                        ),
                    }),
                }],
                ..Default::default()
            }),
            scope_spans: vec![scope_spans],
            ..Default::default()
        };

        // This should not panic and should handle the negative duration gracefully
        let result = transformer.transform(vec![resource_spans]);

        // Verify the transformation succeeded
        assert!(
            result.is_ok(),
            "Transform should not panic with negative duration"
        );

        let payloads = result.unwrap();
        assert_eq!(payloads.len(), 1);
    }
}
