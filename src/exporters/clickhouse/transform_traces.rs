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

                    let (status_code, span_attrs) = (
                        Cow::Borrowed(status_code(&span)),
                        cvattr::convert_into(span.attributes),
                    );

                    let events_count = span.events.len();
                    let mut events_timestamp = Vec::with_capacity(events_count);
                    let mut events_name = Vec::with_capacity(events_count);
                    let mut events_attributes = Vec::with_capacity(events_count);

                    for event in span.events {
                        events_timestamp.push(event.time_unix_nano);
                        events_name.push(event.name);
                        let evt_attrs = cvattr::convert_into(event.attributes);
                        events_attributes.push(self.transform_attrs(&evt_attrs));
                    }

                    // Encode these to stack arrays to reduce memory churn
                    let trace_id = encode_id(&span.trace_id, &mut trace_id_ar);
                    let span_id = encode_id(&span.span_id, &mut span_id_ar);
                    let parent_span_id = encode_id(&span.parent_span_id, &mut parent_id_ar);

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
                        duration: (span.end_time_unix_nano - span.start_time_unix_nano) as i64,
                        status_code,
                        status_message: Cow::Borrowed(status_message),
                        events_timestamp,
                        events_name,
                        events_attributes,
                        // TODO: use into_iter() form for links too
                        links_trace_id: span
                            .links
                            .iter()
                            .map(|l| hex::encode(&l.trace_id))
                            .collect(),
                        links_span_id: span.links.iter().map(|l| hex::encode(&l.span_id)).collect(),
                        links_trace_state: span
                            .links
                            .iter()
                            .map(|l| Cow::Borrowed(l.trace_state.as_str()))
                            .collect(),
                        links_attributes: span
                            .links
                            .iter()
                            .map(|l| {
                                let link_attrs = cvattr::convert(&l.attributes);
                                self.transform_attrs(&link_attrs)
                            })
                            .collect(),
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
            let trace_id = std::str::from_utf8(out).unwrap_or_default();
            trace_id
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
