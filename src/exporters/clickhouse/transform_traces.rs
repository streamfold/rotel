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
        let mut payload_builder = ClickhousePayloadBuilder::new(self.compression.clone());
        for rs in input {
            let res_attrs = rs.resource.unwrap_or_default().attributes;
            let res_attrs = cvattr::convert_into(res_attrs);
            let res_attrs_field = self.transform_attrs(&res_attrs);
            let service_name = find_attribute(SERVICE_NAME, &res_attrs);

            for ss in rs.scope_spans {
                let (scope_name, scope_version) = match ss.scope {
                    Some(scope) => (scope.name, scope.version),
                    None => (String::new(), String::new()),
                };

                for span in ss.spans {
                    let (status_code, status_message, span_attrs) = (
                        status_code(&span),
                        status_message(&span),
                        cvattr::convert_into(span.attributes),
                    );

                    let evt_props: Vec<_> = span
                        .events
                        .into_iter()
                        .map(|e| {
                            let timestamp = e.time_unix_nano;
                            let name = Cow::Owned(e.name);
                            let evt_attrs = cvattr::convert_into(e.attributes);
                            let evt_attrs = self.transform_attrs(&evt_attrs);

                            (timestamp, (name, evt_attrs))
                        })
                        .collect();

                    let (events_timestamp, name_and_attrs): (Vec<_>, Vec<_>) =
                        evt_props.into_iter().unzip();
                    let (events_name, events_attributes): (Vec<_>, Vec<_>) =
                        name_and_attrs.into_iter().unzip();

                    let row = SpanRow {
                        timestamp: span.start_time_unix_nano,
                        trace_id: hex::encode(span.trace_id),
                        span_id: hex::encode(span.span_id),
                        parent_span_id: hex::encode(span.parent_span_id),
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
                        status_message,
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
        "client"
    } else {
        "Unspecified"
    }
}

fn status_message(span: &Span) -> String {
    match &span.status {
        None => "".to_string(),
        Some(s) => s.message.clone(),
    }
}

fn status_code(span: &Span) -> String {
    match &span.status {
        None => "Unset".to_string(),
        Some(s) => match s.code {
            1 => "Ok".to_string(),
            2 => "Error".to_string(),
            _ => "Unset".to_string(),
        },
    }
}
