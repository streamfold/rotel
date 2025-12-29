use crate::exporters::clickhouse::payload::{ClickhousePayload, ClickhousePayloadBuilder};
use crate::exporters::clickhouse::request_builder::TransformPayload;
use crate::exporters::clickhouse::request_mapper::RequestType;
use crate::exporters::clickhouse::schema::SpanRow;
use crate::exporters::clickhouse::transformer::{Transformer, encode_id, find_str_attribute_kv};
use crate::topology::payload::{Message, MessageMetadata};
use opentelemetry_proto::tonic::trace::v1::span::SpanKind;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, Span};
use opentelemetry_semantic_conventions::resource::SERVICE_NAME;
use tower::BoxError;

impl TransformPayload<ResourceSpans> for Transformer {
    fn transform(
        &self,
        input: Vec<Message<ResourceSpans>>,
    ) -> (
        Result<Vec<(RequestType, ClickhousePayload)>, BoxError>,
        Option<Vec<MessageMetadata>>,
    ) {
        let mut trace_id_ar = [0u8; 32];
        let mut span_id_ar = [0u8; 16];
        let mut parent_id_ar = [0u8; 16];

        let mut payload_builder = ClickhousePayloadBuilder::new(self.compression.clone());
        let mut all_metadata = Vec::new();

        for message in input {
            if let Some(metadata) = message.metadata {
                all_metadata.push(metadata);
            }
            for rs in message.payload {
                let res_attrs = rs.resource.unwrap_or_default().attributes;
                let service_name = find_str_attribute_kv(SERVICE_NAME, &res_attrs);
                let res_attrs_field = self.transform_attrs_kv(&res_attrs);

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

                        let (status_code, span_attrs) = (status_code(&span), span.attributes);

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
                            events_attrs.push(event.attributes);
                        }
                        let events_attributes = events_attrs
                            .iter()
                            .map(|attr| self.transform_attrs_kv(&attr))
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
                            links_attrs.push(link.attributes);
                        }
                        let links_attributes = links_attrs
                            .iter()
                            .map(|attr| self.transform_attrs_kv(&attr))
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
                            span_attributes: self.transform_attrs_kv(&span_attrs),
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

                        match payload_builder.add_row(&row) {
                            Ok(_) => {}
                            Err(e) => return (Err(e), None),
                        }
                    }
                }
            }
        }

        let metadata = if all_metadata.is_empty() {
            None
        } else {
            Some(all_metadata)
        };

        let result = payload_builder
            .finish_with_metadata(metadata)
            .map(|payload| vec![(RequestType::Traces, payload)]);

        (result, None)
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
        let transformer = Transformer::new(crate::exporters::clickhouse::Compression::Lz4, true);

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
        let (result, _metadata) = transformer.transform(vec![Message {
            payload: vec![resource_spans],
            metadata: None,
        }]);

        // Verify the transformation succeeded
        assert!(
            result.is_ok(),
            "Transform should not panic with negative duration"
        );

        let payloads = result.unwrap();
        assert_eq!(payloads.len(), 1);
    }

    #[test]
    fn test_keyvalue_attributes_no_conversion() {
        use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueValue;

        let transformer = Transformer::new(crate::exporters::clickhouse::Compression::Lz4, true);

        // Create a span with various attribute types
        let span = Span {
            trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
            parent_span_id: vec![],
            name: "test_span".to_string(),
            kind: SpanKind::Internal as i32,
            start_time_unix_nano: 1000000000,
            end_time_unix_nano: 2000000000,
            attributes: vec![
                KeyValue {
                    key: "string_attr".to_string(),
                    value: Some(AnyValue {
                        value: Some(AnyValueValue::StringValue("test_value".to_string())),
                    }),
                },
                KeyValue {
                    key: "int_attr".to_string(),
                    value: Some(AnyValue {
                        value: Some(AnyValueValue::IntValue(42)),
                    }),
                },
                KeyValue {
                    key: "double_attr".to_string(),
                    value: Some(AnyValue {
                        value: Some(AnyValueValue::DoubleValue(3.14)),
                    }),
                },
                KeyValue {
                    key: "bool_attr".to_string(),
                    value: Some(AnyValue {
                        value: Some(AnyValueValue::BoolValue(true)),
                    }),
                },
            ],
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
                        value: Some(AnyValueValue::StringValue("test_service".to_string())),
                    }),
                }],
                ..Default::default()
            }),
            scope_spans: vec![scope_spans],
            ..Default::default()
        };

        // Transform and verify it succeeds
        let (result, _metadata) = transformer.transform(vec![Message {
            payload: vec![resource_spans],
            metadata: None,
        }]);

        // Verify the transformation succeeded
        assert!(
            result.is_ok(),
            "Transform should succeed with KeyValue attributes"
        );

        let payloads = result.unwrap();
        assert_eq!(payloads.len(), 1);
    }

    #[test]
    fn test_keyvalue_events_and_links() {
        use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueValue;
        use opentelemetry_proto::tonic::trace::v1::span::Event;
        use opentelemetry_proto::tonic::trace::v1::span::Link;

        let transformer = Transformer::new(crate::exporters::clickhouse::Compression::None, false);

        // Create a span with events and links
        let span = Span {
            trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
            parent_span_id: vec![],
            name: "test_span".to_string(),
            kind: SpanKind::Server as i32,
            start_time_unix_nano: 1000000000,
            end_time_unix_nano: 2000000000,
            attributes: vec![],
            events: vec![Event {
                time_unix_nano: 1500000000,
                name: "test_event".to_string(),
                attributes: vec![KeyValue {
                    key: "event_attr".to_string(),
                    value: Some(AnyValue {
                        value: Some(AnyValueValue::StringValue("event_value".to_string())),
                    }),
                }],
                dropped_attributes_count: 0,
            }],
            links: vec![Link {
                trace_id: vec![9, 10, 11, 12, 13, 14, 15, 16, 1, 2, 3, 4, 5, 6, 7, 8],
                span_id: vec![9, 10, 11, 12, 13, 14, 15, 16],
                trace_state: "state".to_string(),
                attributes: vec![KeyValue {
                    key: "link_attr".to_string(),
                    value: Some(AnyValue {
                        value: Some(AnyValueValue::IntValue(123)),
                    }),
                }],
                dropped_attributes_count: 0,
                flags: 0,
            }],
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
                        value: Some(AnyValueValue::StringValue("test_service".to_string())),
                    }),
                }],
                ..Default::default()
            }),
            scope_spans: vec![scope_spans],
            ..Default::default()
        };

        // Transform and verify it succeeds
        let (result, _metadata) = transformer.transform(vec![Message {
            payload: vec![resource_spans],
            metadata: None,
        }]);

        assert!(
            result.is_ok(),
            "Transform should succeed with events and links"
        );

        let payloads = result.unwrap();
        assert_eq!(payloads.len(), 1);
    }

    #[test]
    fn test_kvlist_flattening_in_spans() {
        use opentelemetry_proto::tonic::common::v1::KeyValueList;
        use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueValue;

        let transformer = Transformer::new(crate::exporters::clickhouse::Compression::None, true);

        // Create a span with nested KvlistValue attributes
        let span = Span {
            trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
            parent_span_id: vec![],
            name: "test_span_with_nested_attrs".to_string(),
            kind: SpanKind::Internal as i32,
            start_time_unix_nano: 1000000000,
            end_time_unix_nano: 2000000000,
            attributes: vec![
                KeyValue {
                    key: "simple_attr".to_string(),
                    value: Some(AnyValue {
                        value: Some(AnyValueValue::StringValue("simple_value".to_string())),
                    }),
                },
                KeyValue {
                    key: "http".to_string(),
                    value: Some(AnyValue {
                        value: Some(AnyValueValue::KvlistValue(KeyValueList {
                            values: vec![
                                KeyValue {
                                    key: "method".to_string(),
                                    value: Some(AnyValue {
                                        value: Some(AnyValueValue::StringValue("GET".to_string())),
                                    }),
                                },
                                KeyValue {
                                    key: "status_code".to_string(),
                                    value: Some(AnyValue {
                                        value: Some(AnyValueValue::IntValue(200)),
                                    }),
                                },
                                KeyValue {
                                    key: "headers".to_string(),
                                    value: Some(AnyValue {
                                        value: Some(AnyValueValue::KvlistValue(KeyValueList {
                                            values: vec![KeyValue {
                                                key: "content_type".to_string(),
                                                value: Some(AnyValue {
                                                    value: Some(AnyValueValue::StringValue(
                                                        "application/json".to_string(),
                                                    )),
                                                }),
                                            }],
                                        })),
                                    }),
                                },
                            ],
                        })),
                    }),
                },
            ],
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
                        value: Some(AnyValueValue::StringValue("test_service".to_string())),
                    }),
                }],
                ..Default::default()
            }),
            scope_spans: vec![scope_spans],
            ..Default::default()
        };

        // Transform and verify it succeeds with flattened attributes
        let (result, _metadata) = transformer.transform(vec![Message {
            payload: vec![resource_spans],
            metadata: None,
        }]);

        assert!(
            result.is_ok(),
            "Transform should succeed with nested KvlistValue attributes"
        );

        let payloads = result.unwrap();
        assert_eq!(payloads.len(), 1);
        // The actual flattening is verified by the transformer tests
        // This test ensures the integration works end-to-end
    }
}
