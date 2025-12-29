use crate::exporters::clickhouse::payload::{ClickhousePayload, ClickhousePayloadBuilder};
use crate::exporters::clickhouse::request_builder::TransformPayload;
use crate::exporters::clickhouse::request_mapper::RequestType;
use crate::exporters::clickhouse::schema::LogRecordRow;
use crate::exporters::clickhouse::transformer::{Transformer, encode_id, find_str_attribute_kv};
use crate::topology::payload::{Message, MessageMetadata};
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_semantic_conventions::resource::SERVICE_NAME;
use tower::BoxError;

impl TransformPayload<ResourceLogs> for Transformer {
    fn transform(
        &self,
        input: Vec<Message<ResourceLogs>>,
    ) -> (
        Result<Vec<(RequestType, ClickhousePayload)>, BoxError>,
        Option<Vec<MessageMetadata>>,
    ) {
        let mut payload_builder = ClickhousePayloadBuilder::new(self.compression.clone());
        let mut all_metadata = Vec::new();

        for message in input {
            if let Some(metadata) = message.metadata {
                all_metadata.push(metadata);
            }
            for rl in message.payload {
                let res_attrs = rl.resource.unwrap_or_default().attributes;
                let service_name = find_str_attribute_kv(SERVICE_NAME, &res_attrs);
                let res_attrs_field = self.transform_attrs_kv(&res_attrs);

                let res_schema_url = rl.schema_url;

                for sl in rl.scope_logs {
                    let (scope_name, scope_version, scope_attrs) = match sl.scope {
                        Some(scope) => (scope.name, scope.version, scope.attributes),
                        None => (String::new(), String::new(), Vec::new()),
                    };

                    let scope_attrs = self.transform_attrs_kv(&scope_attrs);

                    for log in sl.log_records {
                        let log_attrs = log.attributes;

                        let body_str = match log.body {
                            None => String::new(),
                            Some(av) => match av.value {
                                Some(Value::StringValue(s)) => s,
                                Some(Value::BoolValue(b)) => b.to_string(),
                                Some(Value::IntValue(i)) => i.to_string(),
                                Some(Value::DoubleValue(d)) => d.to_string(),
                                Some(Value::ArrayValue(a)) => serde_json::json!(a).to_string(),
                                Some(Value::KvlistValue(kv)) => serde_json::json!(kv).to_string(),
                                Some(Value::BytesValue(b)) => hex::encode(b),
                                None => String::new(),
                            },
                        };

                        let mut trace_id_ar = [0u8; 32];
                        let mut span_id_ar = [0u8; 16];
                        let trace_id = encode_id(&log.trace_id, &mut trace_id_ar);
                        let span_id = encode_id(&log.span_id, &mut span_id_ar);

                        let row = LogRecordRow {
                            timestamp: log.time_unix_nano,
                            trace_id,
                            span_id,
                            trace_flags: (log.flags & 0x000000FF) as u8,
                            severity_text: log.severity_text,
                            severity_number: (log.severity_number & 0x000000FF) as u8,
                            service_name: &service_name,
                            body: body_str,
                            resource_schema_url: &res_schema_url,
                            resource_attributes: &res_attrs_field,
                            scope_schema_url: &sl.schema_url,
                            scope_name: &scope_name,
                            scope_version: &scope_version,
                            scope_attributes: &scope_attrs,
                            log_attributes: self.transform_attrs_kv(&log_attrs),
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
            .map(|payload| vec![(RequestType::Logs, payload)]);

        (result, None)
    }
}
