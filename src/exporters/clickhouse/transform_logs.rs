use crate::exporters::clickhouse::payload::{ClickhousePayload, ClickhousePayloadBuilder};
use crate::exporters::clickhouse::request_builder::TransformPayload;
use crate::exporters::clickhouse::request_mapper::RequestType;
use crate::exporters::clickhouse::schema::LogRecordRow;
use crate::exporters::clickhouse::transformer::{Transformer, encode_id, find_attribute};
use crate::otlp::cvattr;
use crate::otlp::cvattr::ConvertedAttrValue;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_semantic_conventions::resource::SERVICE_NAME;
use tower::BoxError;

impl TransformPayload<ResourceLogs> for Transformer {
    fn transform(
        &self,
        input: Vec<ResourceLogs>,
    ) -> Result<Vec<(RequestType, ClickhousePayload)>, BoxError> {
        let mut payload_builder = ClickhousePayloadBuilder::new(self.compression.clone());
        for rl in input {
            let res_attrs = rl.resource.unwrap_or_default().attributes;
            let res_attrs = cvattr::convert_into(res_attrs);
            let service_name = find_attribute(SERVICE_NAME, &res_attrs);
            let res_attrs_field = self.transform_attrs(&res_attrs);

            let res_schema_url = rl.schema_url;

            for sl in rl.scope_logs {
                let (scope_name, scope_version, scope_attrs) = match sl.scope {
                    Some(scope) => (
                        scope.name,
                        scope.version,
                        cvattr::convert_into(scope.attributes),
                    ),
                    None => (String::new(), String::new(), Vec::new()),
                };

                let scope_attrs = self.transform_attrs(&scope_attrs);

                for log in sl.log_records {
                    let log_attrs = cvattr::convert_into(log.attributes);

                    let body_conv: Option<ConvertedAttrValue> = match log.body {
                        None => None,
                        Some(av) => av.value.map(|v| v.into()),
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
                        body: body_conv.map(|av| av.to_string()).unwrap_or_default(),
                        resource_schema_url: &res_schema_url,
                        resource_attributes: &res_attrs_field,
                        scope_schema_url: &sl.schema_url,
                        scope_name: &scope_name,
                        scope_version: &scope_version,
                        scope_attributes: &scope_attrs,
                        log_attributes: self.transform_attrs(&log_attrs),
                    };

                    payload_builder.add_row(&row)?;
                }
            }
        }

        payload_builder
            .finish()
            .map(|payload| vec![(RequestType::Logs, payload)])
    }
}
