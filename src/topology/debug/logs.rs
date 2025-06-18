use crate::topology::batch::BatchSizer;
use crate::topology::debug::DebugLoggable;
use crate::topology::debug::debug_buffer::{DebugBuffer, value_to_string};
use opentelemetry_proto::tonic::logs::v1::{ResourceLogs, SeverityNumber};
use tracing::{Level, event};

impl DebugLoggable for [ResourceLogs] {
    fn log_basic(&self) {
        event!(
            Level::INFO,
            data_type = "logs",
            resource_logs = self.len(),
            logs = self.size_of(),
            "Received logs."
        );
    }

    fn log_detailed(&self) -> DebugBuffer {
        let mut buf = DebugBuffer::new();

        for (i, rl) in self.iter().enumerate() {
            buf.log_entry(format!("ResourceLog #{}", i));
            buf.log_entry(format!("Resource SchemaURL: {}", rl.schema_url));
            if let Some(resource) = &rl.resource {
                buf.log_attributes("Resource attributes", &resource.attributes);
            }

            for (j, ils) in rl.scope_logs.iter().enumerate() {
                buf.log_entry(format!("ScopeLogs #{}", j));
                buf.log_entry(format!("ScopeLogs SchemaURL: {}", ils.schema_url));
                if let Some(scope) = &ils.scope {
                    buf.log_instrumentation_scope(scope);
                }

                for (k, lr) in ils.log_records.iter().enumerate() {
                    buf.log_entry(format!("LogRecord #{}", k));
                    buf.log_entry(format!("ObservedTimestamp: {}", lr.observed_time_unix_nano));
                    buf.log_entry(format!("Timestamp: {}", lr.time_unix_nano));
                    buf.log_entry(format!("SeverityText: {}", lr.severity_text));
                    if let Ok(severity) = SeverityNumber::try_from(lr.severity_number) {
                        buf.log_entry(format!(
                            "SeverityNumber: {}({})",
                            severity.as_str_name(),
                            lr.severity_number
                        ));
                    }
                    if !lr.event_name.is_empty() {
                        buf.log_entry(format!("EventName: {}", lr.event_name));
                    }
                    buf.log_entry(format!("Body: {}", value_to_string(lr.body.as_ref())));
                    buf.log_attributes("Attributes", &lr.attributes);
                    buf.log_attr("Trace ID", &hex::encode(&lr.trace_id));
                    buf.log_attr("Span ID", &hex::encode(&lr.span_id));
                    buf.log_entry(format!("Flags: {}", lr.flags));
                }
            }
        }

        buf
    }
}
