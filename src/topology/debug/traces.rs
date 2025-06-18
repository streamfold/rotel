use crate::topology::batch::BatchSizer;
use crate::topology::debug::DebugLoggable;
use crate::topology::debug::debug_buffer::DebugBuffer;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use opentelemetry_proto::tonic::trace::v1::span::SpanKind;
use opentelemetry_proto::tonic::trace::v1::status::StatusCode;
use tracing::{Level, event};

impl DebugLoggable for [ResourceSpans] {
    fn log_basic(&self) {
        event!(
            Level::INFO,
            data_type = "traces",
            resource_spans = self.len(),
            spans = self.size_of(),
            "Received traces."
        );
    }

    fn log_detailed(&self) -> DebugBuffer {
        let mut buf = DebugBuffer::new();

        for (i, rs) in self.iter().enumerate() {
            buf.log_entry(format!("ResourceSpans #{}", i));
            buf.log_entry(format!("Resource SchemaURL: {}", rs.schema_url));

            if let Some(resource) = &rs.resource {
                buf.log_attributes("Resource attributes", &resource.attributes);
            }

            for (j, ils) in rs.scope_spans.iter().enumerate() {
                buf.log_entry(format!("ScopeSpans #{}", j));
                buf.log_entry(format!("ScopeSpans SchemaURL: {}", ils.schema_url));

                if let Some(scope) = &ils.scope {
                    buf.log_instrumentation_scope(scope);
                }

                for (k, span) in ils.spans.iter().enumerate() {
                    buf.log_entry(format!("Span #{}", k));
                    buf.log_attr("Trace ID", &hex::encode(&span.trace_id));
                    buf.log_attr("Parent ID", &hex::encode(&span.parent_span_id));
                    buf.log_attr("ID", &hex::encode(&span.span_id));
                    buf.log_attr("Name", &span.name);

                    if let Ok(sk) = SpanKind::try_from(span.kind) {
                        buf.log_attr("Kind", &sk.as_str_name());
                    }

                    if !span.trace_state.is_empty() {
                        buf.log_attr("TraceState", &span.trace_state);
                    }

                    buf.log_entry(format!("Start time: {}", span.start_time_unix_nano));
                    buf.log_entry(format!("End time: {}", span.end_time_unix_nano));

                    if let Some(status) = &span.status {
                        if let Ok(status) = StatusCode::try_from(status.code) {
                            buf.log_attr("Status code", &status.as_str_name());
                        }
                        buf.log_attr("Status message", &status.message);
                    }

                    buf.log_attributes("Attributes", &span.attributes);
                    buf.log_events("Events", &span.events);
                    buf.log_links("Links", &span.links);
                }
            }
        }

        buf
    }
}
