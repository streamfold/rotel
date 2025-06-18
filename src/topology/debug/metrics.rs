use crate::topology::batch::BatchSizer;
use crate::topology::debug::DebugLoggable;
use crate::topology::debug::debug_buffer::DebugBuffer;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use tracing::{Level, event};

impl DebugLoggable for [ResourceMetrics] {
    fn log_basic(&self) {
        event!(
            Level::INFO,
            data_type = "metrics",
            resource_metrics = self.len(),
            metrics = self.size_of(),
            "Received metrics."
        );
    }

    fn log_detailed(&self) -> DebugBuffer {
        let mut buf = DebugBuffer::new();

        for (i, rm) in self.iter().enumerate() {
            buf.log_entry(format!("ResourceMetrics #{}", i));
            buf.log_entry(format!("Resource SchemaURL: {}", rm.schema_url));
            if let Some(resource) = &rm.resource {
                buf.log_attributes("Resource attributes", &resource.attributes);
            }

            for (j, ilm) in rm.scope_metrics.iter().enumerate() {
                buf.log_entry(format!("ScopeMetrics #{}", j));
                buf.log_entry(format!("ScopeMetrics SchemaURL: {}", ilm.schema_url));
                if let Some(scope) = &ilm.scope {
                    buf.log_instrumentation_scope(scope);
                }

                for (k, metric) in ilm.metrics.iter().enumerate() {
                    buf.log_entry(format!("Metric #{}", k));
                    buf.log_metric_descriptor(metric);
                    buf.log_metric_data_points(metric);
                }
            }
        }

        buf
    }
}
