use crate::receivers::otlp_output::OTLPOutput;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::metrics::Temporality;
use std::future::Future;

pub struct InternalOTLPMetricsExporter {
    pub metrics_output: Option<OTLPOutput<Vec<ResourceMetrics>>>,
}

impl InternalOTLPMetricsExporter {
    pub fn new(metrics_output: Option<OTLPOutput<Vec<ResourceMetrics>>>) -> Self {
        Self { metrics_output }
    }
}

impl PushMetricExporter for InternalOTLPMetricsExporter {
    async fn export(
        &self,
        metrics: &mut ResourceMetrics,
    ) -> impl Future<Output = OTelSdkResult> + Send {
        match &self.metrics_output {
            None => {}
            Some(mo) => {
                let req: opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest = (&*metrics).into();
                let res = mo.send(req.resource_metrics).await;
                match res {
                    Ok(_) => Ok(()),
                    Err(e) => OTelSdkError::InternalFailure(e.to_string())
                }
            }
        }

        todo!()
    }

    fn force_flush(&self) -> OTelSdkResult {
        todo!()
    }

    fn shutdown(&self) -> OTelSdkResult {
        todo!()
    }

    fn temporality(&self) -> Temporality {
        todo!()
    }
}
