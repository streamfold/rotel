use crate::receivers::otlp_output::OTLPOutput;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::metrics::Temporality;

pub struct InternalOTLPMetricsExporter {
    pub metrics_output:
        Option<OTLPOutput<Vec<opentelemetry_proto::tonic::metrics::v1::ResourceMetrics>>>,
    pub temporality: Temporality,
}

impl InternalOTLPMetricsExporter {
    pub fn new(
        metrics_output: Option<
            OTLPOutput<Vec<opentelemetry_proto::tonic::metrics::v1::ResourceMetrics>>,
        >,
        temporality: Temporality,
    ) -> Self {
        Self {
            metrics_output,
            temporality,
        }
    }
}

impl PushMetricExporter for InternalOTLPMetricsExporter {
    async fn export(&self, metrics: &mut ResourceMetrics) -> OTelSdkResult {
        match &self.metrics_output {
            None => Err(OTelSdkError::InternalFailure(
                "metrics have been disabled".to_string(),
            )),
            Some(mo) => {
                let req = ExportMetricsServiceRequest::from(&*metrics);
                let res = mo.send(req.resource_metrics).await;
                match res {
                    Ok(_) => Ok(()),
                    Err(e) => Err(OTelSdkError::InternalFailure(e.into())),
                }
            }
        }
    }

    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }

    fn shutdown(&self) -> OTelSdkResult {
        Ok(())
    }

    fn temporality(&self) -> Temporality {
        self.temporality
    }
}
