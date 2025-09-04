use crate::receivers::otlp_output::OTLPOutput;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::metrics::Temporality;
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;

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
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        match &self.metrics_output {
            // TODO
            // So here we're using the main metrics OTLPOutput and that has a dependency on whether metrics have been disabled
            // at the receiver. So that means if metrics receiving is disabled for this rotel instance, so is the metrics pipeline.
            // However, we may want to special case that an allow a metrics pipeline to run but only for internal metrics.
            None => Ok(()),
            Some(mo) => {
                let req = ExportMetricsServiceRequest::from(&*metrics);
                let res = mo.send(req.resource_metrics).await;
                match res {
                    Ok(_) => Ok(()),
                    Err(e) => Err(OTelSdkError::InternalFailure(e.to_string())),
                }
            }
        }
    }

    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }

    fn shutdown_with_timeout(&self, _timeout: std::time::Duration) -> OTelSdkResult {
        // TODO, do we want to manually drop the reference to metrics_output or set to None?
        // Technically any call to export() after shutting down should return an error
        Ok(())
    }

    fn temporality(&self) -> Temporality {
        self.temporality
    }
}

#[cfg(test)]
mod tests {
    use crate::bounded_channel::bounded;
    use crate::telemetry;
    use opentelemetry::global;
    use opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue;
    use opentelemetry_proto::tonic::metrics::v1::metric::Data;
    use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value;
    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::metrics::Temporality;

    #[tokio::test]
    async fn test_internal_otlp_metrics_exporter() {
        let (tx, mut rx) = bounded(1);
        let metrics_output = telemetry::internal_exporter::OTLPOutput::new(tx.clone());

        let internal_metrics_exporter =
            telemetry::internal_exporter::InternalOTLPMetricsExporter::new(
                Some(metrics_output.clone()),
                Temporality::Cumulative,
            );

        let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
            .with_periodic_exporter(internal_metrics_exporter)
            .with_resource(Resource::builder().with_service_name("rotel").build())
            .build();
        global::set_meter_provider(meter_provider.clone());

        let test_counter = global::meter("rotel_internal_metrics_test_counter")
            .u64_counter("rotel_test_counter")
            .with_description("This is a test")
            .with_unit("test")
            .build();

        test_counter.add(
            100,
            &[opentelemetry::KeyValue::new("test_key", "test_value")],
        );

        meter_provider.force_flush().unwrap();

        let mut vrm = rx.next().await.unwrap();
        assert_eq!(vrm.len(), 1);
        let resource_metrics = vrm.pop().unwrap();
        let resource = resource_metrics.resource;
        assert!(resource.is_some());
        let attributes = resource.unwrap().attributes;
        assert_eq!(attributes.len(), 4);
        for attr in attributes {
            if attr.key == "service.name" {
                assert_eq!(
                    attr.value.unwrap().value.unwrap(),
                    StringValue("rotel".to_string())
                );
            }
        }
        let mut scopes = resource_metrics.scope_metrics;
        assert_eq!(scopes.len(), 1);
        let scope = scopes.pop().unwrap();
        let inst_scope = scope.scope.unwrap();
        assert_eq!("rotel_internal_metrics_test_counter", inst_scope.name);
        let mut metrics = scope.metrics;
        assert_eq!(metrics.len(), 1);
        let metric = metrics.pop().unwrap();
        assert_eq!("rotel_test_counter", metric.name);
        assert_eq!("This is a test", metric.description);
        assert_eq!("test", metric.unit);
        let data = metric.data.unwrap();
        match data {
            Data::Sum(mut d) => {
                assert_eq!(d.data_points.len(), 1);
                let v = d.data_points.pop().unwrap().value.unwrap();
                match v {
                    Value::AsInt(i) => {
                        assert_eq!(i, 100);
                    }
                    _value => {
                        panic!("unexpected value type");
                    }
                }
                println!("{:?}", v)
            }
            _ => panic!("unexpected type"),
        }
    }
}
