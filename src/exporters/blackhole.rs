// SPDX-License-Identifier: Apache-2.0

use crate::bounded_channel::BoundedReceiver;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use tokio::select;
use tokio_util::sync::CancellationToken;

pub struct BlackholeExporter {
    traces_rx: BoundedReceiver<Vec<ResourceSpans>>,
    metrics_rx: BoundedReceiver<Vec<ResourceMetrics>>,
}

impl BlackholeExporter {
    pub fn new(
        traces_rx: BoundedReceiver<Vec<ResourceSpans>>,
        metrics_rx: BoundedReceiver<Vec<ResourceMetrics>>,
    ) -> Self {
        BlackholeExporter {
            traces_rx,
            metrics_rx,
        }
    }

    pub async fn start(&mut self, cancel_token: CancellationToken) {
        loop {
            select! {
                _ = self.traces_rx.next() => {},
                _ = self.metrics_rx.next() => {},
                _ = cancel_token.cancelled() => break,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::bounded_channel::bounded;
    use crate::exporters::blackhole::BlackholeExporter;
    use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
    use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
    use tokio::{join, spawn};
    use tokio_util::sync::CancellationToken;
    use utilities::otlp::FakeOTLP;

    #[tokio::test]
    async fn send_request() {
        let (tr_tx, tr_rx) = bounded::<Vec<ResourceSpans>>(1);
        let (met_tx, met_rx) = bounded::<Vec<ResourceMetrics>>(1);

        let mut exp = BlackholeExporter::new(tr_rx, met_rx);

        let cancel_token = CancellationToken::new();
        let shut_token = cancel_token.clone();
        let jh = spawn(async move { exp.start(shut_token).await });

        let res = tr_tx
            .send(From::from(FakeOTLP::trace_service_request().resource_spans))
            .await;
        assert!(&res.is_ok());

        let res = met_tx
            .send(From::from(
                FakeOTLP::metrics_service_request().resource_metrics,
            ))
            .await;
        assert!(&res.is_ok());

        cancel_token.cancel();
        let _ = join!(jh);
    }
}
