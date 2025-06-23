// SPDX-License-Identifier: Apache-2.0

use crate::bounded_channel::BoundedReceiver;
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::debug;

pub struct BlackholeExporter<Resource> {
    rx: BoundedReceiver<Vec<Resource>>,
}

impl<Resource> BlackholeExporter<Resource> {
    pub fn new(rx: BoundedReceiver<Vec<Resource>>) -> Self {
        BlackholeExporter { rx }
    }

    pub async fn start(&mut self, cancel_token: CancellationToken) {
        loop {
            select! {
                m = self.rx.next() => if m.is_none() { break },
                _ = cancel_token.cancelled() => break,
            }
        }
        debug!("exiting blackhole exporter")
    }
}

#[cfg(test)]
mod tests {
    use crate::bounded_channel::bounded;
    use crate::exporters::blackhole::BlackholeExporter;
    use tokio::{join, spawn};
    use tokio_util::sync::CancellationToken;
    use utilities::otlp::FakeOTLP;

    #[tokio::test]
    async fn send_request() {
        let (tr_tx, tr_rx) = bounded(1);

        let mut exp = BlackholeExporter::new(tr_rx);

        let cancel_token = CancellationToken::new();
        let shut_token = cancel_token.clone();
        let jh = spawn(async move { exp.start(shut_token).await });

        let res = tr_tx
            .send(From::from(FakeOTLP::trace_service_request().resource_spans))
            .await;
        assert!(&res.is_ok());

        cancel_token.cancel();
        let _ = join!(jh);
    }
}
