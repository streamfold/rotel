// SPDX-License-Identifier: Apache-2.0

use crate::bounded_channel::BoundedReceiver;
use crate::topology::payload::{Ack, Message};
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::debug;

pub struct BlackholeExporter<Resource> {
    rx: BoundedReceiver<Vec<Message<Resource>>>,
}

impl<Resource> BlackholeExporter<Resource> {
    pub fn new(rx: BoundedReceiver<Vec<Message<Resource>>>) -> Self {
        BlackholeExporter { rx }
    }

    pub async fn start(&mut self, cancel_token: CancellationToken) {
        loop {
            select! {
                m = self.rx.next() => {
                    match m {
                        Some(messages) => {
                            // Acknowledge all messages since blackhole "successfully" processes everything
                            for message in messages {
                                if let Some(mut metadata) = message.metadata {
                                    if let Err(e) = metadata.ack().await {
                                        tracing::warn!("Failed to acknowledge blackhole message: {:?}", e);
                                    }
                                }
                                // Discard the message payload (blackhole behavior)
                            }
                        }
                        None => break,
                    }
                },
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
    use crate::topology::payload::Message;
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

        // Create messages with the resource spans
        let resource_spans = FakeOTLP::trace_service_request().resource_spans;
        let messages: Vec<Message<_>> = resource_spans
            .into_iter()
            .map(|span| Message {
                metadata: None,
                payload: vec![span],
            })
            .collect();

        let res = tr_tx.send(messages).await;
        assert!(&res.is_ok());

        cancel_token.cancel();
        let _ = join!(jh);
    }
}
