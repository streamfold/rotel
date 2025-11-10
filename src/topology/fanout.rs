// SPDX-License-Identifier: Apache-2.0

use futures::FutureExt;
use opentelemetry::metrics::ObservableGauge;
use opentelemetry::{KeyValue, global};

use crate::bounded_channel::BoundedSender;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A fanout component that distributes messages to multiple consumers.
///
/// The fanout component takes a message and sends it to all configured consumers.
/// The message is cloned for all consumers except the last one to avoid unnecessary cloning.
///
pub struct Fanout<T> {
    consumers: Vec<BoundedSender<T>>,
    _send_queue_gauge: ObservableGauge<i64>,
}

#[derive(Default)]
pub struct FanoutBuilder<T> {
    telemetry_type: &'static str,
    consumers: Vec<(&'static str, BoundedSender<T>)>,
}

pub struct FanoutFuture<'a, T> {
    consumers: &'a [BoundedSender<T>],
    message: Option<T>,
    current_index: usize,
    current_send: Option<flume::r#async::SendFut<'a, T>>,
}

// FanoutFuture is Unpin because all its fields are Unpin
impl<'a, T> Unpin for FanoutFuture<'a, T> {}

impl<T> Fanout<T>
where
    T: Send + 'static,
{
    /// Creates a new fanout component with the given consumers.
    ///
    /// # Arguments
    /// * `consumers` - A vector of BoundedSender consumers that will receive the messages
    ///
    /// # Panics
    /// Panics if the consumers vector is empty.
    pub fn new(
        telemetry_type: &'static str,
        consumers: Vec<(&'static str, BoundedSender<T>)>,
    ) -> Self {
        if consumers.is_empty() {
            panic!("Fanout requires at least one consumer");
        }

        let consumers_clone: Vec<(&'static str, BoundedSender<T>)> =
            consumers.iter().map(|c| (c.0, c.1.clone())).collect();
        let send_queue_gauge = global::meter("fanout")
            .i64_observable_gauge("sender_queue_len")
            .with_callback(move |observer| {
                for consumer in &consumers_clone {
                    let len = consumer.1.len();
                    let telemetry_type_kv =
                        KeyValue::new("telemetry_type", telemetry_type.to_string());
                    let exporter_type_kv = KeyValue::new("exporter_type", consumer.0.to_string());
                    observer.observe(len as i64, &[telemetry_type_kv, exporter_type_kv]);
                }
            })
            .build();

        Self {
            consumers: consumers.iter().map(|c| c.1.clone()).collect(),
            _send_queue_gauge: send_queue_gauge,
        }
    }

    /// Creates a future that will send a message to all consumers sequentially.
    ///
    /// The message will be cloned for all consumers except the last one.
    /// Sends to each consumer one at a time, waiting for success before
    /// proceeding to the next consumer. If any consumer fails, the operation
    /// stops immediately and returns an error with the index of the failed consumer.
    ///
    /// # Arguments
    /// * `message` - The message to send to all consumers
    ///
    /// # Returns
    /// A future that resolves to `Result<(), FanoutError>` when all sends complete.
    /// On failure, returns `FanoutError::Disconnected` containing the index of the
    /// first consumer that failed to receive the message.
    pub fn send_async(&self, message: T) -> FanoutFuture<'_, T> {
        FanoutFuture::new(&self.consumers, message)
    }
}

impl<'a, T> FanoutFuture<'a, T> {
    fn new(consumers: &'a [BoundedSender<T>], message: T) -> Self {
        Self {
            consumers,
            message: Some(message),
            current_index: 0,
            current_send: None,
        }
    }
}

impl<'a, T> Future for FanoutFuture<'a, T>
where
    T: Clone,
{
    type Output = Result<(), FanoutError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut(); // Safe because FanoutFuture is Unpin

        loop {
            if let Some(send_fut) = &mut this.current_send {
                match send_fut.poll_unpin(cx) {
                    Poll::Ready(Ok(())) => {
                        // Send completed successfully
                        this.current_send = None;
                        this.current_index += 1;

                        // Check if we've sent to all consumers
                        if this.current_index >= this.consumers.len() {
                            return Poll::Ready(Ok(()));
                        }
                        // Continue to next consumer
                    }
                    Poll::Ready(Err(_)) => {
                        return Poll::Ready(Err(FanoutError::Disconnected(this.current_index)));
                    }
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                }
            } else {
                // No pending send, start sending to current consumer
                if this.current_index >= this.consumers.len() {
                    return Poll::Ready(Ok(()));
                }

                let message_to_send = if this.current_index == this.consumers.len() - 1 {
                    // Last consumer gets the original message (no clone needed)
                    this.message.take().expect("Message should be available")
                } else {
                    // All other consumers get a clone
                    this.message
                        .as_ref()
                        .expect("Message should be available")
                        .clone()
                };

                let send_fut = this.consumers[this.current_index].send_async(message_to_send);
                this.current_send = Some(send_fut);
            }
        }
    }
}

impl<T> FanoutBuilder<T>
where
    T: Send + 'static,
{
    /// Creates a new FanoutBuilder.
    pub fn new(telemetry_type: &'static str) -> Self {
        Self {
            telemetry_type,
            consumers: Vec::new(),
        }
    }

    /// Adds a consumer to the fanout.
    ///
    /// # Arguments
    /// * `tx` - A BoundedSender that will receive messages from the fanout
    ///
    /// # Returns
    /// Returns self for method chaining
    pub fn add_tx(mut self, exporter_name: &'static str, tx: BoundedSender<T>) -> Self {
        self.consumers.push((exporter_name, tx));
        self
    }

    /// Builds the Fanout instance.
    ///
    /// # Returns
    /// Returns `Ok(Fanout<T>)` if at least one consumer has been added,
    /// otherwise returns `Err(FanoutBuilderError::NoConsumers)`
    pub fn build(self) -> Result<Fanout<T>, FanoutBuilderError> {
        if self.consumers.is_empty() {
            Err(FanoutBuilderError::NoConsumers)
        } else {
            Ok(Fanout::new(self.telemetry_type, self.consumers))
        }
    }
}

/// Error type for fanout builder operations
#[derive(Debug, PartialEq, Eq)]
pub enum FanoutBuilderError {
    /// No consumers were added to the builder
    NoConsumers,
}

impl std::fmt::Display for FanoutBuilderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FanoutBuilderError::NoConsumers => {
                write!(f, "At least one consumer must be added to the fanout")
            }
        }
    }
}

impl std::error::Error for FanoutBuilderError {}

/// Error type for fanout operations
#[derive(Debug, PartialEq, Eq)]
pub enum FanoutError {
    /// A consumer is disconnected. Contains the index of the first consumer that failed.
    /// Due to sequential processing, only the first failure is reported.
    Disconnected(usize),
}

impl std::fmt::Display for FanoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FanoutError::Disconnected(index) => {
                write!(f, "Consumer at index {} is disconnected", index)
            }
        }
    }
}

impl std::error::Error for FanoutError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bounded_channel::bounded;

    #[tokio::test]
    async fn test_fanout_single_consumer() {
        let (tx, mut rx) = bounded(10);
        let fanout = Fanout::new(
            "test",
            vec![tx].into_iter().map(|t| ("test_exporter", t)).collect(),
        );

        let message = vec![1, 2, 3];
        let send_result = fanout.send_async(message.clone()).await;

        assert!(send_result.is_ok());

        let received = rx.next().await;
        assert_eq!(Some(message), received);
    }

    #[tokio::test]
    async fn test_fanout_multiple_consumers() {
        let (tx1, mut rx1) = bounded(10);
        let (tx2, mut rx2) = bounded(10);
        let (tx3, mut rx3) = bounded(10);

        let fanout = Fanout::new(
            "test",
            vec![tx1, tx2, tx3]
                .into_iter()
                .map(|t| ("test_exporter", t))
                .collect(),
        );

        let message = vec![1, 2, 3];
        let send_result = fanout.send_async(message.clone()).await;

        assert!(send_result.is_ok());

        // All consumers should receive the same message
        assert_eq!(Some(message.clone()), rx1.next().await);
        assert_eq!(Some(message.clone()), rx2.next().await);
        assert_eq!(Some(message), rx3.next().await);
    }

    #[tokio::test]
    async fn test_fanout_disconnected_consumer() {
        let (tx1, mut rx1) = bounded(10);
        let (tx2, _rx2) = bounded(10); // rx2 will be dropped

        let fanout = Fanout::new(
            "test",
            vec![tx1, tx2]
                .into_iter()
                .map(|t| ("test_exporter", t))
                .collect(),
        );

        // Drop rx2 to simulate disconnection
        drop(_rx2);

        let message = vec![1, 2, 3];
        let send_result = fanout.send_async(message.clone()).await;

        // Should get an error indicating consumer 1 (index) is disconnected
        match send_result {
            Err(FanoutError::Disconnected(index)) => {
                assert_eq!(1, index);
            }
            _ => panic!("Expected disconnected error"),
        }

        // First consumer should still receive the message
        assert_eq!(Some(message), rx1.next().await);
    }

    #[tokio::test]
    async fn test_fanout_message_metadata_reference_counting() {
        use crate::topology::payload::{
            Ack, KafkaAcknowledgement, KafkaMetadata, Message, MessageMetadata,
        };
        use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
        use std::time::Duration;

        let (tx1, mut rx1) = bounded(10);
        let (tx2, mut rx2) = bounded(10);
        let (tx3, mut rx3) = bounded(10);

        let fanout = Fanout::new(
            "test",
            vec![tx1, tx2, tx3]
                .into_iter()
                .map(|t| ("test_exporter", t))
                .collect(),
        );

        // Create acknowledgment channel
        let (ack_tx, mut ack_rx) = crate::bounded_channel::bounded(1);

        // Create message with metadata
        let metadata = MessageMetadata::kafka(KafkaMetadata {
            offset: 100,
            partition: 0,
            topic_id: 1,
            ack_chan: Some(ack_tx),
        });

        let test_payload = vec![ResourceSpans::default()];
        let message = Message {
            metadata: Some(metadata),
            payload: test_payload.clone(),
        };

        // Send message through fanout
        let send_result = fanout.send_async(message).await;
        assert!(send_result.is_ok());

        // Receive messages from all consumers
        let msg1 = rx1.next().await.unwrap();
        let msg2 = rx2.next().await.unwrap();
        let msg3 = rx3.next().await.unwrap();

        // Verify all received the same payload
        assert_eq!(msg1.payload, test_payload);
        assert_eq!(msg2.payload, test_payload);
        assert_eq!(msg3.payload, test_payload);

        // Verify reference count is 3 (fanout created 2 clones + 1 original)
        let ref_count = msg1.metadata.as_ref().unwrap().ref_count();
        assert_eq!(
            ref_count, 3,
            "Reference count should be 3 after fanout to 3 consumers"
        );

        // First ack should not send acknowledgment (ref count: 3 -> 2)
        msg1.metadata.as_ref().unwrap().ack().await.unwrap();
        let result = tokio::time::timeout(Duration::from_millis(100), ack_rx.next()).await;
        assert!(
            result.is_err(),
            "Should not receive ack after first consumer"
        );

        // Second ack should not send acknowledgment (ref count: 2 -> 1)
        msg2.metadata.as_ref().unwrap().ack().await.unwrap();
        let result = tokio::time::timeout(Duration::from_millis(100), ack_rx.next()).await;
        assert!(
            result.is_err(),
            "Should not receive ack after second consumer"
        );

        // Third ack should send acknowledgment (ref count: 1 -> 0)
        msg3.metadata.as_ref().unwrap().ack().await.unwrap();
        let ack = tokio::time::timeout(Duration::from_millis(1000), ack_rx.next())
            .await
            .expect("Should receive ack after third consumer")
            .unwrap();

        // Verify the acknowledgment details
        match ack {
            KafkaAcknowledgement::Ack(kafka_ack) => {
                assert_eq!(kafka_ack.offset, 100);
                assert_eq!(kafka_ack.partition, 0);
                assert_eq!(kafka_ack.topic_id, 1);
            }
            _ => panic!("Expected Ack"),
        }
    }
}
