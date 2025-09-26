// SPDX-License-Identifier: Apache-2.0

use crate::bounded_channel::BoundedSender;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A fanout component that distributes messages to multiple consumers.
///
/// The fanout component takes a message and sends it to all configured consumers.
/// The message is cloned for all consumers except the last one to avoid unnecessary cloning.
///
/// # Example
///
/// ```rust
/// use rotel::bounded_channel::bounded;
/// use rotel::topology::fanout::Fanout;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     // Create multiple consumers
///     let (tx1, mut rx1) = bounded(10);
///     let (tx2, mut rx2) = bounded(10);
///     let (tx3, mut rx3) = bounded(10);
///
///     // Create fanout with the consumers
///     let fanout = Fanout::new(vec![tx1, tx2, tx3]);
///
///     // Send a message to all consumers
///     let message = vec![1, 2, 3, 4, 5];
///     fanout.async_send(message.clone()).await?;
///
///     // All consumers receive the same message (sent sequentially)
///     assert_eq!(Some(message.clone()), rx1.next().await);
///     assert_eq!(Some(message.clone()), rx2.next().await);
///     assert_eq!(Some(message), rx3.next().await);
///
///     Ok(())
/// }
/// ```
pub struct Fanout<T> {
    consumers: Vec<BoundedSender<Vec<T>>>,
}

/// Builder for constructing a Fanout component.
///
/// Provides a convenient way to add consumers one at a time and build the final Fanout instance.
///
/// # Example
///
/// ```rust
/// use rotel::bounded_channel::bounded;
/// use rotel::topology::fanout::FanoutBuilder;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let (tx1, _rx1) = bounded(10);
///     let (tx2, _rx2) = bounded(10);
///
///     let fanout = FanoutBuilder::new()
///         .add_tx(tx1)
///         .add_tx(tx2)
///         .build()?;
///
///     // Use fanout...
///     Ok(())
/// }
/// ```
#[derive(Default)]
pub struct FanoutBuilder<T> {
    consumers: Vec<BoundedSender<Vec<T>>>,
}

/// Future returned by `async_send` that manages sequential sending to consumers
pub struct FanoutFuture<'a, T> {
    consumers: &'a [BoundedSender<Vec<T>>],
    message: Option<Vec<T>>,
    current_index: usize,
    current_send: Option<flume::r#async::SendFut<'a, Vec<T>>>,
}

// FanoutFuture is Unpin because all its fields are Unpin
impl<'a, T> Unpin for FanoutFuture<'a, T> {}

impl<T> Fanout<T>
where
    T: Clone,
{
    /// Creates a new fanout component with the given consumers.
    ///
    /// # Arguments
    /// * `consumers` - A vector of BoundedSender consumers that will receive the messages
    ///
    /// # Panics
    /// Panics if the consumers vector is empty.
    pub fn new(consumers: Vec<BoundedSender<Vec<T>>>) -> Self {
        if consumers.is_empty() {
            panic!("Fanout requires at least one consumer");
        }

        Self { consumers }
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
    pub fn async_send(&self, message: Vec<T>) -> FanoutFuture<'_, T> {
        FanoutFuture::new(&self.consumers, message)
    }
}

impl<'a, T> FanoutFuture<'a, T>
where
    T: Clone,
{
    fn new(consumers: &'a [BoundedSender<Vec<T>>], message: Vec<T>) -> Self {
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
            // If we have a pending send, poll it
            if let Some(ref mut send_fut) = this.current_send {
                let send_result = {
                    // SAFETY: We never move the SendFut after creating it
                    let pinned_fut = unsafe { Pin::new_unchecked(send_fut) };
                    pinned_fut.poll(cx)
                };

                match send_result {
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
                        // Send failed
                        return Poll::Ready(Err(FanoutError::Disconnected(vec![
                            this.current_index,
                        ])));
                    }
                    Poll::Pending => {
                        // Send is still in progress
                        return Poll::Pending;
                    }
                }
            } else {
                // No pending send, start sending to current consumer
                if this.current_index >= this.consumers.len() {
                    // All consumers processed
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

                // Start the send operation
                let send_fut = this.consumers[this.current_index].send_async(message_to_send);
                this.current_send = Some(send_fut);
                // Loop back to poll the new send future
            }
        }
    }
}

impl<T> FanoutBuilder<T> {
    /// Creates a new FanoutBuilder.
    pub fn new() -> Self {
        Self {
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
    pub fn add_tx(mut self, tx: BoundedSender<Vec<T>>) -> Self {
        self.consumers.push(tx);
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
            Ok(Fanout {
                consumers: self.consumers,
            })
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
    Disconnected(Vec<usize>), // index of disconnected consumer (will contain single element)
}

impl std::fmt::Display for FanoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FanoutError::Disconnected(indices) => {
                if indices.len() == 1 {
                    write!(f, "Consumer at index {} is disconnected", indices[0])
                } else {
                    write!(f, "Consumers at indices {:?} are disconnected", indices)
                }
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
        let fanout = Fanout::new(vec![tx]);

        let message = vec![1, 2, 3];
        let send_result = fanout.async_send(message.clone()).await;

        assert!(send_result.is_ok());

        let received = rx.next().await;
        assert_eq!(Some(message), received);
    }

    #[tokio::test]
    async fn test_fanout_multiple_consumers() {
        let (tx1, mut rx1) = bounded(10);
        let (tx2, mut rx2) = bounded(10);
        let (tx3, mut rx3) = bounded(10);

        let fanout = Fanout::new(vec![tx1, tx2, tx3]);

        let message = vec![1, 2, 3];
        let send_result = fanout.async_send(message.clone()).await;

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

        let fanout = Fanout::new(vec![tx1, tx2]);

        // Drop rx2 to simulate disconnection
        drop(_rx2);

        let message = vec![1, 2, 3];
        let send_result = fanout.async_send(message.clone()).await;

        // Should get an error indicating consumer 1 (index) is disconnected
        match send_result {
            Err(FanoutError::Disconnected(indices)) => {
                assert_eq!(vec![1], indices);
            }
            _ => panic!("Expected disconnected error"),
        }

        // First consumer should still receive the message
        assert_eq!(Some(message), rx1.next().await);
    }

    #[tokio::test]
    async fn test_fanout_concurrent_sends() {
        let (tx1, mut rx1) = bounded(10);
        let (tx2, mut rx2) = bounded(10);

        let fanout = Fanout::new(vec![tx1, tx2]);

        // Send multiple messages concurrently
        let msg1 = vec![1];
        let msg2 = vec![2];
        let msg3 = vec![3];

        let (result1, result2, result3) = tokio::join!(
            fanout.async_send(msg1.clone()),
            fanout.async_send(msg2.clone()),
            fanout.async_send(msg3.clone())
        );

        assert!(result1.is_ok());
        assert!(result2.is_ok());
        assert!(result3.is_ok());

        // Collect all messages from both receivers
        let mut rx1_msgs = Vec::new();
        let mut rx2_msgs = Vec::new();

        for _ in 0..3 {
            rx1_msgs.push(rx1.next().await.unwrap());
            rx2_msgs.push(rx2.next().await.unwrap());
        }

        // Both receivers should have received all messages (order may vary)
        rx1_msgs.sort();
        rx2_msgs.sort();

        let expected = vec![vec![1], vec![2], vec![3]];
        assert_eq!(expected, rx1_msgs);
        assert_eq!(expected, rx2_msgs);
    }

    #[tokio::test]
    async fn test_fanout_sequential_sending() {
        use std::sync::Arc;
        use tokio::sync::Mutex;

        // Track the order of sends using a shared counter
        let send_order = Arc::new(Mutex::new(Vec::new()));

        // Create custom receivers that track when they receive messages
        let (tx1, mut rx1) = bounded(10);
        let (tx2, mut rx2) = bounded(10);
        let (tx3, mut rx3) = bounded(10);

        let fanout = Fanout::new(vec![tx1, tx2, tx3]);

        let message = vec![1, 2, 3];

        // Create tasks to receive and record order
        let order1 = send_order.clone();
        let order2 = send_order.clone();
        let order3 = send_order.clone();

        let recv_task1 = tokio::spawn(async move {
            if let Some(_msg) = rx1.next().await {
                order1.lock().await.push(1);
            }
        });

        let recv_task2 = tokio::spawn(async move {
            if let Some(_msg) = rx2.next().await {
                order2.lock().await.push(2);
            }
        });

        let recv_task3 = tokio::spawn(async move {
            if let Some(_msg) = rx3.next().await {
                order3.lock().await.push(3);
            }
        });

        // Send the message (should be sequential)
        let send_result = fanout.async_send(message).await;
        assert!(send_result.is_ok());

        // Wait for all receivers to complete
        let _ = tokio::join!(recv_task1, recv_task2, recv_task3);

        // Verify all consumers received the message
        let final_order = send_order.lock().await;
        assert_eq!(final_order.len(), 3);
        assert!(final_order.contains(&1));
        assert!(final_order.contains(&2));
        assert!(final_order.contains(&3));
    }

    #[tokio::test]
    async fn test_fanout_early_failure_stops_processing() {
        // Test that when a consumer fails, processing stops and later consumers don't receive messages
        let (tx1, mut rx1) = bounded(10);
        let (tx2, _rx2) = bounded(10); // rx2 will be dropped to simulate failure
        let (tx3, mut rx3) = bounded(10);

        // Drop rx2 to make the second consumer fail
        drop(_rx2);

        let fanout = Fanout::new(vec![tx1, tx2, tx3]);

        let message = vec![1, 2, 3];
        let send_result = fanout.async_send(message.clone()).await;

        // Should fail at consumer index 1 (the second consumer)
        match send_result {
            Err(FanoutError::Disconnected(indices)) => {
                assert_eq!(vec![1], indices);
            }
            _ => panic!("Expected disconnected error at index 1"),
        }

        // First consumer should have received the message
        assert_eq!(Some(message), rx1.next().await);

        // Third consumer should NOT have received anything since processing stopped at consumer 1
        tokio::select! {
            msg = rx3.next() => {
                panic!("Third consumer should not have received a message, but got: {:?}", msg);
            }
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                // This is expected - no message should be received
            }
        }
    }

    #[test]
    #[should_panic(expected = "Fanout requires at least one consumer")]
    fn test_fanout_empty_consumers() {
        let consumers: Vec<BoundedSender<Vec<i32>>> = vec![];
        Fanout::new(consumers);
    }

    #[test]
    fn test_fanout_builder_basic() {
        let (tx1, _rx1) = bounded::<Vec<i32>>(10);
        let (tx2, _rx2) = bounded::<Vec<i32>>(10);

        let fanout = FanoutBuilder::new().add_tx(tx1).add_tx(tx2).build();

        assert!(fanout.is_ok());
        let fanout: Fanout<i32> = fanout.unwrap();
        assert_eq!(fanout.consumers.len(), 2);
    }

    #[test]
    fn test_fanout_builder_no_consumers() {
        let builder: FanoutBuilder<i32> = FanoutBuilder::new();
        let result = builder.build();

        match result {
            Err(FanoutBuilderError::NoConsumers) => {
                // This is expected
            }
            _ => panic!("Expected NoConsumers error"),
        }
    }

    #[tokio::test]
    async fn test_fanout_builder_functionality() {
        let (tx1, mut rx1) = bounded(10);
        let (tx2, mut rx2) = bounded(10);

        let fanout = FanoutBuilder::new()
            .add_tx(tx1)
            .add_tx(tx2)
            .build()
            .unwrap();

        let message = vec![1, 2, 3];
        let result = fanout.async_send(message.clone()).await;

        assert!(result.is_ok());
        assert_eq!(Some(message.clone()), rx1.next().await);
        assert_eq!(Some(message), rx2.next().await);
    }

    #[test]
    fn test_fanout_builder_single_consumer() {
        let (tx, _rx) = bounded::<Vec<i32>>(10);

        let fanout = FanoutBuilder::new().add_tx(tx).build();

        assert!(fanout.is_ok());
        let fanout: Fanout<i32> = fanout.unwrap();
        assert_eq!(fanout.consumers.len(), 1);
    }

    #[tokio::test]
    async fn test_fanout_builder_complex_chaining() {
        // Test more complex builder usage with multiple add_tx calls
        let (tx1, mut rx1) = bounded(10);
        let (tx2, mut rx2) = bounded(10);
        let (tx3, mut rx3) = bounded(10);
        let (tx4, mut rx4) = bounded(10);

        let mut builder = FanoutBuilder::new();
        builder = builder.add_tx(tx1);
        builder = builder.add_tx(tx2);

        let fanout = builder.add_tx(tx3).add_tx(tx4).build().unwrap();

        let message = vec![42, 43, 44];
        let result = fanout.async_send(message.clone()).await;

        assert!(result.is_ok());

        // All four consumers should receive the message
        assert_eq!(Some(message.clone()), rx1.next().await);
        assert_eq!(Some(message.clone()), rx2.next().await);
        assert_eq!(Some(message.clone()), rx3.next().await);
        assert_eq!(Some(message), rx4.next().await);
    }
}
