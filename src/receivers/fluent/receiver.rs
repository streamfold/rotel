// SPDX-License-Identifier: Apache-2.0

use crate::receivers::fluent::config::FluentReceiverConfig;
use crate::receivers::fluent::convert::convert_to_otlp_logs;
use crate::receivers::fluent::error::{FluentReceiverError, Result};
use crate::receivers::fluent::message::{Message, log_msgpack_structure};
use crate::receivers::otlp_output::OTLPOutput;
use crate::topology::payload;
use bytes::{Buf, BytesMut};
use flume::r#async::SendFut;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use rmp_serde::Deserializer;
use std::fs;
use std::io::Cursor;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};
use tokio::select;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::codec::{Decoder, FramedRead};
use tokio_util::sync::CancellationToken;
use tower::BoxError;
use tracing::{debug, error, info, warn};

const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024; // 16MB max message size

const MAX_BATCH_SIZE: usize = 100;
const MAX_BATCH_TIME_MS: usize = 250;

/// MessagePack decoder that extracts complete MessagePack messages from a stream
struct MessagePackDecoder {
    max_message_size: usize,
}

impl MessagePackDecoder {
    fn new(max_message_size: usize) -> Self {
        Self { max_message_size }
    }
}

impl Decoder for MessagePackDecoder {
    type Item = Message;
    type Error = std::io::Error;

    fn decode(
        &mut self,
        src: &mut BytesMut,
    ) -> std::result::Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        // Try to read a complete MessagePack value to determine its size
        let cursor = Cursor::new(&src[..]);
        let mut d = Deserializer::new(cursor);
        let res: std::result::Result<Message, rmp_serde::decode::Error> =
            serde::Deserialize::deserialize(&mut d);
        match res {
            Ok(value) => {
                // Successfully decoded a value, get the position after reading
                let position = d.position() as usize;

                // Check if the message exceeds max size
                if position > self.max_message_size {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "Message size {} exceeds maximum {}",
                            position, self.max_message_size
                        ),
                    ));
                }

                src.advance(position);

                Ok(Some(value))
            }
            Err(rmp_serde::decode::Error::InvalidMarkerRead(ref io_err))
                if io_err.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                // Need more data
                Ok(None)
            }
            Err(rmp_serde::decode::Error::InvalidDataRead(ref io_err))
                if io_err.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                // Need more data (incomplete frame)
                Ok(None)
            }
            Err(e) => {
                // Other decoding errors
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("MessagePack decode error: {}", e),
                ))
            }
        }
    }
}

pub struct FluentReceiver {
    config: FluentReceiverConfig,
    logs_output: Option<OTLPOutput<payload::Message<ResourceLogs>>>,
}

#[derive(Clone)]
struct ConnectionHandler {
    logs_output: Option<OTLPOutput<payload::Message<ResourceLogs>>>,
    cancel_token: CancellationToken,
}

impl FluentReceiver {
    pub async fn new(
        config: FluentReceiverConfig,
        logs_output: Option<OTLPOutput<payload::Message<ResourceLogs>>>,
    ) -> Result<Self> {
        // Validate that at least one listener type is configured
        if config.socket_path.is_none() && config.endpoint.is_none() {
            return Err(FluentReceiverError::ConfigurationError(
                "At least one of socket_path or endpoint must be configured".to_string(),
            ));
        }

        if let Some(socket_path) = &config.socket_path {
            // Remove the socket file if it already exists
            if socket_path.exists() {
                fs::remove_file(socket_path).map_err(|e| {
                    FluentReceiverError::SocketBindError(format!(
                        "Failed to remove existing socket file {}: {}",
                        socket_path.display(),
                        e
                    ))
                })?;
            }

            // Create parent directories if they don't exist
            if let Some(parent) = socket_path.parent() {
                if !parent.exists() {
                    fs::create_dir_all(parent).map_err(|e| {
                        FluentReceiverError::SocketBindError(format!(
                            "Failed to create parent directories for {}: {}",
                            socket_path.display(),
                            e
                        ))
                    })?;
                }
            }
        }

        Ok(Self {
            config,
            logs_output,
        })
    }

    fn create_handler(&self, cancel_token: CancellationToken) -> ConnectionHandler {
        ConnectionHandler {
            logs_output: self.logs_output.clone(),
            cancel_token,
        }
    }
}

impl ConnectionHandler {
    async fn handle_connection_generic<S>(self, stream: S, connection_type: &str)
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        debug!(
            connection_type = connection_type,
            "New connection accepted on Fluent receiver"
        );

        // Create a framed reader with MessagePack decoder
        let decoder = MessagePackDecoder::new(MAX_MESSAGE_SIZE);
        let mut framed = FramedRead::new(stream, decoder);

        // Use StreamExt to read frames
        use tokio_stream::StreamExt as TokioStreamExt;

        let mut batch = vec![];

        // Track single pending encoding task
        let mut pending_encode: Option<tokio::task::JoinHandle<Option<ResourceLogs>>> = None;

        // Track pending send operation
        let mut pending_send: Option<SendFut<'_, payload::Message<ResourceLogs>>> = None;

        // Create batch timer
        let batch_timeout = tokio::time::Duration::from_millis(MAX_BATCH_TIME_MS as u64);
        let mut batch_timer = tokio::time::interval(batch_timeout);
        batch_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Skip the first immediate tick
        batch_timer.tick().await;
        let mut last_batch_send = Instant::now();

        // Skip the first immediate tick
        batch_timer.tick().await;

        loop {
            select! {
                // Send to logs output
                Some(send_result) = conditional_wait(&mut pending_send), if pending_send.is_some() => match send_result {
                    Ok(_) => pending_send = None,
                    Err(e) => {
                        error!("Failed to send logs to output channel: {}", e);
                        break;
                    }
                },

                _ = batch_timer.tick() => {
                    if pending_encode.is_some() || batch.is_empty() {
                        continue
                    }

                    let now = Instant::now();
                    let last_dur = now.saturating_duration_since(last_batch_send);
                    // avoid uncessary flushing
                    if (last_dur.as_millis() as usize) < MAX_BATCH_TIME_MS / 2 {
                        continue
                    }

                    let curr_batch = std::mem::replace(&mut batch, vec![]);
                    pending_encode = Some(tokio::task::spawn_blocking(move || {
                        Some(convert_to_otlp_logs(curr_batch))
                    }));
                    last_batch_send = now;

                },

                // Wait for encoding to finish
                Some(encode_result) = conditional_wait(&mut pending_encode), if pending_send.is_none() => match encode_result {
                    Ok(Some(resource_logs)) => {
                        pending_encode = None;

                        // Initiate async send without awaiting
                        if let Some(logs_output) = &self.logs_output {
                            let payload_msg = payload::Message::new(None, vec![resource_logs]);
                            pending_send = Some(logs_output.send_async(payload_msg));
                        }
                    }
                    Ok(None) => {
                        pending_encode = None;
                    }
                    Err(e) => {
                        error!("Processing task panicked: {}", e);
                        panic!("Fluent receiver processing task panicked");
                    }
                },

                frame_result = TokioStreamExt::next(&mut framed), if batch.len() < MAX_BATCH_SIZE => {
                    match frame_result {
                        Some(Ok(message)) => {
                            match &message {
                                Message::Unknown(value) => {
                                    error!("Unexpected message");
                                    log_msgpack_structure(&value, 0);
                                    continue
                                }
                                Message::MessageWithOptions(_, _, _, event_options) => {
                                    if let Some(compress) = &event_options.compressed {
                                        error!(compress, "Unsupported compression option");
                                        continue
                                    }
                                    if let Some(_) = &event_options.chunk {
                                        error!("Fluent sender expects acks, not supported yet");
                                    }
                                }
                                Message::ForwardWithOption(_, _, event_options) => {
                                    if let Some(compress) = &event_options.compressed {
                                        error!(compress, "Unsupported compression option");
                                        continue
                                    }
                                    if let Some(_) = &event_options.chunk {
                                        error!("Fluent sender expects acks, not supported yet");
                                    }
                                }
                                _ => {}
                            }

                            batch.push(message);

                            // If we can't push onto the encoders, we will have to wait for the
                            // batch timer. We will only batch up to MAX_BATCH_SIZE due to the arm conditional
                            if batch.len() >= MAX_BATCH_SIZE && pending_encode.is_none() {
                                let curr_batch = std::mem::replace(&mut batch, vec![]);

                                pending_encode = Some(tokio::task::spawn_blocking(move || {
                                    Some(convert_to_otlp_logs(curr_batch))
                                }));

                                last_batch_send = Instant::now();
                            }
                        }
                        Some(Err(e)) => {
                            error!(
                                connection_type = connection_type,
                                "Error reading frame: {}", e
                            );
                            break;
                        }
                        None => {
                            debug!(
                                connection_type = connection_type,
                                "Connection closed by peer"
                            );
                            break;
                        }
                    }
                }

                _ = self.cancel_token.cancelled() => {
                    debug!(
                        connection_type = connection_type,
                        "Connection handler cancelled"
                    );
                    break;
                }
            }
        }

        let drain_timeout = tokio::time::Duration::from_secs(2); // TODO: make configurable
        let drain_deadline = tokio::time::Instant::now()
            .checked_add(drain_timeout)
            .unwrap();

        // Wait for remaining processing task to complete with timeout
        while !batch.is_empty() || pending_encode.is_some() || pending_send.is_some() {
            let now = Instant::now();

            let time_left = drain_deadline.saturating_duration_since(now);
            if time_left.is_zero() {
                error!("Timed out draining fluent receiver");
                break;
            }

            // Must empty the batch, otherwise drop into select and wait on
            // the pending encode
            if !batch.is_empty() && pending_encode.is_none() {
                let curr_batch = std::mem::replace(&mut batch, vec![]);

                pending_encode = Some(tokio::task::spawn_blocking(move || {
                    Some(convert_to_otlp_logs(curr_batch))
                }));
            }

            select! {
                biased;

                Some(send_result) = conditional_wait(&mut pending_send), if pending_send.is_some() => {
                    pending_send = None;
                    if let Err(e) = send_result {
                        error!("Failed to send logs to output channel: {}", e);
                        break;
                    }
                }

                Some(encode_result) = conditional_wait(&mut pending_encode), if pending_send.is_none() => match encode_result {
                    Ok(Some(resource_logs)) => {
                        pending_encode = None;

                        // Initiate async send without awaiting
                        if let Some(logs_output) = &self.logs_output {
                            let payload_msg = payload::Message::new(None, vec![resource_logs]);
                            pending_send = Some(logs_output.send_async(payload_msg));
                        }
                    }
                    Ok(None) => {
                        pending_encode = None;
                    }
                    Err(e) => {
                        error!("Processing task panicked: {}", e);
                        panic!("Fluent receiver processing task panicked");
                    }
                },

                _ = tokio::time::sleep(time_left) => {
                    debug!("Drain timeout reached during select loop");
                    break;
                }
            }
        }

        debug!(
            connection_type = connection_type,
            "Connection handler finished"
        );
    }

    async fn handle_unix_connection(self, stream: UnixStream) {
        self.handle_connection_generic(stream, "unix socket").await
    }

    async fn handle_tcp_connection(self, stream: TcpStream) {
        self.handle_connection_generic(stream, "tcp").await
    }
}

pub async fn conditional_wait<F>(fut_opt: &mut Option<F>) -> Option<F::Output>
where
    F: std::future::Future + Unpin,
{
    match fut_opt.take() {
        None => None,
        Some(fut) => Some(fut.await),
    }
}

impl FluentReceiver {
    pub async fn start(
        self,
        task_set: &mut JoinSet<std::result::Result<(), BoxError>>,
        receivers_cancel: &CancellationToken,
    ) -> std::result::Result<(), BoxError> {
        // Bind + spawn Unix socket listener task if configured
        if let Some(socket_path) = &self.config.socket_path {
            // Bind to the UNIX socket
            let listener = UnixListener::bind(socket_path).map_err(|e| {
                FluentReceiverError::SocketBindError(format!(
                    "Failed to bind to UNIX socket {}: {}",
                    socket_path.display(),
                    e
                ))
            })?;

            info!(
                socket_path = socket_path.display().to_string(),
                "Fluent receiver bound to UNIX socket"
            );

            let handler = self.create_handler(receivers_cancel.clone());
            let cancel = receivers_cancel.clone();
            let socket_path = self.config.socket_path.clone();

            task_set.spawn(async move {
                loop {
                    select! {
                        result = listener.accept() => {
                            match result {
                                Ok((stream, _addr)) => {
                                    debug!("Accepted new Unix socket connection");

                                    let handler_clone = handler.clone();
                                    tokio::spawn(async move {
                                        handler_clone.handle_unix_connection(stream).await;
                                    });
                                }
                                Err(e) => {
                                    error!("Error accepting Unix socket connection: {}", e);
                                }
                            }
                        }
                        _ = cancel.cancelled() => {
                            info!("Unix socket listener shutting down");
                            break;
                        }
                    }
                }

                // Cleanup: remove the socket file
                if let Some(path) = socket_path {
                    if path.exists() {
                        if let Err(e) = fs::remove_file(&path) {
                            warn!("Failed to remove socket file {}: {}", path.display(), e);
                        }
                    }
                }

                Ok(())
            });
        }

        // Spawn TCP listener task if configured
        if let Some(endpoint) = &self.config.endpoint {
            let listener = TcpListener::bind(endpoint).await.map_err(|e| {
                FluentReceiverError::SocketBindError(format!(
                    "Failed to bind to TCP endpoint {}: {}",
                    endpoint, e
                ))
            })?;

            info!(
                endpoint = endpoint.to_string(),
                "Fluent receiver bound to TCP endpoint"
            );

            let handler = self.create_handler(receivers_cancel.clone());
            let cancel = receivers_cancel.clone();
            let endpoint = self.config.endpoint;

            task_set.spawn(async move {
                loop {
                    select! {
                        result = listener.accept() => {
                            match result {
                                Ok((stream, addr)) => {
                                    debug!("Accepted new TCP connection from {}", addr);

                                    let handler_clone = handler.clone();
                                    tokio::spawn(async move {
                                        handler_clone.handle_tcp_connection(stream).await;
                                    });
                                }
                                Err(e) => {
                                    error!("Error accepting TCP connection: {}", e);
                                }
                            }
                        }
                        _ = cancel.cancelled() => {
                            info!(
                                endpoint = endpoint.map(|e| e.to_string()).unwrap_or_default(),
                                "TCP listener shutting down"
                            );
                            break;
                        }
                    }
                }

                Ok(())
            });
        };

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmpv::Value;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_msgpack_decoding() {
        // Create a test MessagePack message
        // Fluentd forward protocol format: [tag, [[timestamp, record], ...]]
        let tag = Value::String("docker.container".into());
        let timestamp = Value::Integer(1234567890.into());
        let record = Value::Map(vec![
            (
                Value::String("log".into()),
                Value::String("Test log message".into()),
            ),
            (
                Value::String("container_id".into()),
                Value::String("abc123".into()),
            ),
        ]);
        let entry = Value::Array(vec![timestamp, record]);
        let entries = Value::Array(vec![entry]);
        let message = Value::Array(vec![tag, entries]);

        // Encode the message
        let mut buffer = Vec::new();
        rmpv::encode::write_value(&mut buffer, &message).unwrap();
        let mut bytes = BytesMut::from(&buffer[..]);

        // Decode the message using MessagePackDecoder
        let mut decoder = MessagePackDecoder::new(MAX_MESSAGE_SIZE);
        let result = decoder.decode(&mut bytes);
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_msgpack_simple_types() {
        // Test various MessagePack types
        let test_values = vec![
            Value::Nil,
            Value::Boolean(true),
            Value::Integer(42.into()),
            Value::F64(3.14),
            Value::String("Hello, World!".into()),
            Value::Array(vec![
                Value::Integer(1.into()),
                Value::Integer(2.into()),
                Value::Integer(3.into()),
            ]),
            Value::Map(vec![
                (Value::String("key1".into()), Value::String("value1".into())),
                (Value::String("key2".into()), Value::Integer(123.into())),
            ]),
        ];

        let mut decoder = MessagePackDecoder::new(MAX_MESSAGE_SIZE);
        for test_value in test_values {
            let mut buffer = Vec::new();
            rmpv::encode::write_value(&mut buffer, &test_value).unwrap();
            let mut bytes = BytesMut::from(&buffer[..]);

            let result = decoder.decode(&mut bytes);
            // These decode successfully but are captured as Unknown message variants
            assert!(result.is_ok());
            if let Some(message) = result.unwrap() {
                // Verify they are Unknown messages (not valid Fluent protocol messages)
                assert!(matches!(message, Message::Unknown(_)));
            }
        }
    }

    #[tokio::test]
    async fn test_invalid_msgpack_data() {
        // Test with incomplete MessagePack data
        // This is an incomplete MessagePack array that should fail to decode
        let mut invalid_msgpack = BytesMut::from(&vec![0x93][..]); // Array with 3 elements but no data following
        let mut decoder = MessagePackDecoder::new(MAX_MESSAGE_SIZE);
        let result = decoder.decode(&mut invalid_msgpack);
        // This should return Ok(None) because the MessagePack is incomplete (needs more data)
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_valid_msgpack_decodes_successfully() {
        // Test that a Nil value is valid MessagePack but not a valid Message struct
        let mut valid_msgpack = BytesMut::from(&vec![0xc0][..]); // Nil in MessagePack
        let mut decoder = MessagePackDecoder::new(MAX_MESSAGE_SIZE);
        let result = decoder.decode(&mut valid_msgpack);
        // This should decode successfully but be captured as an Unknown message
        assert!(result.is_ok());
        if let Some(message) = result.unwrap() {
            assert!(matches!(message, Message::Unknown(_)));
        }
    }

    #[tokio::test]
    async fn test_receiver_with_both_unix_and_tcp() {
        use std::net::SocketAddr;

        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("test.sock");
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();

        let config = FluentReceiverConfig::new(Some(socket_path.clone()), Some(addr));

        let receiver = FluentReceiver::new(config, None).await;
        assert!(receiver.is_ok());

        let receiver = receiver.unwrap();

        let mut tasks = JoinSet::new();
        let cancel_token = CancellationToken::new();

        let r = receiver.start(&mut tasks, &cancel_token).await;
        assert!(r.is_ok());

        assert!(socket_path.exists());
        cancel_token.cancel();

        tasks.join_all().await;
        assert!(!socket_path.exists());
    }

    #[tokio::test]
    async fn test_receiver_fails_without_listeners() {
        let config = FluentReceiverConfig::new(None, None);

        let receiver = FluentReceiver::new(config, None).await;
        assert!(receiver.is_err());

        if let Err(FluentReceiverError::ConfigurationError(msg)) = receiver {
            assert!(msg.contains("At least one of socket_path or endpoint"));
        } else {
            panic!("Expected ConfigurationError");
        }
    }
}
