// SPDX-License-Identifier: Apache-2.0

use crate::receivers::fluent::config::FluentReceiverConfig;
use crate::receivers::fluent::convert::message_to_resource_logs;
use crate::receivers::fluent::error::{FluentReceiverError, Result};
use crate::receivers::fluent::message::Message;
use crate::receivers::otlp_output::OTLPOutput;
use crate::topology::payload;
use bytes::{Bytes, BytesMut};
use flume::r#async::SendFut;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use rmpv::Value;
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
    type Item = BytesMut;
    type Error = std::io::Error;

    fn decode(
        &mut self,
        src: &mut BytesMut,
    ) -> std::result::Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        // Try to read a complete MessagePack value to determine its size
        let mut cursor = Cursor::new(&src[..]);
        match rmpv::decode::read_value(&mut cursor) {
            Ok(_value) => {
                // Successfully decoded a value, get the position after reading
                let position = cursor.position() as usize;

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

                // Extract the message bytes
                let message_bytes = src.split_to(position);
                Ok(Some(message_bytes))
            }
            Err(rmpv::decode::Error::InvalidMarkerRead(ref io_err))
                if io_err.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                // Need more data
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

        // Track single pending encoding task
        let mut pending_encode: Option<tokio::task::JoinHandle<Result<ResourceLogs>>> = None;

        // Track pending send operation
        let mut pending_send: Option<SendFut<'_, payload::Message<ResourceLogs>>> = None;

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

                // Wait for encoding to finish
                Some(encode_result) = conditional_wait(&mut pending_encode), if pending_send.is_none() => match encode_result {
                    Ok(Ok(resource_logs)) => {
                        pending_encode = None;

                        // Initiate async send without awaiting
                        if let Some(logs_output) = &self.logs_output {
                            let payload_msg = payload::Message::new(None, vec![resource_logs]);
                            pending_send = Some(logs_output.send_async(payload_msg));
                        }
                    }
                    Ok(Err(e)) => {
                        pending_encode = None;
                        warn!("Failed to process message: {}", e);
                    }
                    Err(e) => {
                        error!("Processing task panicked: {}", e);
                        panic!("Fluent receiver processing task panicked");
                    }
                },

                frame_result = TokioStreamExt::next(&mut framed), if pending_encode.is_none() => {
                    match frame_result {
                        Some(Ok(message_bytes)) => {
                            debug!(
                                "Received complete message of {} bytes from Fluent {}",
                                message_bytes.len(),
                                connection_type
                            );

                            // Spawn blocking task to process the message
                            // Convert BytesMut to Bytes for cheap cloning (no copy)
                            let data = message_bytes.freeze();
                            pending_encode = Some(tokio::task::spawn_blocking(move || {
                                Self::process_message(&data).map(|msg| message_to_resource_logs(&msg))
                            }));
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
            .checked_sub(drain_timeout)
            .unwrap();

        // Wait for remaining processing task to complete with timeout
        while pending_encode.is_some() || pending_send.is_some() {
            let now = Instant::now();

            let time_left = drain_deadline.saturating_duration_since(now);
            if time_left.is_zero() {
                error!("Timed out draining fluent receiver");
                break;
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
                    Ok(Ok(resource_logs)) => {
                        pending_encode = None;

                        // Initiate async send without awaiting
                        if let Some(logs_output) = &self.logs_output {
                            let payload_msg = payload::Message::new(None, vec![resource_logs]);
                            pending_send = Some(logs_output.send_async(payload_msg));
                        }
                    }
                    Ok(Err(e)) => {
                        pending_encode = None;
                        warn!("Failed to process message: {}", e);
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

    fn process_message(data: &Bytes) -> Result<Message> {
        debug!("Processing message of {} bytes", data.len());

        // Try to decode as Message struct first
        match rmp_serde::from_slice::<Message>(data) {
            Ok(message) => {
                //println!("Decoded Fluent Message: {:?}", message);

                // We do not support compression at the moment
                match &message {
                    Message::MessageWithOptions(_, _, _, event_options) => {
                        if let Some(compress) = &event_options.compressed {
                            return Err(FluentReceiverError::UnsupportedCompression(
                                compress.clone(),
                            ));
                        }
                    }
                    Message::ForwardWithOption(_, _, event_options) => {
                        if let Some(compress) = &event_options.compressed {
                            return Err(FluentReceiverError::UnsupportedCompression(
                                compress.clone(),
                            ));
                        }
                    }
                    _ => {}
                }

                return Ok(message);
            }
            Err(e) => {
                error!("Failed to decode as Message struct: {}", e);
            }
        }

        // Fall back to generic MessagePack decoding for debugging
        let mut cursor = Cursor::new(data);
        match rmpv::decode::read_value(&mut cursor) {
            Ok(value) => {
                //println!("Decoded MessagePack message: {:#?}", value);

                // Log the structure in a more readable format
                println!("Received Fluent MessagePack");
                Self::log_msgpack_structure(&value, 0);

                // Return error since we couldn't parse as Message
                Err(FluentReceiverError::DeserializationError(
                    "Could not parse as Message struct".to_string(),
                ))
            }
            Err(e) => {
                // If MessagePack decoding fails, try to interpret as other formats
                warn!("Failed to decode as MessagePack: {}", e);

                // Try to decode as UTF-8 text
                match std::str::from_utf8(data) {
                    Ok(utf8_str) => {
                        println!("Received UTF-8 text: {}", utf8_str);
                    }
                    Err(_) => {
                        // Hex dump for binary data
                        println!("Received binary data, hex dump:");
                        for (i, chunk) in data.chunks(32).enumerate() {
                            let offset = i * 32;
                            let hex_part: String = chunk
                                .iter()
                                .map(|b| format!("{:02x}", b))
                                .collect::<Vec<_>>()
                                .join(" ");

                            println!("{:08x}  {}", offset, hex_part);
                        }
                    }
                }

                Err(FluentReceiverError::DeserializationError(format!(
                    "Failed to decode message: {}",
                    e
                )))
            }
        }
    }

    fn log_msgpack_structure(value: &Value, indent: usize) {
        let prefix = "  ".repeat(indent);
        match value {
            Value::Nil => println!("{}Nil", prefix),
            Value::Boolean(b) => println!("{}Boolean: {}", prefix, b),
            Value::Integer(i) => println!("{}Integer: {}", prefix, i),
            Value::F32(f) => println!("{}F32: {}", prefix, f),
            Value::F64(f) => println!("{}F64: {}", prefix, f),
            Value::String(s) => match s.as_str() {
                Some(utf8_str) => println!("{}String: {:?}", prefix, utf8_str),
                None => println!("{}String (binary): {:?}", prefix, s),
            },
            Value::Binary(b) => {
                if b.len() <= 64 {
                    println!("{}Binary({} bytes): {:?}", prefix, b.len(), b);
                } else {
                    println!("{}Binary({} bytes): {:?}...", prefix, b.len(), &b[..64]);
                }
            }
            Value::Array(arr) => {
                println!("{}Array({} elements):", prefix, arr.len());
                for (i, item) in arr.iter().enumerate() {
                    println!("{}[{}]:", prefix, i);
                    Self::log_msgpack_structure(item, indent + 1);
                }
            }
            Value::Map(map) => {
                println!("{}Map({} entries):", prefix, map.len());
                for (key, val) in map {
                    println!("{}Key:", prefix);
                    Self::log_msgpack_structure(key, indent + 1);
                    println!("{}Value:", prefix);
                    Self::log_msgpack_structure(val, indent + 1);
                }
            }
            Value::Ext(tag, data) => {
                println!(
                    "{}Ext(tag={}, {} bytes): {:?}",
                    prefix,
                    tag,
                    data.len(),
                    data
                );
            }
        }
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
        let bytes = Bytes::from(buffer);

        // Process the message
        let result = ConnectionHandler::process_message(&bytes);
        assert!(result.is_ok());
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

        for test_value in test_values {
            let mut buffer = Vec::new();
            rmpv::encode::write_value(&mut buffer, &test_value).unwrap();
            let bytes = Bytes::from(buffer);

            let result = ConnectionHandler::process_message(&bytes);
            // These are not valid Message structs, so they should fail
            assert!(result.is_err());
        }
    }

    #[tokio::test]
    async fn test_invalid_msgpack_data() {
        // Test with incomplete MessagePack data
        // This is an incomplete MessagePack array that should fail to decode
        let invalid_msgpack = Bytes::from(vec![0x93]); // Array with 3 elements but no data following
        let result = ConnectionHandler::process_message(&invalid_msgpack);
        // This should fail because the MessagePack is incomplete
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_valid_msgpack_decodes_successfully() {
        // Test that a Nil value is valid MessagePack but not a valid Message struct
        let valid_msgpack = Bytes::from(vec![0xc0]); // Nil in MessagePack
        let result = ConnectionHandler::process_message(&valid_msgpack);
        // This should fail because Nil is not a valid Message
        assert!(result.is_err());
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
