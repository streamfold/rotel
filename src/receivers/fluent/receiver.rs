// SPDX-License-Identifier: Apache-2.0

use crate::receivers::fluent::config::FluentReceiverConfig;
use crate::receivers::fluent::error::{FluentReceiverError, Result};
use crate::receivers::fluent::message::Message;
use crate::receivers::otlp_output::OTLPOutput;
use crate::topology::payload;
use bytes::BytesMut;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use rmpv::Value;
use std::fs;
use std::io::Cursor;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};
use tokio::select;
use tokio::task::JoinSet;
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

        // Check if we have enough data to determine message size
        if src.len() == 0 {
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
    traces_output: Option<OTLPOutput<payload::Message<ResourceSpans>>>,
    metrics_output: Option<OTLPOutput<payload::Message<ResourceMetrics>>>,
    logs_output: Option<OTLPOutput<payload::Message<ResourceLogs>>>,
    unix_listener: Option<UnixListener>,
    tcp_listener: Option<TcpListener>,
}

#[derive(Clone)]
struct ConnectionHandler {
    traces_output: Option<OTLPOutput<payload::Message<ResourceSpans>>>,
    metrics_output: Option<OTLPOutput<payload::Message<ResourceMetrics>>>,
    logs_output: Option<OTLPOutput<payload::Message<ResourceLogs>>>,
    cancel_token: CancellationToken,
}

impl FluentReceiver {
    pub async fn new(
        config: FluentReceiverConfig,
        traces_output: Option<OTLPOutput<payload::Message<ResourceSpans>>>,
        metrics_output: Option<OTLPOutput<payload::Message<ResourceMetrics>>>,
        logs_output: Option<OTLPOutput<payload::Message<ResourceLogs>>>,
    ) -> Result<Self> {
        // Validate that at least one signal type is enabled
        if !config.traces && !config.metrics && !config.logs {
            return Err(FluentReceiverError::ConfigurationError(
                "At least one signal type (traces, metrics, or logs) must be enabled".to_string(),
            ));
        }

        // Validate that at least one listener type is configured
        if config.socket_path.is_none() && config.endpoint.is_none() {
            return Err(FluentReceiverError::ConfigurationError(
                "At least one of socket_path or endpoint must be configured".to_string(),
            ));
        }

        // Setup Unix socket listener if configured
        let unix_listener = if let Some(socket_path) = &config.socket_path {
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

            Some(listener)
        } else {
            None
        };

        // Setup TCP listener if configured
        let tcp_listener = if let Some(endpoint) = config.endpoint {
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

            Some(listener)
        } else {
            None
        };

        Ok(Self {
            config,
            traces_output,
            metrics_output,
            logs_output,
            unix_listener,
            tcp_listener,
        })
    }

    fn create_handler(&self, cancel_token: CancellationToken) -> ConnectionHandler {
        ConnectionHandler {
            traces_output: self.traces_output.clone(),
            metrics_output: self.metrics_output.clone(),
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
        use tokio_stream::StreamExt;

        loop {
            select! {
                frame_result = framed.next() => {
                    match frame_result {
                        Some(Ok(message_bytes)) => {
                            debug!(
                                "Received complete message of {} bytes from Fluent {}",
                                message_bytes.len(),
                                connection_type
                            );

                            // Process the received message
                            if let Err(e) = self.process_message(&message_bytes).await {
                                warn!("Failed to process message: {}", e);
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

    async fn process_message(&self, data: &[u8]) -> Result<()> {
        debug!("Processing message of {} bytes", data.len());

        // Try to decode as Message struct first
        match rmp_serde::from_slice::<Message>(data) {
            Ok(message) => {
                println!("Decoded Fluent Message: {:?}", message);
                // TODO: Convert to OTLP format and send to appropriate output channel
                return Ok(());
            }
            Err(e) => {
                error!("Failed to decode as Message struct: {}", e);
            }
        }

        // Fall back to generic MessagePack decoding
        let mut cursor = Cursor::new(data);
        match rmpv::decode::read_value(&mut cursor) {
            Ok(value) => {
                //println!("Decoded MessagePack message: {:#?}", value);

                // Log the structure in a more readable format
                println!("Received Fluent MessagePack");
                self.log_msgpack_structure(&value, 0);

                // TODO: Convert to OTLP format and send to appropriate output channel
                Ok(())
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

    fn log_msgpack_structure(&self, value: &Value, indent: usize) {
        let prefix = "  ".repeat(indent);
        match value {
            Value::Nil => println!("{}Nil", prefix),
            Value::Boolean(b) => println!("{}Boolean: {}", prefix, b),
            Value::Integer(i) => println!("{}Integer: {}", prefix, i),
            Value::F32(f) => println!("{}F32: {}", prefix, f),
            Value::F64(f) => println!("{}F64: {}", prefix, f),
            Value::String(s) => {
                if let Some(utf8_str) = s.as_str() {
                    println!("{}String: \"{}\"", prefix, utf8_str);
                } else {
                    println!("{}String (binary): {:?}", prefix, s);
                }
            }
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
                    self.log_msgpack_structure(item, indent + 1);
                }
            }
            Value::Map(map) => {
                println!("{}Map({} entries):", prefix, map.len());
                for (key, val) in map {
                    println!("{}Key:", prefix);
                    self.log_msgpack_structure(key, indent + 1);
                    println!("{}Value:", prefix);
                    self.log_msgpack_structure(val, indent + 1);
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

impl FluentReceiver {
    pub fn start(
        &mut self,
        task_set: &mut JoinSet<std::result::Result<(), BoxError>>,
        receivers_cancel: &CancellationToken,
    ) {
        info!("Starting Fluent receiver");

        // Spawn Unix socket listener task if configured
        if let Some(unix_listener) = self.unix_listener.take() {
            let handler = self.create_handler(receivers_cancel.clone());
            let cancel = receivers_cancel.clone();
            let socket_path = self.config.socket_path.clone();

            task_set.spawn(async move {
                info!("Unix socket listener task started");
                loop {
                    select! {
                        result = unix_listener.accept() => {
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
        if let Some(tcp_listener) = self.tcp_listener.take() {
            let handler = self.create_handler(receivers_cancel.clone());
            let cancel = receivers_cancel.clone();
            let endpoint = self.config.endpoint;

            task_set.spawn(async move {
                info!("TCP listener task started");
                loop {
                    select! {
                        result = tcp_listener.accept() => {
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
    }
}

impl Drop for FluentReceiver {
    fn drop(&mut self) {
        // Ensure socket file is cleaned up
        if let Some(socket_path) = &self.config.socket_path {
            if socket_path.exists() {
                let _ = fs::remove_file(socket_path);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_receiver_creation_with_valid_config() {
        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("test.sock");

        let config = FluentReceiverConfig::new(Some(socket_path.clone()), None).with_logs(true);

        let receiver = FluentReceiver::new(config, None, None, None).await;
        assert!(receiver.is_ok());

        // Cleanup
        drop(receiver);
        assert!(!socket_path.exists());
    }

    #[tokio::test]
    async fn test_receiver_creation_fails_without_signals() {
        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("test.sock");

        let config = FluentReceiverConfig::new(Some(socket_path), None)
            .with_traces(false)
            .with_metrics(false)
            .with_logs(false);

        let receiver = FluentReceiver::new(config, None, None, None).await;
        assert!(receiver.is_err());

        if let Err(FluentReceiverError::ConfigurationError(msg)) = receiver {
            assert!(msg.contains("At least one signal type"));
        } else {
            panic!("Expected ConfigurationError");
        }
    }

    #[tokio::test]
    async fn test_socket_cleanup_on_drop() {
        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("test.sock");

        let config = FluentReceiverConfig::new(Some(socket_path.clone()), None).with_logs(true);

        let receiver = FluentReceiver::new(config, None, None, None).await.unwrap();
        assert!(socket_path.exists());

        drop(receiver);
        assert!(!socket_path.exists());
    }

    #[tokio::test]
    async fn test_msgpack_decoding() {
        use rmpv::Value;

        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("test.sock");

        let config = FluentReceiverConfig::new(Some(socket_path.clone()), None).with_logs(true);
        let receiver = FluentReceiver::new(config, None, None, None).await.unwrap();
        let cancel_token = CancellationToken::new();
        let handler = receiver.create_handler(cancel_token);

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

        // Process the message
        let result = handler.process_message(&buffer).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_msgpack_simple_types() {
        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("test.sock");

        let config = FluentReceiverConfig::new(Some(socket_path.clone()), None).with_logs(true);
        let receiver = FluentReceiver::new(config, None, None, None).await.unwrap();
        let cancel_token = CancellationToken::new();
        let handler = receiver.create_handler(cancel_token);

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

            let result = handler.process_message(&buffer).await;
            assert!(result.is_ok());
        }
    }

    #[tokio::test]
    async fn test_invalid_msgpack_data() {
        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("test.sock");

        let config = FluentReceiverConfig::new(Some(socket_path.clone()), None).with_logs(true);
        let receiver = FluentReceiver::new(config, None, None, None).await.unwrap();
        let cancel_token = CancellationToken::new();
        let handler = receiver.create_handler(cancel_token);

        // Test with incomplete MessagePack data
        // This is an incomplete MessagePack array that should fail to decode
        let invalid_msgpack = vec![0x93]; // Array with 3 elements but no data following
        let result = handler.process_message(&invalid_msgpack).await;
        // This should fail because the MessagePack is incomplete
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_valid_msgpack_decodes_successfully() {
        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("test.sock");

        let config = FluentReceiverConfig::new(Some(socket_path.clone()), None).with_logs(true);
        let receiver = FluentReceiver::new(config, None, None, None).await.unwrap();
        let cancel_token = CancellationToken::new();
        let handler = receiver.create_handler(cancel_token);

        // Even some random bytes might decode as valid MessagePack
        // (e.g., 0xFF is a valid negative fixint in MessagePack)
        // So we just verify that valid MessagePack works
        let valid_msgpack = vec![0xc0]; // Nil in MessagePack
        let result = handler.process_message(&valid_msgpack).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_receiver_with_tcp_endpoint() {
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap(); // Use port 0 for automatic assignment
        let config = FluentReceiverConfig::new(None, Some(addr)).with_logs(true);

        let receiver = FluentReceiver::new(config, None, None, None).await;
        assert!(receiver.is_ok());

        let receiver = receiver.unwrap();
        assert!(receiver.unix_listener.is_none());
        assert!(receiver.tcp_listener.is_some());
    }

    #[tokio::test]
    async fn test_receiver_with_both_unix_and_tcp() {
        use std::net::SocketAddr;

        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("test.sock");
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();

        let config =
            FluentReceiverConfig::new(Some(socket_path.clone()), Some(addr)).with_logs(true);

        let receiver = FluentReceiver::new(config, None, None, None).await;
        assert!(receiver.is_ok());

        let receiver = receiver.unwrap();
        assert!(receiver.unix_listener.is_some());
        assert!(receiver.tcp_listener.is_some());
        assert!(socket_path.exists());

        drop(receiver);
        assert!(!socket_path.exists());
    }

    #[tokio::test]
    async fn test_receiver_fails_without_listeners() {
        let config = FluentReceiverConfig::new(None, None).with_logs(true);

        let receiver = FluentReceiver::new(config, None, None, None).await;
        assert!(receiver.is_err());

        if let Err(FluentReceiverError::ConfigurationError(msg)) = receiver {
            assert!(msg.contains("At least one of socket_path or endpoint"));
        } else {
            panic!("Expected ConfigurationError");
        }
    }
}
