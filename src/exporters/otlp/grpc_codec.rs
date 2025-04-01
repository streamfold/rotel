// SPDX-License-Identifier: Apache-2.0

use crate::exporters::otlp::errors::ExporterError;
use crate::telemetry::Counter;
use bytes::{BufMut, Bytes, BytesMut};
use flate2::read::GzEncoder;
use flate2::write::GzDecoder;
use flate2::Compression as GZCompression;
use opentelemetry::KeyValue;
use std::io::{Read, Write};
use tonic::Status;

// one byte for the compression flag plus four bytes for the length
const GRPC_HEADER_SIZE: usize = 5;

// cap our response buffer allocation
const GRPC_MAX_RESPONSE_SIZE: u32 = 1024 * 1024;

/// Decode a gRPC response
pub fn grpc_decode_body<T: prost::Message + Default>(
    body: Bytes,
    failed: Box<dyn Counter<u64> + Send + Sync + 'static>,
    count: u64,
) -> Result<T, ExporterError> {
    if body.len() < GRPC_HEADER_SIZE {
        failed.add(
            count,
            &[
                KeyValue::new("error", "grpc.header.size"),
                KeyValue::new("value", body.len().to_string()),
            ],
        );
        return Err(ExporterError::Generic(format!(
            "invalid response size: {}",
            body.len()
        )));
    }

    let is_gz = body.first().unwrap();
    let len = u32::from_be_bytes(body.slice(1..5).as_ref().try_into().unwrap());
    if len == 0 {
        return Ok(T::default());
    }
    if len > GRPC_MAX_RESPONSE_SIZE {
        failed.add(
            count,
            &[
                KeyValue::new("error", "grpc.body.size"),
                KeyValue::new("value", body.len().to_string()),
            ],
        );
        return Err(ExporterError::Grpc(Status::failed_precondition(
            "message too large",
        )));
    }

    let body = body.slice(5..);

    if *is_gz == 0 {
        match T::decode(body) {
            Ok(r) => Ok(r),
            Err(e) => {
                failed.add(count, &[KeyValue::new("error", "grpc.decode")]);
                Err(ExporterError::Generic(format!(
                    "failed to decode response: {}",
                    e
                )))
            }
        }
    } else {
        let buf_out = Vec::new();
        let mut dec = GzDecoder::new(buf_out);
        if let Err(e) = dec.write_all(body.as_ref()) {
            failed.add(count, &[KeyValue::new("error", "grpc.decode.gzip")]);
            return Err(ExporterError::Generic(format!(
                "failed to gzip decode response: {}",
                e
            )));
        }

        match dec.finish() {
            Ok(buf) => match T::decode(Bytes::from(buf)) {
                Ok(r) => Ok(r),
                Err(e) => {
                    failed.add(count, &[KeyValue::new("error", "grpc.decode")]);
                    Err(ExporterError::Generic(format!(
                        "failed to decode response: {}",
                        e
                    )))
                }
            },

            Err(e) => {
                failed.add(count, &[KeyValue::new("error", "grpc.decode.gzip")]);
                Err(ExporterError::Generic(format!(
                    "failed to finish gzip decode of response: {}",
                    e
                )))
            }
        }
    }
}

// taken from <https://github.com/hyperium/tonic/blob/5aa8ae1fec27377cd4c2a41d309945d7e38087d0/examples/src/grpc-web/client.rs#L45-L75>
pub fn grpc_encode_body<T>(msg: T, compress: bool) -> Result<Bytes, Box<dyn std::error::Error>>
where
    T: prost::Message,
{
    let mut buf = BytesMut::with_capacity(1024);

    // first skip past the header
    // cannot write it yet since we don't know the size of the
    // encoded message
    buf.reserve(GRPC_HEADER_SIZE);
    unsafe {
        buf.advance_mut(GRPC_HEADER_SIZE);
    }

    if compress {
        let mut uncompressed = BytesMut::with_capacity(1024);

        msg.encode(&mut uncompressed).unwrap();

        let mut gz = GzEncoder::new(&uncompressed[..], GZCompression::default());
        let mut buffer = Vec::new();

        // todo: Can we eliminate this extra buffer?
        if let Err(e) = gz.read_to_end(&mut buffer) {
            return Err(format!("unable to gzip encode buffer: {}", e).into());
        }

        buf.put(&buffer[..]);
    } else {
        // write the message
        msg.encode(&mut buf).unwrap();
    }

    // now we know the size of encoded message and can write the
    // header
    let len = buf.len() - GRPC_HEADER_SIZE;
    {
        let mut buf = &mut buf[..GRPC_HEADER_SIZE];

        // compression flag, 0 means "no compression"
        buf.put_u8(compress as u8);

        buf.put_u32(len as u32);
    }

    Ok(buf.split_to(len + GRPC_HEADER_SIZE).freeze())
}
