// SPDX-License-Identifier: Apache-2.0

use crate::exporters::otlp::errors::ExporterError;
use bytes::{Bytes, BytesMut};
use flate2::read::GzEncoder;
use flate2::write::GzDecoder;
use flate2::Compression as GZCompression;
use std::io::{Read, Write};

pub fn http_decode_body<T: prost::Message + Default>(
    body: Bytes,
    compress: bool,
) -> Result<T, ExporterError> {
    if !compress {
        match T::decode(body) {
            Ok(r) => Ok(r),
            Err(e) => Err(ExporterError::Generic(format!(
                "failed to decode response: {}",
                e
            ))),
        }
    } else {
        let buf_out = Vec::new();
        let mut dec = GzDecoder::new(buf_out);
        if let Err(e) = dec.write_all(body.as_ref()) {
            return Err(ExporterError::Generic(format!(
                "failed to gzip decode response: {}",
                e
            )));
        }

        match dec.finish() {
            Ok(buf) => match T::decode(Bytes::from(buf)) {
                Ok(r) => Ok(r),
                Err(e) => Err(ExporterError::Generic(format!(
                    "failed to decode response: {}",
                    e
                ))),
            },
            Err(e) => Err(ExporterError::Generic(format!(
                "failed to finish gzip decode of response: {}",
                e
            ))),
        }
    }
}

pub fn http_encode_body<T>(msg: T, compress: bool) -> Result<Bytes, Box<dyn std::error::Error>>
where
    T: prost::Message,
{
    let mut uncompressed = BytesMut::with_capacity(1024);

    msg.encode(&mut uncompressed).unwrap();

    if !compress {
        return Ok(uncompressed.freeze());
    }

    let mut gz = GzEncoder::new(&uncompressed[..], GZCompression::default());
    let mut buffer = Vec::new();

    if let Err(e) = gz.read_to_end(&mut buffer) {
        return Err(format!("unable to gzip encode buffer: {}", e).into());
    }

    Ok(Bytes::from(buffer))
}
