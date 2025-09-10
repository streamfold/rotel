// SPDX-License-Identifier: Apache-2.0

#[derive(Clone)]
pub struct Transformer {
    transformer: TraceTransformer,
}

impl Transformer {
    pub fn new(environment: String) -> Self {
        Self {
            transformer: TraceTransformer::new(environment),
        }
    }
}

impl TransformPayload<ResourceSpans> for Transformer {
    fn transform(&self, res_spans: Vec<ResourceSpans>) -> Result<Vec<Value>, ExportError> {
        let mut payload = Vec::with_capacity(res_spans.len());
        for rs in res_spans {
            for ss in rs.scope_spans {
                for span in ss.spans {
                    let v = self.transformer.apply(span)?;
                    payload.push(v)
                }
            }
        }
        Ok(payload)
    }
}

#[derive(Clone, Debug)]
pub struct TraceTransformer {
    environment: String,
}

/// Represents errors that can occur during export.
#[derive(Debug, Error)]
pub enum ExportError {
    #[error("Timestamp error")]
    TimestampError,
    #[error("Exporter is shut down")]
    Shutdown,
    #[error("Export timeout after {0:?}")]
    Timeout(Duration),
    #[error("Serialization error: {0}")]
    Serialization(#[from] JsonError),
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Utf8 error: {0}")]
    Utf8Error(#[from] Utf8Error),
    #[error("Utf8 error: {0}")]
    FromUtf8Error(#[from] FromUtf8Error),
    #[error("Export failed: {message}, trace id: {trace_id}")]
    ExportFailed { message: String, trace_id: String },
}

/// A simple enum to control validation behavior.
#[derive(Debug)]
pub enum ValueType {
    HttpRequest,
    HttpResponse,
    #[allow(dead_code)]
    Exception,
    Annotation,
    Metadata,
}

impl From<ExportError> for TraceError {
    fn from(err: ExportError) -> Self {
        TraceError::Other(Box::new(err))
    }
}


impl TraceTransformer {
    pub fn new(environment: String) -> Self {
        Self { environment }
    }

    pub fn apply(&self, span: Span) -> Result<Value, ExportError> {
    }
}
