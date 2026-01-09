use pyo3::{pyclass, pymethods, PyResult};
use std::collections::HashMap;

#[pyclass]
#[derive(Clone)]
pub enum RequestContext {
    HttpContext(HttpContext),
    GrpcContext(GrpcContext),
}

#[pyclass]
#[derive(Clone)]
pub struct HttpContext {
    pub headers: HashMap<String, String>,
}

#[pyclass]
#[derive(Clone)]
pub struct GrpcContext {
    pub metadata: HashMap<String, String>,
}

#[pymethods]
impl RequestContext {
    #[getter]
    fn http_context(&self) -> PyResult<HttpContext> {
        match self {
            RequestContext::HttpContext(ctx) => Ok(ctx.clone()),
            _ => Err(pyo3::exceptions::PyAttributeError::new_err(
                "not an HttpContext variant",
            )),
        }
    }

    #[getter]
    fn grpc_context(&self) -> PyResult<GrpcContext> {
        match self {
            RequestContext::GrpcContext(ctx) => Ok(ctx.clone()),
            _ => Err(pyo3::exceptions::PyAttributeError::new_err(
                "not an GrpcContext variant",
            )),
        }
    }
}

#[pymethods]
impl HttpContext {
    #[new]
    fn new(headers: HashMap<String, String>) -> PyResult<Self> {
        Ok(HttpContext { headers })
    }

    #[getter]
    fn headers(&self) -> PyResult<HashMap<String, String>> {
        Ok(self.headers.clone())
    }
}

#[pymethods]
impl GrpcContext {
    #[new]
    fn new(metadata: HashMap<String, String>) -> PyResult<Self> {
        Ok(GrpcContext { metadata })
    }

    #[getter]
    fn metadata(&self) -> PyResult<HashMap<String, String>> {
        Ok(self.metadata.clone())
    }
}
