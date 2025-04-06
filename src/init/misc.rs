use crate::listener::Listener;
use std::collections::HashMap;
use std::net::SocketAddr;
use tower::BoxError;

// Is it possible to bind these concurrently for faster startup?
pub fn bind_endpoints(endpoints: &[SocketAddr]) -> Result<HashMap<SocketAddr, Listener>, BoxError> {
    endpoints
        .iter()
        .map(|endpoint| match Listener::listen_std(*endpoint) {
            Ok(l) => Ok((*endpoint, l)),
            Err(e) => Err(e),
        })
        .collect()
}
