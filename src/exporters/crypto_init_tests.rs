#[cfg(test)]
use std::sync::Once;

#[cfg(test)]
static INIT_CRYPTO: Once = Once::new();

// Allowing dead_code because this is for unit tests ONLY!
#[cfg(test)]
pub fn init_crypto() {
    INIT_CRYPTO.call_once(|| {
        rustls::crypto::ring::default_provider()
            .install_default()
            .unwrap()
    });
}
