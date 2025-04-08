#[cfg(test)]
use std::sync::Once;

#[cfg(test)]
static INIT_CRYPTO: Once = Once::new();

#[cfg(test)]
pub fn init_crypto() {
    INIT_CRYPTO.call_once(|| {
        rustls::crypto::ring::default_provider()
            .install_default()
            .unwrap()
    });
}
