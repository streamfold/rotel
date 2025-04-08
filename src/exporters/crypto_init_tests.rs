use std::sync::Once;

static INIT_CRYPTO: Once = Once::new();

// Allowing dead_code because this is for unit tests ONLY!
#[allow(dead_code)]
pub fn init_crypto() {
    INIT_CRYPTO.call_once(|| {
        rustls::crypto::ring::default_provider()
            .install_default()
            .unwrap()
    });
}
