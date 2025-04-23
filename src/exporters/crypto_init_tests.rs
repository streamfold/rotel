#[cfg(test)]
use crate::crypto::init_crypto_provider;
#[cfg(test)]
use std::sync::Once;

#[cfg(test)]
static INIT_CRYPTO: Once = Once::new();

#[cfg(test)]
pub fn init_crypto() {
    INIT_CRYPTO.call_once(|| init_crypto_provider().unwrap());
}
