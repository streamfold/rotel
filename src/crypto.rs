use rustls::crypto::CryptoProvider;
use tower::BoxError;

pub fn init_crypto_provider() -> Result<(), BoxError> {
    if CryptoProvider::get_default().is_none() {
        return match rustls::crypto::aws_lc_rs::default_provider().install_default() {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("failed to initialize crypto library: {:?}", e).into()),
        };
    }
    Ok(())
}
