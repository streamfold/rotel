// SPDX-License-Identifier: Apache-2.0

use hyper_rustls::ConfigBuilderExt;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
use rustls_pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use std::error::Error;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use tower::BoxError;

#[derive(Default)]
pub struct Config {
    skip_verify: bool,
    ca_certificate: Option<CertificateDer<'static>>, // if absent, it will use native roots
    identity: Option<ClientIdentity<'static>>,
}

pub struct ClientIdentity<'a>(CertificateDer<'a>, PrivateKeyDer<'a>);

#[derive(Clone, Debug)]
pub(crate) enum FileType {
    File(String),
    Pem(String),
}

#[derive(Clone, Default)]
pub struct ConfigBuilder {
    ca: Option<FileType>,
    cert: Option<FileType>,
    key: Option<FileType>,
    tls_skip_verify: bool,
}

impl Config {
    pub fn builder() -> ConfigBuilder {
        Default::default()
    }

    pub fn into_client_config(self) -> Result<ClientConfig, Box<dyn Error + Send + Sync>> {
        let client_config: rustls::ConfigBuilder<_, _>;

        if self.skip_verify {
            client_config = ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(SkipServerVerification::new());
        } else if self.ca_certificate.is_none() {
            client_config = ClientConfig::builder().with_native_roots()?
        } else {
            let cert = self.ca_certificate.clone().unwrap();
            let mut root_store = RootCertStore::empty();
            root_store.add(cert)?;

            client_config = ClientConfig::builder().with_root_certificates(root_store);
        }

        let client_config = if let Some(identity) = &self.identity {
            let cert_vec = vec![identity.0.clone()];
            client_config.with_client_auth_cert(cert_vec, identity.1.clone_key())?
        } else {
            client_config.with_no_client_auth()
        };

        Ok(client_config)
    }
}

impl ConfigBuilder {
    pub fn with_cert_file(mut self, cert_file: String) -> Self {
        self.cert = Some(FileType::File(cert_file));
        self
    }

    pub fn with_cert_pem(mut self, cert_pem: String) -> Self {
        self.cert = Some(FileType::Pem(cert_pem));
        self
    }

    pub fn with_key_file(mut self, key_file: String) -> Self {
        self.key = Some(FileType::File(key_file));
        self
    }

    pub fn with_key_pem(mut self, key_pem: String) -> Self {
        self.key = Some(FileType::Pem(key_pem));
        self
    }

    pub fn with_ca_file(mut self, ca_file: String) -> Self {
        self.ca = Some(FileType::File(ca_file));
        self
    }

    pub fn with_ca_pem(mut self, ca_pem: String) -> Self {
        self.ca = Some(FileType::Pem(ca_pem));
        self
    }

    pub fn with_tls_skip_verify(mut self, skip_verify: bool) -> Self {
        self.tls_skip_verify = skip_verify;
        self
    }

    pub fn build(self) -> Result<Config, BoxError> {
        // Handle incomplete TLS identity settings
        if (self.cert.is_some() && self.key.is_none())
            || (self.cert.is_none() && self.key.is_some())
        {
            return Err("Either both or neither of cert and key must be provided".into());
        }

        // Check and see if the user provided a CA certificate, if they didn't we're going to use system roots
        let ca_certificate = self.get_optional_ca_certificate()?;
        let skip_verify = self.tls_skip_verify;

        // We only need to do an identity if both cert and key are set. If neither are set we can
        // exit this function early without building an identity.
        if self.cert.is_none() && self.key.is_none() {
            let tls = if ca_certificate.is_none() {
                Config {
                    skip_verify,
                    ..Default::default()
                }
            } else {
                Config {
                    skip_verify,
                    ca_certificate,
                    ..Default::default()
                }
            };
            return Ok(tls);
        }

        // Unfortunately, both cert and key are set so we need to build an identity.
        // We're safe to unwrap both of these directly here as we checked for none cases above.
        let cert = load_cert(self.cert.clone().unwrap())?;
        let key = load_key(self.key.clone().unwrap())?;

        let client_identity = ClientIdentity(cert, key);

        // Alright now return a configuration that also includes the identity.
        let tls = if ca_certificate.is_some() {
            Config {
                skip_verify,
                identity: Some(client_identity),
                ca_certificate,
            }
        } else {
            Config {
                skip_verify,
                identity: Some(client_identity),
                ..Default::default()
            }
        };

        Ok(tls)
    }

    fn get_optional_ca_certificate(
        &self,
    ) -> Result<Option<CertificateDer<'static>>, Box<dyn Error + Send + Sync>> {
        if self.ca.is_none() {
            return Ok(None);
        }

        let cert = load_cert(self.ca.clone().unwrap())?;
        Ok(Some(cert.into_owned()))
    }
}

fn load_cert(cert_file: FileType) -> Result<CertificateDer<'static>, Box<dyn Error + Send + Sync>> {
    match cert_file {
        FileType::File(f) => Ok(CertificateDer::from_pem_file(f)?),
        FileType::Pem(f) => Ok(CertificateDer::from_pem_reader(f.as_bytes())?),
    }
}

fn load_key(key_file: FileType) -> Result<PrivateKeyDer<'static>, Box<dyn Error + Send + Sync>> {
    match key_file {
        FileType::File(f) => Ok(PrivateKeyDer::from_pem_file(f)?),
        FileType::Pem(f) => Ok(PrivateKeyDer::from_pem_reader(f.as_bytes())?),
    }
}

// Implementation of `ServerCertVerifier` that verifies everything as trustworthy.
// Note: This is dangerous but has been added for parity with OTel collectors insecure_skip_verify,
// see: https://github.com/open-telemetry/opentelemetry-collector/blob/main/config/configtls/README.md
//
// THIS SHOULD ONLY BE USED IN SITUATIONS WHERE YOU ABSOLUTELY NEED TO BYPASS SSL
// VERIFICATION FOR TESTING PURPOSES OR WHEN CONNECTING TO A SERVER WITH A SELF-SIGNED CERTIFICATE
// THAT YOU FULLY TRUST!!!
struct SkipServerVerification;

impl SkipServerVerification {
    fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

impl Debug for SkipServerVerification {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SkipServerVerification")
    }
}

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA1,
            SignatureScheme::ECDSA_SHA1_Legacy,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ED25519,
            SignatureScheme::ED448,
        ]
    }
}
