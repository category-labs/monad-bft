use std::{error::Error, sync::Arc};

use rustls::{
    client::{ServerCertVerified, ServerCertVerifier},
    server::ClientCertVerifier,
};

fn make_certificate() -> Result<(rustls::Certificate, rustls::PrivateKey), Box<dyn Error>> {
    let rcgen_key = rcgen::KeyPair::generate(&rcgen::PKCS_ED25519)?;
    let key = rustls::PrivateKey(rcgen_key.serialize_der());

    let mut params = rcgen::CertificateParams::new(Vec::new());
    params.alg = &rcgen::PKCS_ED25519;
    params.key_pair = Some(rcgen_key);

    let certificate =
        rustls::Certificate(rcgen::Certificate::from_params(params)?.serialize_der()?);

    Ok((certificate, key))
}

pub struct UnsafeTlsVerifier;

impl UnsafeTlsVerifier {
    pub fn make_server_config() -> rustls::ServerConfig {
        let (certificate, cert_keypair) =
            make_certificate().expect("making certificate should always succed");
        rustls::ServerConfig::builder()
            .with_safe_defaults()
            .with_client_cert_verifier(Arc::new(Self))
            .with_single_cert(vec![certificate], cert_keypair)
            .expect("building ServerConfig should always succed")
    }

    pub fn make_client_config() -> rustls::ClientConfig {
        let (certificate, cert_keypair) =
            make_certificate().expect("making certificate should always succed");
        rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_custom_certificate_verifier(Arc::new(Self))
            .with_client_auth_cert(vec![certificate], cert_keypair)
            .expect("building ClientConfig should always succeed")
    }
}

impl ClientCertVerifier for UnsafeTlsVerifier {
    fn client_auth_root_subjects(&self) -> &[rustls::DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        _end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _now: std::time::SystemTime,
    ) -> Result<rustls::server::ClientCertVerified, rustls::Error> {
        Ok(rustls::server::ClientCertVerified::assertion())
    }
}

impl ServerCertVerifier for UnsafeTlsVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }
}
