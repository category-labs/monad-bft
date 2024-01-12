use std::{error::Error, marker::PhantomData, sync::Arc};

pub use rustls::Certificate;
use rustls::{
    cipher_suite::{
        TLS13_AES_128_GCM_SHA256, TLS13_AES_256_GCM_SHA384, TLS13_CHACHA20_POLY1305_SHA256,
    },
    client::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    server::ClientCertVerifier,
    SignatureScheme, SupportedCipherSuite, SupportedProtocolVersion,
};
use x509_parser::{oid_registry::Oid, prelude::X509Certificate, x509::SubjectPublicKeyInfo};

use crate::certificate_signature::{CertificateSignaturePubKey, CertificateSignatureRecoverable};

/// all TLS1.3 suites
static CIPHER_SUITES: &[SupportedCipherSuite] = &[
    TLS13_AES_256_GCM_SHA384,
    TLS13_AES_128_GCM_SHA256,
    TLS13_CHACHA20_POLY1305_SHA256,
];

static PROTOCOL_VERSIONS: &[&SupportedProtocolVersion] = &[&rustls::version::TLS13];
static RUSTLS_SIGNATURE_SCHEME: SignatureScheme = SignatureScheme::ED25519;
static RING_SIGNATURE_SCHEME: &ring::signature::EdDSAParameters = &ring::signature::ED25519;
static RCGEN_SIGNATURE_SCHEME: &rcgen::SignatureAlgorithm = &rcgen::PKCS_ED25519;

/// TlsVerifier needs to validate 3 things:
/// 1) Certificate is properly self-signed
/// 2) Pubkey can be recovered properly from the signature encoded in the certificate extension
/// 3) `message` passed in `verify_tls13_signature` has a valid signature created by the certificate
///
/// NOTE: THE USER OF TlsVerifier MUST MANUALLY VALIDATE THAT THE PUBKEY RECOVERED IS AS EXPECTED
/// This should be done with `parse_cert` and `recover_node_pubkey`
pub struct TlsVerifier<ST: CertificateSignatureRecoverable>(PhantomData<ST>);

impl<ST: CertificateSignatureRecoverable> Default for TlsVerifier<ST> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<ST: CertificateSignatureRecoverable> TlsVerifier<ST> {
    const OID_MONAD_EXTENSION: [u64; 6] = [0, 1, 2, 3, 4, 5];

    pub fn new() -> Self {
        Self::default()
    }

    /// Generates a random certificate, and signs it with node_keypair
    fn make_certificate(
        node_keypair: &ST::KeyPairType,
    ) -> Result<(Certificate, rustls::PrivateKey), Box<dyn Error>> {
        let rcgen_key = rcgen::KeyPair::generate(RCGEN_SIGNATURE_SCHEME)?;
        let rustls_key = rustls::PrivateKey(rcgen_key.serialize_der());

        let tls_key_signature = ST::sign(&rcgen_key.public_key_der(), node_keypair);

        let mut params = rcgen::CertificateParams::new(Vec::new());
        params.alg = RCGEN_SIGNATURE_SCHEME;
        params.key_pair = Some(rcgen_key);
        params
            .custom_extensions
            .push(rcgen::CustomExtension::from_oid_content(
                &Self::OID_MONAD_EXTENSION,
                tls_key_signature.serialize().to_vec(),
            ));

        let certificate = Certificate(rcgen::Certificate::from_params(params)?.serialize_der()?);

        Ok((certificate, rustls_key))
    }

    /// Generates a random certificate, and signs it with node_keypair
    pub fn make_server_config(node_keypair: &ST::KeyPairType) -> rustls::ServerConfig {
        let (certificate, cert_keypair) =
            Self::make_certificate(node_keypair).expect("making certificate should always succeed");
        rustls::ServerConfig::builder()
            .with_cipher_suites(CIPHER_SUITES)
            .with_safe_default_kx_groups()
            .with_protocol_versions(PROTOCOL_VERSIONS)
            .expect("server config shouldn't fail")
            .with_client_cert_verifier(Arc::new(Self::new()))
            .with_single_cert(vec![certificate], cert_keypair)
            .expect("building ServerConfig should always succed")
    }

    /// Passing in a `tls_ed25519_key` of None will cause a random one to be generated
    pub fn make_client_config(node_keypair: &ST::KeyPairType) -> rustls::ClientConfig {
        let (certificate, cert_keypair) =
            Self::make_certificate(node_keypair).expect("making certificate should always succeed");
        rustls::ClientConfig::builder()
            .with_cipher_suites(CIPHER_SUITES)
            .with_safe_default_kx_groups()
            .with_protocol_versions(PROTOCOL_VERSIONS)
            .expect("client config shouldn't fail")
            .with_custom_certificate_verifier(Arc::new(Self::new()))
            .with_client_auth_cert(vec![certificate], cert_keypair)
            .expect("building ClientConfig should always succeed")
    }

    /// Checks that the given certificate is well-formed, self-signed, and that the certificate is
    /// signed by the node_pubkey recovered from the extension
    fn verify_cert(
        end_entity: &Certificate,
        intermediates: &[Certificate],
    ) -> Result<(), rustls::Error> {
        if !intermediates.is_empty() {
            return Err(rustls::Error::General(
                "no intermediate certificates expected".to_owned(),
            ));
        }

        let cert = Self::parse_cert(end_entity)?;

        // Verifies that the certificate is self-signed
        Self::verify_cert_signature(
            cert.public_key(),
            cert.tbs_certificate.as_ref(),
            cert.signature_value.as_ref(),
        )?;

        // Make sure that a pubkey can be recovered from the certificate
        Self::recover_node_pubkey(&cert)?;

        Ok(())
    }

    /// Parse x509 certificate
    pub fn parse_cert(cert: &Certificate) -> Result<X509Certificate<'_>, rustls::Error> {
        use x509_parser::prelude::*;
        let (_, cert) = X509Certificate::from_der(&cert.0).map_err(|_err| {
            rustls::Error::InvalidCertificate(rustls::CertificateError::BadEncoding)
        })?;
        Ok(cert)
    }

    /// Fetch the validator pubkey attached to the certificate
    /// Does not verify that this pubkey signs over the certificate
    pub fn recover_node_pubkey(
        cert: &X509Certificate<'_>,
    ) -> Result<CertificateSignaturePubKey<ST>, rustls::Error> {
        // only 1 extension - the one containing the key is allowed
        if cert.extensions().len() != 1 {
            return Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::InvalidPurpose,
            ));
        }

        let extension = &cert.extensions()[0];
        // extension OID must match
        if extension.oid != Oid::from(&Self::OID_MONAD_EXTENSION).expect("oid is valid") {
            return Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::InvalidPurpose,
            ));
        }

        let signature = ST::deserialize(extension.value).map_err(|_err| {
            rustls::Error::InvalidCertificate(rustls::CertificateError::BadEncoding)
        })?;

        let pubkey = signature
            .recover_pubkey(cert.public_key().raw)
            .map_err(|_err| {
                rustls::Error::InvalidCertificate(rustls::CertificateError::BadSignature)
            })?;

        Ok(pubkey)
    }

    /// Checks that `message` is signed by the certificate ed25519 key
    fn verify_cert_signature(
        cert_pubkey: &SubjectPublicKeyInfo,
        message: &[u8],
        signature: &[u8],
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        let pubkey = ring::signature::UnparsedPublicKey::new(
            RING_SIGNATURE_SCHEME,
            cert_pubkey.subject_public_key.as_ref(),
        );
        pubkey.verify(message, signature).map_err(|_err| {
            rustls::Error::InvalidCertificate(rustls::CertificateError::BadSignature)
        })?;
        Ok(HandshakeSignatureValid::assertion())
    }
}

impl<ST: CertificateSignatureRecoverable> ClientCertVerifier for TlsVerifier<ST> {
    /// Certificates are self-signed, so this can be blank
    fn client_auth_root_subjects(&self) -> &[rustls::DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &Certificate,
        intermediates: &[Certificate],
        // Time is irrelevant
        _now: std::time::SystemTime,
    ) -> Result<rustls::server::ClientCertVerified, rustls::Error> {
        Self::verify_cert(end_entity, intermediates)?;
        Ok(rustls::server::ClientCertVerified::assertion())
    }

    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        true
    }

    /// We only pass in TLS 1.3 into ClientConfig::with_cipher_suites
    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &Certificate,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::HandshakeSignatureValid, rustls::Error> {
        unreachable!("TLS 1.2 not supported")
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &Certificate,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        let cert = Self::parse_cert(cert)?;
        let signature = dss.signature();
        Self::verify_cert_signature(cert.public_key(), message, signature)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![RUSTLS_SIGNATURE_SCHEME]
    }
}

impl<ST: CertificateSignatureRecoverable> ServerCertVerifier for TlsVerifier<ST> {
    fn verify_server_cert(
        &self,
        end_entity: &Certificate,
        intermediates: &[Certificate],
        // server_name doesn't matter, because we're verifying the NodeId attached to the
        // certificate
        _server_name: &rustls::ServerName,
        // SCTS is for certificate transparency validation, which dsoesn't apply to self-signed
        // certificates
        _scts: &mut dyn Iterator<Item = &[u8]>,
        // OCSCP (Online Certificate Status Protocol) doesn't apply to self-signed certificates
        _ocsp_response: &[u8],
        // Time is irrelevant
        _now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        Self::verify_cert(end_entity, intermediates)?;
        Ok(ServerCertVerified::assertion())
    }

    /// We only pass in TLS 1.3 into ServerConfig::with_cipher_suites
    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &Certificate,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::HandshakeSignatureValid, rustls::Error> {
        unreachable!("TLS 1.2 not supported")
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &Certificate,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        let cert = Self::parse_cert(cert)?;
        let signature = dss.signature();
        Self::verify_cert_signature(cert.public_key(), message, signature)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![RUSTLS_SIGNATURE_SCHEME]
    }

    /// SCTS is for certificate transparency validation, which doesn't apply to self-signed
    /// certificates
    fn request_scts(&self) -> bool {
        false
    }
}
