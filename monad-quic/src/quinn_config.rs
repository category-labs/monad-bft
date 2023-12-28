use std::{sync::Arc, time::Duration};

use monad_crypto::{
    rustls::{self, TlsVerifier},
    secp256k1::KeyPair,
};
use monad_types::NodeId;
use quinn_proto::{congestion::CubicConfig, TransportConfig};

/// QuinnConfig encapsulates all quinn-specific configuration
pub trait QuinnConfig {
    fn transport(&self) -> Arc<TransportConfig>;
    fn client(&self) -> Arc<dyn quinn_proto::crypto::ClientConfig>;
    fn server(&self) -> Arc<dyn quinn_proto::crypto::ServerConfig>;

    /// Get the NodeId of the remote for a given quinn connection
    fn remote_peer_id(connection: &quinn::Connection) -> NodeId;
}

pub struct SafeQuinnConfig {
    transport: Arc<TransportConfig>,
    client: Arc<dyn quinn_proto::crypto::ClientConfig>,
    server: Arc<dyn quinn_proto::crypto::ServerConfig>,
}

impl SafeQuinnConfig {
    /// bandwidth_Mbps is in Megabit/s
    pub fn new(identity: &KeyPair, max_rtt: Duration, bandwidth_Mbps: u16) -> Self {
        let mut transport_config = TransportConfig::default();
        let bandwidth_Bps = bandwidth_Mbps as u64 * 125_000;
        let rwnd = bandwidth_Bps * max_rtt.as_millis() as u64 / 1000;
        transport_config
            .stream_receive_window(u32::try_from(rwnd).unwrap().into())
            .send_window(8 * rwnd)
            .initial_rtt(max_rtt) // not exactly initial.... because of quinn pacer
            .congestion_controller_factory(Arc::new({
                // this is necessary for seeding the quinn pacer correctly on init
                let mut cubic_config = CubicConfig::default();
                cubic_config.initial_window(rwnd);
                cubic_config
            }));
        Self {
            transport: Arc::new(transport_config),
            client: Arc::new(TlsVerifier::make_client_config(identity)),
            server: Arc::new(TlsVerifier::make_server_config(identity)),
        }
    }
}

impl QuinnConfig for SafeQuinnConfig {
    fn transport(&self) -> Arc<TransportConfig> {
        self.transport.clone()
    }

    fn client(&self) -> Arc<dyn quinn_proto::crypto::ClientConfig> {
        self.client.clone()
    }

    fn server(&self) -> Arc<dyn quinn_proto::crypto::ServerConfig> {
        self.server.clone()
    }

    fn remote_peer_id(connection: &quinn::Connection) -> NodeId {
        let identity = connection
            .peer_identity()
            .expect("all quic sessions have TLS identity");
        let certificates: Box<Vec<rustls::Certificate>> = identity
            .downcast()
            .expect("always is rustls cert for default quinn");

        let raw_cert = certificates.first().expect("TLS verifier should have cert");
        let cert = TlsVerifier::parse_cert(raw_cert).expect("cert must be x509 at this point");
        let pubkey =
            TlsVerifier::recover_node_pubkey(&cert).expect("must have valid pubkey at this point");
        NodeId(pubkey)
    }
}
