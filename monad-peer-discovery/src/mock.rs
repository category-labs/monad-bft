use std::{collections::HashMap, marker::PhantomData, net::SocketAddrV4};

use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::NodeId;
use tracing::debug;

use crate::{
    PeerDiscMetrics, PeerDiscoveryAlgo, PeerDiscoveryAlgoBuilder, PeerDiscoveryCommand,
    PeerLookupRequest, PeerLookupResponse, Ping, Pong,
};

pub struct NopDiscovery<ST: CertificateSignatureRecoverable> {
    metrics: PeerDiscMetrics,
    pd: PhantomData<ST>,
}

pub struct NopDiscoveryBuilder<ST: CertificateSignatureRecoverable> {
    pd: PhantomData<ST>,
}

impl<ST: CertificateSignatureRecoverable> Default for NopDiscoveryBuilder<ST> {
    fn default() -> Self {
        Self { pd: PhantomData }
    }
}

impl<ST: CertificateSignatureRecoverable> PeerDiscoveryAlgoBuilder for NopDiscoveryBuilder<ST> {
    type PeerDiscoveryAlgoType = NopDiscovery<ST>;

    fn build(
        self,
    ) -> (
        Self::PeerDiscoveryAlgoType,
        Vec<
            PeerDiscoveryCommand<<Self::PeerDiscoveryAlgoType as PeerDiscoveryAlgo>::SignatureType>,
        >,
    ) {
        let state = NopDiscovery {
            metrics: HashMap::new(),
            pd: PhantomData,
        };
        let cmds = Vec::new();

        (state, cmds)
    }
}

impl<ST> PeerDiscoveryAlgo for NopDiscovery<ST>
where
    ST: CertificateSignatureRecoverable,
{
    type SignatureType = ST;

    fn send_ping(
        &mut self,
        target: NodeId<CertificateSignaturePubKey<ST>>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?target, "handle send ping");

        Vec::new()
    }

    fn handle_ping(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<ST>>,
        ping: Ping<Self::SignatureType>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?from, ?ping, "handle ping");

        Vec::new()
    }

    fn handle_pong(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<ST>>,
        pong: Pong,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?from, ?pong, "handle pong");

        Vec::new()
    }

    fn handle_ping_timeout(
        &mut self,
        to: NodeId<CertificateSignaturePubKey<ST>>,
        ping_id: u32,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?to, ?ping_id, "handling ping timeout");

        Vec::new()
    }

    fn send_peer_lookup_request(
        &mut self,
        to: NodeId<CertificateSignaturePubKey<ST>>,
        target: NodeId<CertificateSignaturePubKey<ST>>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?to, ?target, "sending peer lookup request");

        Vec::new()
    }

    fn handle_peer_lookup_request(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<ST>>,
        request: PeerLookupRequest<ST>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?from, ?request, "handling peer lookup request");

        Vec::new()
    }

    fn handle_peer_lookup_response(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<ST>>,
        response: PeerLookupResponse<ST>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?from, ?response, "handling peer lookup response");

        Vec::new()
    }

    fn handle_peer_lookup_timeout(
        &mut self,
        _to: NodeId<CertificateSignaturePubKey<ST>>,
        _target: NodeId<CertificateSignaturePubKey<ST>>,
        lookup_id: u32,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?lookup_id, "peer lookup request timeout");

        Vec::new()
    }

    fn prune(&mut self) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!("pruning unresponsive peer nodes");

        Vec::new()
    }

    fn metrics(&self) -> &PeerDiscMetrics {
        &self.metrics
    }

    fn get_sock_addr_by_id(
        &self,
        _id: &NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
    ) -> Option<SocketAddrV4> {
        None
    }
}
