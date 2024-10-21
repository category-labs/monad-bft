use std::marker::PhantomData;

use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};

use crate::{BootstrapPeer, Discovery, MonadNameRecord};

pub struct StakedDiscovery<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub local_name_record: MonadNameRecord<SCT::NodeIdPubKey>,
    pub bootstrap_peers: Vec<BootstrapPeer<SCT::NodeIdPubKey>>,
    pub phantom_data: PhantomData<ST>,
}

impl<ST, SCT> StakedDiscovery<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub fn new(
        local_name_record: MonadNameRecord<SCT::NodeIdPubKey>,
        bootstrap_peers: Vec<BootstrapPeer<SCT::NodeIdPubKey>>,
    ) -> Self {
        Self {
            local_name_record,
            bootstrap_peers,
            phantom_data: Default::default(),
        }
    }
}

impl<ST, SCT> Discovery<SCT::NodeIdPubKey> for StakedDiscovery<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn bootstrap_peers(&self) -> Vec<BootstrapPeer<SCT::NodeIdPubKey>> {
        self.bootstrap_peers.clone()
    }
}
