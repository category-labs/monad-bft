use std::{
    collections::{BTreeMap, BTreeSet},
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use futures::Stream;
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{BootstrapPeer, DiscoveryCommand, MonadEvent, MonadNameRecord};
use monad_types::{Epoch, NodeId};

pub struct StakedDiscovery<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    local_name_record: MonadNameRecord<SCT::NodeIdPubKey>,
    bootstrap_peers: Vec<BootstrapPeer<SCT::NodeIdPubKey>>,
    epoch_validators: BTreeMap<Epoch, BTreeSet<NodeId<SCT::NodeIdPubKey>>>,
    known_peers: BTreeMap<NodeId<SCT::NodeIdPubKey>, MonadNameRecord<SCT::NodeIdPubKey>>,

    waker: Option<Waker>,
    metrics: ExecutorMetrics,
    _phantom_data: PhantomData<ST>,
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
            epoch_validators: Default::default(),
            known_peers: Default::default(),
            waker: None,
            metrics: Default::default(),
            _phantom_data: PhantomData,
        }
    }
}

impl<ST, SCT> Executor for StakedDiscovery<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type Command = DiscoveryCommand<SCT::NodeIdPubKey>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {}
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}

impl<ST, SCT> Stream for StakedDiscovery<ST, SCT>
where
    Self: Unpin,
    ST: CertificateSignatureRecoverable + Unpin,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Unpin,
{
    type Item = MonadEvent<ST, SCT>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}
