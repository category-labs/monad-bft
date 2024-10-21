use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::{Executor, ExecutorMetricsChain};
use monad_executor_glue::{BootstrapPeer, DiscoveryCommand, MonadEvent, MonadNameRecord};
use monad_types::{Epoch, NodeId};

#[derive(Debug)]
pub struct NopDiscovery<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    phantom_data: PhantomData<(ST, SCT)>,
}

impl<ST, SCT> Default for NopDiscovery<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn default() -> Self {
        Self {
            phantom_data: PhantomData,
        }
    }
}

impl<ST, SCT> NopDiscovery<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub fn new() -> Self {
        Self {
            phantom_data: PhantomData,
        }
    }
}

impl<ST, SCT> Executor for NopDiscovery<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type Command = DiscoveryCommand<SCT::NodeIdPubKey>;

    fn exec(&mut self, commands: Vec<Self::Command>) {}

    fn metrics(&self) -> ExecutorMetricsChain {
        Default::default()
    }
}

impl<ST, SCT> Stream for NopDiscovery<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type Item = MonadEvent<ST, SCT>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}

pub trait MockableDiscovery:
    Executor<Command = DiscoveryCommand<CertificateSignaturePubKey<Self::SignatureType>>> + Unpin
{
    type SignatureType: CertificateSignatureRecoverable;
    type SignatureCollectionType: SignatureCollection<
        NodeIdPubKey = CertificateSignaturePubKey<Self::SignatureType>,
    >;

    fn ready(&self) -> bool;
    fn pop(&mut self) -> Option<MonadEvent<Self::SignatureType, Self::SignatureCollectionType>>;
}

impl<T: MockableDiscovery + ?Sized> MockableDiscovery for Box<T> {
    type SignatureType = T::SignatureType;
    type SignatureCollectionType = T::SignatureCollectionType;

    fn ready(&self) -> bool {
        (**self).ready()
    }
    fn pop(&mut self) -> Option<MonadEvent<Self::SignatureType, Self::SignatureCollectionType>> {
        (**self).pop()
    }
}

#[derive(Debug)]
pub struct MockStakedDiscovery<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    local_name_record: Option<MonadNameRecord<SCT::NodeIdPubKey>>,
    bootstrap_peers: Vec<BootstrapPeer<SCT::NodeIdPubKey>>,
    epoch_validators: BTreeMap<Epoch, HashSet<NodeId<SCT::NodeIdPubKey>>>,
    known_peers: BTreeMap<NodeId<SCT::NodeIdPubKey>, MonadNameRecord<SCT::NodeIdPubKey>>,
    events: VecDeque<MonadEvent<ST, SCT>>,
    phantom_data: PhantomData<ST>,
}

impl<ST, SCT> MockStakedDiscovery<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub fn new() -> Self {
        Self::default()
    }
}

impl<ST, SCT> Default for MockStakedDiscovery<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn default() -> Self {
        Self {
            local_name_record: None,
            bootstrap_peers: vec![],
            epoch_validators: Default::default(),
            known_peers: Default::default(),
            events: Default::default(),
            phantom_data: PhantomData,
        }
    }
}

impl<ST, SCT> Executor for MockStakedDiscovery<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type Command = DiscoveryCommand<SCT::NodeIdPubKey>;

    fn exec(&mut self, commands: Vec<Self::Command>) {}

    fn metrics(&self) -> ExecutorMetricsChain {
        Default::default()
    }
}

impl<ST, SCT> Stream for MockStakedDiscovery<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type Item = MonadEvent<ST, SCT>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}

impl<ST, SCT> MockableDiscovery for MockStakedDiscovery<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type SignatureType = ST;
    type SignatureCollectionType = SCT;

    fn ready(&self) -> bool {
        !self.events.is_empty()
    }

    fn pop(&mut self) -> Option<MonadEvent<ST, SCT>> {
        self.events.pop_front()
    }
}
