use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    marker::PhantomData,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use futures::Stream;
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{
    BootstrapPeer, DiscoveryCommand, DiscoveryEvent, DiscoveryNetworkMessage, DiscoveryRequest,
    MonadEvent, MonadNameRecord, OutboundDiscoveryMessage,
};
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
    current_epoch: Epoch,
    pending_events: VecDeque<MonadEvent<ST, SCT>>,

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
            current_epoch: Epoch(0),
            pending_events: Default::default(),
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
        for command in commands {
            match command {
                DiscoveryCommand::BootstrapPeers => {
                    for bootstrap_peer in &self.bootstrap_peers {
                        self.pending_events.push_back(MonadEvent::DiscoveryEvent(
                            DiscoveryEvent::Outbound(OutboundDiscoveryMessage {
                                recipient: bootstrap_peer.node_id,
                                message: DiscoveryNetworkMessage::Request(DiscoveryRequest {
                                    sender: self.local_name_record.clone(),
                                }),
                            }),
                        ));
                    }
                }
                DiscoveryCommand::Message(_) => {
                    todo!()
                }
                DiscoveryCommand::AddEpochValidatorSet {
                    epoch,
                    validator_set,
                } => {
                    if let Some(epoch_validators) = self.epoch_validators.get(&epoch) {
                        assert_eq!(validator_set.len(), epoch_validators.len());
                        assert_eq!(
                            epoch_validators.clone().into_iter().collect::<Vec<_>>(),
                            validator_set
                        );
                        tracing::warn!(
                            "duplicate validator set update (this is safe but unexpected)"
                        )
                    } else {
                        assert!(self
                            .epoch_validators
                            .insert(epoch, validator_set.into_iter().collect())
                            .is_none());
                    }
                }
                DiscoveryCommand::UpdateCurrentRound(epoch, _) => {
                    assert!(epoch >= self.current_epoch);
                    self.current_epoch = epoch;

                    while let Some(entry) = self.epoch_validators.first_entry() {
                        if *entry.key() + Epoch(1) < self.current_epoch {
                            entry.remove();
                        } else {
                            break;
                        }
                    }
                }
            }
        }
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
        let this = self.deref_mut();

        if let Some(event) = this.pending_events.pop_front() {
            return Poll::Ready(Some(event));
        }
        Poll::Pending
    }
}
