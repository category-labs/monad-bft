use std::marker::PhantomData;

use bytes::Bytes;
use itertools::Itertools;
use monad_consensus_types::{
    block::BlockPolicy, block_validator::BlockValidator, metrics::Metrics,
    payload::StateRootValidator, signature_collection::SignatureCollection, txpool::TxPool,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_executor_glue::{Command, MempoolEvent, MonadEvent, RouterCommand};
use monad_state_backend::StateBackend;
use monad_types::{NodeId, Round, RouterTarget};
use monad_validator::{
    epoch_manager::EpochManager,
    leader_election::LeaderElection,
    validator_set::{ValidatorSetType, ValidatorSetTypeFactory},
    validators_epoch_mapping::ValidatorsEpochMapping,
};

use crate::{ConsensusMode, MonadState, VerifiedMonadMessage};

// TODO configurable
const NUM_LEADERS_FORWARD: usize = 3;

pub(super) struct MempoolChildState<'a, ST, SCT, BPT, SBT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT, SBT>,
    SBT: StateBackend,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    TT: TxPool<SCT, BPT, SBT>,
    BVT: BlockValidator<SCT, BPT, SBT>,

    SVT: StateRootValidator,
{
    txpool: &'a mut TT,
    block_policy: &'a BPT,
    state_backend: &'a SBT,

    metrics: &'a mut Metrics,
    nodeid: &'a NodeId<CertificateSignaturePubKey<ST>>,
    consensus: &'a ConsensusMode<SCT, BPT, SBT>,
    leader_election: &'a LT,
    epoch_manager: &'a EpochManager,
    val_epoch_map: &'a ValidatorsEpochMapping<VTF, SCT>,

    _phantom: PhantomData<(ST, SCT, BPT, VTF, LT, TT, BVT, SVT, ASVT)>,
}

pub(super) enum MempoolCommand<PT: PubKey> {
    ForwardTxns(Vec<NodeId<PT>>, Vec<Bytes>),
}

impl<'a, ST, SCT, BPT, SBT, VTF, LT, TT, BVT, SVT, ASVT>
    MempoolChildState<'a, ST, SCT, BPT, SBT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT, SBT>,
    SBT: StateBackend,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    TT: TxPool<SCT, BPT, SBT>,
    BVT: BlockValidator<SCT, BPT, SBT>,
    SVT: StateRootValidator,
{
    pub(super) fn new(
        monad_state: &'a mut MonadState<ST, SCT, BPT, SBT, VTF, LT, TT, BVT, SVT, ASVT>,
    ) -> Self {
        Self {
            txpool: &mut monad_state.txpool,
            metrics: &mut monad_state.metrics,
            block_policy: &monad_state.block_policy,
            state_backend: &monad_state.state_backend,

            nodeid: &monad_state.nodeid,
            consensus: &monad_state.consensus,
            leader_election: &monad_state.leader_election,
            epoch_manager: &monad_state.epoch_manager,
            val_epoch_map: &monad_state.val_epoch_map,

            _phantom: PhantomData,
        }
    }

    fn get_leader(&self, round: Round) -> NodeId<CertificateSignaturePubKey<ST>> {
        let epoch = self
            .epoch_manager
            .get_epoch(round)
            .expect("epoch for current and future rounds always exist");
        let Some(next_validator_set) = self.val_epoch_map.get_val_set(&epoch) else {
            todo!("handle non-existent validatorset for next k round epoch");
        };
        let members = next_validator_set.get_members();
        self.leader_election.get_leader(round, members)
    }

    pub(super) fn update(
        &mut self,
        event: MempoolEvent<CertificateSignaturePubKey<ST>>,
    ) -> Vec<MempoolCommand<CertificateSignaturePubKey<ST>>> {
        let ConsensusMode::Live(consensus) = self.consensus else {
            tracing::trace!("ignoring MempoolEvent, not live yet");
            self.txpool.clear();
            return vec![];
        };
        match event {
            MempoolEvent::UserTxns(txns) => {
                let num_txns = txns.len() as u64;
                let valid_encoded_txs =
                    self.txpool
                        .insert_tx(txns, self.block_policy, self.state_backend);

                let num_valid_txns = valid_encoded_txs.len() as u64;
                self.metrics.txpool_events.local_inserted_txns += num_valid_txns;
                self.metrics.txpool_events.dropped_txns += num_txns - num_valid_txns;

                let round = consensus.get_current_round();
                let next_k_leaders = (round.0..)
                    .map(|round| self.get_leader(Round(round)))
                    .take(NUM_LEADERS_FORWARD)
                    .unique()
                    .filter(|leader| leader != self.nodeid)
                    .collect_vec();
                vec![MempoolCommand::ForwardTxns(
                    next_k_leaders,
                    valid_encoded_txs,
                )]
            }
            MempoolEvent::ForwardedTxns { sender, txns } => {
                let num_txns = txns.len() as u64;
                let valid_encoded_txs =
                    self.txpool
                        .insert_tx(txns, self.block_policy, self.state_backend);

                let num_valid_txns = valid_encoded_txs.len() as u64;
                self.metrics.txpool_events.external_inserted_txns += num_valid_txns;

                if num_valid_txns != num_txns {
                    tracing::warn!(?sender, "sender forwarded bad txns");
                }

                vec![]
            }
            MempoolEvent::Clear => {
                self.txpool.clear();
                vec![]
            }
        }
    }
}

impl<ST, SCT> From<MempoolCommand<CertificateSignaturePubKey<ST>>>
    for Vec<Command<MonadEvent<ST, SCT>, VerifiedMonadMessage<ST, SCT>, SCT>>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn from(value: MempoolCommand<CertificateSignaturePubKey<ST>>) -> Self {
        match value {
            MempoolCommand::ForwardTxns(targets, txns) => targets
                .into_iter()
                .map(|target| {
                    // TODO ideally we could batch these all as one RouterCommand(PointToPoint) so
                    // that we can:
                    // 1. avoid cloning txns
                    // 2. avoid serializing multiple times
                    // 3. avoid raptor coding multiple times
                    // 4. use 1 sendmmsg in the router
                    Command::RouterCommand(RouterCommand::Publish {
                        target: RouterTarget::PointToPoint(target),
                        message: VerifiedMonadMessage::ForwardedTx(txns.clone()),
                    })
                })
                .collect(),
        }
    }
}
