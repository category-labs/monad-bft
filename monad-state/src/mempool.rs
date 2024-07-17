use std::marker::PhantomData;

use bytes::Bytes;
use itertools::Itertools;
use monad_consensus_state::ConsensusState;
use monad_consensus_types::{
    block::{Block, BlockPolicy},
    block_validator::BlockValidator,
    metrics::Metrics,
    payload::StateRootValidator,
    signature_collection::SignatureCollection,
    txpool::TxPool,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_eth_reserve_balance::{state_backend::StateBackend, ReserveBalanceCacheTrait};
use monad_executor_glue::{Command, MempoolEvent, MonadEvent, RouterCommand};
use monad_types::{NodeId, Round, RouterTarget};
use monad_validator::{
    epoch_manager::EpochManager,
    leader_election::LeaderElection,
    validator_set::{ValidatorSetType, ValidatorSetTypeFactory},
    validators_epoch_mapping::ValidatorsEpochMapping,
};

use crate::{MonadState, VerifiedMonadMessage};

// TODO configurable
const NUM_LEADERS_FORWARD: usize = 3;

pub(super) struct MempoolChildState<'a, ST, SCT, BPT, SBT, RBCT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT, SBT, RBCT>,
    SBT: StateBackend,
    RBCT: ReserveBalanceCacheTrait<SBT>,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    TT: TxPool<SCT, BPT, SBT, RBCT>,
    BVT: BlockValidator<SCT, BPT, SBT, RBCT>,

    SVT: StateRootValidator,
{
    txpool: &'a mut TT,
    block_policy: &'a BPT,
    reserve_balance_cache: &'a mut RBCT,

    metrics: &'a mut Metrics,
    nodeid: &'a NodeId<CertificateSignaturePubKey<ST>>,
    consensus: &'a ConsensusState<ST, SCT, BPT, SBT, RBCT>,
    leader_election: &'a LT,
    epoch_manager: &'a EpochManager,
    val_epoch_map: &'a ValidatorsEpochMapping<VTF, SCT>,

    _phantom: PhantomData<(ST, SCT, BPT, RBCT, VTF, LT, TT, BVT, SVT, ASVT)>,
}

pub(super) enum MempoolCommand<PT: PubKey> {
    ForwardTxns(Vec<NodeId<PT>>, Vec<Bytes>),
}

impl<'a, ST, SCT, BPT, SBT, RBCT, VTF, LT, TT, BVT, SVT, ASVT>
    MempoolChildState<'a, ST, SCT, BPT, SBT, RBCT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT, SBT, RBCT>,
    SBT: StateBackend,
    RBCT: ReserveBalanceCacheTrait<SBT>,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    TT: TxPool<SCT, BPT, SBT, RBCT>,
    BVT: BlockValidator<SCT, BPT, SBT, RBCT>,
    SVT: StateRootValidator,
{
    pub(super) fn new(
        monad_state: &'a mut MonadState<ST, SCT, BPT, SBT, RBCT, VTF, LT, TT, BVT, SVT, ASVT>,
    ) -> Self {
        Self {
            txpool: &mut monad_state.txpool,
            metrics: &mut monad_state.metrics,
            block_policy: &monad_state.block_policy,
            reserve_balance_cache: &mut monad_state.reserve_balance_cache,

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
        match event {
            MempoolEvent::UserTxns(txns) => {
                let txs = txns
                    .into_iter()
                    .filter_map(|tx| {
                        if let Err(err) = self.txpool.insert_tx(
                            tx.clone(),
                            self.block_policy,
                            self.reserve_balance_cache,
                        ) {
                            self.metrics.txpool_events.dropped_txns += 1;
                            tracing::warn!("failed to insert rpc tx: {:?}", err);
                            None
                        } else {
                            self.metrics.txpool_events.local_inserted_txns += 1;
                            Some(tx)
                        }
                    })
                    .collect();

                let round = self.consensus.get_current_round();
                let next_k_leaders = (round.0..)
                    .map(|round| self.get_leader(Round(round)))
                    .take(NUM_LEADERS_FORWARD)
                    .unique()
                    .filter(|leader| leader != self.nodeid)
                    .collect_vec();
                vec![MempoolCommand::ForwardTxns(next_k_leaders, txs)]
            }
            MempoolEvent::ForwardedTxns { sender, txns } => {
                for tx in txns {
                    if let Err(err) =
                        self.txpool
                            .insert_tx(tx, self.block_policy, self.reserve_balance_cache)
                    {
                        tracing::warn!(?sender, "failed to insert forwarded tx: {:?}", err);
                    } else {
                        self.metrics.txpool_events.external_inserted_txns += 1;
                    }
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
    for Vec<Command<MonadEvent<ST, SCT>, VerifiedMonadMessage<ST, SCT>, Block<SCT>, SCT>>
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
