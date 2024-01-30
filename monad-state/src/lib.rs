use std::{fmt::Debug, marker::PhantomData};

use blocksync::BlockSyncChildState;
use bytes::Bytes;
use consensus::ConsensusChildState;
use epoch::EpochChildState;
use mempool::MempoolChildState;
use monad_blocktree::blocktree::BlockTree;
use monad_consensus::{
    messages::{
        consensus_message::ConsensusMessage,
        message::{BlockSyncResponseMessage, CascadeTxMessage, RequestBlockSyncMessage},
    },
    validation::signing::{Unvalidated, Unverified, Validated, Verified},
};
use monad_consensus_state::{command::Checkpoint, ConsensusConfig, ConsensusState};
use monad_consensus_types::{
    block::Block,
    block_validator::BlockValidator,
    metrics::Metrics,
    payload::StateRootValidator,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    txpool::TxPool,
    validation,
    validator_data::ValidatorData,
};
use monad_crypto::certificate_signature::{
    CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::EthAddress;
use monad_executor_glue::{
    BlockSyncEvent, Command, ConsensusEvent, MempoolEvent, Message, MonadEvent, ValidatorEvent,
};
use monad_types::{Epoch, NodeId, Round, SeqNum, TimeoutVariant};
use monad_validator::{
    epoch_manager::EpochManager,
    leader_election::LeaderElection,
    validator_set::{ValidatorSetType, ValidatorSetTypeFactory},
    validators_epoch_mapping::ValidatorsEpochMapping,
};

use crate::blocksync::BlockSyncResponder;

mod blocksync;
mod consensus;
pub mod convert;
mod epoch;
mod mempool;

pub(crate) fn handle_validation_error(e: validation::Error, metrics: &mut Metrics) {
    match e {
        validation::Error::InvalidAuthor => {
            metrics.validation_errors.invalid_author += 1;
        }
        validation::Error::NotWellFormed => {
            metrics.validation_errors.not_well_formed_sig += 1;
        }
        validation::Error::InvalidSignature => {
            metrics.validation_errors.invalid_signature += 1;
        }
        validation::Error::AuthorNotSender => {
            metrics.validation_errors.author_not_sender += 1;
        }
        validation::Error::InvalidTcRound => {
            metrics.validation_errors.invalid_tc_round += 1;
        }
        validation::Error::InsufficientStake => {
            metrics.validation_errors.insufficient_stake += 1;
        }
        validation::Error::InvalidSeqNum => {
            metrics.validation_errors.invalid_seq_num += 1;
        }
        validation::Error::ValidatorDataUnavailable => {
            metrics.validation_errors.val_data_unavailable += 1;
        }
        validation::Error::InvalidVoteMessage => {
            metrics.validation_errors.invalid_vote_message += 1;
        }
    };
}

pub struct MonadState<ST, SCT, VTF, LT, TT, BVT, SVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    /// Core consensus algorithm state machine
    consensus: ConsensusState<ST, SCT, BVT, SVT>,
    /// Handle responses to block sync requests from other nodes
    block_sync_responder: BlockSyncResponder,

    /// Algorithm for choosing leaders for the consensus algorithm
    leader_election: LT,
    /// Track the information for epochs
    epoch_manager: EpochManager,
    /// Maps the epoch number to validator stakes and certificate pubkeys
    val_epoch_map: ValidatorsEpochMapping<VTF, SCT>,
    /// Transaction pool is the source of Proposals
    txpool: TT,

    /// Metrics counters for events and errors
    metrics: Metrics,

    _pd: PhantomData<ST>,
}

impl<ST, SCT, VTF, LT, TT, BVT, SVT> MonadState<ST, SCT, VTF, LT, TT, BVT, SVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    TT: TxPool,
    BVT: BlockValidator,
    SVT: StateRootValidator,
{
    pub fn consensus(&self) -> &ConsensusState<ST, SCT, BVT, SVT> {
        &self.consensus
    }

    pub fn epoch_manager(&self) -> &EpochManager {
        &self.epoch_manager
    }

    pub fn pubkey(&self) -> SCT::NodeIdPubKey {
        self.consensus.get_pubkey()
    }

    pub fn blocktree(&self) -> &BlockTree<SCT> {
        self.consensus.blocktree()
    }

    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    pub fn validator_epoch_map(&self) -> &ValidatorsEpochMapping<VTF, SCT> {
        &self.val_epoch_map
    }

    pub fn leader_election(&self) -> &LT {
        &self.leader_election
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifiedMonadMessage<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    Consensus(Verified<ST, Validated<ConsensusMessage<SCT>>>),
    BlockSyncRequest(Validated<RequestBlockSyncMessage>),
    BlockSyncResponse(Validated<BlockSyncResponseMessage<SCT>>),
    CascadeTxns(Validated<CascadeTxMessage>),
}

impl<ST, SCT> From<Verified<ST, Validated<ConsensusMessage<SCT>>>> for VerifiedMonadMessage<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn from(value: Verified<ST, Validated<ConsensusMessage<SCT>>>) -> Self {
        Self::Consensus(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MonadMessage<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    /// Consensus protocol message
    Consensus(Unverified<ST, Unvalidated<ConsensusMessage<SCT>>>),

    /// Request a missing block given BlockId
    BlockSyncRequest(Unvalidated<RequestBlockSyncMessage>),

    /// Block sync response
    BlockSyncResponse(Unvalidated<BlockSyncResponseMessage<SCT>>),

    /// Cascade TxPool transactions
    CascadeTxns(Unvalidated<CascadeTxMessage>),
}

impl<ST, SCT> monad_types::Serializable<Bytes> for VerifiedMonadMessage<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn serialize(&self) -> Bytes {
        crate::convert::interface::serialize_verified_monad_message(self)
    }
}

impl<ST, SCT> monad_types::Serializable<MonadMessage<ST, SCT>> for VerifiedMonadMessage<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn serialize(&self) -> MonadMessage<ST, SCT> {
        match self.clone() {
            VerifiedMonadMessage::Consensus(msg) => MonadMessage::Consensus(msg.into()),
            VerifiedMonadMessage::BlockSyncRequest(msg) => {
                MonadMessage::BlockSyncRequest(msg.into())
            }
            VerifiedMonadMessage::BlockSyncResponse(msg) => {
                MonadMessage::BlockSyncResponse(msg.into())
            }
            VerifiedMonadMessage::CascadeTxns(msg) => MonadMessage::CascadeTxns(msg.into()),
        }
    }
}

impl<ST, SCT> monad_types::Deserializable<Bytes> for MonadMessage<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type ReadError = monad_proto::error::ProtoError;

    fn deserialize(message: &Bytes) -> Result<Self, Self::ReadError> {
        crate::convert::interface::deserialize_monad_message(message.clone())
    }
}

impl<ST, SCT> From<VerifiedMonadMessage<ST, SCT>> for MonadMessage<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn from(value: VerifiedMonadMessage<ST, SCT>) -> Self {
        match value {
            VerifiedMonadMessage::Consensus(msg) => MonadMessage::Consensus(msg.into()),
            VerifiedMonadMessage::BlockSyncRequest(msg) => {
                MonadMessage::BlockSyncRequest(msg.into())
            }
            VerifiedMonadMessage::BlockSyncResponse(msg) => {
                MonadMessage::BlockSyncResponse(msg.into())
            }
            VerifiedMonadMessage::CascadeTxns(msg) => MonadMessage::CascadeTxns(msg.into()),
        }
    }
}

impl<ST, SCT> Message for MonadMessage<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type NodeIdPubKey = CertificateSignaturePubKey<ST>;
    type Event = MonadEvent<ST, SCT>;

    fn event(self, from: NodeId<Self::NodeIdPubKey>) -> Self::Event {
        // MUST assert that output is valid and came from the `from` NodeId
        // `from` must somehow be guaranteed to be staked at this point so that subsequent
        // malformed stuff (that gets added to event log) can be slashed? TODO
        match self {
            MonadMessage::Consensus(msg) => MonadEvent::ConsensusEvent(ConsensusEvent::Message {
                sender: from.pubkey(),
                unverified_message: msg,
            }),

            MonadMessage::BlockSyncRequest(msg) => {
                MonadEvent::BlockSyncEvent(BlockSyncEvent::BlockSyncRequest {
                    sender: from.pubkey(),
                    unvalidated_request: msg,
                })
            }
            MonadMessage::BlockSyncResponse(msg) => {
                MonadEvent::ConsensusEvent(ConsensusEvent::BlockSyncResponse {
                    sender: from.pubkey(),
                    unvalidated_response: msg,
                })
            }
            MonadMessage::CascadeTxns(msg) => MonadEvent::MempoolEvent(MempoolEvent::CascadeTxns {
                sender: from.pubkey(),
                txns: msg,
            }),
        }
    }
}

pub struct MonadStateBuilder<ST, SCT, VTF, LT, TT, BVT, SVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    LT: LeaderElection<
        NodeIdPubKey = CertificateSignaturePubKey<ST>,
        NodeSignatureCollection = SCT,
    >,
    TT: TxPool,
    BVT: BlockValidator,
    SVT: StateRootValidator,
{
    pub validator_set_factory: VTF,
    pub leader_election: LT,
    pub transaction_pool: TT,
    pub block_validator: BVT,
    pub state_root_validator: SVT,
    pub validators: ValidatorData<SCT>,
    pub key: ST::KeyPairType,
    pub certkey: SignatureCollectionKeyPairType<SCT>,
    pub val_set_update_interval: SeqNum,
    pub epoch_start_delay: Round,
    pub beneficiary: EthAddress,

    pub consensus_config: ConsensusConfig,
}

impl<ST, SCT, VTF, LT, TT, BVT, SVT> MonadStateBuilder<ST, SCT, VTF, LT, TT, BVT, SVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    LT: LeaderElection<
        NodeIdPubKey = CertificateSignaturePubKey<ST>,
        NodeSignatureCollection = SCT,
    >,
    TT: TxPool,
    BVT: BlockValidator,
    SVT: StateRootValidator,
{
    pub fn build(
        self,
    ) -> (
        MonadState<ST, SCT, VTF, LT, TT, BVT, SVT>,
        Vec<
            Command<
                MonadEvent<ST, SCT>,
                VerifiedMonadMessage<ST, SCT>,
                Block<SCT>,
                Checkpoint<SCT>,
                SCT,
            >,
        >,
    ) {
        let val_epoch_map = ValidatorsEpochMapping::new(self.validator_set_factory);

        let consensus_process = ConsensusState::new(
            self.block_validator,
            self.state_root_validator,
            self.key.pubkey(),
            self.consensus_config,
            self.beneficiary,
            self.key,
            self.certkey,
        );

        let mut monad_state = MonadState {
            epoch_manager: EpochManager::new(self.val_set_update_interval, self.epoch_start_delay),
            val_epoch_map,
            leader_election: self.leader_election,
            consensus: consensus_process,
            block_sync_responder: BlockSyncResponder {},
            txpool: self.transaction_pool,
            metrics: Metrics::default(),

            _pd: PhantomData,
        };

        let mut init_cmds = Vec::new();
        init_cmds.extend(monad_state.update(MonadEvent::ValidatorEvent(
            ValidatorEvent::UpdateValidators((self.validators, Epoch(1))),
        )));

        init_cmds.extend(
            monad_state.update(MonadEvent::ConsensusEvent(ConsensusEvent::Timeout(
                TimeoutVariant::Pacemaker,
            ))),
        );

        (monad_state, init_cmds)
    }
}

impl<ST, SCT, VTF, LT, TT, BVT, SVT> MonadState<ST, SCT, VTF, LT, TT, BVT, SVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    LT: LeaderElection<
        NodeIdPubKey = CertificateSignaturePubKey<ST>,
        NodeSignatureCollection = SCT,
    >,
    TT: TxPool,
    BVT: BlockValidator,
    SVT: StateRootValidator,
{
    pub fn update(
        &mut self,
        event: MonadEvent<ST, SCT>,
    ) -> Vec<
        Command<
            MonadEvent<ST, SCT>,
            VerifiedMonadMessage<ST, SCT>,
            Block<SCT>,
            Checkpoint<SCT>,
            SCT,
        >,
    > {
        let _event_span = tracing::info_span!("event_span", ?event).entered();

        match event {
            MonadEvent::ConsensusEvent(consensus_event) => {
                let consensus_cmds = ConsensusChildState::new(self).update(consensus_event);

                consensus_cmds
                    .into_iter()
                    .flat_map(Into::<Vec<Command<_, _, _, _, _>>>::into)
                    .collect::<Vec<_>>()
            }

            MonadEvent::BlockSyncEvent(block_sync_event) => {
                let block_sync_cmds = BlockSyncChildState::new(self).update(block_sync_event);

                block_sync_cmds
                    .into_iter()
                    .flat_map(Into::<Vec<Command<_, _, _, _, _>>>::into)
                    .collect::<Vec<_>>()
            }

            MonadEvent::ValidatorEvent(validator_event) => {
                match validator_event {
                    ValidatorEvent::UpdateValidators(ref update_validators) => {
                        self.leader_election.update(update_validators);
                    }
                }
                let validator_cmds = EpochChildState::new(self).update(validator_event);
                validator_cmds
                    .into_iter()
                    .flat_map(Into::<Vec<Command<_, _, _, _, _>>>::into)
                    .collect::<Vec<_>>()
            }

            MonadEvent::MempoolEvent(mempool_event) => {
                let mempool_cmds = MempoolChildState::new(self).update(mempool_event);

                mempool_cmds
                    .into_iter()
                    .flat_map(Into::<Vec<Command<_, _, _, _, _>>>::into)
                    .collect::<Vec<_>>()
            }
        }
    }
}
