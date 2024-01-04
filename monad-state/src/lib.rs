use std::{fmt::Debug, marker::PhantomData};

use blocksync::BlockSyncChildState;
use bytes::Bytes;
use consensus::ConsensusChildState;
use epoch::EpochChildState;
use monad_blocktree::blocktree::BlockTree;
use monad_consensus::{
    messages::{
        consensus_message::ConsensusMessage,
        message::{BlockSyncResponseMessage, RequestBlockSyncMessage},
    },
    validation::signing::{Unvalidated, Unverified, Validated, Verified},
};
use monad_consensus_state::{command::Checkpoint, ConsensusConfig, ConsensusProcess};
use monad_consensus_types::{
    block::Block,
    signature_collection::{
        SignatureCollection, SignatureCollectionKeyPairType, SignatureCollectionPubKeyType,
    },
    txpool::TxPool,
    validation,
    validator_data::ValidatorData,
};
use monad_crypto::certificate_signature::{
    CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::EthAddress;
use monad_executor::State;
use monad_executor_glue::{
    BlockSyncEvent, Command, ConsensusEvent, MempoolEvent, Message, MonadEvent, ValidatorEvent,
};
use monad_tracing_counter::inc_count;
use monad_types::{Epoch, NodeId, Round, SeqNum, Stake, TimeoutVariant};
use monad_validator::{
    epoch_manager::EpochManager, leader_election::LeaderElection, validator_set::ValidatorSetType,
    validators_epoch_mapping::ValidatorsEpochMapping,
};

use crate::blocksync::BlockSyncResponder;

mod blocksync;
mod consensus;
pub mod convert;
mod epoch;

pub(crate) fn handle_validation_error(e: validation::Error) {
    match e {
        validation::Error::InvalidAuthor => {
            inc_count!(invalid_author)
        }
        validation::Error::NotWellFormed => {
            inc_count!(not_well_formed_sig)
        }
        validation::Error::InvalidSignature => {
            inc_count!(invalid_signature)
        }
        validation::Error::AuthorNotSender => {
            inc_count!(author_not_sender)
        }
        validation::Error::InvalidTcRound => {
            inc_count!(invalid_tc_round)
        }
        validation::Error::InsufficientStake => {
            inc_count!(insufficient_stake)
        }
        validation::Error::InvalidSeqNum => {
            inc_count!(invalid_seq_num)
        }
        validation::Error::ValidatorDataUnavailable => {
            inc_count!(val_data_unavailable)
        }
        validation::Error::InvalidVoteMessage => {
            inc_count!(invalid_vote_message)
        }
    };
}

pub struct MonadState<CT, ST, SCT, VT, LT, TT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
{
    /// Core consensus algorithm state machine
    consensus: CT,
    /// Handle responses to block sync requests from other nodes
    block_sync_responder: BlockSyncResponder,

    /// Algorithm for choosing leaders for the consensus algorithm
    leader_election: LT,
    /// Track the information for epochs
    epoch_manager: EpochManager,
    /// Maps the epoch number to validator stakes and certificate pubkeys
    val_epoch_map: ValidatorsEpochMapping<VT, SCT>,
    /// Transaction pool is the source of Proposals
    txpool: TT,

    _pd: PhantomData<ST>,
}

impl<CT, ST, SCT, VT, LT, TT> MonadState<CT, ST, SCT, VT, LT, TT>
where
    CT: ConsensusProcess<ST, SCT>,
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
    TT: TxPool,
{
    pub fn consensus(&self) -> &CT {
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
        }
    }
}

pub struct MonadConfig<ST, SCT, BV>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub block_validator: BV,
    pub validators: Vec<(SCT::NodeIdPubKey, Stake, SignatureCollectionPubKeyType<SCT>)>,
    pub key: ST::KeyPairType,
    pub certkey: SignatureCollectionKeyPairType<SCT>,
    pub val_set_update_interval: SeqNum,
    pub epoch_start_delay: Round,
    pub beneficiary: EthAddress,

    pub consensus_config: ConsensusConfig,
}

impl<CT, ST, SCT, VT, LT, TT> State for MonadState<CT, ST, SCT, VT, LT, TT>
where
    CT: ConsensusProcess<ST, SCT>,
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
    LT: LeaderElection,
    TT: TxPool,
{
    type Config = MonadConfig<ST, SCT, CT::BlockValidatorType>;
    type Event = MonadEvent<ST, SCT>;
    type Message = MonadMessage<ST, SCT>;
    type OutboundMessage = VerifiedMonadMessage<ST, SCT>;
    type Block = Block<SCT>;
    type Checkpoint = Checkpoint<SCT>;
    type NodeIdSignature = ST;
    type SignatureCollection = SCT;

    fn init(
        config: Self::Config,
    ) -> (
        Self,
        Vec<
            Command<
                Self::Event,
                Self::OutboundMessage,
                Self::Block,
                Self::Checkpoint,
                Self::SignatureCollection,
            >,
        >,
    ) {
        let validator_data = ValidatorData(
            config
                .validators
                .into_iter()
                .map(|(pubkey, stake, certpubkey)| (NodeId::new(pubkey), stake, certpubkey))
                .collect::<Vec<_>>(),
        );

        let val_epoch_map = ValidatorsEpochMapping::default();

        let election = LT::new();

        let consensus_process = CT::new(
            config.block_validator,
            config.key.pubkey(),
            config.consensus_config,
            config.beneficiary,
            config.key,
            config.certkey,
        );

        let mut monad_state: MonadState<CT, ST, SCT, VT, LT, TT> = Self {
            epoch_manager: EpochManager::new(
                config.val_set_update_interval,
                config.epoch_start_delay,
            ),
            val_epoch_map,
            leader_election: election,
            consensus: consensus_process,
            block_sync_responder: BlockSyncResponder {},
            txpool: TT::new(),

            _pd: PhantomData,
        };

        let mut init_cmds = Vec::new();
        init_cmds.extend(monad_state.update(MonadEvent::ValidatorEvent(
            ValidatorEvent::UpdateValidators((validator_data, Epoch(1))),
        )));

        init_cmds.extend(
            monad_state.update(MonadEvent::ConsensusEvent(ConsensusEvent::Timeout(
                TimeoutVariant::Pacemaker,
            ))),
        );

        (monad_state, init_cmds)
    }

    fn update(
        &mut self,
        event: Self::Event,
    ) -> Vec<
        Command<
            Self::Event,
            Self::OutboundMessage,
            Self::Block,
            Self::Checkpoint,
            Self::SignatureCollection,
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
                let validator_cmds = EpochChildState::new(self).update(validator_event);

                validator_cmds
                    .into_iter()
                    .flat_map(Into::<Vec<Command<_, _, _, _, _>>>::into)
                    .collect::<Vec<_>>()
            }

            MonadEvent::MempoolEvent(mempool_event) => match mempool_event {
                MempoolEvent::UserTx(_tx) => {
                    todo!()
                }
            },
        }
    }
}
