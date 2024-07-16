use std::{fmt::Debug, marker::PhantomData, ops::Deref};

use async_state_verify::AsyncStateVerifyChildState;
use blocksync::BlockSyncChildState;
use bytes::Bytes;
use consensus::ConsensusChildState;
use epoch::EpochChildState;
use mempool::MempoolChildState;
use monad_async_state_verify::AsyncStateVerifyProcess;
use monad_blocktree::blocktree::BlockTree;
use monad_consensus::{
    messages::{
        consensus_message::ConsensusMessage,
        message::{BlockSyncResponseMessage, PeerStateRootMessage, RequestBlockSyncMessage},
    },
    validation::signing::{verify_qc, Unvalidated, Unverified, Validated, Verified},
};
use monad_consensus_state::{ConsensusConfig, ConsensusState};
use monad_consensus_types::{
    block::{Block, BlockPolicy},
    block_validator::BlockValidator,
    checkpoint::{Checkpoint, RootInfo},
    metrics::Metrics,
    payload::StateRootValidator,
    quorum_certificate::{QuorumCertificate, GENESIS_BLOCK_ID},
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    state_root_hash::StateRootHash,
    txpool::TxPool,
    validation,
    validator_data::{ParsedValidatorData, Validator, ValidatorSetData, ValidatorSetDataWithEpoch},
    voting::ValidatorMapping,
};
use monad_crypto::certificate_signature::{
    CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_reserve_balance::ReserveBalanceCacheTrait;
use monad_eth_types::EthAddress;
use monad_executor_glue::{
    AsyncStateVerifyEvent, BlockSyncEvent, ClearMetrics, Command, ConsensusEvent,
    ControlPanelCommand, ControlPanelEvent, GetValidatorSet, MempoolEvent, Message, MonadEvent,
    ReadCommand, StateRootHashCommand, ValidatorEvent, WriteCommand,
};
use monad_types::{Epoch, NodeId, Round, SeqNum, TimeoutVariant, GENESIS_SEQ_NUM};
use monad_validator::{
    epoch_manager::EpochManager,
    leader_election::LeaderElection,
    validator_set::{ValidatorSetType, ValidatorSetTypeFactory},
    validators_epoch_mapping::ValidatorsEpochMapping,
};

use crate::blocksync::BlockSyncResponder;

mod async_state_verify;
mod blocksync;
mod consensus;
pub mod convert;
mod epoch;
mod mempool;

const CLIENT_MAJOR_VERSION: u16 = 0;
const CLIENT_MINOR_VERSION: u16 = 1;

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
        validation::Error::ValidatorSetDataUnavailable => {
            // This error occurs when the node knows when the next epoch starts,
            // but didn't get enough execution deltas to build the next
            // validator set.
            // TODO: This should trigger statesync
            metrics.validation_errors.val_data_unavailable += 1;
        }
        validation::Error::InvalidVoteMessage => {
            metrics.validation_errors.invalid_vote_message += 1;
        }
        validation::Error::InvalidVersion => {
            metrics.validation_errors.invalid_version += 1;
        }
        validation::Error::InvalidEpoch => {
            // TODO: If the node is not actively participating, getting this
            // error can indicate that the node is behind by more than an epoch
            // and needs state sync. Else if actively participating, this is
            // spam
            metrics.validation_errors.invalid_epoch += 1;
        }
    };
}

pub struct MonadVersion {
    pub protocol_version: &'static str,
    client_version_maj: u16,
    client_version_min: u16,
}

impl MonadVersion {
    pub fn new(protocol_version: &'static str) -> Self {
        Self {
            protocol_version,
            client_version_maj: CLIENT_MAJOR_VERSION,
            client_version_min: CLIENT_MINOR_VERSION,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ForkpointValidationError {
    TooFewValidatorSets,
    TooManyValidatorSets,
    ValidatorSetsNotConsecutive,
    InvalidValidatorSetStartRound,
    InvalidValidatorSetStartEpoch,
    /// high_qc cannot be verified
    InvalidQC,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Forkpoint<SCT: SignatureCollection>(pub Checkpoint<SCT>);

impl<SCT: SignatureCollection> From<Checkpoint<SCT>> for Forkpoint<SCT> {
    fn from(checkpoint: Checkpoint<SCT>) -> Self {
        Self(checkpoint)
    }
}

impl<SCT: SignatureCollection> Deref for Forkpoint<SCT> {
    type Target = Checkpoint<SCT>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<SCT: SignatureCollection> Forkpoint<SCT> {
    pub fn genesis(validator_set: ValidatorSetData<SCT>, state_root: StateRootHash) -> Self {
        Checkpoint {
            root: RootInfo {
                block_id: GENESIS_BLOCK_ID,
                round: Round(0),
                seq_num: GENESIS_SEQ_NUM,
                epoch: Epoch(1),
                state_root,
            },
            high_qc: QuorumCertificate::genesis_qc(),
            validator_sets: vec![
                ValidatorSetDataWithEpoch {
                    epoch: Epoch(1),
                    round: Some(Round(0)),
                    validators: validator_set.clone(),
                },
                ValidatorSetDataWithEpoch {
                    epoch: Epoch(2),
                    round: None,
                    validators: validator_set,
                },
            ],
        }
        .into()
    }

    pub fn get_epoch_starts(&self) -> Vec<(Epoch, Round)> {
        let mut known = Vec::new();
        for validator_set in self.validator_sets.iter() {
            if let Some(round) = validator_set.round {
                known.push((validator_set.epoch, round));
            }
        }
        known
    }

    // Trust assumptions:
    // 1. Root is fully trusted
    // 2. Each validator set is trusted to have been valid at some point in time
    //
    // This validation function will verify that:
    // 1. The *set* of validator_sets included in the forkpoint are enough to bootstrap
    // 2. The `start_round` of each validator_set is coherent in the context of the forkpoint
    // 3. The high_qc certificate signature is verified against the appropriate validator_set
    //
    // Concrete verification steps:
    // 1. 2 <= validator_sets.len() <= 3
    // 2. validator_sets have consecutive epochs
    // 3. assert_eq!(root.epoch, validator_sets[0].epoch)
    // 4. assert!(validator_sets[0].round.is_some())
    // 5. if root.seq_num.to_epoch() == root.epoch
    //        if root_seq_num.is_epoch_end():
    //            // round is set after boundary block is committed
    //            assert!(validator_sets[1].round.is_some())
    //
    //            // this assertion could be omitted if we don't want to be careful about which
    //            // validator sets we include in forkpoint
    //            // note that if we omit, we must check that validator_sets[2].round.is_none()
    //            // (obviously, only if validator_sets[2] exists)
    //            assert_eq!(validator_sets.len(), 2)
    //        else:
    //            // round can't be set if boundary block isn't committed
    //            assert!(validator_sets[1].round.is_none())
    //            // we haven't committed boundary block yet, so can be max 2 validator sets
    //            assert_eq!(validator_sets.len(), 2)
    //    else:
    //        // we are in epoch_start_delay period of root.epoch + 1
    //        assert_eq!(root.epoch + 1, root.seq_num.to_epoch())
    //        // round is set after boundary block is committed
    //        assert!(validator_sets[1].round.is_some())
    //        if (root.seq_num - state_root_delay).to_epoch() < root.seq_num.to_epoch():
    //            // we will statesync to root.seq_num - state_root_delay
    //            // boundary block is between root.seq_num - state_root_delay and root.seq_num
    //            // validator_sets[2] will be populated from boundary block state
    //
    //            // this assertion could be omitted if we don't want to be careful about which
    //            // validator sets we include in forkpoint
    //            // note that if we omit, we must check that validator_sets[2].round.is_none()
    //            // (obviously, only if validator_sets[2] exists)
    //            assert_eq!(validator_sets.len(), 2)
    //        else:
    //            assert_eq!((root.seq_num - state_root_delay).to_epoch(), root.seq_num.to_epoch())
    //            // we are statesyncing to after boundary block, so validator set must be
    //            // populated
    //            assert_eq!(validator_sets.len(), 3)
    //            assert!(validator_sets[2].maybe_start_round.is_none())
    // 6. high_qc is valid against matching epoch validator_set
    pub fn validate(
        &self,
        state_root_delay: SeqNum,
        validator_set_factory: &impl ValidatorSetTypeFactory<NodeIdPubKey = SCT::NodeIdPubKey>,
        val_set_update_interval: SeqNum,
    ) -> Result<(), ForkpointValidationError> {
        // 1.
        if self.validator_sets.len() < 2 {
            return Err(ForkpointValidationError::TooFewValidatorSets);
        }
        if self.validator_sets.len() > 3 {
            return Err(ForkpointValidationError::TooManyValidatorSets);
        }

        // 2.
        if !self
            .validator_sets
            .iter()
            .zip(self.validator_sets.iter().skip(1))
            .all(|(set_1, set_2)| set_1.epoch + Epoch(1) == set_2.epoch)
        {
            return Err(ForkpointValidationError::ValidatorSetsNotConsecutive);
        }

        // 3.
        if self.validator_sets[0].epoch != self.0.root.epoch {
            return Err(ForkpointValidationError::InvalidValidatorSetStartEpoch);
        }

        // 4.
        let Some(validator_set_0_round) = self.validator_sets[0].round else {
            return Err(ForkpointValidationError::InvalidValidatorSetStartRound);
        };
        // this is an assertion because the validator set is trusted to have been valid at some
        // point in time
        assert!(validator_set_0_round <= self.root.round);

        // 5.
        let root_seq_num_epoch = self.root.seq_num.to_epoch(val_set_update_interval);
        if root_seq_num_epoch == self.0.root.epoch {
            if self.0.root.seq_num.is_epoch_end(val_set_update_interval) {
                // round is set after boundary block is committed
                let Some(validator_set_1_round) = self.validator_sets[1].round else {
                    return Err(ForkpointValidationError::InvalidValidatorSetStartRound);
                };
                // this is an assertion because the validator set is trusted to have been valid at some
                // point in time
                assert!(self.root.round < validator_set_1_round);

                if self
                    .validator_sets
                    .get(2)
                    .is_some_and(|validator_set| validator_set.round.is_some())
                {
                    return Err(ForkpointValidationError::InvalidValidatorSetStartRound);
                }
            } else {
                // round can't be set if boundary block isn't committed
                if self.validator_sets[1].round.is_some() {
                    return Err(ForkpointValidationError::InvalidValidatorSetStartRound);
                }
                // we haven't committed boundary block yet, so can be max 2 validator sets
                if self.validator_sets.len() != 2 {
                    return Err(ForkpointValidationError::TooManyValidatorSets);
                }
            }
        } else {
            // we are in epoch_start_delay period of root.epoch + 1
            assert_eq!(self.0.root.epoch + Epoch(1), root_seq_num_epoch);
            // round is set after boundary block is committed
            let Some(validator_set_1_round) = self.validator_sets[1].round else {
                return Err(ForkpointValidationError::InvalidValidatorSetStartRound);
            };
            // this is an assertion because the validator set is trusted to have been valid at some
            // point in time
            assert!(self.root.round < validator_set_1_round);

            let state_sync_seq_num_epoch = (self.root.seq_num.max(state_root_delay)
                - state_root_delay)
                .to_epoch(val_set_update_interval);
            if state_sync_seq_num_epoch < root_seq_num_epoch {
                // we will statesync to root.seq_num - state_root_delay
                // boundary block is between root.seq_num - state_root_delay and root.seq_num
                // validator_sets[2] will be populated from boundary block state

                if self
                    .validator_sets
                    .get(2)
                    .is_some_and(|validator_set| validator_set.round.is_some())
                {
                    return Err(ForkpointValidationError::InvalidValidatorSetStartRound);
                }
            } else {
                assert_eq!(state_sync_seq_num_epoch, root_seq_num_epoch);
                // we are statesyncing to after boundary block, so validator set must be populated
                if self.validator_sets.len() != 3 {
                    return Err(ForkpointValidationError::TooFewValidatorSets);
                }
                if self.validator_sets[2].round.is_some() {
                    return Err(ForkpointValidationError::InvalidValidatorSetStartRound);
                }
            }
        }

        // 6.
        self.validate_and_verify_qc(validator_set_factory)?;

        Ok(())
    }

    fn validate_and_verify_qc(
        &self,
        validator_set_factory: &impl ValidatorSetTypeFactory<NodeIdPubKey = SCT::NodeIdPubKey>,
    ) -> Result<(), ForkpointValidationError> {
        let qc_validator_set = self
            .validator_sets
            .iter()
            .find(|data| data.epoch == self.high_qc.get_epoch())
            .ok_or(ForkpointValidationError::InvalidQC)?;

        let vset_stake = qc_validator_set
            .validators
            .0
            .iter()
            .map(|data| (data.node_id, data.stake))
            .collect::<Vec<_>>();

        let qc_vmap = ValidatorMapping::new(
            qc_validator_set
                .validators
                .0
                .iter()
                .map(|data| (data.node_id, data.cert_pubkey))
                .collect::<Vec<_>>(),
        );

        let qc_vset = validator_set_factory
            .create(vset_stake)
            .expect("ValidatorSetTypeFactory failed to init validator set");

        // 2.
        if verify_qc(&qc_vset, &qc_vmap, &self.high_qc).is_err() {
            return Err(ForkpointValidationError::InvalidQC);
        }

        Ok(())
    }
}

pub struct MonadState<ST, SCT, BPT, RBCT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    RBCT: ReserveBalanceCacheTrait,
    BPT: BlockPolicy<SCT, RBCT>,
    BVT: BlockValidator<SCT, RBCT, BPT>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    keypair: ST::KeyPairType,
    cert_keypair: SignatureCollectionKeyPairType<SCT>,
    nodeid: NodeId<CertificateSignaturePubKey<ST>>,

    consensus_config: ConsensusConfig,

    /// Core consensus algorithm state machine
    consensus: ConsensusState<ST, SCT, RBCT, BPT>,
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
    /// Async state verification
    async_state_verify: ASVT,

    state_root_validator: SVT,
    block_validator: BVT,
    block_policy: BPT,
    reserve_balance_cache: RBCT,
    beneficiary: EthAddress,

    /// Metrics counters for events and errors
    metrics: Metrics,

    /// Versions for client and protocol validation
    version: MonadVersion,
}

impl<ST, SCT, BPT, RBCT, VTF, LT, TT, BVT, SVT, ASVT>
    MonadState<ST, SCT, BPT, RBCT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT, RBCT>,
    RBCT: ReserveBalanceCacheTrait,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    TT: TxPool<SCT, BPT, RBCT>,
    BVT: BlockValidator<SCT, RBCT, BPT>,
    SVT: StateRootValidator,
    ASVT: AsyncStateVerifyProcess<
        SignatureCollectionType = SCT,
        ValidatorSetType = VTF::ValidatorSetType,
    >,
{
    pub fn consensus(&self) -> &ConsensusState<ST, SCT, RBCT, BPT> {
        &self.consensus
    }

    pub fn epoch_manager(&self) -> &EpochManager {
        &self.epoch_manager
    }

    pub fn validators_epoch_mapping(&self) -> &ValidatorsEpochMapping<VTF, SCT> {
        &self.val_epoch_map
    }

    pub fn state_root_validator(&self) -> &SVT {
        &self.state_root_validator
    }

    pub fn pubkey(&self) -> SCT::NodeIdPubKey {
        self.nodeid.pubkey()
    }

    pub fn blocktree(&self) -> &BlockTree<SCT, RBCT, BPT> {
        self.consensus.blocktree()
    }

    pub fn metrics(&self) -> &Metrics {
        &self.metrics
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
    PeerStateRootMessage(Validated<PeerStateRootMessage<SCT>>),
    ForwardedTx(Vec<Bytes>),
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

    /// Async state verification msgs
    PeerStateRoot(Unvalidated<PeerStateRootMessage<SCT>>),

    /// Forwarded transactions
    ForwardedTx(Vec<Bytes>),
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
            VerifiedMonadMessage::PeerStateRootMessage(msg) => {
                MonadMessage::PeerStateRoot(msg.into())
            }
            VerifiedMonadMessage::ForwardedTx(msg) => MonadMessage::ForwardedTx(msg),
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
            VerifiedMonadMessage::PeerStateRootMessage(msg) => {
                MonadMessage::PeerStateRoot(msg.into())
            }
            VerifiedMonadMessage::ForwardedTx(msg) => MonadMessage::ForwardedTx(msg),
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

    // FIXME-2: from: NodeId is immediately converted to pubkey. All other msgs
    // put the NodeId wrap back on again, except ConsensusMessage when verifying
    // the consensus signature
    fn event(self, from: NodeId<Self::NodeIdPubKey>) -> Self::Event {
        // MUST assert that output is valid and came from the `from` NodeId
        // `from` must somehow be guaranteed to be staked at this point so that subsequent
        // malformed stuff (that gets added to event log) can be slashed? TODO
        match self {
            MonadMessage::Consensus(msg) => MonadEvent::ConsensusEvent(ConsensusEvent::Message {
                sender: from,
                unverified_message: msg,
            }),

            MonadMessage::BlockSyncRequest(msg) => {
                MonadEvent::BlockSyncEvent(BlockSyncEvent::BlockSyncRequest {
                    sender: from,
                    unvalidated_request: msg,
                })
            }
            MonadMessage::BlockSyncResponse(msg) => {
                MonadEvent::ConsensusEvent(ConsensusEvent::BlockSyncResponse {
                    sender: from,
                    unvalidated_response: msg,
                })
            }
            MonadMessage::PeerStateRoot(msg) => {
                MonadEvent::AsyncStateVerifyEvent(AsyncStateVerifyEvent::PeerStateRoot {
                    sender: from,
                    unvalidated_message: msg,
                })
            }
            MonadMessage::ForwardedTx(msg) => {
                MonadEvent::MempoolEvent(MempoolEvent::ForwardedTxns {
                    sender: from,
                    txns: msg,
                })
            }
        }
    }
}

pub struct MonadStateBuilder<ST, SCT, BPT, RBCT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT, RBCT>,
    RBCT: ReserveBalanceCacheTrait,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    TT: TxPool<SCT, BPT, RBCT>,
    BVT: BlockValidator<SCT, RBCT, BPT>,
    SVT: StateRootValidator,
    ASVT: AsyncStateVerifyProcess<
        SignatureCollectionType = SCT,
        ValidatorSetType = VTF::ValidatorSetType,
    >,
{
    pub version: MonadVersion,

    pub validator_set_factory: VTF,
    pub leader_election: LT,
    pub transaction_pool: TT,
    pub block_validator: BVT,
    pub block_policy: BPT,
    pub reserve_balance_cache: RBCT,
    pub state_root_validator: SVT,
    pub async_state_verify: ASVT,
    pub forkpoint: Forkpoint<SCT>,
    pub key: ST::KeyPairType,
    pub certkey: SignatureCollectionKeyPairType<SCT>,
    pub val_set_update_interval: SeqNum,
    pub epoch_start_delay: Round,
    pub beneficiary: EthAddress,

    pub consensus_config: ConsensusConfig,
    pub _pd: PhantomData<BPT>,
}

impl<ST, SCT, BPT, RBCT, VTF, LT, TT, BVT, SVT, ASVT>
    MonadStateBuilder<ST, SCT, BPT, RBCT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT, RBCT>,
    RBCT: ReserveBalanceCacheTrait,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    TT: TxPool<SCT, BPT, RBCT>,
    BVT: BlockValidator<SCT, RBCT, BPT>,
    SVT: StateRootValidator,
    ASVT: AsyncStateVerifyProcess<
        SignatureCollectionType = SCT,
        ValidatorSetType = VTF::ValidatorSetType,
    >,
{
    pub fn build(
        self,
    ) -> (
        MonadState<ST, SCT, BPT, RBCT, VTF, LT, TT, BVT, SVT, ASVT>,
        Vec<Command<MonadEvent<ST, SCT>, VerifiedMonadMessage<ST, SCT>, Block<SCT>, SCT>>,
    ) {
        assert_eq!(
            self.forkpoint.validate(
                self.state_root_validator.get_delay(),
                &self.validator_set_factory,
                self.val_set_update_interval
            ),
            Ok(())
        );

        let val_epoch_map = ValidatorsEpochMapping::new(self.validator_set_factory);

        let epoch_manager = EpochManager::new(
            self.val_set_update_interval,
            self.epoch_start_delay,
            &self.forkpoint.get_epoch_starts(),
        );

        let consensus = ConsensusState::new(
            &self.consensus_config,
            self.forkpoint.root.clone(),
            self.forkpoint.high_qc.clone(),
            epoch_manager
                .get_epoch(self.forkpoint.high_qc.get_round() + Round(1))
                .expect("forkpoint has epoch for high_qc"),
        );

        let nodeid = NodeId::new(self.key.pubkey());
        let delay = self.state_root_validator.get_delay();

        let mut monad_state = MonadState {
            keypair: self.key,
            cert_keypair: self.certkey,
            nodeid,

            consensus_config: self.consensus_config,
            consensus,
            block_sync_responder: BlockSyncResponder {},

            leader_election: self.leader_election,
            epoch_manager,
            val_epoch_map,
            txpool: self.transaction_pool,
            async_state_verify: self.async_state_verify,

            state_root_validator: self.state_root_validator,
            block_validator: self.block_validator,
            block_policy: self.block_policy,
            reserve_balance_cache: self.reserve_balance_cache,
            beneficiary: self.beneficiary,

            metrics: Metrics::default(),
            version: self.version,
        };

        let mut init_cmds = Vec::new();

        let Forkpoint(Checkpoint {
            root,
            high_qc: _,
            validator_sets,
        }) = self.forkpoint;

        for validator_set in validator_sets.into_iter() {
            init_cmds.extend(monad_state.update(MonadEvent::ValidatorEvent(
                ValidatorEvent::UpdateValidators((validator_set.validators, validator_set.epoch)),
            )));
        }

        let root_seq_num = root.seq_num;

        // if root is N, we need to request the roots from (N-delay, N]
        let state_root_queue_range =
            ((root_seq_num + SeqNum(1)).0.saturating_sub(delay.0)..=root_seq_num.0).map(SeqNum);
        init_cmds.extend(state_root_queue_range.map(|committed_seq_num| {
            Command::StateRootHashCommand(StateRootHashCommand::Request(committed_seq_num))
        }));

        init_cmds.extend(
            monad_state.update(MonadEvent::ConsensusEvent(ConsensusEvent::Timeout(
                TimeoutVariant::Pacemaker,
            ))),
        );

        (monad_state, init_cmds)
    }
}

impl<ST, SCT, BPT, RBCT, VTF, LT, TT, BVT, SVT, ASVT>
    MonadState<ST, SCT, BPT, RBCT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT, RBCT>,
    RBCT: ReserveBalanceCacheTrait,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    TT: TxPool<SCT, BPT, RBCT>,
    BVT: BlockValidator<SCT, RBCT, BPT>,
    SVT: StateRootValidator,
    ASVT: AsyncStateVerifyProcess<
        SignatureCollectionType = SCT,
        ValidatorSetType = VTF::ValidatorSetType,
    >,
{
    pub fn update(
        &mut self,
        event: MonadEvent<ST, SCT>,
    ) -> Vec<Command<MonadEvent<ST, SCT>, VerifiedMonadMessage<ST, SCT>, Block<SCT>, SCT>> {
        let _event_span = tracing::debug_span!("event_span", ?event).entered();

        match event {
            MonadEvent::ConsensusEvent(consensus_event) => {
                let consensus_cmds = ConsensusChildState::new(self).update(consensus_event);

                consensus_cmds
                    .into_iter()
                    .flat_map(Into::<Vec<Command<_, _, _, _>>>::into)
                    .collect::<Vec<_>>()
            }

            MonadEvent::BlockSyncEvent(block_sync_event) => {
                let block_sync_cmds = BlockSyncChildState::new(self).update(block_sync_event);

                block_sync_cmds
                    .into_iter()
                    .flat_map(Into::<Vec<Command<_, _, _, _>>>::into)
                    .collect::<Vec<_>>()
            }

            MonadEvent::ValidatorEvent(validator_event) => {
                let validator_cmds = EpochChildState::new(self).update(validator_event);

                validator_cmds
                    .into_iter()
                    .flat_map(Into::<Vec<Command<_, _, _, _>>>::into)
                    .collect::<Vec<_>>()
            }

            MonadEvent::MempoolEvent(mempool_event) => {
                let mempool_cmds = MempoolChildState::new(self).update(mempool_event);

                mempool_cmds
                    .into_iter()
                    .flat_map(Into::<Vec<Command<_, _, _, _>>>::into)
                    .collect::<Vec<_>>()
            }
            MonadEvent::StateRootEvent(info) => {
                // state root hashes are produced when blocks are executed. They can
                // arrive after the delay-gap between execution so they need to be handled
                // asynchronously
                self.metrics.consensus_events.state_root_update += 1;
                self.state_root_validator
                    .add_state_root(info.seq_num, info.state_root_hash);
                Vec::new()
            }

            MonadEvent::AsyncStateVerifyEvent(async_state_verify_event) => {
                let current_round_estimate = self.consensus.get_current_round();
                let async_state_verify_cmds = AsyncStateVerifyChildState::new(self)
                    .update(async_state_verify_event, current_round_estimate);

                async_state_verify_cmds
                    .into_iter()
                    .flat_map(Into::<Vec<Command<_, _, _, _>>>::into)
                    .collect::<Vec<_>>()
            }
            MonadEvent::ControlPanelEvent(control_panel_event) => match control_panel_event {
                ControlPanelEvent::GetValidatorSet => {
                    let round = self.consensus.get_current_round();
                    let epoch = self.epoch_manager.get_epoch(round).unwrap();

                    let validator_set = self
                        .val_epoch_map
                        .get_val_set(&epoch)
                        .unwrap()
                        .get_members();

                    let cert_pubkeys = self
                        .val_epoch_map
                        .get_cert_pubkeys(&epoch)
                        .unwrap()
                        .map
                        .clone();
                    let validators = cert_pubkeys
                        .iter()
                        .map(|(node_id, cert_pub_key)| {
                            let stake = validator_set.get(node_id).unwrap();
                            Validator {
                                node_id: *node_id,
                                stake: *stake,
                                cert_pubkey: *cert_pub_key,
                            }
                        })
                        .collect::<Vec<_>>();

                    vec![Command::ControlPanelCommand(ControlPanelCommand::Read(
                        ReadCommand::GetValidatorSet(GetValidatorSet::Response(
                            ParsedValidatorData { epoch, validators },
                        )),
                    ))]
                }
                ControlPanelEvent::ClearMetricsEvent => {
                    self.metrics = Default::default();
                    vec![Command::ControlPanelCommand(ControlPanelCommand::Write(
                        WriteCommand::ClearMetrics(ClearMetrics::Response(self.metrics)),
                    ))]
                }
            },
        }
    }
}

#[cfg(test)]
mod test {
    use monad_bls::BlsSignatureCollection;
    use monad_consensus_types::{
        ledger::CommitResult,
        quorum_certificate::{QcInfo, QuorumCertificate},
        signature_collection::SignatureCollection,
        state_root_hash::StateRootHash,
        validator_data::ValidatorSetData,
        voting::{Vote, VoteInfo},
    };
    use monad_crypto::{
        certificate_signature::CertificateSignaturePubKey,
        hasher::{Hash, Hasher, HasherType},
    };
    use monad_secp::SecpSignature;
    use monad_testutil::validators::create_keys_w_validators;
    use monad_types::{BlockId, NodeId, Round, SeqNum, Stake};
    use monad_validator::validator_set::ValidatorSetFactory;

    use super::*;

    type SignatureType = SecpSignature;
    type SignatureCollectionType =
        BlsSignatureCollection<CertificateSignaturePubKey<SignatureType>>;

    const EPOCH_LENGTH: SeqNum = SeqNum(1000);
    const STATE_ROOT_DELAY: SeqNum = SeqNum(10);

    fn get_forkpoint() -> Forkpoint<SignatureCollectionType> {
        let (keys, cert_keys, _valset, valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(4, ValidatorSetFactory::default());

        let qc_info = QcInfo {
            vote: Vote {
                vote_info: VoteInfo {
                    id: BlockId(Hash([0x06_u8; 32])),
                    epoch: Epoch(3),
                    round: Round(4030),
                    parent_id: BlockId(Hash([0x06_u8; 32])),
                    parent_round: Round(4027),
                    seq_num: SeqNum(2999),
                },
                ledger_commit_info: CommitResult::NoCommit,
            },
        };

        let qc_info_hash = HasherType::hash_object(&qc_info.vote);

        let mut sigs = Vec::new();

        for (key, cert_key) in keys.iter().zip(cert_keys.iter()) {
            let node_id = NodeId::new(key.pubkey());
            let sig = cert_key.sign(qc_info_hash.as_ref());
            sigs.push((node_id, sig));
        }

        let sigcol: BlsSignatureCollection<monad_secp::PubKey> =
            SignatureCollectionType::new(sigs, &valmap, qc_info_hash.as_ref()).unwrap();

        let qc = QuorumCertificate::new(qc_info, sigcol);

        let state_root = StateRootHash(Hash([(qc.get_seq_num() - STATE_ROOT_DELAY).0 as u8; 32]));

        let mut validators = Vec::new();

        for (key, cert_key) in keys.iter().zip(cert_keys.iter()) {
            validators.push((key.pubkey(), Stake(7), cert_key.pubkey()));
        }

        let validator_data = ValidatorSetData::<SignatureCollectionType>::new(validators);

        let forkpoint: Forkpoint<BlsSignatureCollection<monad_secp::PubKey>> = Checkpoint {
            root: RootInfo {
                block_id: qc.get_block_id(),
                seq_num: qc.get_seq_num(),
                epoch: qc.get_epoch(),
                round: qc.get_round(),
                state_root,
            },
            high_qc: qc,
            validator_sets: vec![
                ValidatorSetDataWithEpoch {
                    epoch: Epoch(3),
                    round: Some(Round(3050)),
                    validators: validator_data.clone(),
                },
                ValidatorSetDataWithEpoch {
                    epoch: Epoch(4),
                    round: None,
                    validators: validator_data,
                },
            ],
        }
        .into();

        forkpoint
    }

    #[test]
    fn test_forkpoint_serde() {
        let forkpoint = get_forkpoint();
        assert!(forkpoint
            .validate(
                STATE_ROOT_DELAY,
                &ValidatorSetFactory::default(),
                EPOCH_LENGTH
            )
            .is_ok());
        let ser = toml::to_string_pretty(&forkpoint.0).unwrap();

        println!("{}", ser);

        let deser = toml::from_str(&ser).unwrap();
        assert_eq!(forkpoint.0, deser);
    }

    #[test]
    fn test_forkpoint_validate_1() {
        let mut forkpoint = get_forkpoint();
        let popped = forkpoint.0.validator_sets.pop().unwrap();

        assert_eq!(
            forkpoint.validate(
                STATE_ROOT_DELAY,
                &ValidatorSetFactory::default(),
                EPOCH_LENGTH
            ),
            Err(ForkpointValidationError::TooFewValidatorSets)
        );

        forkpoint.0.validator_sets.push(popped.clone());
        forkpoint.0.validator_sets.push(popped.clone());
        forkpoint.0.validator_sets.push(popped);
        assert_eq!(
            forkpoint.validate(
                STATE_ROOT_DELAY,
                &ValidatorSetFactory::default(),
                EPOCH_LENGTH
            ),
            Err(ForkpointValidationError::TooManyValidatorSets)
        );
    }

    #[test]
    fn test_forkpoint_validate_2() {
        let mut forkpoint = get_forkpoint();
        forkpoint.0.validator_sets[0].epoch.0 -= 1;

        assert_eq!(
            forkpoint.validate(
                STATE_ROOT_DELAY,
                &ValidatorSetFactory::default(),
                EPOCH_LENGTH
            ),
            Err(ForkpointValidationError::ValidatorSetsNotConsecutive)
        );
    }

    #[test]
    fn test_forkpoint_validate_3() {
        let mut forkpoint = get_forkpoint();
        forkpoint.0.root.epoch.0 -= 1;

        assert_eq!(
            forkpoint.validate(
                STATE_ROOT_DELAY,
                &ValidatorSetFactory::default(),
                EPOCH_LENGTH
            ),
            Err(ForkpointValidationError::InvalidValidatorSetStartEpoch)
        );
    }

    #[test]
    fn test_forkpoint_validate_4() {
        let mut forkpoint = get_forkpoint();

        forkpoint.0.validator_sets[0].round = None;

        assert_eq!(
            forkpoint.validate(
                STATE_ROOT_DELAY,
                &ValidatorSetFactory::default(),
                EPOCH_LENGTH
            ),
            Err(ForkpointValidationError::InvalidValidatorSetStartRound)
        );
    }

    // TODO test every branch of 5
    // the mock-swarm forkpoint tests sort of cover these, but we should unit-test these eventually
    // for completeness

    #[test]
    fn test_forkpoint_validate_6() {
        let mut forkpoint = get_forkpoint();
        // change qc content so signature collection is invalid
        forkpoint.0.high_qc.info.vote.vote_info.round = forkpoint.0.high_qc.get_round() - Round(1);

        assert_eq!(
            forkpoint.validate(
                STATE_ROOT_DELAY,
                &ValidatorSetFactory::default(),
                EPOCH_LENGTH
            ),
            Err(ForkpointValidationError::InvalidQC)
        );
    }
}
