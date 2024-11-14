use std::{fmt::Debug, ops::Deref};

use async_state_verify::AsyncStateVerifyChildState;
use blocksync::BlockSyncChildState;
use bytes::Bytes;
use consensus::ConsensusChildState;
use epoch::EpochChildState;
use itertools::Itertools;
use mempool::MempoolChildState;
use monad_async_state_verify::AsyncStateVerifyProcess;
use monad_blocksync::{
    blocksync::{BlockSync, BlockSyncSelfRequester},
    messages::message::{BlockSyncRequestMessage, BlockSyncResponseMessage},
};
use monad_blocktree::blocktree::BlockTree;
use monad_consensus::{
    messages::{consensus_message::ConsensusMessage, message::PeerStateRootMessage},
    validation::signing::{verify_qc, Unvalidated, Unverified, Validated, Verified},
};
use monad_consensus_state::{timestamp::BlockTimestamp, ConsensusConfig, ConsensusState};
use monad_consensus_types::{
    block::{BlockPolicy, BlockType},
    block_validator::BlockValidator,
    checkpoint::{Checkpoint, RootInfo},
    metrics::Metrics,
    payload::StateRootValidator,
    quorum_certificate::{QuorumCertificate, GENESIS_BLOCK_ID},
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    state_root_hash::{StateRootHash, StateRootHashInfo},
    txpool::TxPool,
    validation,
    validator_data::{ValidatorData, ValidatorSetData, ValidatorSetDataWithEpoch},
    voting::ValidatorMapping,
};
use monad_crypto::certificate_signature::{
    CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::EthAddress;
use monad_executor_glue::{
    AsyncStateVerifyEvent, BlockSyncEvent, ClearMetrics, Command, ConsensusEvent,
    ControlPanelCommand, ControlPanelEvent, GetExecutorMetrics, GetFullNodes, GetMetrics, GetPeers,
    GetValidatorSet, LedgerCommand, MempoolEvent, Message, MonadEvent, ReadCommand, RouterCommand,
    StateRootHashCommand, StateSyncCommand, StateSyncEvent, StateSyncNetworkMessage,
    UpdateFullNodes, UpdatePeers, ValidatorEvent, WriteCommand,
};
use monad_state_backend::StateBackend;
use monad_types::{Epoch, NodeId, Round, RouterTarget, SeqNum, GENESIS_SEQ_NUM};
use monad_validator::{
    epoch_manager::EpochManager,
    leader_election::LeaderElection,
    validator_set::{ValidatorSetType, ValidatorSetTypeFactory},
    validators_epoch_mapping::ValidatorsEpochMapping,
};
use statesync::BlockBuffer;

mod async_state_verify;
mod blocksync;
mod consensus;
pub mod convert;
mod epoch;
mod mempool;
mod statesync;

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
        validation::Error::InvalidVote => {
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

enum ConsensusMode<SCT, BPT, SBT>
where
    SCT: SignatureCollection,
    BPT: BlockPolicy<SCT, SBT>,
    SBT: StateBackend,
{
    Sync {
        root: RootInfo,
        high_qc: QuorumCertificate<SCT>,

        block_buffer: BlockBuffer<SCT>,

        done_db_sync: bool,

        // this is set to true when in the process of updating to a new target
        // used for deduplicating ConsensusMode::Sync(n) -> ConsensusMode::Sync(n') transitions
        // ideally we can deprecate this and update our target synchronously (w/o loopback executor)
        updating_target: bool,
    },
    Live(ConsensusState<SCT, BPT, SBT>),
}

impl<SCT, BPT, SBT> ConsensusMode<SCT, BPT, SBT>
where
    SCT: SignatureCollection,
    BPT: BlockPolicy<SCT, SBT>,
    SBT: StateBackend,
{
    fn start_sync(
        root: RootInfo,
        high_qc: QuorumCertificate<SCT>,
        block_buffer: BlockBuffer<SCT>,
    ) -> Self {
        Self::Sync {
            root,
            high_qc,
            block_buffer,

            done_db_sync: false,

            updating_target: false,
        }
    }

    fn current_epoch(&self) -> Epoch {
        match self {
            ConsensusMode::Sync { high_qc, .. } => {
                // TODO do we need to check the boundary condition if high_qc is on the epoch
                // boundary? Probably doesn't matter that much
                high_qc.get_epoch()
            }
            ConsensusMode::Live(consensus) => consensus.get_current_epoch(),
        }
    }
}

pub struct MonadState<ST, SCT, BPT, SBT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT, SBT>,
    SBT: StateBackend,
    BVT: BlockValidator<SCT, BPT, SBT>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    keypair: ST::KeyPairType,
    cert_keypair: SignatureCollectionKeyPairType<SCT>,
    nodeid: NodeId<CertificateSignaturePubKey<ST>>,

    consensus_config: ConsensusConfig,

    /// Core consensus algorithm state machine
    consensus: ConsensusMode<SCT, BPT, SBT>,
    /// Handles blocksync servicing
    block_sync: BlockSync<ST, SCT>,

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
    block_timestamp: BlockTimestamp,
    block_validator: BVT,
    block_policy: BPT,
    state_backend: SBT,
    beneficiary: EthAddress,

    /// Metrics counters for events and errors
    metrics: Metrics,

    /// Versions for client and protocol validation
    version: MonadVersion,
}

// execution needs NumBlockHash blocks before tip to execute tip
const NUM_BLOCK_HASH: SeqNum = SeqNum(256);

impl<ST, SCT, BPT, SBT, VTF, LT, TT, BVT, SVT, ASVT>
    MonadState<ST, SCT, BPT, SBT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT, SBT>,
    SBT: StateBackend,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    TT: TxPool<SCT, BPT, SBT>,
    BVT: BlockValidator<SCT, BPT, SBT>,
    SVT: StateRootValidator,
    ASVT: AsyncStateVerifyProcess<
        SignatureCollectionType = SCT,
        ValidatorSetType = VTF::ValidatorSetType,
    >,
{
    pub fn consensus(&self) -> Option<&ConsensusState<SCT, BPT, SBT>> {
        match &self.consensus {
            ConsensusMode::Sync { .. } => None,
            ConsensusMode::Live(consensus) => Some(consensus),
        }
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

    pub fn blocktree(&self) -> Option<&BlockTree<SCT, BPT, SBT>> {
        match &self.consensus {
            ConsensusMode::Sync { .. } => None,
            ConsensusMode::Live(consensus) => Some(consensus.blocktree()),
        }
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
    BlockSyncRequest(BlockSyncRequestMessage),
    BlockSyncResponse(BlockSyncResponseMessage<SCT>),
    PeerStateRootMessage(Validated<PeerStateRootMessage<SCT>>),
    ForwardedTx(Vec<Bytes>),
    StateSyncMessage(StateSyncNetworkMessage),
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
    BlockSyncRequest(BlockSyncRequestMessage),

    /// Block sync response
    BlockSyncResponse(BlockSyncResponseMessage<SCT>),

    /// Async state verification msgs
    PeerStateRoot(Unvalidated<PeerStateRootMessage<SCT>>),

    /// Forwarded transactions
    ForwardedTx(Vec<Bytes>),

    /// State Sync msgs
    StateSyncMessage(StateSyncNetworkMessage),
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
            VerifiedMonadMessage::BlockSyncRequest(msg) => MonadMessage::BlockSyncRequest(msg),
            VerifiedMonadMessage::BlockSyncResponse(msg) => MonadMessage::BlockSyncResponse(msg),
            VerifiedMonadMessage::PeerStateRootMessage(msg) => {
                MonadMessage::PeerStateRoot(msg.into())
            }
            VerifiedMonadMessage::ForwardedTx(msg) => MonadMessage::ForwardedTx(msg),
            VerifiedMonadMessage::StateSyncMessage(msg) => MonadMessage::StateSyncMessage(msg),
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
            VerifiedMonadMessage::BlockSyncRequest(msg) => MonadMessage::BlockSyncRequest(msg),
            VerifiedMonadMessage::BlockSyncResponse(msg) => MonadMessage::BlockSyncResponse(msg),
            VerifiedMonadMessage::PeerStateRootMessage(msg) => {
                MonadMessage::PeerStateRoot(msg.into())
            }
            VerifiedMonadMessage::ForwardedTx(msg) => MonadMessage::ForwardedTx(msg),
            VerifiedMonadMessage::StateSyncMessage(msg) => MonadMessage::StateSyncMessage(msg),
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

            MonadMessage::BlockSyncRequest(request) => {
                MonadEvent::BlockSyncEvent(BlockSyncEvent::Request {
                    sender: from,
                    request,
                })
            }
            MonadMessage::BlockSyncResponse(response) => {
                MonadEvent::BlockSyncEvent(BlockSyncEvent::Response {
                    sender: from,
                    response,
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
            MonadMessage::StateSyncMessage(msg) => {
                MonadEvent::StateSyncEvent(StateSyncEvent::Inbound(from, msg))
            }
        }
    }
}

pub struct MonadStateBuilder<ST, SCT, BPT, SBT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT, SBT>,
    SBT: StateBackend,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    TT: TxPool<SCT, BPT, SBT>,
    BVT: BlockValidator<SCT, BPT, SBT>,

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
    pub state_backend: SBT,
    pub state_root_validator: SVT,
    pub async_state_verify: ASVT,
    pub forkpoint: Forkpoint<SCT>,
    pub key: ST::KeyPairType,
    pub certkey: SignatureCollectionKeyPairType<SCT>,
    pub val_set_update_interval: SeqNum,
    pub epoch_start_delay: Round,
    pub beneficiary: EthAddress,

    pub consensus_config: ConsensusConfig,
}

impl<ST, SCT, BPT, SBT, VTF, LT, TT, BVT, SVT, ASVT>
    MonadStateBuilder<ST, SCT, BPT, SBT, VTF, LT, TT, BVT, SVT, ASVT>
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
    ASVT: AsyncStateVerifyProcess<
        SignatureCollectionType = SCT,
        ValidatorSetType = VTF::ValidatorSetType,
    >,
{
    pub fn build(
        self,
    ) -> (
        MonadState<ST, SCT, BPT, SBT, VTF, LT, TT, BVT, SVT, ASVT>,
        Vec<Command<MonadEvent<ST, SCT>, VerifiedMonadMessage<ST, SCT>, SCT>>,
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

        let nodeid = NodeId::new(self.key.pubkey());
        let delta: u64 = self
            .consensus_config
            .delta
            .as_millis()
            .try_into()
            .expect("consensus config delta should not be too large for a u64");
        let block_timestamp = BlockTimestamp::new(
            5 * delta,
            self.consensus_config.timestamp_latency_estimate_ms,
        );
        let statesync_to_live_threshold = self.consensus_config.statesync_to_live_threshold;
        let mut monad_state = MonadState {
            keypair: self.key,
            cert_keypair: self.certkey,
            nodeid,

            consensus_config: self.consensus_config,
            consensus: ConsensusMode::start_sync(
                self.forkpoint.root.clone(),
                self.forkpoint.high_qc.clone(),
                BlockBuffer::new(
                    self.state_root_validator.get_delay(),
                    self.forkpoint.root.seq_num,
                    statesync_to_live_threshold,
                ),
            ),
            block_sync: BlockSync::default(),

            leader_election: self.leader_election,
            epoch_manager,
            val_epoch_map,
            txpool: self.transaction_pool,
            async_state_verify: self.async_state_verify,

            state_root_validator: self.state_root_validator,
            block_timestamp,
            block_validator: self.block_validator,
            block_policy: self.block_policy,
            state_backend: self.state_backend,
            beneficiary: self.beneficiary,

            metrics: Metrics::default(),
            version: self.version,
        };

        let mut init_cmds = Vec::new();

        let Forkpoint(Checkpoint {
            root,
            high_qc,
            validator_sets,
        }) = self.forkpoint;

        for validator_set in validator_sets.into_iter() {
            init_cmds.extend(monad_state.update(MonadEvent::ValidatorEvent(
                ValidatorEvent::UpdateValidators((validator_set.validators, validator_set.epoch)),
            )));
        }

        tracing::info!(?root, ?high_qc, "starting up, syncing");
        init_cmds.extend(monad_state.update(MonadEvent::StateSyncEvent(
            StateSyncEvent::RequestSync { root, high_qc },
        )));

        (monad_state, init_cmds)
    }
}

impl<ST, SCT, BPT, SBT, VTF, LT, TT, BVT, SVT, ASVT>
    MonadState<ST, SCT, BPT, SBT, VTF, LT, TT, BVT, SVT, ASVT>
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
    ASVT: AsyncStateVerifyProcess<
        SignatureCollectionType = SCT,
        ValidatorSetType = VTF::ValidatorSetType,
    >,
{
    pub fn update(
        &mut self,
        event: MonadEvent<ST, SCT>,
    ) -> Vec<Command<MonadEvent<ST, SCT>, VerifiedMonadMessage<ST, SCT>, SCT>> {
        let _event_span = tracing::debug_span!("event_span", ?event).entered();

        match event {
            MonadEvent::ConsensusEvent(consensus_event) => {
                let consensus_cmds = ConsensusChildState::new(self).update(consensus_event);

                consensus_cmds
                    .into_iter()
                    .flat_map(Into::<Vec<Command<_, _, _>>>::into)
                    .collect::<Vec<_>>()
            }

            MonadEvent::BlockSyncEvent(block_sync_event) => {
                let block_sync_cmds = BlockSyncChildState::new(self).update(block_sync_event);

                block_sync_cmds
                    .into_iter()
                    .flat_map(Into::<Vec<Command<_, _, _>>>::into)
                    .collect::<Vec<_>>()
            }

            MonadEvent::ValidatorEvent(validator_event) => {
                let validator_cmds = EpochChildState::new(self).update(validator_event);

                validator_cmds
                    .into_iter()
                    .flat_map(Into::<Vec<Command<_, _, _>>>::into)
                    .collect::<Vec<_>>()
            }

            MonadEvent::MempoolEvent(mempool_event) => {
                let mempool_cmds = MempoolChildState::new(self).update(mempool_event);

                mempool_cmds
                    .into_iter()
                    .flat_map(Into::<Vec<Command<_, _, _>>>::into)
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
                let ConsensusMode::Live(consensus) = &mut self.consensus else {
                    tracing::trace!("ignoring AsyncStateVerifyEvent, not live yet");
                    return vec![];
                };
                let current_round_estimate = consensus.get_current_round();
                let async_state_verify_cmds = AsyncStateVerifyChildState::new(self)
                    .update(async_state_verify_event, current_round_estimate);

                async_state_verify_cmds
                    .into_iter()
                    .flat_map(Into::<Vec<Command<_, _, _>>>::into)
                    .collect::<Vec<_>>()
            }
            MonadEvent::StateSyncEvent(state_sync_event) => match state_sync_event {
                StateSyncEvent::Inbound(sender, message) => {
                    // TODO we need to add some sort of throttling to who we service... right now
                    // we'll service indiscriminately
                    vec![Command::StateSyncCommand(StateSyncCommand::Message((
                        sender, message,
                    )))]
                }
                StateSyncEvent::Outbound(to, message) => {
                    vec![Command::RouterCommand(RouterCommand::Publish {
                        target: RouterTarget::TcpPointToPoint(to),
                        message: VerifiedMonadMessage::StateSyncMessage(message),
                    })]
                }
                StateSyncEvent::RequestSync { root, high_qc } => {
                    let mut commands = Vec::new();

                    let delay = self.state_root_validator.get_delay();
                    let root_seq_num = root.seq_num;
                    let state_root_seq_num = root_seq_num.max(delay) - delay;

                    let latest_block = self.state_backend.raw_read_latest_block();
                    assert!(
                        latest_block <= root_seq_num,
                        "tried to statesync backwards: {latest_block:?} <= {root_seq_num:?}"
                    );

                    // we do a <= check instead of < because latest_block currently returns 0 even
                    // if triedb is uninitialized
                    if latest_block <= state_root_seq_num {
                        let state_root_hash_info = StateRootHashInfo {
                            seq_num: state_root_seq_num,
                            state_root_hash: root.state_root,
                        };

                        // TODO we can seed this from the RequestSync to save on blocksyncs
                        //      specificially for ConsensusMode::Live -> Sync transition

                        let block_buffer = match &mut self.consensus {
                            ConsensusMode::Live(_) => BlockBuffer::new(
                                delay,
                                root_seq_num,
                                self.consensus_config.statesync_to_live_threshold,
                            ),
                            ConsensusMode::Sync { block_buffer, .. } => {
                                block_buffer.clone().re_root(root_seq_num)
                            }
                        };
                        self.consensus = ConsensusMode::start_sync(root, high_qc, block_buffer);
                        commands.push(Command::StateSyncCommand(StateSyncCommand::RequestSync(
                            state_root_hash_info,
                        )));
                    } else {
                        // if latest_block > state_root_seq_num, we can't RequestSync because we
                        // would be trying to sync backwards.

                        assert!(self.state_backend.raw_read_earliest_block() <= state_root_seq_num);
                        // TODO assert state root matches?
                        commands.extend(self.update(MonadEvent::StateSyncEvent(
                            StateSyncEvent::DoneSync(state_root_seq_num),
                        )));
                    }

                    let ConsensusMode::Sync {
                        block_buffer, root, ..
                    } = &self.consensus
                    else {
                        unreachable!("not in Sync mode at end of RequestSync");
                    };

                    // committed-block-sync
                    if let Some(block_range) = block_buffer.needs_blocksync(root) {
                        commands.extend(self.update(MonadEvent::BlockSyncEvent(
                            BlockSyncEvent::SelfRequest {
                                requester: BlockSyncSelfRequester::StateSync,
                                block_range,
                            },
                        )));
                    }

                    commands
                }
                StateSyncEvent::DoneSync(n) => {
                    let ConsensusMode::Sync {
                        root, done_db_sync, ..
                    } = &mut self.consensus
                    else {
                        unreachable!("DoneSync invoked while ConsensusState is live")
                    };
                    assert!(!*done_db_sync);

                    let root_seq_num = root.seq_num;
                    let delay = self.state_root_validator.get_delay();
                    let target = root_seq_num.max(delay) - delay;
                    assert!(n <= target);

                    if n < target {
                        tracing::debug!(?n, ?target, "dropping DoneSync, n < target");
                        Vec::new()
                    } else {
                        assert_eq!(n, target);
                        assert!(self.state_backend.raw_read_earliest_block() <= n);
                        assert!(self.state_backend.raw_read_latest_block() >= n);

                        tracing::info!(?n, "done db statesync");
                        *done_db_sync = true;

                        self.maybe_start_consensus()
                    }
                }
                StateSyncEvent::BlockSync {
                    block_range,
                    full_blocks,
                } => {
                    let ConsensusMode::Sync {
                        root, block_buffer, ..
                    } = &mut self.consensus
                    else {
                        return Vec::new();
                    };

                    let mut commands = Vec::new();

                    for full_block in full_blocks {
                        block_buffer.handle_blocksync(full_block);
                    }

                    // committed-block-sync
                    if let Some(block_range) = block_buffer.needs_blocksync(root) {
                        commands.extend(self.update(MonadEvent::BlockSyncEvent(
                            BlockSyncEvent::SelfRequest {
                                requester: BlockSyncSelfRequester::StateSync,
                                block_range,
                            },
                        )));
                    }
                    commands.extend(self.maybe_start_consensus());
                    commands
                }
            },
            MonadEvent::ControlPanelEvent(control_panel_event) => match control_panel_event {
                ControlPanelEvent::GetValidatorSet => {
                    let epoch = self.consensus.current_epoch();

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
                        .iter();
                    let validators = ValidatorSetData(
                        cert_pubkeys
                            .map(|(node_id, cert_pub_key)| {
                                let stake = validator_set.get(node_id).unwrap();
                                ValidatorData {
                                    node_id: *node_id,
                                    stake: *stake,
                                    cert_pubkey: *cert_pub_key,
                                }
                            })
                            .collect::<Vec<_>>(),
                    );

                    vec![Command::ControlPanelCommand(ControlPanelCommand::Read(
                        ReadCommand::GetValidatorSet(GetValidatorSet::Response(
                            ValidatorSetDataWithEpoch {
                                epoch,
                                round: self.epoch_manager.epoch_starts.get(&epoch).copied(),
                                validators,
                            },
                        )),
                    ))]
                }
                ControlPanelEvent::GetMetricsEvent => {
                    vec![Command::ControlPanelCommand(ControlPanelCommand::Read(
                        ReadCommand::GetMetrics(GetMetrics::Response(self.metrics)),
                    ))]
                }
                ControlPanelEvent::GetExecutorMetricsEvent => {
                    vec![Command::ControlPanelCommand(ControlPanelCommand::Read(
                        ReadCommand::GetExecutorMetrics(GetExecutorMetrics::Response(
                            // the real executor metrics have to be filled
                            // out by the parent executor
                            None,
                        )),
                    ))]
                }
                ControlPanelEvent::ClearMetricsEvent => {
                    self.metrics = Default::default();
                    vec![Command::ControlPanelCommand(ControlPanelCommand::Write(
                        WriteCommand::ClearMetrics(ClearMetrics::Response(self.metrics)),
                    ))]
                }
                ControlPanelEvent::UpdateValidators(ValidatorSetDataWithEpoch {
                    epoch,
                    validators,
                    ..
                }) => {
                    vec![Command::StateRootHashCommand(
                        StateRootHashCommand::UpdateValidators((validators, epoch)),
                    )]
                }
                ControlPanelEvent::UpdateLogFilter(filter) => {
                    vec![Command::ControlPanelCommand(ControlPanelCommand::Write(
                        WriteCommand::UpdateLogFilter(filter),
                    ))]
                }
                ControlPanelEvent::GetPeers(req_resp) => match req_resp {
                    GetPeers::Request => {
                        vec![Command::RouterCommand(RouterCommand::GetPeers)]
                    }
                    GetPeers::Response(resp) => {
                        vec![Command::ControlPanelCommand(ControlPanelCommand::Read(
                            ReadCommand::GetPeers(GetPeers::Response(resp)),
                        ))]
                    }
                },
                ControlPanelEvent::UpdatePeers(req_resp) => match req_resp {
                    UpdatePeers::Request(vec) => {
                        vec![Command::RouterCommand(RouterCommand::UpdatePeers(vec))]
                    }
                    UpdatePeers::Response => {
                        vec![Command::ControlPanelCommand(ControlPanelCommand::Write(
                            WriteCommand::UpdatePeers(UpdatePeers::Response),
                        ))]
                    }
                },
                ControlPanelEvent::GetFullNodes(req_resp) => match req_resp {
                    GetFullNodes::Request => {
                        vec![Command::RouterCommand(RouterCommand::GetFullNodes)]
                    }
                    GetFullNodes::Response(vec) => {
                        vec![Command::ControlPanelCommand(ControlPanelCommand::Read(
                            ReadCommand::GetFullNodes(GetFullNodes::Response(vec)),
                        ))]
                    }
                },
                ControlPanelEvent::UpdateFullNodes(req_resp) => match req_resp {
                    UpdateFullNodes::Request(vec) => {
                        vec![Command::RouterCommand(RouterCommand::UpdateFullNodes(vec))]
                    }
                    UpdateFullNodes::Response => {
                        vec![Command::ControlPanelCommand(ControlPanelCommand::Write(
                            WriteCommand::UpdateFullNodes(UpdateFullNodes::Response),
                        ))]
                    }
                },
            },
            MonadEvent::TimestampUpdateEvent(t) => {
                self.block_timestamp.update_time(t);
                vec![]
            }
        }
    }

    fn maybe_start_consensus(
        &mut self,
    ) -> Vec<Command<MonadEvent<ST, SCT>, VerifiedMonadMessage<ST, SCT>, SCT>> {
        let ConsensusMode::Sync {
            root,
            high_qc,
            block_buffer,
            done_db_sync,
            updating_target: _,
        } = &mut self.consensus
        else {
            unreachable!("maybe_start_consensus invoked while ConsensusState is live")
        };

        let root_seq_num = root.seq_num;
        let delay = self.state_root_validator.get_delay();

        let root_parent_chain = block_buffer.root_parent_chain(root);
        // check:
        // 1. done_db_sync
        // 2. earliest_block is early enough to start consensus (at least NUM_BLOCK_HASH committed)
        let done_blocksync = block_buffer.needs_blocksync(root).is_none();
        if !*done_db_sync || !done_blocksync {
            tracing::info!(
                ?done_db_sync,
                earliest_block =? root_parent_chain.last().map(|block| block.get_seq_num()),
                ?root_seq_num,
                "still syncing..."
            );
            return Vec::new();
        }

        let mut commands = Vec::new();

        let mut parent_block_id = root.block_id;
        for block in &root_parent_chain {
            assert_eq!(parent_block_id, block.get_id());
            parent_block_id = block.get_parent_id();
        }
        let delay_non_null_validated_blocks_from_root: Vec<_> = root_parent_chain
            .iter()
            .filter(|block| !block.is_empty_block())
            .map(|full_block| {
                self.block_validator
                    .validate(
                        full_block.block.clone(),
                        full_block.payload.clone(),
                        // we don't need to validate bls pubkey fields (randao)
                        // this is because these blocks are already committed by majority
                        None,
                    )
                    .expect("majority committed invalid block")
            })
            .take(delay.0 as usize)
            .collect();
        let blocks_to_commit: Vec<_> = root_parent_chain.into_iter().cloned().rev().collect();
        // reset block_policy
        self.block_policy.reset(
            delay_non_null_validated_blocks_from_root
                .iter()
                .rev()
                .collect(),
        );
        // commit blocks
        commands.push(Command::LedgerCommand(LedgerCommand::LedgerCommit(
            blocks_to_commit,
        )));

        // if root is N, we need to request the roots from (N-delay, N]
        let first_root_to_request = (root_seq_num + SeqNum(1)).max(delay) - delay;
        let state_root_queue_range = (first_root_to_request.0..=root_seq_num.0).map(SeqNum);
        commands.extend(state_root_queue_range.map(|committed_seq_num| {
            Command::StateRootHashCommand(StateRootHashCommand::Request(committed_seq_num))
        }));
        commands.push(Command::StateRootHashCommand(
            // upon committing block N, we no longer need state_root_N-delay
            // therefore, we cancel below state_root_N-delay+1
            //
            // we'll be left with (state_root_N-delay, state_root_N] queued up, which is
            // exactly `delay` number of roots
            StateRootHashCommand::CancelBelow(first_root_to_request),
        ));

        let cached_proposals = block_buffer.proposals().cloned().collect_vec();

        // Invariants:
        // let N == root_qc_seq_num
        // n in DoneSync(n) == N - delay
        // (N-2*delay, N] have been committed
        // (N-delay-256, N-delay] block hashes are available to execution
        // (N-delay, N] roots have been requested
        let consensus = ConsensusState::new(
            &self.epoch_manager,
            &self.consensus_config,
            root.clone(),
            high_qc.clone(),
        );
        tracing::info!(?root, ?high_qc, "done syncing, initializing consensus");
        self.consensus = ConsensusMode::Live(consensus);
        commands.extend(self.update(MonadEvent::ConsensusEvent(ConsensusEvent::Timeout)));
        for (sender, proposal) in cached_proposals {
            // handle proposals in reverse order because later blocks are more likely to pass
            // timestamp validation
            //
            // earlier proposals will then get short-circuited via blocksync codepath if certified
            let mut consensus = ConsensusChildState::new(self);
            commands.extend(
                consensus
                    .handle_validated_proposal(sender, proposal)
                    .into_iter()
                    .flat_map(Into::<Vec<Command<_, _, _>>>::into),
            );
        }
        commands
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
                    timestamp: 1,
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
