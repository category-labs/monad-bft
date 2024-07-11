use std::{collections::HashSet, fmt::Debug, marker::PhantomData};

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
use monad_consensus_state::{command::Checkpoint, ConsensusConfig, ConsensusState};
use monad_consensus_types::{
    block::{Block, BlockPolicy},
    block_validator::BlockValidator,
    metrics::Metrics,
    payload::StateRootValidator,
    quorum_certificate::QuorumCertificate,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    state_root_hash::{StateRootHash, StateRootHashInfo},
    txpool::TxPool,
    validation,
    validator_data::{ParsedValidatorData, Validator, ValidatorSetData, ValidatorSetDataWithEpoch},
    voting::ValidatorMapping,
};
use monad_crypto::certificate_signature::{
    CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::EthAddress;
use monad_executor_glue::{
    AsyncStateVerifyEvent, BlockSyncEvent, ClearMetrics, Command, ConsensusEvent,
    ControlPanelCommand, ControlPanelEvent, GetValidatorSet, MempoolEvent, Message, MetricsCommand,
    MetricsEvent, MonadEvent, ReadCommand, ValidatorEvent, WriteCommand,
};
use monad_types::{Epoch, NodeId, Round, SeqNum, TimeoutVariant, GENESIS_SEQ_NUM};
use monad_validator::{
    epoch_manager::EpochManager,
    leader_election::LeaderElection,
    validator_set::{ValidatorSetType, ValidatorSetTypeFactory},
    validators_epoch_mapping::ValidatorsEpochMapping,
};
use serde::{Deserialize, Serialize};

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
    /// Too few state roots to validate the next proposals
    InsufficientStateRoots,
    /// Missing validator sets to validate consensus messages
    MissingValidatorSet,
    /// Validator set is not populated with the right fields
    InvalidValidatorSet,
    /// root_qc cannot be verified
    InvalidQC,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]

pub struct Forkpoint<SCT: SignatureCollection> {
    // FIXME: root qc root of block tree, not high qc. If all nodes reboot and
    // start from root of their local block trees, we have a safety violation.
    // In a minimal version, we should lock on high qc round, and wait until
    // observing a higher qc that traces back to our local qc, with block syncs.
    #[serde(bound(
        serialize = "SCT: SignatureCollection",
        deserialize = "SCT: SignatureCollection",
    ))]
    pub root_qc: QuorumCertificate<SCT>,
    pub state_roots: Vec<StateRootHashInfo>,

    #[serde(bound(
        serialize = "SCT: SignatureCollection",
        deserialize = "SCT: SignatureCollection",
    ))]
    pub validator_sets: Vec<ValidatorSetDataWithEpoch<SCT>>,
}

impl<SCT: SignatureCollection> Forkpoint<SCT> {
    pub fn genesis(validator_set: ValidatorSetData<SCT>, state_root_hash: StateRootHash) -> Self {
        Self {
            root_qc: QuorumCertificate::genesis_qc(),
            state_roots: vec![StateRootHashInfo {
                seq_num: GENESIS_SEQ_NUM,
                state_root_hash,
            }],
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
    }

    pub fn get_epoch_starts(&self) -> Vec<(Epoch, Round)> {
        let mut known = Vec::new();
        for validator_set in self.validator_sets.iter() {
            if validator_set.round.is_some() {
                known.push((validator_set.epoch, validator_set.round.unwrap()));
            }
        }
        known
    }

    /// A valid forkpoint must satisfy the following requirements
    /// 1. Enough state roots to validate the next proposal it accepts
    /// 2. Carries the validator set to verify QC
    /// 3. QC is valid against the validator set
    /// 4. QC epoch-round and validator set epoch-round are set correctly
    /// 5. Has n+1 validator set locked
    /// 6. If the last ledger block is on epoch boundary, n+2 validator set is
    ///    locked
    /// 7. Every validator set except the last one is scheduled to start on a
    ///    round
    pub fn validate<VTF>(
        &self,
        state_root_delay: SeqNum,
        validator_set_factory: &VTF,
        val_set_update_interval: SeqNum,
    ) -> Result<(), ForkpointValidationError>
    where
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = SCT::NodeIdPubKey>,
    {
        // 1. Validate enough state roots are attached to continue verifying the
        // next blocks
        self.validate_state_root(state_root_delay)?;

        // 2. Validator set exists for qc.epoch
        let qc_vset_data = self
            .get_validator_set(self.root_qc.get_epoch())
            .ok_or(ForkpointValidationError::MissingValidatorSet)?;

        // 5. Epoch n+1 validator set must be locked
        let n_1_vset_data = self
            .get_validator_set(self.root_qc.get_epoch() + Epoch(1))
            .ok_or(ForkpointValidationError::MissingValidatorSet)?;

        // 3/4/7
        self.validate_and_verify_qc(qc_vset_data, n_1_vset_data.round, validator_set_factory)?;

        // 6. If QC is from the boundary block, epoch n+2 must also be locked
        if (self.root_qc.get_seq_num() + SeqNum(1)).is_epoch_end(val_set_update_interval) {
            // 7. n+1 validator set is scheduled when n+2 is locked
            if n_1_vset_data.round.is_none() {
                return Err(ForkpointValidationError::InvalidValidatorSet);
            }

            let n_2_exists = self.get_validator_set(self.root_qc.get_epoch() + Epoch(2));
            if n_2_exists.is_none() {
                return Err(ForkpointValidationError::MissingValidatorSet);
            }
        }

        Ok(())
    }

    fn validate_state_root(
        &self,
        state_root_delay: SeqNum,
    ) -> Result<(), ForkpointValidationError> {
        if self.root_qc != QuorumCertificate::genesis_qc() {
            let state_root_low =
                (self.root_qc.get_seq_num() + SeqNum(1)).max(state_root_delay) - state_root_delay;
            let state_root_high = self.root_qc.get_seq_num() + SeqNum(2);
            let mut expected_state_roots = (state_root_low.0..state_root_high.0)
                .map(SeqNum)
                .collect::<HashSet<SeqNum>>();

            for state_root in self.state_roots.iter() {
                expected_state_roots.remove(&state_root.seq_num);
            }

            if !expected_state_roots.is_empty() {
                return Err(ForkpointValidationError::InsufficientStateRoots);
            }
        }
        Ok(())
    }

    fn get_validator_set(&self, epoch: Epoch) -> Option<&ValidatorSetDataWithEpoch<SCT>> {
        self.validator_sets.iter().find(|data| data.epoch == epoch)
    }

    fn validate_and_verify_qc<VTF>(
        &self,
        qc_vset_data: &ValidatorSetDataWithEpoch<SCT>,
        next_epoch_start: Option<Round>,
        validator_set_factory: &VTF,
    ) -> Result<(), ForkpointValidationError>
    where
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = SCT::NodeIdPubKey>,
    {
        assert_eq!(qc_vset_data.epoch, self.root_qc.get_epoch());
        // 7.
        if qc_vset_data.round.is_none() {
            return Err(ForkpointValidationError::InvalidValidatorSet);
        }

        // 4. Check QC carries consistent round numbers
        if self.root_qc.get_round() < qc_vset_data.round.expect("asserted before")
            || (next_epoch_start.is_some()
                && self.root_qc.get_round() >= next_epoch_start.expect("asserted before"))
        {
            return Err(ForkpointValidationError::InvalidQC);
        }

        let vset_stake = qc_vset_data
            .validators
            .0
            .iter()
            .map(|data| (data.node_id, data.stake))
            .collect::<Vec<_>>();

        let qc_vmap = ValidatorMapping::new(
            qc_vset_data
                .validators
                .0
                .iter()
                .map(|data| (data.node_id, data.cert_pubkey))
                .collect::<Vec<_>>(),
        );

        let qc_vset = validator_set_factory
            .create(vset_stake)
            .map_err(|_| ForkpointValidationError::InvalidValidatorSet)?;

        // 3.
        if verify_qc(&qc_vset, &qc_vmap, &self.root_qc).is_err() {
            return Err(ForkpointValidationError::InvalidQC);
        }

        Ok(())
    }
}

pub struct MonadState<ST, SCT, BPT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT>,
    BVT: BlockValidator<SCT, BPT>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    keypair: ST::KeyPairType,
    cert_keypair: SignatureCollectionKeyPairType<SCT>,
    nodeid: NodeId<CertificateSignaturePubKey<ST>>,

    consensus_config: ConsensusConfig,

    /// Core consensus algorithm state machine
    consensus: ConsensusState<ST, SCT, BPT>,
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
    beneficiary: EthAddress,

    /// Metrics counters for events and errors
    metrics: Metrics,

    /// Versions for client and protocol validation
    version: MonadVersion,
}

impl<ST, SCT, BPT, VTF, LT, TT, BVT, SVT, ASVT>
    MonadState<ST, SCT, BPT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    TT: TxPool<SCT, BPT>,
    BVT: BlockValidator<SCT, BPT>,
    SVT: StateRootValidator,
    ASVT: AsyncStateVerifyProcess<
        SignatureCollectionType = SCT,
        ValidatorSetType = VTF::ValidatorSetType,
    >,
{
    pub fn consensus(&self) -> &ConsensusState<ST, SCT, BPT> {
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

    pub fn blocktree(&self) -> &BlockTree<SCT, BPT> {
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

pub struct MonadStateBuilder<ST, SCT, BPT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    TT: TxPool<SCT, BPT>,
    BVT: BlockValidator<SCT, BPT>,
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

impl<ST, SCT, BPT, VTF, LT, TT, BVT, SVT, ASVT>
    MonadStateBuilder<ST, SCT, BPT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    TT: TxPool<SCT, BPT>,
    BVT: BlockValidator<SCT, BPT>,
    SVT: StateRootValidator,
    ASVT: AsyncStateVerifyProcess<
        SignatureCollectionType = SCT,
        ValidatorSetType = VTF::ValidatorSetType,
    >,
{
    pub fn build(
        self,
    ) -> (
        MonadState<ST, SCT, BPT, VTF, LT, TT, BVT, SVT, ASVT>,
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
        assert!(self
            .forkpoint
            .validate(
                self.state_root_validator.get_delay(),
                &self.validator_set_factory,
                self.val_set_update_interval
            )
            .is_ok());

        let val_epoch_map = ValidatorsEpochMapping::new(self.validator_set_factory);

        let mut state_root_validator = self.state_root_validator;
        // initialize state_root_validator with forkpoint.state_roots and remove
        // any outdated roots
        for StateRootHashInfo {
            state_root_hash,
            seq_num,
        } in self.forkpoint.state_roots.iter().cloned()
        {
            state_root_validator.add_state_root(seq_num, state_root_hash);
        }

        let epoch_manager = EpochManager::new(
            self.val_set_update_interval,
            self.epoch_start_delay,
            &self.forkpoint.get_epoch_starts(),
        );

        let consensus = ConsensusState::new(
            &self.consensus_config,
            self.forkpoint.root_qc.clone(),
            epoch_manager
                .get_epoch(self.forkpoint.root_qc.get_round() + Round(1))
                .expect("forkpoint has epoch for root qc"),
        );

        let nodeid = NodeId::new(self.key.pubkey());

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

            state_root_validator,
            block_validator: self.block_validator,
            block_policy: self.block_policy,
            beneficiary: self.beneficiary,

            metrics: Metrics::default(),
            version: self.version,
        };

        let mut init_cmds = Vec::new();

        for validator_set in self.forkpoint.validator_sets.into_iter() {
            init_cmds.extend(monad_state.update(MonadEvent::ValidatorEvent(
                ValidatorEvent::UpdateValidators((validator_set.validators, validator_set.epoch)),
            )));
        }

        init_cmds.extend(
            monad_state.update(MonadEvent::ConsensusEvent(ConsensusEvent::Timeout(
                TimeoutVariant::Pacemaker,
            ))),
        );

        init_cmds.extend(monad_state.update(MonadEvent::MetricsEvent(MetricsEvent::Timeout)));

        (monad_state, init_cmds)
    }
}

impl<ST, SCT, BPT, VTF, LT, TT, BVT, SVT, ASVT>
    MonadState<ST, SCT, BPT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    TT: TxPool<SCT, BPT>,
    BVT: BlockValidator<SCT, BPT>,
    SVT: StateRootValidator,
    ASVT: AsyncStateVerifyProcess<
        SignatureCollectionType = SCT,
        ValidatorSetType = VTF::ValidatorSetType,
    >,
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
        let _event_span = tracing::debug_span!("event_span", ?event).entered();

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

            MonadEvent::MempoolEvent(mempool_event) => {
                let mempool_cmds = MempoolChildState::new(self).update(mempool_event);

                mempool_cmds
                    .into_iter()
                    .flat_map(Into::<Vec<Command<_, _, _, _, _>>>::into)
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
                    .flat_map(Into::<Vec<Command<_, _, _, _, _>>>::into)
                    .collect::<Vec<_>>()
            }
            MonadEvent::MetricsEvent(metrics_event) => match metrics_event {
                MetricsEvent::Timeout => {
                    vec![Command::MetricsCommand(MetricsCommand::RecordMetrics(
                        self.metrics,
                    ))]
                }
            },
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
        state_root_hash::{StateRootHash, StateRootHashInfo},
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

        let mut state_roots = Vec::new();

        for i in qc.get_seq_num().0 + 1 - STATE_ROOT_DELAY.0..=qc.get_seq_num().0 + 1 {
            state_roots.push(StateRootHashInfo {
                state_root_hash: StateRootHash(Hash([i as u8; 32])),
                seq_num: SeqNum(i),
            });
        }

        let mut validators = Vec::new();

        for (key, cert_key) in keys.iter().zip(cert_keys.iter()) {
            validators.push((key.pubkey(), Stake(7), cert_key.pubkey()));
        }

        let validator_data = ValidatorSetData::<SignatureCollectionType>::new(validators);

        let forkpoint: Forkpoint<BlsSignatureCollection<monad_secp::PubKey>> = Forkpoint {
            root_qc: qc,
            state_roots,
            validator_sets: vec![
                ValidatorSetDataWithEpoch {
                    epoch: Epoch(3),
                    round: Some(Round(3050)),
                    validators: validator_data.clone(),
                },
                ValidatorSetDataWithEpoch {
                    epoch: Epoch(4),
                    round: Some(Round(4080)),
                    validators: validator_data.clone(),
                },
                ValidatorSetDataWithEpoch {
                    epoch: Epoch(5),
                    round: None,
                    validators: validator_data,
                },
            ],
        };

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
        let ser = toml::to_string_pretty(&forkpoint).unwrap();

        println!("{}", ser);

        let deser = toml::from_str(&ser).unwrap();
        assert_eq!(forkpoint, deser);
    }

    #[test]
    fn test_forkpoint_validate_1() {
        let mut forkpoint = get_forkpoint();
        forkpoint.state_roots.remove(0);

        assert_eq!(
            forkpoint.validate(
                STATE_ROOT_DELAY,
                &ValidatorSetFactory::default(),
                EPOCH_LENGTH
            ),
            Err(ForkpointValidationError::InsufficientStateRoots)
        );

        let mut forkpoint = get_forkpoint();
        forkpoint
            .state_roots
            .remove(forkpoint.state_roots.len() - 1);

        assert_eq!(
            forkpoint.validate(
                STATE_ROOT_DELAY,
                &ValidatorSetFactory::default(),
                EPOCH_LENGTH
            ),
            Err(ForkpointValidationError::InsufficientStateRoots)
        );

        let mut forkpoint = get_forkpoint();
        forkpoint.state_roots.remove(5);

        assert_eq!(
            forkpoint.validate(
                STATE_ROOT_DELAY,
                &ValidatorSetFactory::default(),
                EPOCH_LENGTH
            ),
            Err(ForkpointValidationError::InsufficientStateRoots)
        );
    }

    #[test]
    fn test_forkpoint_validate_2() {
        let mut forkpoint = get_forkpoint();
        forkpoint
            .validator_sets
            .retain(|data| data.epoch != forkpoint.root_qc.get_epoch());

        assert_eq!(
            forkpoint.validate(
                STATE_ROOT_DELAY,
                &ValidatorSetFactory::default(),
                EPOCH_LENGTH
            ),
            Err(ForkpointValidationError::MissingValidatorSet)
        );
    }

    #[test]
    fn test_forkpoint_validate_3() {
        let mut forkpoint = get_forkpoint();
        // change qc content so signature collection is invalid
        forkpoint.root_qc.info.vote.vote_info.round = forkpoint.root_qc.get_round() - Round(1);

        assert_eq!(
            forkpoint.validate(
                STATE_ROOT_DELAY,
                &ValidatorSetFactory::default(),
                EPOCH_LENGTH
            ),
            Err(ForkpointValidationError::InvalidQC)
        );
    }

    #[test]
    fn test_forkpoint_validate_4() {
        // epoch n is set to start later than root qc
        let mut forkpoint = get_forkpoint();

        let qc_vset = forkpoint
            .validator_sets
            .iter_mut()
            .find(|data| data.epoch == forkpoint.root_qc.get_epoch())
            .unwrap();
        qc_vset.round = Some(forkpoint.root_qc.get_round() + Round(1));

        assert_eq!(
            forkpoint.validate(
                STATE_ROOT_DELAY,
                &ValidatorSetFactory::default(),
                EPOCH_LENGTH
            ),
            Err(ForkpointValidationError::InvalidQC)
        );

        // epoch n+1 is set to start earlier than root qc
        let mut forkpoint = get_forkpoint();

        let qc_vset = forkpoint
            .validator_sets
            .iter_mut()
            .find(|data| data.epoch == forkpoint.root_qc.get_epoch() + Epoch(1))
            .unwrap();
        qc_vset.round = Some(forkpoint.root_qc.get_round());

        assert_eq!(
            forkpoint.validate(
                STATE_ROOT_DELAY,
                &ValidatorSetFactory::default(),
                EPOCH_LENGTH
            ),
            Err(ForkpointValidationError::InvalidQC)
        );
    }

    #[test]
    fn test_forkpoint_validate_5() {
        let mut forkpoint = get_forkpoint();
        // remove validator set n+1
        forkpoint
            .validator_sets
            .retain(|data| data.epoch != Epoch(forkpoint.root_qc.get_epoch().0 + 1));

        assert_eq!(
            forkpoint.validate(
                STATE_ROOT_DELAY,
                &ValidatorSetFactory::default(),
                EPOCH_LENGTH
            ),
            Err(ForkpointValidationError::MissingValidatorSet)
        );
    }

    #[test]
    fn test_forkpoint_validate_6() {
        let mut forkpoint = get_forkpoint();
        assert!((forkpoint.root_qc.get_seq_num() + SeqNum(1)).is_epoch_end(EPOCH_LENGTH));
        // remove validator set n+2
        forkpoint
            .validator_sets
            .retain(|data| data.epoch != Epoch(forkpoint.root_qc.get_epoch().0 + 2));

        assert_eq!(
            forkpoint.validate(
                STATE_ROOT_DELAY,
                &ValidatorSetFactory::default(),
                EPOCH_LENGTH
            ),
            Err(ForkpointValidationError::MissingValidatorSet)
        );
    }

    #[test]
    fn test_forkpoint_validate_7() {
        let forkpoint = get_forkpoint();
        for i in 0..forkpoint.validator_sets.len() - 1 {
            let mut forkpoint = get_forkpoint();
            forkpoint.validator_sets.get_mut(i).unwrap().round = None;
            println!("{:#?}", forkpoint);
            assert_eq!(
                forkpoint.validate(
                    STATE_ROOT_DELAY,
                    &ValidatorSetFactory::default(),
                    EPOCH_LENGTH
                ),
                Err(ForkpointValidationError::InvalidValidatorSet)
            );
        }
    }
}
