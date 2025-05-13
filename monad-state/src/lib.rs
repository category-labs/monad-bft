use std::{fmt::Debug, marker::PhantomData, ops::Deref};

use alloy_rlp::{encode_list, Decodable, Encodable, Header};
use bytes::{Bytes, BytesMut};
use itertools::Itertools;
use monad_blocksync::{
    blocksync::{BlockSync, BlockSyncSelfRequester},
    messages::message::{BlockSyncRequestMessage, BlockSyncResponseMessage},
};
use monad_blocktree::blocktree::BlockTree;
use monad_chain_config::{
    execution_revision::ExecutionChainParams,
    revision::{ChainParams, ChainRevision},
    ChainConfig,
};
use monad_consensus::{
    messages::consensus_message::ConsensusMessage,
    validation::signing::{verify_qc, Unvalidated, Unverified, Validated, Verified},
};
use monad_consensus_state::{timestamp::BlockTimestamp, ConsensusConfig, ConsensusState};
use monad_consensus_types::{
    block::{BlockPolicy, OptimisticCommit},
    block_validator::BlockValidator,
    checkpoint::{Checkpoint, LockedEpoch},
    metrics::Metrics,
    quorum_certificate::QuorumCertificate,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    validation,
    validator_data::ValidatorSetDataWithEpoch,
    voting::ValidatorMapping,
};
use monad_crypto::certificate_signature::{
    CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor_glue::{
    BlockSyncEvent, ClearMetrics, Command, ConfigEvent, ConfigReloadCommand, ConsensusEvent,
    ControlPanelCommand, ControlPanelEvent, GetFullNodes, GetMetrics, GetPeers, LedgerCommand,
    MempoolEvent, Message, MonadEvent, ReadCommand, ReloadConfig, RouterCommand,
    StateRootHashCommand, StateSyncCommand, StateSyncEvent, StateSyncNetworkMessage, TxPoolCommand,
    ValidatorEvent, WriteCommand,
};
use monad_raptorcast::message::OutboundRouterMessage;
use monad_state_backend::StateBackend;
use monad_types::{
    Epoch, ExecutionProtocol, MonadVersion, NodeId, Round, RouterTarget, SeqNum, GENESIS_BLOCK_ID,
    GENESIS_ROUND,
};
use monad_validator::{
    epoch_manager::EpochManager, leader_election::LeaderElection,
    validator_set::ValidatorSetTypeFactory, validators_epoch_mapping::ValidatorsEpochMapping,
};

use self::{
    blocksync::BlockSyncChildState, consensus::ConsensusChildState, epoch::EpochChildState,
    statesync::BlockBuffer,
};

mod blocksync;
mod consensus;
pub mod convert;
mod epoch;
mod statesync;

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
    pub fn genesis() -> Self {
        Checkpoint {
            root: GENESIS_BLOCK_ID,
            high_qc: QuorumCertificate::genesis_qc(),
            validator_sets: vec![
                LockedEpoch {
                    epoch: Epoch(1),
                    round: Some(GENESIS_ROUND),
                },
                LockedEpoch {
                    epoch: Epoch(2),
                    round: None,
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

    /// locked_validator_sets must correspond 1:1 with the epochs in Checkpoint::validator_sets
    // Concrete verification steps:
    // 1. 2 <= validator_sets.len() <= 3
    // 2. validator_sets have consecutive epochs
    // 3. assert!(validator_sets[0].round.is_some())
    // 4. high_qc is valid against matching epoch validator_set
    pub fn validate(
        &self,
        validator_set_factory: &impl ValidatorSetTypeFactory<NodeIdPubKey = SCT::NodeIdPubKey>,
        locked_validator_sets: &[ValidatorSetDataWithEpoch<SCT>],
    ) -> Result<(), ForkpointValidationError> {
        // 1.
        if self.validator_sets.len() < 2 {
            return Err(ForkpointValidationError::TooFewValidatorSets);
        }
        if self.validator_sets.len() > 3 {
            return Err(ForkpointValidationError::TooManyValidatorSets);
        }

        assert_eq!(self.validator_sets.len(), locked_validator_sets.len());

        // 2.
        if !self
            .validator_sets
            .iter()
            .zip(self.validator_sets.iter().skip(1))
            .all(|(set_1, set_2)| set_1.epoch + Epoch(1) == set_2.epoch)
        {
            return Err(ForkpointValidationError::ValidatorSetsNotConsecutive);
        }

        assert!(locked_validator_sets
            .iter()
            .zip(&self.validator_sets)
            .all(|(locked_vset, forkpoint_vset)| locked_vset.epoch == forkpoint_vset.epoch));

        // 3.
        let Some(_validator_set_0_round) = self.validator_sets[0].round else {
            return Err(ForkpointValidationError::InvalidValidatorSetStartRound);
        };

        // 6.
        self.validate_and_verify_qc(validator_set_factory, locked_validator_sets)?;

        Ok(())
    }

    fn validate_and_verify_qc(
        &self,
        validator_set_factory: &impl ValidatorSetTypeFactory<NodeIdPubKey = SCT::NodeIdPubKey>,
        locked_validator_sets: &[ValidatorSetDataWithEpoch<SCT>],
    ) -> Result<(), ForkpointValidationError> {
        let qc_validator_set = locked_validator_sets
            .iter()
            .find(|locked| locked.epoch == self.high_qc.get_epoch())
            .map(|locked| &locked.validators)
            .ok_or(ForkpointValidationError::InvalidQC)?;

        let vset_stake = qc_validator_set
            .0
            .iter()
            .map(|data| (data.node_id, data.stake))
            .collect::<Vec<_>>();

        let qc_vmap = ValidatorMapping::new(
            qc_validator_set
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

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum DbSyncStatus {
    Waiting,
    Started,
    Done,
}

enum ConsensusMode<ST, SCT, EPT, BPT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
    Sync {
        high_qc: QuorumCertificate<SCT>,

        block_buffer: BlockBuffer<ST, SCT, EPT>,

        db_status: DbSyncStatus,

        // this is set to true when in the process of updating to a new target
        // used for deduplicating ConsensusMode::Sync(n) -> ConsensusMode::Sync(n') transitions
        // ideally we can deprecate this and update our target synchronously (w/o loopback executor)
        updating_target: bool,
    },
    Live(ConsensusState<ST, SCT, EPT, BPT, SBT, CCT, CRT>),
}

impl<ST, SCT, EPT, BPT, SBT, CCT, CRT> ConsensusMode<ST, SCT, EPT, BPT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    fn start_sync(
        high_qc: QuorumCertificate<SCT>,
        block_buffer: BlockBuffer<ST, SCT, EPT>,
    ) -> Self {
        Self::Sync {
            high_qc,
            block_buffer,

            db_status: DbSyncStatus::Waiting,

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

pub struct MonadState<ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
    BVT: BlockValidator<ST, SCT, EPT, BPT, SBT>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    keypair: ST::KeyPairType,
    cert_keypair: SignatureCollectionKeyPairType<SCT>,
    nodeid: NodeId<CertificateSignaturePubKey<ST>>,

    consensus_config: ConsensusConfig<CCT, CRT>,

    /// Core consensus algorithm state machine
    consensus: ConsensusMode<ST, SCT, EPT, BPT, SBT, CCT, CRT>,
    /// Handles blocksync servicing
    block_sync: BlockSync<ST, SCT, EPT>,

    /// Algorithm for choosing leaders for the consensus algorithm
    leader_election: LT,
    /// Track the information for epochs
    epoch_manager: EpochManager,
    /// Maps the epoch number to validator stakes and certificate pubkeys
    val_epoch_map: ValidatorsEpochMapping<VTF, SCT>,

    block_timestamp: BlockTimestamp,
    block_validator: BVT,
    block_policy: BPT,
    state_backend: SBT,
    beneficiary: [u8; 20],

    /// Metrics counters for events and errors
    metrics: Metrics,

    /// Versions for client and protocol validation
    version: MonadVersion,
}

impl<ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT>
    MonadState<ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BVT: BlockValidator<ST, SCT, EPT, BPT, SBT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    pub fn consensus(&self) -> Option<&ConsensusState<ST, SCT, EPT, BPT, SBT, CCT, CRT>> {
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

    pub fn pubkey(&self) -> SCT::NodeIdPubKey {
        self.nodeid.pubkey()
    }

    pub fn blocktree(&self) -> Option<&BlockTree<ST, SCT, EPT, BPT, SBT>> {
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
pub enum VerifiedMonadMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    Consensus(Verified<ST, Validated<ConsensusMessage<ST, SCT, EPT>>>),
    BlockSyncRequest(BlockSyncRequestMessage),
    BlockSyncResponse(BlockSyncResponseMessage<ST, SCT, EPT>),
    ForwardedTx(Vec<Bytes>),
    StateSyncMessage(StateSyncNetworkMessage),
}

impl<ST, SCT, EPT> From<Verified<ST, Validated<ConsensusMessage<ST, SCT, EPT>>>>
    for VerifiedMonadMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn from(value: Verified<ST, Validated<ConsensusMessage<ST, SCT, EPT>>>) -> Self {
        Self::Consensus(value)
    }
}

impl<ST, SCT, EPT> Encodable for VerifiedMonadMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        let monad_version = MonadVersion::version();

        match self {
            Self::Consensus(m) => {
                let wire: Unverified<ST, Unvalidated<ConsensusMessage<ST, SCT, EPT>>> =
                    m.clone().into();
                let enc: [&dyn Encodable; 3] = [&monad_version, &1u8, &wire];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::BlockSyncRequest(m) => {
                let enc: [&dyn Encodable; 3] = [&monad_version, &2u8, &m];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::BlockSyncResponse(m) => {
                let enc: [&dyn Encodable; 3] = [&monad_version, &3u8, &m];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::ForwardedTx(m) => {
                let enc: [&dyn Encodable; 3] = [&monad_version, &4u8, &m];
                // TODO does tx bytes need a prefix?
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::StateSyncMessage(m) => {
                let enc: [&dyn Encodable; 3] = [&monad_version, &5u8, &m];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }

    fn length(&self) -> usize {
        let monad_version = MonadVersion::version();

        match self {
            Self::Consensus(m) => {
                let wire: Unverified<ST, Unvalidated<ConsensusMessage<ST, SCT, EPT>>> =
                    m.clone().into();
                let enc: Vec<&dyn Encodable> = vec![&monad_version, &1u8, &wire];
                Encodable::length(&enc)
            }
            Self::BlockSyncRequest(m) => {
                let enc: Vec<&dyn Encodable> = vec![&monad_version, &2u8, &m];
                Encodable::length(&enc)
            }
            Self::BlockSyncResponse(m) => {
                let enc: Vec<&dyn Encodable> = vec![&monad_version, &3u8, &m];
                Encodable::length(&enc)
            }
            Self::ForwardedTx(m) => {
                let enc: Vec<&dyn Encodable> = vec![&monad_version, &4u8, &m];
                // TODO does tx bytes need a prefix?
                Encodable::length(&enc)
            }
            Self::StateSyncMessage(m) => {
                let enc: Vec<&dyn Encodable> = vec![&monad_version, &5u8, &m];
                Encodable::length(&enc)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MonadMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    /// Consensus protocol message
    Consensus(Unverified<ST, Unvalidated<ConsensusMessage<ST, SCT, EPT>>>),

    /// Request a missing block given BlockId
    BlockSyncRequest(BlockSyncRequestMessage),

    /// Block sync response
    BlockSyncResponse(BlockSyncResponseMessage<ST, SCT, EPT>),

    /// Forwarded transactions
    ForwardedTx(Vec<Bytes>),
    /// State Sync msgs
    StateSyncMessage(StateSyncNetworkMessage),
}

impl<ST, SCT, EPT> Decodable for MonadMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let monad_version = MonadVersion::decode(&mut payload)?;

        match u8::decode(&mut payload)? {
            1 => Ok(Self::Consensus(Unverified::<
                ST,
                Unvalidated<ConsensusMessage<ST, SCT, EPT>>,
            >::decode(&mut payload)?)),
            2 => Ok(Self::BlockSyncRequest(BlockSyncRequestMessage::decode(
                &mut payload,
            )?)),
            3 => Ok(Self::BlockSyncResponse(BlockSyncResponseMessage::decode(
                &mut payload,
            )?)),
            4 => Ok(Self::ForwardedTx(Vec::<Bytes>::decode(&mut payload)?)),
            5 => Ok(Self::StateSyncMessage(StateSyncNetworkMessage::decode(
                &mut payload,
            )?)),
            _ => Err(alloy_rlp::Error::Custom(
                "failed to decode unknown MonadMessage",
            )),
        }
    }
}

impl<ST, SCT, EPT> monad_types::Serializable<Bytes> for VerifiedMonadMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn serialize(&self) -> Bytes {
        rlp_serialize_verified_monad_message(self)
    }
}

fn rlp_serialize_verified_monad_message<ST, SCT, EPT>(
    msg: &VerifiedMonadMessage<ST, SCT, EPT>,
) -> Bytes
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    let mut _encode_span = tracing::trace_span!("encode_span").entered();
    let mut buf = BytesMut::new();
    msg.encode(&mut buf);
    buf.into()
}

impl<ST, SCT, EPT> monad_types::Deserializable<Bytes> for MonadMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type ReadError = alloy_rlp::Error;

    fn deserialize(message: &Bytes) -> Result<Self, Self::ReadError> {
        rlp_deserialize_monad_message(message.clone())
    }
}

fn rlp_deserialize_monad_message<ST, SCT, EPT>(
    data: Bytes,
) -> Result<MonadMessage<ST, SCT, EPT>, alloy_rlp::Error>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    let message_len = data.len();
    let mut _decode_span = tracing::trace_span!("decode_span", ?message_len).entered();

    MonadMessage::<ST, SCT, EPT>::decode(&mut data.as_ref())
}

impl<ST, SCT, EPT> From<VerifiedMonadMessage<ST, SCT, EPT>> for MonadMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn from(value: VerifiedMonadMessage<ST, SCT, EPT>) -> Self {
        match value {
            VerifiedMonadMessage::Consensus(msg) => MonadMessage::Consensus(msg.into()),
            VerifiedMonadMessage::BlockSyncRequest(msg) => MonadMessage::BlockSyncRequest(msg),
            VerifiedMonadMessage::BlockSyncResponse(msg) => MonadMessage::BlockSyncResponse(msg),
            VerifiedMonadMessage::ForwardedTx(msg) => MonadMessage::ForwardedTx(msg),
            VerifiedMonadMessage::StateSyncMessage(msg) => MonadMessage::StateSyncMessage(msg),
        }
    }
}

impl<ST, SCT, EPT> Message for MonadMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type NodeIdPubKey = CertificateSignaturePubKey<ST>;
    type Event = MonadEvent<ST, SCT, EPT>;

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
            MonadMessage::ForwardedTx(msg) => {
                MonadEvent::MempoolEvent(MempoolEvent::ForwardedTxs {
                    sender: from,
                    txs: msg,
                })
            }
            MonadMessage::StateSyncMessage(msg) => {
                MonadEvent::StateSyncEvent(StateSyncEvent::Inbound(from, msg))
            }
        }
    }
}

pub struct MonadStateBuilder<ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BVT: BlockValidator<ST, SCT, EPT, BPT, SBT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    pub validator_set_factory: VTF,
    pub leader_election: LT,
    pub block_validator: BVT,
    pub block_policy: BPT,
    pub state_backend: SBT,
    pub forkpoint: Forkpoint<SCT>,
    pub locked_epoch_validators: Vec<ValidatorSetDataWithEpoch<SCT>>,
    pub key: ST::KeyPairType,
    pub certkey: SignatureCollectionKeyPairType<SCT>,
    pub val_set_update_interval: SeqNum,
    pub epoch_start_delay: Round,
    pub beneficiary: [u8; 20],
    pub block_sync_override_peers: Vec<NodeId<SCT::NodeIdPubKey>>,

    pub consensus_config: ConsensusConfig<CCT, CRT>,

    pub _phantom: PhantomData<EPT>,
}

impl<ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT>
    MonadStateBuilder<ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BVT: BlockValidator<ST, SCT, EPT, BPT, SBT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    pub fn build(
        self,
    ) -> (
        MonadState<ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT>,
        Vec<
            Command<
                MonadEvent<ST, SCT, EPT>,
                OutboundRouterMessage<VerifiedMonadMessage<ST, SCT, EPT>, ST>,
                ST,
                SCT,
                EPT,
                BPT,
                SBT,
            >,
        >,
    ) {
        assert_eq!(
            self.forkpoint
                .validate(&self.validator_set_factory, &self.locked_epoch_validators),
            Ok(())
        );

        let val_epoch_map = ValidatorsEpochMapping::new(self.validator_set_factory);

        let epoch_manager = EpochManager::new(
            self.val_set_update_interval,
            self.epoch_start_delay,
            &self.forkpoint.get_epoch_starts(),
        );

        let nodeid = NodeId::new(self.key.pubkey());
        let block_timestamp = BlockTimestamp::new(
            5 * self.consensus_config.delta.as_nanos(),
            self.consensus_config.timestamp_latency_estimate_ns,
        );
        let statesync_to_live_threshold = self.consensus_config.statesync_to_live_threshold;
        let mut monad_state = MonadState {
            keypair: self.key,
            cert_keypair: self.certkey,
            nodeid,

            consensus_config: self.consensus_config,
            consensus: ConsensusMode::start_sync(
                self.forkpoint.high_qc.clone(),
                BlockBuffer::new(
                    self.consensus_config.execution_delay,
                    self.forkpoint.root,
                    statesync_to_live_threshold,
                ),
            ),
            block_sync: BlockSync::new(self.block_sync_override_peers),

            leader_election: self.leader_election,
            epoch_manager,
            val_epoch_map,

            block_timestamp,
            block_validator: self.block_validator,
            block_policy: self.block_policy,
            state_backend: self.state_backend,
            beneficiary: self.beneficiary,

            metrics: Metrics::default(),
            version: MonadVersion::version(),
        };

        let mut init_cmds = Vec::new();

        let Forkpoint(Checkpoint {
            root,
            high_qc,
            validator_sets: _,
        }) = self.forkpoint;

        for vset in self.locked_epoch_validators {
            init_cmds.extend(monad_state.update(MonadEvent::ValidatorEvent(
                ValidatorEvent::UpdateValidators(vset),
            )));
        }

        tracing::info!(?root, ?high_qc, "starting up, syncing");
        init_cmds.extend(monad_state.maybe_start_consensus());

        (monad_state, init_cmds)
    }
}

impl<ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT>
    MonadState<ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BVT: BlockValidator<ST, SCT, EPT, BPT, SBT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    pub fn update(
        &mut self,
        event: MonadEvent<ST, SCT, EPT>,
    ) -> Vec<
        Command<
            MonadEvent<ST, SCT, EPT>,
            OutboundRouterMessage<VerifiedMonadMessage<ST, SCT, EPT>, ST>,
            ST,
            SCT,
            EPT,
            BPT,
            SBT,
        >,
    > {
        let _event_span = tracing::debug_span!("event_span", ?event).entered();

        match event {
            MonadEvent::ConsensusEvent(consensus_event) => {
                let consensus_cmds = ConsensusChildState::new(self).update(consensus_event);

                consensus_cmds
                    .into_iter()
                    .flat_map(Into::<Vec<Command<_, _, _, _, _, _, _>>>::into)
                    .collect::<Vec<_>>()
            }

            MonadEvent::BlockSyncEvent(block_sync_event) => {
                let block_sync_cmds = BlockSyncChildState::new(self).update(block_sync_event);

                block_sync_cmds
                    .into_iter()
                    .flat_map(Into::<Vec<Command<_, _, _, _, _, _, _>>>::into)
                    .collect::<Vec<_>>()
            }

            MonadEvent::ValidatorEvent(validator_event) => {
                let validator_cmds = EpochChildState::new(self).update(validator_event);

                validator_cmds
                    .into_iter()
                    .flat_map(Into::<Vec<Command<_, _, _, _, _, _, _>>>::into)
                    .collect::<Vec<_>>()
            }

            MonadEvent::MempoolEvent(event) => {
                // TODO(andr-dev): Don't allow ConsensusChildState to produce Command<...> directly (requires IPC->TxPool refactor)
                ConsensusChildState::new(self).handle_mempool_event(event)
            }
            MonadEvent::ExecutionResultEvent(event) => {
                self.metrics.consensus_events.state_root_update += 1;
                let consensus_cmds = ConsensusChildState::new(self).handle_execution_result(event);
                consensus_cmds
                    .into_iter()
                    .flat_map(Into::<Vec<Command<_, _, _, _, _, _, _>>>::into)
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
                StateSyncEvent::Outbound(to, message, completion) => {
                    vec![Command::RouterCommand(RouterCommand::Publish {
                        target: RouterTarget::TcpPointToPoint { to, completion },
                        message: OutboundRouterMessage::AppMessage(
                            VerifiedMonadMessage::StateSyncMessage(message),
                        ),
                    })]
                }
                StateSyncEvent::RequestSync {
                    root: new_root,
                    high_qc: new_high_qc,
                } => {
                    let ConsensusMode::Sync {
                        high_qc,
                        block_buffer,
                        db_status,
                        updating_target,
                    } = &mut self.consensus
                    else {
                        unreachable!("Live -> RequestSync is an invalid state transition")
                    };

                    *high_qc = new_high_qc;
                    block_buffer.re_root(new_root);
                    *db_status = DbSyncStatus::Waiting;
                    *updating_target = false;

                    self.maybe_start_consensus()
                }
                StateSyncEvent::DoneSync(n) => {
                    let ConsensusMode::Sync {
                        db_status,
                        block_buffer,
                        ..
                    } = &mut self.consensus
                    else {
                        unreachable!("DoneSync invoked while ConsensusState is live")
                    };
                    assert_eq!(db_status, &DbSyncStatus::Started);

                    let delay = self.consensus_config.execution_delay;
                    let maybe_target = block_buffer
                        .root_seq_num()
                        .map(|root| root.max(delay) - delay);
                    match maybe_target {
                        Some(target) if n >= target => {
                            assert_eq!(n, target);
                            assert!(
                                self.state_backend
                                    .raw_read_earliest_finalized_block()
                                    .expect("earliest_finalized doesn't exist")
                                    <= n
                            );
                            assert!(
                                self.state_backend
                                    .raw_read_latest_finalized_block()
                                    .expect("latest_finalized doesn't exist")
                                    >= n
                            );

                            tracing::info!(?n, "done db statesync");
                            *db_status = DbSyncStatus::Done;

                            self.maybe_start_consensus()
                        }
                        _ => {
                            tracing::debug!(?n, ?maybe_target, "dropping DoneSync, n < target");
                            Vec::new()
                        }
                    }
                }
                StateSyncEvent::BlockSync {
                    block_range,
                    full_blocks,
                } => {
                    let ConsensusMode::Sync { block_buffer, .. } = &mut self.consensus else {
                        return Vec::new();
                    };

                    let mut commands = Vec::new();

                    for full_block in full_blocks {
                        block_buffer.handle_blocksync(full_block);
                    }

                    commands.extend(self.maybe_start_consensus());
                    commands
                }
            },
            MonadEvent::ControlPanelEvent(control_panel_event) => match control_panel_event {
                ControlPanelEvent::GetMetricsEvent => {
                    vec![Command::ControlPanelCommand(ControlPanelCommand::Read(
                        ReadCommand::GetMetrics(GetMetrics::Response(self.metrics)),
                    ))]
                }
                ControlPanelEvent::ClearMetricsEvent => {
                    self.metrics = Default::default();
                    vec![Command::ControlPanelCommand(ControlPanelCommand::Write(
                        WriteCommand::ClearMetrics(ClearMetrics::Response(self.metrics)),
                    ))]
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
                ControlPanelEvent::ReloadConfig(req_resp) => match req_resp {
                    ReloadConfig::Request => {
                        vec![Command::ConfigReloadCommand(
                            ConfigReloadCommand::ReloadConfig,
                        )]
                    }
                    ReloadConfig::Response(resp) => {
                        vec![Command::ControlPanelCommand(ControlPanelCommand::Write(
                            WriteCommand::ReloadConfig(ReloadConfig::Response(resp)),
                        ))]
                    }
                },
            },
            MonadEvent::TimestampUpdateEvent(t) => {
                self.block_timestamp.update_time(t);
                vec![]
            }
            MonadEvent::ConfigEvent(config_event) => match config_event {
                ConfigEvent::ConfigUpdate(config_update) => {
                    self.block_sync
                        .set_override_peers(config_update.blocksync_override_peers);
                    let mut cmds = Vec::new();
                    cmds.push(Command::RouterCommand(RouterCommand::UpdateFullNodes(
                        config_update.full_nodes,
                    )));

                    cmds.push(Command::ControlPanelCommand(ControlPanelCommand::Write(
                        WriteCommand::ReloadConfig(ReloadConfig::Response("Success".to_string())),
                    )));

                    cmds
                }
                ConfigEvent::LoadError(err_msg) => {
                    vec![Command::ControlPanelCommand(ControlPanelCommand::Write(
                        WriteCommand::ReloadConfig(ReloadConfig::Response(err_msg)),
                    ))]
                }
                ConfigEvent::KnownPeersUpdate(known_peers_update) => {
                    vec![Command::RouterCommand(RouterCommand::UpdatePeers(
                        known_peers_update.known_peers,
                    ))]
                }
            },
        }
    }

    fn maybe_start_consensus(
        &mut self,
    ) -> Vec<
        Command<
            MonadEvent<ST, SCT, EPT>,
            OutboundRouterMessage<VerifiedMonadMessage<ST, SCT, EPT>, ST>,
            ST,
            SCT,
            EPT,
            BPT,
            SBT,
        >,
    > {
        let ConsensusMode::Sync {
            high_qc,
            block_buffer,
            db_status,
            updating_target: _,
        } = &mut self.consensus
        else {
            unreachable!("maybe_start_consensus invoked while ConsensusState is live")
        };

        let root_parent_chain = block_buffer.root_parent_chain();
        // check:
        // 1. earliest_block is early enough to start consensus
        // 2. db_status == Done

        // 1. committed-block-sync
        if let Some(block_range) = block_buffer.needs_blocksync() {
            tracing::info!(
                ?db_status,
                earliest_block =? root_parent_chain.last().map(|block| block.get_seq_num()),
                root_seq_num =? block_buffer.root_seq_num(),
                "still syncing..."
            );
            return self.update(MonadEvent::BlockSyncEvent(BlockSyncEvent::SelfRequest {
                requester: BlockSyncSelfRequester::StateSync,
                block_range,
            }));
        }

        let root_info = block_buffer
            .root_info()
            .expect("blocksync done, root block should be known");
        let root_seq_num = root_info.seq_num;

        if db_status == &DbSyncStatus::Waiting {
            *db_status = DbSyncStatus::Started;
            let delay = self.consensus_config.execution_delay;
            let state_root_seq_num = root_seq_num.max(delay) - delay;

            let latest_block = self.state_backend.raw_read_latest_finalized_block();
            assert!(
                latest_block.unwrap_or(SeqNum(0)) <= root_seq_num,
                "tried to statesync backwards: {latest_block:?} <= {root_seq_num:?}"
            );

            if latest_block.is_none()
                || latest_block.is_some_and(|latest_block| latest_block < state_root_seq_num)
            {
                let delayed_execution_result = block_buffer
                    .root_delayed_execution_result()
                    .expect("is DB state empty? load genesis.json file if so");
                assert_eq!(
                    delayed_execution_result.len(),
                    1,
                    "always 1 execution result after first k-1 blocks for now"
                );
                return vec![
                    Command::LedgerCommand(LedgerCommand::LedgerClearWal),
                    Command::StateSyncCommand(StateSyncCommand::RequestSync(
                        delayed_execution_result
                            .first()
                            .expect("asserted 1 execution result")
                            .clone(),
                    )),
                ];
            } else {
                // if latest_block > state_root_seq_num, we can't RequestSync because we
                // would be trying to sync backwards.

                assert!(
                    self.state_backend
                        .raw_read_earliest_finalized_block()
                        .expect("latest_finalized_block exists")
                        <= state_root_seq_num
                );
                // TODO assert state root matches?
                return self.update(MonadEvent::StateSyncEvent(StateSyncEvent::DoneSync(
                    state_root_seq_num,
                )));
            }
        } else if db_status == &DbSyncStatus::Started {
            tracing::info!(
                ?db_status,
                earliest_block =? root_parent_chain.last().map(|block| block.get_seq_num()),
                ?root_seq_num,
                "still syncing..."
            );
            return Vec::new();
        }

        assert_eq!(db_status, &DbSyncStatus::Done);
        let mut commands = Vec::new();

        let delay = self.consensus_config.execution_delay;
        let last_delay_committed_blocks: Vec<_> = root_parent_chain
            .iter()
            .map(|full_block| {
                let ChainParams {
                    tx_limit,
                    proposal_gas_limit,
                    proposal_byte_limit,
                    vote_pace: _,
                } = self
                    .consensus_config
                    .chain_config
                    .get_chain_revision(full_block.header().round)
                    .chain_params();
                let ExecutionChainParams { max_code_size } = {
                    // u64::MAX seconds is ~500 Billion years
                    let timestamp_s: u64 = (full_block.header().timestamp_ns / 1_000_000_000)
                        .try_into()
                        .unwrap();
                    self.consensus_config
                        .chain_config
                        .get_execution_chain_revision(timestamp_s)
                        .execution_chain_params()
                };
                self.block_validator
                    .validate(
                        full_block.header().clone(),
                        full_block.body().clone(),
                        // we don't need to validate bls pubkey fields (randao)
                        // this is because these blocks are already committed by majority
                        None,
                        *tx_limit,
                        *proposal_gas_limit,
                        *proposal_byte_limit,
                        *max_code_size,
                    )
                    .expect("majority committed invalid block")
            })
            .take(delay.0 as usize)
            .rev()
            .collect();

        // reset block_policy and txpool
        self.block_policy
            .reset(last_delay_committed_blocks.iter().collect());
        commands.push(Command::TxPoolCommand(TxPoolCommand::Reset {
            last_delay_committed_blocks: last_delay_committed_blocks.clone(),
        }));

        // commit blocks
        for block in last_delay_committed_blocks {
            commands.push(Command::LedgerCommand(LedgerCommand::LedgerCommit(
                OptimisticCommit::Proposed(block.deref().to_owned()),
            )));
            commands.push(Command::LedgerCommand(LedgerCommand::LedgerCommit(
                OptimisticCommit::Finalized(block.deref().to_owned()),
            )));
            commands.push(Command::StateRootHashCommand(
                StateRootHashCommand::RequestFinalized(block.get_seq_num()),
            ));
        }

        // this is necessary for genesis, because we'll never request root otherwise
        commands.push(Command::StateRootHashCommand(
            StateRootHashCommand::RequestFinalized(root_info.seq_num),
        ));

        let first_root_to_request = (root_info.seq_num + SeqNum(1)).max(delay) - delay;
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
            root_info,
            high_qc.clone(),
        );
        tracing::info!(?root_info, ?high_qc, "done syncing, initializing consensus");
        self.consensus = ConsensusMode::Live(consensus);
        commands.push(Command::StateSyncCommand(StateSyncCommand::StartExecution));
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
                    .flat_map(Into::<Vec<Command<_, _, _, _, _, _, _>>>::into),
            );
        }
        commands
    }
}

#[cfg(test)]
mod test {
    use monad_bls::BlsSignatureCollection;
    use monad_consensus_types::{
        quorum_certificate::QuorumCertificate,
        signature_collection::SignatureCollection,
        validator_data::{ValidatorData, ValidatorSetData, ValidatorsConfig},
        voting::Vote,
    };
    use monad_crypto::certificate_signature::CertificateSignaturePubKey;
    use monad_eth_types::EthExecutionProtocol;
    use monad_secp::SecpSignature;
    use monad_testutil::validators::create_keys_w_validators;
    use monad_types::{BlockId, Hash, NodeId, Round, SeqNum, Stake};
    use monad_validator::validator_set::ValidatorSetFactory;

    use super::*;

    type SignatureType = SecpSignature;
    type SignatureCollectionType =
        BlsSignatureCollection<CertificateSignaturePubKey<SignatureType>>;
    type ExecutionProtocolType = EthExecutionProtocol;

    const EPOCH_LENGTH: SeqNum = SeqNum(1000);
    const STATE_ROOT_DELAY: SeqNum = SeqNum(10);

    fn get_forkpoint() -> (
        Forkpoint<SignatureCollectionType>,
        Vec<ValidatorSetDataWithEpoch<SignatureCollectionType>>,
    ) {
        let (keys, cert_keys, _valset, valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(4, ValidatorSetFactory::default());

        let vote = Vote {
            id: BlockId(Hash([0x06_u8; 32])),
            epoch: Epoch(3),
            round: Round(4030),
            parent_id: BlockId(Hash([0x06_u8; 32])),
            parent_round: Round(4027),
        };
        let qc_seq_num = SeqNum(2998); // one block before boundary block

        let encoded_vote = alloy_rlp::encode(vote);

        let mut sigs = Vec::new();

        for (key, cert_key) in keys.iter().zip(cert_keys.iter()) {
            let node_id = NodeId::new(key.pubkey());
            let sig = cert_key.sign(encoded_vote.as_ref());
            sigs.push((node_id, sig));
        }

        let sigcol: BlsSignatureCollection<monad_secp::PubKey> =
            SignatureCollectionType::new(sigs, &valmap, encoded_vote.as_ref()).unwrap();

        let qc = QuorumCertificate::new(vote, sigcol);

        let forkpoint: Forkpoint<BlsSignatureCollection<monad_secp::PubKey>> = Checkpoint {
            root: qc.get_block_id(),
            high_qc: qc,
            validator_sets: vec![
                LockedEpoch {
                    epoch: Epoch(3),
                    round: Some(Round(3050)),
                },
                LockedEpoch {
                    epoch: Epoch(4),
                    round: None,
                },
            ],
        }
        .into();

        let mut validators = Vec::new();
        for (key, cert_key) in keys.iter().zip(cert_keys.iter()) {
            validators.push((key.pubkey(), Stake(7), cert_key.pubkey()));
        }
        let validator_data = ValidatorSetData::<SignatureCollectionType>::new(validators);
        let validator_sets = forkpoint
            .validator_sets
            .iter()
            .map(|vset| ValidatorSetDataWithEpoch {
                epoch: vset.epoch,
                validators: validator_data.clone(),
            })
            .collect();

        (forkpoint, validator_sets)
    }

    #[test]
    fn test_forkpoint_serde() {
        let (forkpoint, locked_validator_sets) = get_forkpoint();
        assert!(forkpoint
            .validate(&ValidatorSetFactory::default(), &locked_validator_sets)
            .is_ok());
        let ser = toml::to_string_pretty(&forkpoint.0).unwrap();

        println!("{}", ser);

        let deser = toml::from_str(&ser).unwrap();
        assert_eq!(forkpoint.0, deser);
    }

    #[test]
    fn test_forkpoint_validate_1() {
        let (mut forkpoint, mut locked_validator_sets) = get_forkpoint();
        let popped_1 = forkpoint.0.validator_sets.pop().unwrap();
        let popped_2 = locked_validator_sets.pop().unwrap();

        assert_eq!(
            forkpoint.validate(&ValidatorSetFactory::default(), &locked_validator_sets),
            Err(ForkpointValidationError::TooFewValidatorSets)
        );

        forkpoint.0.validator_sets.push(popped_1.clone());
        forkpoint.0.validator_sets.push(popped_1.clone());
        forkpoint.0.validator_sets.push(popped_1);
        locked_validator_sets.push(popped_2.clone());
        locked_validator_sets.push(popped_2.clone());
        locked_validator_sets.push(popped_2);
        assert_eq!(
            forkpoint.validate(&ValidatorSetFactory::default(), &locked_validator_sets),
            Err(ForkpointValidationError::TooManyValidatorSets)
        );
    }

    #[test]
    fn test_forkpoint_validate_2() {
        let (mut forkpoint, locked_validator_sets) = get_forkpoint();
        forkpoint.0.validator_sets[0].epoch.0 -= 1;

        assert_eq!(
            forkpoint.validate(&ValidatorSetFactory::default(), &locked_validator_sets),
            Err(ForkpointValidationError::ValidatorSetsNotConsecutive)
        );
    }

    #[test]
    fn test_forkpoint_validate_4() {
        let (mut forkpoint, locked_validator_sets) = get_forkpoint();

        forkpoint.0.validator_sets[0].round = None;

        assert_eq!(
            forkpoint.validate(&ValidatorSetFactory::default(), &locked_validator_sets),
            Err(ForkpointValidationError::InvalidValidatorSetStartRound)
        );
    }

    // TODO test every branch of 5
    // the mock-swarm forkpoint tests sort of cover these, but we should unit-test these eventually
    // for completeness

    #[test]
    fn test_forkpoint_validate_6() {
        let (mut forkpoint, locked_validator_sets) = get_forkpoint();
        // change qc content so signature collection is invalid
        forkpoint.0.high_qc.info.round = forkpoint.0.high_qc.get_round() - Round(1);

        assert_eq!(
            forkpoint.validate(&ValidatorSetFactory::default(), &locked_validator_sets),
            Err(ForkpointValidationError::InvalidQC)
        );
    }

    #[test]
    fn test_validators_config() {
        let (keys, cert_keys, _valset, _valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(1, ValidatorSetFactory::default());

        let make_val_set_data = |stake: Stake| {
            ValidatorSetData(vec![ValidatorData {
                node_id: NodeId::new(keys[0].pubkey()),
                cert_pubkey: cert_keys[0].pubkey(),
                stake,
            }])
        };

        let validators_config: ValidatorsConfig<SignatureCollectionType> = ValidatorsConfig {
            validators: vec![
                (Epoch(1), make_val_set_data(Stake(1))),
                (Epoch(2), make_val_set_data(Stake(2))),
                (Epoch(4), make_val_set_data(Stake(3))),
                (Epoch(10), make_val_set_data(Stake(4))),
            ]
            .into_iter()
            .collect(),
        };

        let expected = vec![
            (Epoch(1), make_val_set_data(Stake(1))),
            (Epoch(2), make_val_set_data(Stake(2))),
            (Epoch(3), make_val_set_data(Stake(2))),
            (Epoch(4), make_val_set_data(Stake(3))),
            (Epoch(5), make_val_set_data(Stake(3))),
            (Epoch(6), make_val_set_data(Stake(3))),
            (Epoch(7), make_val_set_data(Stake(3))),
            (Epoch(8), make_val_set_data(Stake(3))),
            (Epoch(9), make_val_set_data(Stake(3))),
            (Epoch(10), make_val_set_data(Stake(4))),
            (Epoch(11), make_val_set_data(Stake(4))),
            (Epoch(12), make_val_set_data(Stake(4))),
        ];

        for (epoch, val_set) in &expected {
            assert_eq!(val_set, validators_config.get_validator_set(epoch))
        }
    }

    // Confirm that version values greather than 2^16 for version fields don't cause deser issue
    // and are ignored correctly.
    #[test]
    fn monad_message_encoding_version_test() {
        // 0xcb -> 11 bytes
        // 0xc8 -> list of 8 bytes for version
        // [0x83, 0x01, 0xff, 0xff] -> 131071 in decimal, larger than 2^16 limit of version field
        let rlp_encoded_monad_message = vec![
            0xcb, 0xc8, 0x01, 0x80, 0x01, 0x01, 0x83, 0x01, 0xff, 0xff, 0x05, 0xc0,
        ];

        let decoded = alloy_rlp::decode_exact::<
            MonadMessage<SignatureType, SignatureCollectionType, ExecutionProtocolType>,
        >(rlp_encoded_monad_message);

        assert!(decoded.is_err());
    }

    /*
    #[test]
    fn monad_message_encoding_sanity_test() {
        let verified_message =
            VerifiedMonadMessage::<SignatureType, SignatureCollectionType>::ForwardedTx(vec![
                Bytes::from_static(&[1, 2, 3]),
            ]);
        let bytes: Bytes = verified_message.serialize();

        let message = MonadMessage::<SignatureType, SignatureCollectionType>::deserialize(&bytes)
            .expect("failed to deserialize");

        todo!("assert bytes equal");
    }
    */
}
