pub mod convert;

use std::{fmt::Debug, net::SocketAddr};

use bytes::{BufMut, Bytes, BytesMut};
use chrono::{DateTime, Utc};
use monad_blocksync::{
    blocksync::BlockSyncSelfRequester,
    messages::message::{BlockSyncRequestMessage, BlockSyncResponseMessage},
};
use monad_consensus::{
    messages::{consensus_message::ConsensusMessage, message::PeerStateRootMessage},
    validation::signing::{Unvalidated, Unverified},
};
use monad_consensus_types::{
    block::{BlockRange, FullBlock},
    checkpoint::{Checkpoint, RootInfo},
    metrics::Metrics,
    payload::PayloadId,
    quorum_certificate::{QuorumCertificate, TimestampAdjustment},
    signature_collection::SignatureCollection,
    state_root_hash::StateRootHashInfo,
    validator_data::{ValidatorSetData, ValidatorSetDataWithEpoch},
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_types::{Epoch, NodeId, Round, RouterTarget, SeqNum, Stake};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
pub enum RouterCommand<PT: PubKey, OM> {
    // Publish should not be replayed
    Publish {
        target: RouterTarget<PT>,
        message: OM,
    },
    AddEpochValidatorSet {
        epoch: Epoch,
        validator_set: Vec<(NodeId<PT>, Stake)>,
    },
    UpdateCurrentRound(Epoch, Round),
    GetPeers,
    UpdatePeers(Vec<(NodeId<PT>, SocketAddr)>),
    GetFullNodes,
    UpdateFullNodes(Vec<NodeId<PT>>),
}

pub trait Message: Clone + Send + Sync {
    type NodeIdPubKey: PubKey;
    type Event: Send + Sync;

    // TODO-3 NodeId -> &NodeId
    fn event(self, from: NodeId<Self::NodeIdPubKey>) -> Self::Event;
}

/// TimeoutVariant distinguishes the source of the timer scheduled
/// - `Pacemaker`: consensus pacemaker round timeout
/// - `BlockSync`: timeout for a specific blocksync request
#[derive(Hash, Debug, Clone, PartialEq, Eq, Copy)]
pub enum TimeoutVariant {
    Pacemaker,
    BlockSync(BlockSyncRequestMessage),
    SendVote,
}

#[derive(Debug)]
pub enum TimerCommand<E> {
    /// ScheduleReset should ALMOST ALWAYS be emitted by the state machine after handling E
    /// This is to prevent E from firing twice on replay
    // TODO-2 create test to demonstrate faulty behavior if written improperly
    Schedule {
        duration: std::time::Duration,
        variant: TimeoutVariant,
        on_timeout: E,
    },
    ScheduleReset(TimeoutVariant),
}

pub enum LedgerCommand<SCT: SignatureCollection> {
    LedgerCommit(Vec<FullBlock<SCT>>),
    LedgerFetchHeaders(BlockRange),
    LedgerFetchPayload(PayloadId),
}

impl<SCT: SignatureCollection> std::fmt::Debug for LedgerCommand<SCT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LedgerCommand::LedgerCommit(blocks) => {
                f.debug_tuple("LedgerCommit").field(blocks).finish()
            }
            LedgerCommand::LedgerFetchHeaders(block_range) => f
                .debug_tuple("LedgerFetchHeaders")
                .field(block_range)
                .finish(),
            LedgerCommand::LedgerFetchPayload(payload_id) => f
                .debug_tuple("LedgerFetchPayload")
                .field(payload_id)
                .finish(),
        }
    }
}

pub enum CheckpointCommand<SCT: SignatureCollection> {
    Save(Checkpoint<SCT>),
}

pub enum StateRootHashCommand<SCT>
where
    SCT: SignatureCollection,
{
    Request(SeqNum),
    CancelBelow(SeqNum),
    UpdateValidators((ValidatorSetData<SCT>, Epoch)),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum GetValidatorSet<SCT: SignatureCollection> {
    Request,
    #[serde(bound = "SCT: SignatureCollection")]
    Response(ValidatorSetDataWithEpoch<SCT>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum GetMetrics {
    Request,
    Response(Metrics),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum GetPeers<PT: PubKey> {
    Request,
    #[serde(bound = "PT: PubKey")]
    Response(Vec<(NodeId<PT>, SocketAddr)>),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum GetFullNodes<PT: PubKey> {
    Request,
    #[serde(bound = "PT: PubKey")]
    Response(Vec<NodeId<PT>>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ReadCommand<SCT: SignatureCollection + Clone> {
    #[serde(bound = "SCT: SignatureCollection")]
    GetValidatorSet(GetValidatorSet<SCT>),
    GetMetrics(GetMetrics),
    #[serde(bound = "SCT: SignatureCollection")]
    GetPeers(GetPeers<SCT::NodeIdPubKey>),
    #[serde(bound = "SCT: SignatureCollection")]
    GetFullNodes(GetFullNodes<SCT::NodeIdPubKey>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum UpdateValidatorSet<SCT: SignatureCollection> {
    #[serde(bound(
        deserialize = "SCT: SignatureCollection",
        serialize = "SCT: SignatureCollection",
    ))]
    Request(ValidatorSetDataWithEpoch<SCT>),
    Response,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClearMetrics {
    Request,
    Response(Metrics),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum UpdatePeers<PT: PubKey> {
    #[serde(bound = "PT: PubKey")]
    Request(Vec<(NodeId<PT>, SocketAddr)>),
    Response,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum UpdateFullNodes<PT: PubKey> {
    #[serde(bound = "PT: PubKey")]
    Request(Vec<NodeId<PT>>),
    Response,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WriteCommand<SCT: SignatureCollection> {
    #[serde(bound = "SCT: SignatureCollection")]
    UpdateValidatorSet(UpdateValidatorSet<SCT>),
    ClearMetrics(ClearMetrics),
    UpdateLogFilter(String),
    #[serde(bound = "SCT: SignatureCollection")]
    UpdatePeers(UpdatePeers<SCT::NodeIdPubKey>),
    #[serde(bound = "SCT: SignatureCollection")]
    UpdateFullNodes(UpdateFullNodes<SCT::NodeIdPubKey>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ControlPanelCommand<SCT: SignatureCollection> {
    #[serde(bound = "SCT: SignatureCollection")]
    Read(ReadCommand<SCT>),
    #[serde(bound = "SCT: SignatureCollection")]
    Write(WriteCommand<SCT>),
}

#[derive(Debug)]
pub enum LoopbackCommand<E> {
    Forward(E),
}

#[derive(Debug)]
pub enum TimestampCommand {
    AdjustDelta(TimestampAdjustment),
}

pub enum StateSyncCommand<PT: PubKey> {
    /// The *last* RequestSync(n) called is guaranteed to be followed up with DoneSync(n).
    ///
    /// Note that if RequestSync(n') is invoked before receiving DoneSync(n), it is not guaranteed
    /// that DoneSync(n) will be received - so the caller should drop any DoneSync < n'
    RequestSync(StateRootHashInfo),
    Message((NodeId<PT>, StateSyncNetworkMessage)),
    StartExecution,
}

pub enum Command<E, OM, SCT: SignatureCollection> {
    RouterCommand(RouterCommand<SCT::NodeIdPubKey, OM>),
    TimerCommand(TimerCommand<E>),

    LedgerCommand(LedgerCommand<SCT>),
    CheckpointCommand(CheckpointCommand<SCT>),
    StateRootHashCommand(StateRootHashCommand<SCT>),
    LoopbackCommand(LoopbackCommand<E>),
    ControlPanelCommand(ControlPanelCommand<SCT>),
    TimestampCommand(TimestampCommand),
    StateSyncCommand(StateSyncCommand<SCT::NodeIdPubKey>),
}

impl<E, OM, SCT: SignatureCollection> Command<E, OM, SCT> {
    pub fn split_commands(
        commands: Vec<Self>,
    ) -> (
        Vec<RouterCommand<SCT::NodeIdPubKey, OM>>,
        Vec<TimerCommand<E>>,
        Vec<LedgerCommand<SCT>>,
        Vec<CheckpointCommand<SCT>>,
        Vec<StateRootHashCommand<SCT>>,
        Vec<LoopbackCommand<E>>,
        Vec<ControlPanelCommand<SCT>>,
        Vec<TimestampCommand>,
        Vec<StateSyncCommand<SCT::NodeIdPubKey>>,
    ) {
        let mut router_cmds = Vec::new();
        let mut timer_cmds = Vec::new();
        let mut ledger_cmds = Vec::new();
        let mut checkpoint_cmds = Vec::new();
        let mut state_root_hash_cmds = Vec::new();
        let mut loopback_cmds = Vec::new();
        let mut control_panel_cmds = Vec::new();
        let mut timestamp_cmds = Vec::new();
        let mut state_sync_cmds = Vec::new();

        for command in commands {
            match command {
                Command::RouterCommand(cmd) => router_cmds.push(cmd),
                Command::TimerCommand(cmd) => timer_cmds.push(cmd),
                Command::LedgerCommand(cmd) => ledger_cmds.push(cmd),
                Command::CheckpointCommand(cmd) => checkpoint_cmds.push(cmd),
                Command::StateRootHashCommand(cmd) => state_root_hash_cmds.push(cmd),
                Command::LoopbackCommand(cmd) => loopback_cmds.push(cmd),
                Command::ControlPanelCommand(cmd) => control_panel_cmds.push(cmd),
                Command::TimestampCommand(cmd) => timestamp_cmds.push(cmd),
                Command::StateSyncCommand(cmd) => state_sync_cmds.push(cmd),
            }
        }
        (
            router_cmds,
            timer_cmds,
            ledger_cmds,
            checkpoint_cmds,
            state_root_hash_cmds,
            loopback_cmds,
            control_panel_cmds,
            timestamp_cmds,
            state_sync_cmds,
        )
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum ConsensusEvent<ST, SCT: SignatureCollection> {
    Message {
        sender: NodeId<SCT::NodeIdPubKey>,
        unverified_message: Unverified<ST, Unvalidated<ConsensusMessage<SCT>>>,
    },
    Timeout,
    /// a block that was previously requested
    /// this is an invariant
    BlockSync {
        block_range: BlockRange,
        full_blocks: Vec<FullBlock<SCT>>,
    },
    SendVote(Round),
}

impl<S: Debug, SCT: Debug + SignatureCollection> Debug for ConsensusEvent<S, SCT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsensusEvent::Message {
                sender,
                unverified_message,
            } => f
                .debug_struct("Message")
                .field("sender", sender)
                .field("msg", unverified_message)
                .finish(),
            ConsensusEvent::Timeout => f.debug_struct("Timeout").finish(),
            ConsensusEvent::BlockSync {
                block_range,
                full_blocks,
            } => f
                .debug_struct("BlockSync")
                .field("block_range", block_range)
                .field("full_blocks", full_blocks)
                .finish(),
            ConsensusEvent::SendVote(round) => {
                f.debug_struct("SendVote").field("round", round).finish()
            }
        }
    }
}

/// BlockSync related events
#[derive(Clone, PartialEq, Eq)]
pub enum BlockSyncEvent<SCT: SignatureCollection> {
    /// A peer (not self) requesting for a missing block
    Request {
        sender: NodeId<SCT::NodeIdPubKey>,
        request: BlockSyncRequestMessage,
    },
    /// Outbound request timed out
    Timeout(BlockSyncRequestMessage),
    /// self requesting for a missing block
    /// this request must be retried if necessary
    SelfRequest {
        requester: BlockSyncSelfRequester,
        block_range: BlockRange,
    },
    /// cancel request for block
    SelfCancelRequest {
        requester: BlockSyncSelfRequester,
        block_range: BlockRange,
    },
    /// A peer (not self) sending us a block
    Response {
        sender: NodeId<SCT::NodeIdPubKey>,
        response: BlockSyncResponseMessage<SCT>,
    },
    /// self sending us missing block (from ledger)
    SelfResponse {
        response: BlockSyncResponseMessage<SCT>,
    },
}

impl<SCT: SignatureCollection> Debug for BlockSyncEvent<SCT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Request { sender, request } => f
                .debug_struct("BlockSyncRequest")
                .field("sender", sender)
                .field("request", request)
                .finish(),
            Self::SelfRequest {
                requester,
                block_range,
            } => f
                .debug_struct("BlockSyncSelfRequest")
                .field("requester", requester)
                .field("block_range", block_range)
                .finish(),
            Self::SelfCancelRequest {
                requester,
                block_range,
            } => f
                .debug_struct("BlockSyncSelfCancelRequest")
                .field("requester", requester)
                .field("block_range", block_range)
                .finish(),
            Self::Response { sender, response } => f
                .debug_struct("BlockSyncResponse")
                .field("sender", sender)
                .field("response", response)
                .finish(),
            Self::SelfResponse { response } => f
                .debug_struct("BlockSyncSelfResponse")
                .field("response", response)
                .finish(),
            Self::Timeout(request) => f.debug_struct("Timeout").field("request", request).finish(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidatorEvent<SCT: SignatureCollection> {
    UpdateValidators((ValidatorSetData<SCT>, Epoch)),
}

#[derive(Clone, PartialEq, Eq)]
pub enum MempoolEvent<PT: PubKey> {
    /// Txns that are incoming via RPC (users)
    UserTxns(Vec<Bytes>),
    /// Txns that are incoming via other nodes
    ForwardedTxns {
        sender: NodeId<PT>,
        txns: Vec<Bytes>,
    },
    /// Remove transactions that were not included in proposal
    Clear,
}

impl<PT: PubKey> Debug for MempoolEvent<PT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UserTxns(txns) => f
                .debug_struct("UserTxns")
                .field(
                    "txns_len_bytes",
                    &txns.iter().map(Bytes::len).sum::<usize>(),
                )
                .finish(),
            Self::ForwardedTxns { sender, txns } => f
                .debug_struct("ForwardedTxns")
                .field("sender", &sender)
                .field(
                    "txns_len_bytes",
                    &txns.iter().map(Bytes::len).sum::<usize>(),
                )
                .finish(),
            Self::Clear => f.debug_struct("Clear").finish(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AsyncStateVerifyEvent<SCT: SignatureCollection> {
    PeerStateRoot {
        sender: NodeId<SCT::NodeIdPubKey>,
        unvalidated_message: Unvalidated<PeerStateRootMessage<SCT>>,
    },
    LocalStateRoot(StateRootHashInfo),
}

pub const SELF_STATESYNC_VERSION: StateSyncVersion = StateSyncVersion { major: 1, minor: 0 };

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct StateSyncVersion {
    major: u16,
    minor: u16,
}

impl StateSyncVersion {
    pub fn from_u32(version: u32) -> Self {
        Self {
            major: (version >> 16) as u16,
            minor: (version & 0xFFFF) as u16,
        }
    }

    pub fn to_u32(&self) -> u32 {
        (self.major as u32) << 16 | (self.minor as u32)
    }

    pub fn is_compatible(&self) -> bool {
        self.major == SELF_STATESYNC_VERSION.major
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct StateSyncRequest {
    pub version: StateSyncVersion,

    pub prefix: u64,
    pub prefix_bytes: u8,
    pub target: u64,
    pub from: u64,
    pub until: u64,
    pub old_target: u64,
}

#[derive(Clone, PartialEq, Eq, Copy)]
pub enum StateSyncUpsertType {
    Code,
    Account,
    Storage,
    AccountDelete,
    StorageDelete,
}

#[derive(Clone, PartialEq, Eq)]
pub struct StateSyncResponse {
    pub version: StateSyncVersion,
    pub nonce: u64,
    pub response_index: u32,

    pub request: StateSyncRequest,
    // consensus state must validate that this sender is "trusted"
    pub response: Vec<(StateSyncUpsertType, Vec<u8>)>,
    pub response_n: u64,
}

impl Debug for StateSyncResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateSyncResponse")
            .field("version", &self.version)
            .field("nonce", &self.nonce)
            .field("response_index", &self.response_index)
            .field("request", &self.request)
            .field("response_len", &self.response.len())
            .field("response_n", &self.response_n)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StateSyncNetworkMessage {
    Request(StateSyncRequest),   // Request for state sync from client to server
    Response(StateSyncResponse), // Partial response from server to client
    Completion,                  // Response processing completion message from client to server
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StateSyncEvent<SCT: SignatureCollection> {
    Inbound(NodeId<SCT::NodeIdPubKey>, StateSyncNetworkMessage),
    Outbound(NodeId<SCT::NodeIdPubKey>, StateSyncNetworkMessage),

    /// Execution done syncing
    DoneSync(SeqNum),

    // Statesync-requested block
    BlockSync {
        block_range: BlockRange,
        full_blocks: Vec<FullBlock<SCT>>,
    },

    /// Consensus request sync
    RequestSync {
        root: RootInfo,
        high_qc: QuorumCertificate<SCT>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlPanelEvent<SCT>
where
    SCT: SignatureCollection,
{
    GetValidatorSet,
    GetMetricsEvent,
    ClearMetricsEvent,
    UpdateValidators(ValidatorSetDataWithEpoch<SCT>),
    UpdateLogFilter(String),
    GetPeers(GetPeers<SCT::NodeIdPubKey>),
    UpdatePeers(UpdatePeers<SCT::NodeIdPubKey>),
    GetFullNodes(GetFullNodes<SCT::NodeIdPubKey>),
    UpdateFullNodes(UpdateFullNodes<SCT::NodeIdPubKey>),
}

/// MonadEvent are inputs to MonadState
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MonadEvent<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection,
{
    /// Events for consensus state
    ConsensusEvent(ConsensusEvent<ST, SCT>),
    /// Events for block sync responder
    BlockSyncEvent(BlockSyncEvent<SCT>),
    /// Events to update validator set
    ValidatorEvent(ValidatorEvent<SCT>),
    /// Events to mempool
    MempoolEvent(MempoolEvent<CertificateSignaturePubKey<ST>>),
    /// State Root updates
    StateRootEvent(StateRootHashInfo),
    /// Events to async state verification
    AsyncStateVerifyEvent(AsyncStateVerifyEvent<SCT>),
    /// Events for the debug control panel
    ControlPanelEvent(ControlPanelEvent<SCT>),
    /// Events to update the block timestamper
    TimestampUpdateEvent(u64),
    /// Events to statesync
    StateSyncEvent(StateSyncEvent<SCT>),
}

impl<ST, SCT> monad_types::Deserializable<[u8]> for MonadEvent<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type ReadError = monad_proto::error::ProtoError;

    fn deserialize(data: &[u8]) -> Result<Self, Self::ReadError> {
        crate::convert::interface::deserialize_event(data)
    }
}

impl<ST, SCT> monad_types::Serializable<Bytes> for MonadEvent<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn serialize(&self) -> Bytes {
        crate::convert::interface::serialize_event(self)
    }
}

impl<ST, SCT> std::fmt::Display for MonadEvent<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection,
{
    // TODO impl Display for each individual event instead
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s: String = match self {
            MonadEvent::ConsensusEvent(ConsensusEvent::Message {
                sender,
                unverified_message: _,
            }) => {
                format!("ConsensusEvent::Message from {sender}")
            }
            MonadEvent::ConsensusEvent(ConsensusEvent::Timeout) => {
                format!("ConsensusEvent::Timeout Pacemaker local timeout")
            }
            MonadEvent::ConsensusEvent(_) => "CONSENSUS".to_string(),
            MonadEvent::BlockSyncEvent(_) => "BLOCKSYNC".to_string(),
            MonadEvent::ValidatorEvent(_) => "VALIDATOR".to_string(),
            MonadEvent::MempoolEvent(MempoolEvent::UserTxns(txns)) => {
                format!("MempoolEvent::UserTxns -- number of txns: {}", txns.len())
            }
            MonadEvent::MempoolEvent(MempoolEvent::ForwardedTxns { sender, txns }) => {
                format!(
                    "MempoolEvent::ForwardedTxns -- from {sender} number of txns: {}",
                    txns.len()
                )
            }
            MonadEvent::MempoolEvent(MempoolEvent::Clear) => "CLEARMEMPOOL".to_string(),
            MonadEvent::StateRootEvent(_) => "STATE_ROOT".to_string(),
            MonadEvent::AsyncStateVerifyEvent(AsyncStateVerifyEvent::LocalStateRoot(root)) => {
                format!(
                    "AsyncStateVerifyEvent::LocalStateRoot -- seqnum:{} hash:{}",
                    root.seq_num.0,
                    root.state_root_hash.0.to_string()
                )
            }
            MonadEvent::AsyncStateVerifyEvent(_) => "ASYNCSTATEVERIFY".to_string(),
            MonadEvent::ControlPanelEvent(_) => "CONTROLPANELEVENT".to_string(),
            MonadEvent::TimestampUpdateEvent(t) => format!("MempoolEvent::TimestampUpdate: {t}"),
            MonadEvent::StateSyncEvent(_) => "STATESYNC".to_string(),
        };

        write!(f, "{}", s)
    }
}

/// Wrapper around MonadEvent to capture more information that is useful in logs for
/// retrospection
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogFriendlyMonadEvent<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection,
{
    pub timestamp: DateTime<Utc>,
    pub event: MonadEvent<ST, SCT>,
}

type EventHeaderType = u32;
const EVENT_HEADER_LEN: usize = std::mem::size_of::<EventHeaderType>();

impl<ST, SCT> monad_types::Deserializable<[u8]> for LogFriendlyMonadEvent<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type ReadError = monad_proto::error::ProtoError;
    fn deserialize(data: &[u8]) -> Result<Self, Self::ReadError> {
        let mut offset = 0;
        let header: [u8; 4] = data[0..EVENT_HEADER_LEN].try_into().unwrap();
        let ts_size = EventHeaderType::from_le_bytes(header) as usize;
        offset += EVENT_HEADER_LEN;

        let ts: DateTime<Utc> = bincode::deserialize(&data[offset..offset + ts_size]).unwrap();
        offset += ts_size;

        let event = crate::convert::interface::deserialize_event(&data[offset..])?;

        Ok(LogFriendlyMonadEvent {
            timestamp: ts,
            event,
        })
    }
}

impl<ST, SCT> monad_types::Serializable<Bytes> for LogFriendlyMonadEvent<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn serialize(&self) -> Bytes {
        let mut b = BytesMut::new();

        let ts = bincode::serialize(&self.timestamp).unwrap();
        let len = (ts.len() as EventHeaderType).to_le_bytes();

        b.put(&len[..]);
        b.put(&ts[..]);

        let ev = crate::convert::interface::serialize_event(&self.event);
        b.put(&ev[..]);

        b.into()
    }
}
