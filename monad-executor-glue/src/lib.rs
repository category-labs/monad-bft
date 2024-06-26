pub mod convert;

use std::fmt::Debug;

use bytes::{BufMut, Bytes, BytesMut};
use chrono::{DateTime, Utc};
use monad_consensus::{
    messages::{
        consensus_message::ConsensusMessage,
        message::{BlockSyncResponseMessage, PeerStateRootMessage, RequestBlockSyncMessage},
    },
    validation::signing::{Unvalidated, Unverified},
};
use monad_consensus_types::{
    block::Block, metrics::Metrics, signature_collection::SignatureCollection,
    state_root_hash::StateRootHashInfo, validator_data::ValidatorSetData,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_types::{BlockId, Epoch, NodeId, Round, RouterTarget, Stake, TimeoutVariant};

#[derive(Clone)]
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
}

pub trait Message: Clone + Send + Sync {
    type NodeIdPubKey: PubKey;
    type Event: Send + Sync;

    // TODO-3 NodeId -> &NodeId
    fn event(self, from: NodeId<Self::NodeIdPubKey>) -> Self::Event;
}

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

pub enum LedgerCommand<PT: PubKey, B, E> {
    LedgerCommit(Vec<B>),
    // LedgerFetch should not be replayed
    LedgerFetch(
        NodeId<PT>,
        BlockId,
        Box<dyn (FnOnce(Option<B>) -> E) + Send + Sync>,
    ),
}

pub enum ExecutionLedgerCommand<SCT: SignatureCollection> {
    LedgerCommit(Vec<Block<SCT>>),
}

pub enum CheckpointCommand<C> {
    Save(C),
}

pub enum StateRootHashCommand<B> {
    LedgerCommit(B),
}

pub enum LoopbackCommand<E> {
    Forward(E),
}

pub enum MetricsCommand {
    RecordMetrics(Metrics),
}

pub enum Command<E, OM, B, C, SCT: SignatureCollection> {
    RouterCommand(RouterCommand<SCT::NodeIdPubKey, OM>),
    TimerCommand(TimerCommand<E>),

    LedgerCommand(LedgerCommand<SCT::NodeIdPubKey, B, E>),
    ExecutionLedgerCommand(ExecutionLedgerCommand<SCT>),
    CheckpointCommand(CheckpointCommand<C>),
    StateRootHashCommand(StateRootHashCommand<B>),
    LoopbackCommand(LoopbackCommand<E>),
    MetricsCommand(MetricsCommand),
}

impl<E, OM, B, C, SCT: SignatureCollection> Command<E, OM, B, C, SCT> {
    pub fn split_commands(
        commands: Vec<Self>,
    ) -> (
        Vec<RouterCommand<SCT::NodeIdPubKey, OM>>,
        Vec<TimerCommand<E>>,
        Vec<LedgerCommand<SCT::NodeIdPubKey, B, E>>,
        Vec<ExecutionLedgerCommand<SCT>>,
        Vec<CheckpointCommand<C>>,
        Vec<StateRootHashCommand<B>>,
        Vec<LoopbackCommand<E>>,
        Vec<MetricsCommand>,
    ) {
        let mut router_cmds = Vec::new();
        let mut timer_cmds = Vec::new();
        let mut ledger_cmds = Vec::new();
        let mut execution_ledger_cmds = Vec::new();
        let mut checkpoint_cmds = Vec::new();
        let mut state_root_hash_cmds = Vec::new();
        let mut loopback_cmds = Vec::new();
        let mut metrics_cmds = Vec::new();

        for command in commands {
            match command {
                Command::RouterCommand(cmd) => router_cmds.push(cmd),
                Command::TimerCommand(cmd) => timer_cmds.push(cmd),
                Command::LedgerCommand(cmd) => ledger_cmds.push(cmd),
                Command::ExecutionLedgerCommand(cmd) => execution_ledger_cmds.push(cmd),
                Command::CheckpointCommand(cmd) => checkpoint_cmds.push(cmd),
                Command::StateRootHashCommand(cmd) => state_root_hash_cmds.push(cmd),
                Command::LoopbackCommand(cmd) => loopback_cmds.push(cmd),
                Command::MetricsCommand(cmd) => metrics_cmds.push(cmd),
            }
        }
        (
            router_cmds,
            timer_cmds,
            ledger_cmds,
            execution_ledger_cmds,
            checkpoint_cmds,
            state_root_hash_cmds,
            loopback_cmds,
            metrics_cmds,
        )
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum ConsensusEvent<ST, SCT: SignatureCollection> {
    Message {
        sender: NodeId<SCT::NodeIdPubKey>,
        unverified_message: Unverified<ST, Unvalidated<ConsensusMessage<SCT>>>,
    },
    Timeout(TimeoutVariant),
    BlockSyncResponse {
        sender: NodeId<SCT::NodeIdPubKey>,
        unvalidated_response: Unvalidated<BlockSyncResponseMessage<SCT>>,
    },
}

impl<S: Debug, SCT: Debug + SignatureCollection> Debug for ConsensusEvent<S, SCT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsensusEvent::Message {
                sender,
                unverified_message,
            } => f
                .debug_struct("Message")
                .field("sender", &sender)
                .field("msg", &unverified_message)
                .finish(),
            ConsensusEvent::Timeout(p) => p.fmt(f),
            ConsensusEvent::BlockSyncResponse {
                sender,
                unvalidated_response,
            } => f
                .debug_struct("BlockSyncResponse")
                .field("sender", &sender)
                .field("response", &unvalidated_response)
                .finish(),
        }
    }
}

/// FetchedBlock is a consensus block fetched from the consensus ledger. It's
/// used to respond to a block sync request
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedBlock<SCT: SignatureCollection> {
    /// The node that requested this block
    pub requester: NodeId<SCT::NodeIdPubKey>,

    /// id of the requested block
    pub block_id: BlockId,

    /// FetchedBlock results should only be used to send block data to nodes
    /// over the network so we should unverify it before sending to consensus
    /// to prevent it from being used for anything else
    pub unverified_block: Option<Block<SCT>>,
}

/// BlockSync related events
#[derive(Clone, PartialEq, Eq)]
pub enum BlockSyncEvent<SCT: SignatureCollection> {
    /// A peer requesting for a missing block
    BlockSyncRequest {
        sender: NodeId<SCT::NodeIdPubKey>,
        unvalidated_request: Unvalidated<RequestBlockSyncMessage>,
    },
    /// Fetched full block from the consensus ledger
    ///
    /// BlockSyncResponder issues a fetch to consensus ledger if the block is
    /// not found in the block tree
    FetchedBlock(FetchedBlock<SCT>),
}

impl<SCT: SignatureCollection> Debug for BlockSyncEvent<SCT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BlockSyncRequest {
                sender,
                unvalidated_request,
            } => f
                .debug_struct("BlockSyncRequest")
                .field("sender", &sender)
                .field("unvalidated_request", &unvalidated_request)
                .finish(),
            Self::FetchedBlock(fetched_block) => fetched_block.fmt(f),
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetricsEvent {
    /// Used to drive periodic collection of metrics.
    Timeout,
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
    /// Events for metrics
    MetricsEvent(MetricsEvent),
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
            MonadEvent::ConsensusEvent(ConsensusEvent::Timeout(TimeoutVariant::Pacemaker)) => {
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
                    "AsyncStateVerifyEvent::LocalStateRoot -- round:{} seqnum:{} hash:{}",
                    root.round.0,
                    root.seq_num.0,
                    root.state_root_hash.0.to_string()
                )
            }
            MonadEvent::AsyncStateVerifyEvent(_) => "ASYNCSTATEVERIFY".to_string(),
            MonadEvent::MetricsEvent(_) => "METRICS".to_string(),
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
