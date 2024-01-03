pub mod convert;

use std::fmt::Debug;

use bytes::Bytes;
use monad_consensus::{
    messages::{
        consensus_message::ConsensusMessage,
        message::{BlockSyncResponseMessage, RequestBlockSyncMessage},
    },
    validation::signing::{Unvalidated, Unverified},
};
use monad_consensus_types::{
    block::{Block, UnverifiedBlock},
    message_signature::MessageSignature,
    signature_collection::SignatureCollection,
    validator_data::ValidatorData,
};
use monad_crypto::{hasher::Hash as ConsensusHash, secp256k1::PubKey};
use monad_types::{BlockId, Epoch, NodeId, RouterTarget, SeqNum, TimeoutVariant};

#[derive(Clone)]
pub enum RouterCommand<OM> {
    // Publish should not be replayed
    Publish { target: RouterTarget, message: OM },
    // TODO-2 add a RouterCommand for setting peer set for broadcast
}

pub trait Message: Clone {
    type Event;

    // TODO-3 NodeId -> &NodeId
    fn event(self, from: NodeId) -> Self::Event;
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

pub enum LedgerCommand<B, E> {
    LedgerCommit(Vec<B>),
    // LedgerFetch should not be replayed
    LedgerFetch(
        NodeId,
        BlockId,
        Box<dyn (FnOnce(Option<B>) -> E) + Send + Sync>,
    ),
}

pub enum ExecutionLedgerCommand<SCT> {
    LedgerCommit(Vec<Block<SCT>>),
}

pub enum CheckpointCommand<C> {
    Save(C),
}

pub enum StateRootHashCommand<B> {
    LedgerCommit(B),
}

pub enum Command<E, OM, B, C, SCT> {
    RouterCommand(RouterCommand<OM>),
    TimerCommand(TimerCommand<E>),

    LedgerCommand(LedgerCommand<B, E>),
    ExecutionLedgerCommand(ExecutionLedgerCommand<SCT>),
    CheckpointCommand(CheckpointCommand<C>),
    StateRootHashCommand(StateRootHashCommand<B>),
}

impl<E, OM, B, C, SCT> Command<E, OM, B, C, SCT> {
    pub fn split_commands(
        commands: Vec<Self>,
    ) -> (
        Vec<RouterCommand<OM>>,
        Vec<TimerCommand<E>>,
        Vec<LedgerCommand<B, E>>,
        Vec<ExecutionLedgerCommand<SCT>>,
        Vec<CheckpointCommand<C>>,
        Vec<StateRootHashCommand<B>>,
    ) {
        let mut router_cmds = Vec::new();
        let mut timer_cmds = Vec::new();
        let mut ledger_cmds = Vec::new();
        let mut execution_ledger_cmds = Vec::new();
        let mut checkpoint_cmds = Vec::new();
        let mut state_root_hash_cmds = Vec::new();

        for command in commands {
            match command {
                Command::RouterCommand(cmd) => router_cmds.push(cmd),
                Command::TimerCommand(cmd) => timer_cmds.push(cmd),
                Command::LedgerCommand(cmd) => ledger_cmds.push(cmd),
                Command::ExecutionLedgerCommand(cmd) => execution_ledger_cmds.push(cmd),
                Command::CheckpointCommand(cmd) => checkpoint_cmds.push(cmd),
                Command::StateRootHashCommand(cmd) => state_root_hash_cmds.push(cmd),
            }
        }
        (
            router_cmds,
            timer_cmds,
            ledger_cmds,
            execution_ledger_cmds,
            checkpoint_cmds,
            state_root_hash_cmds,
        )
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum ConsensusEvent<ST, SCT: SignatureCollection> {
    Message {
        sender: PubKey,
        unverified_message: Unverified<ST, Unvalidated<ConsensusMessage<SCT>>>,
    },
    Timeout(TimeoutVariant),
    StateUpdate((SeqNum, ConsensusHash)),
    BlockSyncResponse {
        sender: PubKey,
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
            ConsensusEvent::StateUpdate(e) => e.fmt(f),
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
pub struct FetchedBlock<SCT> {
    /// The node that requested this block
    pub requester: NodeId,

    /// id of the requested block
    pub block_id: BlockId,

    /// FetchedBlock results should only be used to send block data to nodes
    /// over the network so we should unverify it before sending to consensus
    /// to prevent it from being used for anything else
    pub unverified_block: Option<UnverifiedBlock<SCT>>,
}

/// BlockSync related events
#[derive(Clone, PartialEq, Eq)]
pub enum BlockSyncEvent<SCT: SignatureCollection> {
    /// A peer requesting for a missing block
    BlockSyncRequest {
        sender: PubKey,
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
    UpdateValidators((ValidatorData<SCT>, Epoch)),
}

/// MonadEvent are inputs to MonadState
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MonadEvent<ST, SCT>
where
    ST: MessageSignature,
    SCT: SignatureCollection,
{
    /// Events for consensus state
    ConsensusEvent(ConsensusEvent<ST, SCT>),
    /// Events for block sync responder
    BlockSyncEvent(BlockSyncEvent<SCT>),
    /// Events to update validator set
    ValidatorEvent(ValidatorEvent<SCT>),
}

impl monad_types::Deserializable<[u8]>
    for MonadEvent<
        monad_crypto::NopSignature,
        monad_consensus_types::multi_sig::MultiSig<monad_crypto::NopSignature>,
    >
{
    type ReadError = monad_proto::error::ProtoError;

    fn deserialize(data: &[u8]) -> Result<Self, Self::ReadError> {
        crate::convert::interface::deserialize_event(data)
    }
}

impl monad_types::Serializable<Bytes>
    for MonadEvent<
        monad_crypto::NopSignature,
        monad_consensus_types::multi_sig::MultiSig<monad_crypto::NopSignature>,
    >
{
    fn serialize(&self) -> Bytes {
        crate::convert::interface::serialize_event(self)
    }
}

impl monad_types::Deserializable<[u8]>
    for MonadEvent<
        monad_crypto::secp256k1::SecpSignature,
        monad_consensus_types::bls::BlsSignatureCollection,
    >
{
    type ReadError = monad_proto::error::ProtoError;

    fn deserialize(data: &[u8]) -> Result<Self, Self::ReadError> {
        crate::convert::interface::deserialize_event(data)
    }
}

impl monad_types::Serializable<Bytes>
    for MonadEvent<
        monad_crypto::secp256k1::SecpSignature,
        monad_consensus_types::bls::BlsSignatureCollection,
    >
{
    fn serialize(&self) -> Bytes {
        crate::convert::interface::serialize_event(self)
    }
}

impl monad_types::Deserializable<[u8]>
    for MonadEvent<
        monad_crypto::secp256k1::SecpSignature,
        monad_consensus_types::multi_sig::MultiSig<monad_crypto::secp256k1::SecpSignature>,
    >
{
    type ReadError = monad_proto::error::ProtoError;

    fn deserialize(data: &[u8]) -> Result<Self, Self::ReadError> {
        crate::convert::interface::deserialize_event(data)
    }
}

impl monad_types::Serializable<Bytes>
    for MonadEvent<
        monad_crypto::secp256k1::SecpSignature,
        monad_consensus_types::multi_sig::MultiSig<monad_crypto::secp256k1::SecpSignature>,
    >
{
    fn serialize(&self) -> Bytes {
        crate::convert::interface::serialize_event(self)
    }
}
