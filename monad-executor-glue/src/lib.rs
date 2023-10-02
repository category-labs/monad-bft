pub mod convert;

use std::{fmt::Debug, hash::Hash};

use monad_consensus::{
    messages::consensus_message::ConsensusMessage, validation::signing::Unverified,
};
use monad_consensus_types::{
    command::{FetchedBlock, FetchedFullTxs, FetchedTxs},
    message_signature::MessageSignature,
    payload::{FullTransactionList, TransactionList},
    signature_collection::SignatureCollection,
};
use monad_crypto::secp256k1::PubKey;
use monad_types::{BlockId, Epoch, Hash as ConsensusHash, NodeId, TimeoutVariant, ValidatorData};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PeerId(pub PubKey);

impl std::fmt::Debug for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<&NodeId> for PeerId {
    fn from(id: &NodeId) -> Self {
        PeerId(id.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RouterTarget {
    Broadcast,
    PointToPoint(PeerId),
}

pub enum RouterCommand<M, OM>
where
    M: Message,
{
    // TODO add a RouterCommand for setting peer set for broadcast
    Publish { target: RouterTarget, message: OM },
    Unpublish { target: RouterTarget, id: M::Id },
}

pub trait Message: Identifiable + Clone {
    type Event;

    // TODO PeerId -> &PeerId
    fn event(self, from: PeerId) -> Self::Event;
}

pub trait Identifiable {
    type Id: Eq + Hash + Clone;

    fn id(&self) -> Self::Id;
}

pub enum TimerCommand<E> {
    /// ScheduleReset should ALMOST ALWAYS be emitted by the state machine after handling E
    /// This is to prevent E from firing twice on replay
    // TODO create test to demonstrate faulty behavior if written improperly
    Schedule {
        duration: std::time::Duration,
        event: TimeoutVariant,
        on_timeout: Box<dyn (FnOnce(TimeoutVariant) -> E) + Send + Sync>,
    },
    ScheduleReset(TimeoutVariant),
}

pub enum MempoolCommand<E> {
    // TODO consider moving away from dynamic dispatch
    /// FetchReset should ALMOST ALWAYS be emitted by the state machine after handling E
    /// This is to prevent E from firing twice on replay
    // TODO create test to demonstrate faulty behavior if written improperly
    FetchTxs(
        usize,
        Vec<TransactionList>,
        Box<dyn (FnOnce(TransactionList) -> E) + Send + Sync>,
    ),
    FetchReset,
    FetchFullTxs(
        TransactionList,
        Box<dyn (FnOnce(Option<FullTransactionList>) -> E) + Send + Sync>,
    ),
    FetchFullReset,
    DrainTxs(Vec<TransactionList>),
}

pub enum LedgerCommand<B, E> {
    LedgerCommit(Vec<B>),
    LedgerFetch(
        NodeId,
        BlockId,
        Box<dyn (FnOnce(Option<B>) -> E) + Send + Sync>,
    ),
    LedgerFetchReset(NodeId, BlockId),
}

pub enum CheckpointCommand<C> {
    Save(C),
}

pub enum StateRootHashCommand<B, E> {
    LedgerCommit(B, Box<dyn (FnOnce(u64, ConsensusHash) -> E) + Send + Sync>),
}

pub enum Command<M, OM, B, C>
where
    M: Message,
{
    RouterCommand(RouterCommand<M, OM>),
    TimerCommand(TimerCommand<M::Event>),

    MempoolCommand(MempoolCommand<M::Event>),
    LedgerCommand(LedgerCommand<B, M::Event>),
    CheckpointCommand(CheckpointCommand<C>),
    StateRootHashCommand(StateRootHashCommand<B, M::Event>),
}

impl<M, OM, B, C> Command<M, OM, B, C>
where
    M: Message,
{
    pub fn split_commands(
        commands: Vec<Self>,
    ) -> (
        Vec<RouterCommand<M, OM>>,
        Vec<TimerCommand<M::Event>>,
        Vec<MempoolCommand<M::Event>>,
        Vec<LedgerCommand<B, M::Event>>,
        Vec<CheckpointCommand<C>>,
        Vec<StateRootHashCommand<B, M::Event>>,
    ) {
        let mut router_cmds = Vec::new();
        let mut timer_cmds = Vec::new();
        let mut mempool_cmds = Vec::new();
        let mut ledger_cmds = Vec::new();
        let mut checkpoint_cmds = Vec::new();
        let mut state_root_hash_cmds = Vec::new();
        for command in commands {
            match command {
                Command::RouterCommand(cmd) => router_cmds.push(cmd),
                Command::TimerCommand(cmd) => timer_cmds.push(cmd),
                Command::MempoolCommand(cmd) => mempool_cmds.push(cmd),
                Command::LedgerCommand(cmd) => ledger_cmds.push(cmd),
                Command::CheckpointCommand(cmd) => checkpoint_cmds.push(cmd),
                Command::StateRootHashCommand(cmd) => state_root_hash_cmds.push(cmd),
            }
        }
        (
            router_cmds,
            timer_cmds,
            mempool_cmds,
            ledger_cmds,
            checkpoint_cmds,
            state_root_hash_cmds,
        )
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum ConsensusEvent<ST, SCT: SignatureCollection> {
    Message {
        sender: PubKey,
        unverified_message: Unverified<ST, ConsensusMessage<SCT>>,
    },
    Timeout(TimeoutVariant),
    FetchedTxs(FetchedTxs<SCT>),
    FetchedFullTxs(FetchedFullTxs<SCT>),
    FetchedBlock(FetchedBlock<SCT>),
    LoadEpoch(Epoch, ValidatorData, ValidatorData),
    AdvanceEpoch(Option<ValidatorData>),
    StateUpdate((u64, ConsensusHash)),
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
            ConsensusEvent::FetchedTxs(p) => p.fmt(f),
            ConsensusEvent::FetchedFullTxs(p) => f
                .debug_struct("FetchedFullTxs")
                .field("author", &p.author)
                .field("proposal block", &p.p_block)
                .field("proposal tc", &p.p_last_round_tc)
                .field("txns", &p.txns)
                .finish(),
            ConsensusEvent::FetchedBlock(b) => f
                .debug_struct("FetchedBlock")
                .field("unverified_full_block", &b.unverified_full_block)
                .finish(),
            ConsensusEvent::LoadEpoch(e, _, _) => e.fmt(f),
            ConsensusEvent::AdvanceEpoch(e) => e.fmt(f),
            ConsensusEvent::StateUpdate(e) => e.fmt(f),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MonadEvent<ST, SCT>
where
    ST: MessageSignature,
    SCT: SignatureCollection,
{
    ConsensusEvent(ConsensusEvent<ST, SCT>),
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

impl monad_types::Serializable<Vec<u8>>
    for MonadEvent<
        monad_crypto::NopSignature,
        monad_consensus_types::multi_sig::MultiSig<monad_crypto::NopSignature>,
    >
{
    fn serialize(&self) -> Vec<u8> {
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

impl monad_types::Serializable<Vec<u8>>
    for MonadEvent<
        monad_crypto::secp256k1::SecpSignature,
        monad_consensus_types::bls::BlsSignatureCollection,
    >
{
    fn serialize(&self) -> Vec<u8> {
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

impl monad_types::Serializable<Vec<u8>>
    for MonadEvent<
        monad_crypto::secp256k1::SecpSignature,
        monad_consensus_types::multi_sig::MultiSig<monad_crypto::secp256k1::SecpSignature>,
    >
{
    fn serialize(&self) -> Vec<u8> {
        crate::convert::interface::serialize_event(self)
    }
}
