pub mod convert;

use std::{fmt::Debug, hash::Hash};

use monad_consensus::{
    messages::consensus_message::ConsensusMessage, validation::signing::Unverified,
};
use monad_consensus_types::{
    block::FullBlock,
    command::{FetchFullTxParams, FetchTxParams, FetchedBlock},
    message_signature::MessageSignature,
    payload::{FullTransactionList, TransactionHashList},
    signature_collection::SignatureCollection,
    validator_data::ValidatorData,
};
use monad_crypto::{hasher::Hash as ConsensusHash, secp256k1::PubKey};
use monad_types::{BlockId, NodeId, RouterTarget, SeqNum, TimeoutVariant};

#[derive(Clone)]
pub enum RouterCommand<OM> {
    // TODO-2 add a RouterCommand for setting peer set for broadcast
    Publish { target: RouterTarget, message: OM },
}

pub trait Message: Identifiable + Clone {
    type Event;

    // TODO-3 NodeId -> &NodeId
    fn event(self, from: NodeId) -> Self::Event;
}

pub trait Identifiable {
    type Id: Eq + Hash + Clone;

    fn id(&self) -> Self::Id;
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

pub enum MempoolCommand<SCT> {
    /// FetchReset should ALMOST ALWAYS be emitted by the state machine after handling E
    /// This is to prevent E from firing twice on replay
    // TODO-2 create test to demonstrate faulty behavior if written improperly
    FetchTxs(
        /// max number of txns to fetch
        usize,
        /// list of txns to avoid fetching (as they are already in pending blocks)
        Vec<TransactionHashList>,
        /// params of the proposal of this fetch
        FetchTxParams<SCT>,
    ),
    FetchReset,
    FetchFullTxs(
        /// Transaction hashes of the Full transaction to be fetched
        TransactionHashList,
        /// params of the proposal of this fetch
        FetchFullTxParams<SCT>,
    ),
    FetchFullReset,
    DrainTxs(Vec<TransactionHashList>),
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

pub enum ExecutionLedgerCommand<SCT> {
    LedgerCommit(Vec<FullBlock<SCT>>),
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

    MempoolCommand(MempoolCommand<SCT>),
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
        Vec<MempoolCommand<SCT>>,
        Vec<LedgerCommand<B, E>>,
        Vec<ExecutionLedgerCommand<SCT>>,
        Vec<CheckpointCommand<C>>,
        Vec<StateRootHashCommand<B>>,
    ) {
        let mut router_cmds = Vec::new();
        let mut timer_cmds = Vec::new();
        let mut mempool_cmds = Vec::new();
        let mut ledger_cmds = Vec::new();
        let mut execution_ledger_cmds = Vec::new();
        let mut checkpoint_cmds = Vec::new();
        let mut state_root_hash_cmds = Vec::new();

        for command in commands {
            match command {
                Command::RouterCommand(cmd) => router_cmds.push(cmd),
                Command::TimerCommand(cmd) => timer_cmds.push(cmd),
                Command::MempoolCommand(cmd) => mempool_cmds.push(cmd),
                Command::LedgerCommand(cmd) => ledger_cmds.push(cmd),
                Command::ExecutionLedgerCommand(cmd) => execution_ledger_cmds.push(cmd),
                Command::CheckpointCommand(cmd) => checkpoint_cmds.push(cmd),
                Command::StateRootHashCommand(cmd) => state_root_hash_cmds.push(cmd),
            }
        }
        (
            router_cmds,
            timer_cmds,
            mempool_cmds,
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
        unverified_message: Unverified<ST, ConsensusMessage<SCT>>,
    },
    Timeout(TimeoutVariant),
    FetchedTxs(FetchTxParams<SCT>, TransactionHashList),
    FetchedFullTxs(FetchFullTxParams<SCT>, Option<FullTransactionList>),
    FetchedBlock(FetchedBlock<SCT>),
    UpdateNextValSet(ValidatorData<SCT>),
    StateUpdate((SeqNum, ConsensusHash)),
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
            ConsensusEvent::FetchedTxs(p, t) => {
                f.debug_tuple("FetchedTxs").field(p).field(t).finish()
            }
            ConsensusEvent::FetchedFullTxs(p, _) => f
                .debug_struct("FetchedFullTxs")
                .field("author", &p.author)
                .field("proposal block", &p.p_block)
                .field("proposal tc", &p.p_last_round_tc)
                .finish(),
            ConsensusEvent::FetchedBlock(b) => f
                .debug_struct("FetchedBlock")
                .field("unverified_full_block", &b.unverified_full_block)
                .finish(),
            ConsensusEvent::UpdateNextValSet(e) => e.fmt(f),
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
