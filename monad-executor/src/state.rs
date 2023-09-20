use std::hash::Hash;

use monad_consensus_types::{
    block::BlockType,
    payload::{FullTransactionList, TransactionList},
};
use monad_crypto::secp256k1::PubKey;
use monad_types::{BlockId, Hash as ConsensusHash, NodeId};

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

pub enum TimerCommand<E> {
    /// ScheduleReset should ALMOST ALWAYS be emitted by the state machine after handling E
    /// This is to prevent E from firing twice on replay
    // TODO create test to demonstrate faulty behavior if written improperly
    Schedule {
        duration: std::time::Duration,
        on_timeout: E,
    },
    ScheduleReset,
}

pub enum MempoolCommand<E> {
    // TODO consider moving away from dynamic dispatch
    /// FetchReset should ALMOST ALWAYS be emitted by the state machine after handling E
    /// This is to prevent E from firing twice on replay
    // TODO create test to demonstrate faulty behavior if written improperly
    FetchTxs(usize, Box<dyn (FnOnce(TransactionList) -> E) + Send + Sync>),
    FetchReset,
    FetchFullTxs(
        TransactionList,
        Box<dyn (FnOnce(Option<FullTransactionList>) -> E) + Send + Sync>,
    ),
    FetchFullReset,
}

pub enum LedgerCommand<B, E> {
    LedgerCommit(Vec<B>),
    LedgerFetch(BlockId, Box<dyn (FnOnce(Option<B>) -> E) + Send + Sync>),
    LedgerFetchReset,
}

pub enum CheckpointCommand<C> {
    Save(C),
}

pub enum StateRootHashCommand<B, E> {
    LedgerCommit(B, Box<dyn (FnOnce(u64, ConsensusHash) -> E) + Send + Sync>),
}

pub trait Executor {
    type Command;
    fn exec(&mut self, commands: Vec<Self::Command>);
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

pub trait State: Sized {
    type Config;
    type Event: Clone;
    type OutboundMessage: Into<Self::Message> + AsRef<Self::Message>;
    type Message: Message<Event = Self::Event>;
    type Block: BlockType;
    type Checkpoint;

    fn init(
        config: Self::Config,
    ) -> (
        Self,
        Vec<Command<Self::Message, Self::OutboundMessage, Self::Block, Self::Checkpoint>>,
    );
    fn update(
        &mut self,
        event: Self::Event,
    ) -> Vec<Command<Self::Message, Self::OutboundMessage, Self::Block, Self::Checkpoint>>;
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
