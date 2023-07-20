use std::hash::Hash;

use monad_consensus_types::transaction::TransactionCollection;
use monad_crypto::secp256k1::PubKey;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PeerId(pub PubKey);

impl std::fmt::Debug for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.0.fmt(f)
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

pub enum MempoolCommand<E, TC> {
    // TODO consider moving away from dynamic dispatch
    /// FetchReset should ALMOST ALWAYS be emitted by the state machine after handling E
    /// This is to prevent E from firing twice on replay
    // TODO create test to demonstrate faulty behavior if written improperly
    FetchTxs(Box<dyn (FnOnce(Vec<u8>) -> E) + Send + Sync>),
    FetchReset,
    FetchFullTxs(Box<dyn (FnOnce(TC) -> E) + Send + Sync>),
    FetchFullReset,
}

pub enum LedgerCommand<B> {
    LedgerCommit(B),
}

pub trait Executor {
    type Command;
    fn exec(&mut self, commands: Vec<Self::Command>);
}

pub enum Command<M, OM, B, TC>
where
    M: Message,
    TC: TransactionCollection,
{
    RouterCommand(RouterCommand<M, OM>),
    TimerCommand(TimerCommand<M::Event<TC>>),

    MempoolCommand(MempoolCommand<M::Event<TC>, TC>),
    LedgerCommand(LedgerCommand<B>),
}

impl<M, OM, B, TC> Command<M, OM, B, TC>
where
    M: Message,
    TC: TransactionCollection,
{
    pub fn split_commands(
        commands: Vec<Self>,
    ) -> (
        Vec<RouterCommand<M, OM>>,
        Vec<TimerCommand<M::Event<TC>>>,
        Vec<MempoolCommand<M::Event<TC>, TC>>,
        Vec<LedgerCommand<B>>,
    ) {
        let mut router_cmds = Vec::new();
        let mut timer_cmds = Vec::new();
        let mut mempool_cmds = Vec::new();
        let mut ledger_cmds = Vec::new();
        for command in commands {
            match command {
                Command::RouterCommand(cmd) => router_cmds.push(cmd),
                Command::TimerCommand(cmd) => timer_cmds.push(cmd),
                Command::MempoolCommand(cmd) => mempool_cmds.push(cmd),
                Command::LedgerCommand(cmd) => ledger_cmds.push(cmd),
            }
        }
        (router_cmds, timer_cmds, mempool_cmds, ledger_cmds)
    }
}

pub trait State: Sized {
    type Config;
    type Event: Clone;
    type OutboundMessage: Into<Self::Message> + AsRef<Self::Message>;
    type Message: Message<Event<Self::TransactionCollection> = Self::Event>;
    type Block;
    type TransactionCollection: TransactionCollection;

    fn init(
        config: Self::Config,
    ) -> (
        Self,
        Vec<
            Command<Self::Message, Self::OutboundMessage, Self::Block, Self::TransactionCollection>,
        >,
    );
    fn update(
        &mut self,
        event: Self::Event,
    ) -> Vec<Command<Self::Message, Self::OutboundMessage, Self::Block, Self::TransactionCollection>>;
}

pub trait Message: Clone {
    type Event<TC: TransactionCollection>;
    type Id: Eq + Hash + Clone;

    fn id(&self) -> Self::Id;
    // TODO PeerId -> &PeerId
    fn event<TC: TransactionCollection>(self, from: PeerId) -> Self::Event<TC>;
}
