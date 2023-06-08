use std::hash::Hash;

use monad_crypto::secp256k1::PubKey;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PeerId(pub PubKey);

impl std::fmt::Debug for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

pub enum RouterCommand<M, OM>
where
    M: Message,
{
    Publish {
        to: PeerId,
        message: OM,
        on_ack: M::Event,
    },
    Unpublish {
        to: PeerId,
        id: M::Id,
    },
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
    FetchTxs(Box<dyn (FnOnce(Vec<u8>) -> E) + Send + Sync>),
    FetchReset,
}

pub enum LedgerCommand<B> {
    LedgerCommit(B),
}

pub trait Executor {
    type Command;
    fn exec(&mut self, commands: Vec<Self::Command>);
}

pub enum Command<M, OM, B>
where
    M: Message,
{
    RouterCommand(RouterCommand<M, OM>),
    TimerCommand(TimerCommand<M::Event>),

    MempoolCommand(MempoolCommand<M::Event>),
    LedgerCommand(LedgerCommand<B>),
}

impl<M, OM, B> Command<M, OM, B>
where
    M: Message,
{
    pub fn split_commands(
        commands: Vec<Self>,
    ) -> (
        Vec<RouterCommand<M, OM>>,
        Vec<TimerCommand<M::Event>>,
        Vec<MempoolCommand<M::Event>>,
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
    type Message: Message<Event = Self::Event>;
    type Block;

    fn init(
        config: Self::Config,
    ) -> (
        Self,
        Vec<Command<Self::Message, Self::OutboundMessage, Self::Block>>,
    );
    fn update(
        &mut self,
        event: Self::Event,
    ) -> Vec<Command<Self::Message, Self::OutboundMessage, Self::Block>>;
}

pub trait Message: Clone {
    type Event;
    type Id: Eq + Hash + Clone;

    fn id(&self) -> Self::Id;
    // TODO PeerId -> &PeerId
    fn event(self, from: PeerId) -> Self::Event;
}
