use std::{error::Error, hash::Hash};

use monad_crypto::secp256k1::PubKey;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PeerId(pub PubKey);
pub enum RouterCommand<E, M, OM>
where
    M: Message<Event = E>,
    OM: Into<M>,
{
    Publish { to: PeerId, message: OM, on_ack: E },
    Unpublish { to: PeerId, id: M::Id },
}

pub enum TimerCommand<E> {
    // overwrites previous Schedule if exists
    Schedule {
        duration: std::time::Duration,
        on_timeout: E,
    },
    Unschedule,
}

pub trait Executor {
    type Command;
    fn exec(&mut self, commands: Vec<Self::Command>);
}

pub enum Command<E, M, OM>
where
    M: Message<Event = E>,
    OM: Into<M>,
{
    RouterCommand(RouterCommand<E, M, OM>),
    TimerCommand(TimerCommand<E>),
}

impl<E, M, OM> Command<E, M, OM>
where
    OM: Into<M>,
    M: Message<Event = E>,
{
    pub fn split_commands(
        commands: Vec<Self>,
    ) -> (Vec<RouterCommand<E, M, OM>>, Vec<TimerCommand<E>>) {
        let mut router_cmds = Vec::new();
        let mut timer_cmds = Vec::new();
        for command in commands {
            match command {
                Command::RouterCommand(cmd) => router_cmds.push(cmd),
                Command::TimerCommand(cmd) => timer_cmds.push(cmd),
            }
        }
        (router_cmds, timer_cmds)
    }
}

pub trait State: Sized {
    type Config;
    type Event: Clone + Unpin;
    type OutboundMessage: Into<Self::Message> + Clone + Unpin;
    type Message: Message<Event = Self::Event>;

    fn init(
        config: Self::Config,
    ) -> (
        Self,
        Vec<Command<Self::Event, Self::Message, Self::OutboundMessage>>,
    );
    fn update(
        &mut self,
        event: Self::Event,
    ) -> Vec<Command<Self::Event, Self::Message, Self::OutboundMessage>>;
}

pub trait Serializable {
    fn serialize(&self) -> Vec<u8>;
}

pub trait Deserializable: Sized + Send + Sync + 'static {
    type ReadError: Error + Send + Sync;

    fn deserialize(message: &[u8]) -> Result<Self, Self::ReadError>;
}

pub trait Message: Clone + Unpin {
    type Event: Unpin;
    type Id: Eq + Hash + Clone + Unpin;

    fn id(&self) -> Self::Id;
    // TODO PeerId -> &PeerId
    fn event(self, from: PeerId) -> Self::Event;
}
