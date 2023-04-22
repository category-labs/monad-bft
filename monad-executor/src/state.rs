use std::{error::Error, hash::Hash};

use monad_crypto::secp256k1::PubKey;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PeerId(pub PubKey);
pub enum RouterCommand<S>
where
    S: State,
{
    Publish {
        to: PeerId,
        message: S::OutboundMessage,
        on_ack: S::Event,
    },
    Unpublish {
        to: PeerId,
        id: <S::Message as Message>::Id,
    },
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

pub enum Command<S>
where
    S: State,
{
    RouterCommand(RouterCommand<S>),
    TimerCommand(TimerCommand<S::Event>),
}

impl<S> Command<S>
where
    S: State,
{
    pub fn split_commands(
        commands: Vec<Self>,
    ) -> (Vec<RouterCommand<S>>, Vec<TimerCommand<S::Event>>) {
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
    type Event: Clone;
    type OutboundMessage: Into<Self::Message> + Clone;
    type Message: Message<Event = Self::Event>;

    fn init(config: Self::Config) -> (Self, Vec<Command<Self>>);
    fn update(&mut self, event: Self::Event) -> Vec<Command<Self>>;
}

pub trait Serializable {
    fn serialize(&self) -> Vec<u8>;
}

pub trait Deserializable: Sized {
    type ReadError: Error + Send + Sync;

    fn deserialize(message: &[u8]) -> Result<Self, Self::ReadError>;
}

pub trait Message: Clone {
    type Event;
    type Id: Eq + Hash + Clone;

    fn id(&self) -> Self::Id;
    // TODO PeerId -> &PeerId
    fn event(self, from: PeerId) -> Self::Event;
}
