use std::hash::Hash;

use monad_crypto::secp256k1::PubKey;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PeerId(pub PubKey);
pub enum RouterCommand<E, M>
where
    M: Message<Event = E>,
{
    Publish { to: PeerId, message: M, on_ack: E },
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

pub enum Command<E, M>
where
    M: Message<Event = E>,
{
    RouterCommand(RouterCommand<E, M>),
    TimerCommand(TimerCommand<E>),
}

impl<E, M> Command<E, M>
where
    M: Message<Event = E>,
{
    pub fn split_commands(commands: Vec<Self>) -> (Vec<RouterCommand<E, M>>, Vec<TimerCommand<E>>) {
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
    type Event: Clone + Unpin;
    type Message: Message<Event = Self::Event>;

    fn init() -> (Self, Vec<Command<Self::Event, Self::Message>>);
    fn update(&mut self, event: Self::Event) -> Vec<Command<Self::Event, Self::Message>>;
}

pub trait Message: Sized + Clone + Unpin {
    type Event;
    type ReadError;
    type Id: Eq + Hash + Clone + Unpin;

    fn deserialize(from: PeerId, message: &[u8]) -> Result<Self, Self::ReadError>;
    fn serialize(&self) -> Vec<u8>;
    fn id(&self) -> Self::Id;
    fn event(self, from: PeerId) -> Self::Event;
}
