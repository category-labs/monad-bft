#[derive(Clone, PartialEq, Eq, Hash)]
pub struct PeerId;
pub enum RouterCommand<E> {
    Publish {
        to: PeerId,
        payload: Vec<u8>,
        on_ack: E,
    },
    Unpublish {
        to: PeerId,
        payload: Vec<u8>, // this can be shortened to a hash
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

pub enum Command<E> {
    RouterCommand(RouterCommand<E>),
    TimerCommand(TimerCommand<E>),
}

impl<E> Command<E> {
    pub fn split_commands(commands: Vec<Self>) -> (Vec<RouterCommand<E>>, Vec<TimerCommand<E>>) {
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

    fn init() -> (Self, Vec<Command<Self::Event>>);
    fn update(&mut self, event: Self::Event) -> Vec<Command<Self::Event>>;
}

pub trait Codec {
    type Event: Clone + Unpin;
    type ParseError;

    fn parse_peer_payload(from: PeerId, payload: &[u8]) -> Result<Self::Event, Self::ParseError>;
}
