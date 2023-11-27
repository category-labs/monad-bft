pub mod replay_nodes;
pub mod timed_event;

use std::{ops::DerefMut, pin::Pin};

use monad_consensus_types::block::BlockType;
use monad_executor_glue::{Command, Message};

pub trait Executor {
    type Command;
    fn exec(&mut self, commands: Vec<Self::Command>);

    fn boxed<'a>(self) -> BoxExecutor<'a, Self::Command>
    where
        Self: Sized + Send + Unpin + 'a,
    {
        Box::pin(self)
    }
}

impl<E: Executor + ?Sized> Executor for Box<E> {
    type Command = E::Command;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        (**self).exec(commands)
    }
}

impl<P> Executor for Pin<P>
where
    P: DerefMut,
    P::Target: Executor + Unpin,
{
    type Command = <P::Target as Executor>::Command;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        Pin::get_mut(Pin::as_mut(self)).exec(commands)
    }
}

pub type BoxExecutor<'a, C> = Pin<Box<dyn Executor<Command = C> + Send + Unpin + 'a>>;

pub trait State: Sized {
    type Config;
    type Message: Message<Event = Self::Event>;

    type Event: Clone;
    type OutboundMessage: Clone + Into<Self::Message> + AsRef<Self::Message>;
    type Block: BlockType;
    type Checkpoint;
    type SignatureCollection;

    fn init(
        config: Self::Config,
    ) -> (
        Self,
        Vec<
            Command<
                Self::Event,
                Self::OutboundMessage,
                Self::Block,
                Self::Checkpoint,
                Self::SignatureCollection,
            >,
        >,
    );
    fn update(
        &mut self,
        event: Self::Event,
    ) -> Vec<
        Command<
            Self::Event,
            Self::OutboundMessage,
            Self::Block,
            Self::Checkpoint,
            Self::SignatureCollection,
        >,
    >;
}
