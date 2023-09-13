use monad_consensus_types::block::BlockType;
use monad_executor_glue::{Command, Message};

pub trait Executor {
    type Command;
    fn exec(&mut self, commands: Vec<Self::Command>);
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
