pub mod replay_nodes;
pub mod timed_event;

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
    type SignatureCollection;
    #[cfg(feature = "monad_test")]
    type ConsensusState: PartialEq + Eq;

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
    #[cfg(feature = "monad_test")]
    fn consensus(&self) -> &Self::ConsensusState;
}
