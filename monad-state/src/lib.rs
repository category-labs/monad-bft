use message::MessageState;
use monad_executor::{Command, Message, PeerId, RouterCommand, State};

mod message;

pub struct MonadState {
    message_state: MessageState<MonadMessage>,
}

#[derive(Clone)]
pub enum MonadEvent {
    Ack {
        peer: PeerId,
        id: MonadMessage,
        round: u64,
    },
}

#[derive(PartialEq, Eq, Hash, Clone)]
pub struct MonadMessage {}

impl Message for MonadMessage {
    type Event = MonadEvent;
    type ReadError = ();

    type Id = Self;

    fn deserialize(from: PeerId, message: &[u8]) -> Result<Self, Self::ReadError> {
        todo!()
    }

    fn serialize(&self) -> Vec<u8> {
        todo!()
    }

    fn id(&self) -> Self::Id {
        self.clone()
    }

    fn event(self, _from: PeerId) -> Self::Event {
        todo!()
    }
}

impl State for MonadState {
    type Event = MonadEvent;
    type Message = MonadMessage;

    fn init() -> (Self, Vec<Command<Self::Event, Self::Message>>) {
        todo!()
    }

    fn update(&mut self, event: Self::Event) -> Vec<Command<Self::Event, Self::Message>> {
        match event {
            MonadEvent::Ack { peer, id, round } => self
                .message_state
                .handle_ack(round, peer, id)
                .into_iter()
                .map(|cmd| {
                    Command::RouterCommand(RouterCommand::Unpublish {
                        to: cmd.to,
                        id: cmd.id,
                    })
                })
                .collect(),
        }
    }
}
