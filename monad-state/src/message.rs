use std::collections::{HashSet, VecDeque};

use monad_executor::{Message, PeerId};

pub struct MessageState<M: Message> {
    messages: VecDeque<HashSet<(PeerId, M::Id)>>,
    round: u64, // round # of messages.back()
    // min round == round - messages.len() + 1
    peers: Vec<PeerId>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct MessageActionPublish<M>
where
    M: Message,
{
    pub to: PeerId,
    pub message: M,
}

#[derive(Debug, PartialEq, Eq)]
pub struct MessageActionUnpublish<M>
where
    M: Message,
{
    pub to: PeerId,
    pub id: M::Id,
}

impl<M> MessageState<M>
where
    M: Message,
{
    pub fn new(max_rounds_cached: u64, peers: Vec<PeerId>) -> Self {
        let mut messages: VecDeque<_> = std::iter::repeat(HashSet::new())
            .take(max_rounds_cached as usize)
            .collect();
        messages.make_contiguous();
        Self {
            messages,
            round: 0,
            peers,
        }
    }

    pub fn max_rounds_cached(&self) -> u64 {
        self.messages.len() as u64
    }

    pub fn min_round(&self) -> u64 {
        if self.round < self.max_rounds_cached() + 1 {
            0
        } else {
            self.round - self.max_rounds_cached() + 1
        }
    }

    pub fn max_round(&self) -> u64 {
        self.round
    }

    pub fn set_round(&mut self, round: u64, peers: Vec<PeerId>) -> Vec<MessageActionUnpublish<M>> {
        assert!(round > self.round);
        let mut commands = Vec::new();

        // TODO this can be short-circuited for (round - self.round) > self.messages.len()
        while round > self.round {
            self.messages.rotate_left(1);
            let back = self.messages.back_mut().unwrap();
            for (peer, message_id) in back.drain() {
                commands.push(MessageActionUnpublish {
                    to: peer,
                    id: message_id,
                })
            }
            self.round += 1;
        }

        self.peers = peers;

        commands
    }

    pub fn send(&mut self, peer: PeerId, message: M) -> MessageActionPublish<M> {
        self.messages
            .back_mut()
            .unwrap()
            .insert((peer.clone(), message.id()));

        MessageActionPublish { to: peer, message }
    }

    pub fn broadcast(&mut self, message: M) -> Vec<MessageActionPublish<M>> {
        let mut commands = Vec::new();
        for peer in self.peers.to_vec() {
            commands.push(self.send(peer.clone(), message.clone()));
        }

        commands
    }

    pub fn handle_ack(
        &mut self,
        round: u64,
        peer: PeerId,
        id: M::Id,
    ) -> Option<MessageActionUnpublish<M>> {
        let max_round = self.max_round();
        if round >= self.min_round() && round <= max_round {
            let back_idx = self.max_rounds_cached() - 1;
            let key = (peer, id);
            assert!(self.messages[(back_idx - (max_round - round)) as usize].remove(&key));
            Some(MessageActionUnpublish {
                to: key.0,
                id: key.1,
            })
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use monad_executor::{Message, PeerId};

    use crate::message::{MessageActionUnpublish, MessageState};

    #[derive(Debug, PartialEq, Eq, Hash, Clone)]
    struct TestMessage;

    impl Message for TestMessage {
        type Event = TestMessage;
        type ReadError = ();
        type Id = TestMessage;

        fn deserialize(from: PeerId, message: &[u8]) -> Result<Self, Self::ReadError> {
            todo!()
        }

        fn serialize(&self) -> Vec<u8> {
            todo!()
        }

        fn id(&self) -> Self::Id {
            self.clone()
        }

        fn event(self) -> Self::Event {
            todo!()
        }
    }

    #[test]
    fn init() {
        let state = MessageState::<TestMessage>::new(5, Vec::new());
        assert_eq!(state.max_rounds_cached(), 5);
        assert_eq!(state.min_round(), 0);
        assert_eq!(state.max_round(), 0);
    }

    #[test]
    fn set_round() {
        let mut state = MessageState::<TestMessage>::new(5, Vec::new());
        let _ = state.set_round(10, Vec::new());
        assert_eq!(state.max_rounds_cached(), 5);
        assert_eq!(state.min_round(), 6);
        assert_eq!(state.max_round(), 10);
    }

    #[test]
    fn send() {
        let mut state = MessageState::<TestMessage>::new(5, Vec::new());
        let action = state.send(PeerId, TestMessage);

        assert_eq!(action.to, PeerId);
        assert_eq!(action.message, TestMessage);
    }

    #[test]
    fn set_round_eviction() {
        let mut state = MessageState::<TestMessage>::new(5, Vec::new());
        let _ = state.send(PeerId, TestMessage);

        let evicted = state.set_round(10, Vec::new());
        assert_eq!(
            evicted,
            vec![MessageActionUnpublish {
                to: PeerId,
                id: TestMessage,
            }],
        )
    }

    #[test]
    fn handle_ack() {
        let mut state = MessageState::<TestMessage>::new(5, Vec::new());
        let _ = state.send(PeerId, TestMessage);

        let evicted = state.handle_ack(0, PeerId, TestMessage);
        assert_eq!(
            evicted,
            Some(MessageActionUnpublish {
                to: PeerId,
                id: TestMessage,
            }),
        )
    }

    #[test]
    fn evicted_handle_ack() {
        let mut state = MessageState::<TestMessage>::new(5, Vec::new());
        let _ = state.send(PeerId, TestMessage);

        let _ = state.set_round(10, Vec::new());

        let evicted = state.handle_ack(0, PeerId, TestMessage);
        assert_eq!(evicted, None,)
    }
}
