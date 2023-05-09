use std::{
    collections::{HashSet, VecDeque},
    fmt::Debug,
    marker::PhantomData,
};

use monad_executor::{Message, PeerId};
use monad_types::Round;

pub struct MessageState<M: Message, OM: Into<M> + Clone> {
    messages: VecDeque<HashSet<(PeerId, M::Id)>>,
    round: Round, // round # of messages.back()
    // min round == round - messages.len() + 1
    peers: Vec<PeerId>,
    _marker: PhantomData<OM>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct MessageActionPublish<M, OM>
where
    M: Message,
    OM: Into<M>,
{
    pub to: PeerId,
    pub message: OM,
    _marker: PhantomData<M>,
}

impl<M, OM> MessageActionPublish<M, OM>
where
    M: Message,
    OM: Into<M>,
{
    fn new(to: PeerId, message: OM) -> Self {
        Self {
            to,
            message,
            _marker: PhantomData,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct MessageActionUnpublish<M>
where
    M: Message,
{
    pub to: PeerId,
    pub id: M::Id,
}

impl<M, OM> MessageState<M, OM>
where
    M: Message,
    OM: Into<M> + Clone,
{
    pub fn new(max_rounds_cached: u64, peers: Vec<PeerId>) -> Self {
        let mut messages: VecDeque<_> = std::iter::repeat(HashSet::new())
            .take(max_rounds_cached as usize)
            .collect();
        messages.make_contiguous();
        Self {
            messages,
            round: Round(0),
            peers,
            _marker: PhantomData,
        }
    }

    pub fn max_rounds_cached(&self) -> u64 {
        self.messages.len() as u64
    }

    pub fn min_round(&self) -> Round {
        if self.round < Round(self.max_rounds_cached()) + Round(1) {
            Round(0)
        } else {
            self.round - Round(self.max_rounds_cached()) + Round(1)
        }
    }

    pub fn max_round(&self) -> Round {
        self.round
    }

    pub fn round(&self) -> Round {
        self.round
    }

    pub fn set_round(
        &mut self,
        round: Round,
        peers: Vec<PeerId>,
    ) -> Vec<MessageActionUnpublish<M>> {
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
            self.round += Round(1);
        }

        self.peers = peers;

        commands
    }

    pub fn send(&mut self, peer: PeerId, message: OM) -> MessageActionPublish<M, OM> {
        self.messages
            .back_mut()
            .unwrap()
            .insert((peer, message.clone().into().id()));

        MessageActionPublish::new(peer, message)
    }

    pub fn broadcast(&mut self, message: OM) -> Vec<MessageActionPublish<M, OM>>
    where
        <M as monad_executor::Message>::Id: Debug,
        OM: Debug,
    {
        let mut commands = Vec::new();
        for peer in self.peers.to_vec() {
            commands.push(self.send(peer, message.clone()));
        }

        commands
    }

    pub fn handle_ack(
        &mut self,
        round: Round,
        peer: PeerId,
        id: M::Id,
    ) -> Option<MessageActionUnpublish<M>>
    where
        <M as monad_executor::Message>::Id: Debug,
    {
        let max_round = self.max_round();
        if round >= self.min_round() && round <= max_round {
            let back_idx = self.max_rounds_cached() - 1;
            let key = (peer, id);

            assert!(self.messages[(back_idx - (max_round - round).0) as usize].remove(&key));
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
    use monad_testutil::signing::node_id;
    use monad_types::Round;

    use crate::message::{MessageActionUnpublish, MessageState};

    #[derive(Debug, PartialEq, Eq, Hash, Clone)]
    struct TestMessage;

    impl Message for TestMessage {
        type Event = TestMessage;
        type Id = TestMessage;

        fn id(&self) -> Self::Id {
            self.clone()
        }

        fn event(self, _from: PeerId) -> Self::Event {
            unreachable!()
        }
    }

    #[test]
    fn init() {
        let state = MessageState::<TestMessage, TestMessage>::new(5, Vec::new());
        assert_eq!(state.max_rounds_cached(), 5);
        assert_eq!(state.min_round(), Round(0));
        assert_eq!(state.max_round(), Round(0));
    }

    #[test]
    fn set_round() {
        let mut state = MessageState::<TestMessage, TestMessage>::new(5, Vec::new());
        let _ = state.set_round(Round(10), Vec::new());
        assert_eq!(state.max_rounds_cached(), 5);
        assert_eq!(state.min_round(), Round(6));
        assert_eq!(state.max_round(), Round(10));
    }

    #[test]
    fn send() {
        let mut state = MessageState::<TestMessage, TestMessage>::new(5, Vec::new());
        let action = state.send(PeerId(node_id().0), TestMessage);

        assert_eq!(action.to, PeerId(node_id().0));
        assert_eq!(action.message, TestMessage);
    }

    #[test]
    fn set_round_eviction() {
        let mut state = MessageState::<TestMessage, TestMessage>::new(5, Vec::new());
        let _ = state.send(PeerId(node_id().0), TestMessage);

        let evicted = state.set_round(Round(10), Vec::new());
        assert_eq!(
            evicted,
            vec![MessageActionUnpublish {
                to: PeerId(node_id().0),
                id: TestMessage,
            }],
        )
    }

    #[test]
    fn handle_ack() {
        let mut state = MessageState::<TestMessage, TestMessage>::new(5, Vec::new());
        let _ = state.send(PeerId(node_id().0), TestMessage);

        let evicted = state.handle_ack(Round(0), PeerId(node_id().0), TestMessage);
        assert_eq!(
            evicted,
            Some(MessageActionUnpublish {
                to: PeerId(node_id().0),
                id: TestMessage,
            }),
        )
    }

    #[test]
    fn evicted_handle_ack() {
        let mut state = MessageState::<TestMessage, TestMessage>::new(5, Vec::new());
        let _ = state.send(PeerId(node_id().0), TestMessage);

        let _ = state.set_round(Round(10), Vec::new());

        let evicted = state.handle_ack(Round(0), PeerId(node_id().0), TestMessage);
        assert_eq!(evicted, None,)
    }
}
