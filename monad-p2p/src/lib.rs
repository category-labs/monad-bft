use std::{
    collections::{HashMap, VecDeque},
    ops::DerefMut,
    sync::Arc,
    task::Poll,
};

use futures::{Stream, StreamExt};
use monad_executor::{Deserializable, Message, Serializable};

use libp2p::{request_response::RequestId, swarm::SwarmBuilder, Transport};

mod behavior;
use behavior::Behavior;

pub struct Service<M, OM>
where
    M: Message + Deserializable + Send + Sync + 'static,
    <M as Deserializable>::ReadError: 'static,
    OM: Serializable + Send + Sync + 'static,

    M: Serializable, // FIXME
{
    swarm: libp2p::Swarm<Behavior<M, OM>>,

    outbound_messages: HashMap<RequestId, (Arc<OM>, M::Event)>,
    outbound_messages_lookup: HashMap<(libp2p::PeerId, M::Id), RequestId>,

    self_events: VecDeque<M::Event>,
}

impl<M, OM> Service<M, OM>
where
    M: Message + Deserializable + Send + Sync + 'static,
    <M as Deserializable>::ReadError: 'static,
    OM: Into<M> + AsRef<M> + Serializable + Send + Sync + 'static,

    M: Serializable, // FIXME
{
    pub fn without_executor(identity: libp2p::identity::Keypair) -> Self {
        let transport = libp2p::core::transport::MemoryTransport::default()
            .upgrade(libp2p::core::upgrade::Version::V1Lazy)
            .authenticate(libp2p::noise::NoiseAuthenticated::xx(&identity).unwrap())
            .multiplex(libp2p::mplex::MplexConfig::new())
            .boxed();

        let pubkey = identity.public();
        let behavior = Behavior::new(&pubkey);

        let local_peer_id: libp2p::PeerId = pubkey.into();
        let mut swarm = SwarmBuilder::without_executor(transport, behavior, local_peer_id).build();
        swarm.listen_on("/memory/0".parse().unwrap()).unwrap();

        Self {
            swarm,
            outbound_messages: HashMap::new(),
            outbound_messages_lookup: HashMap::new(),

            self_events: VecDeque::new(),
        }
    }

    pub fn publish_message(&mut self, to: &monad_executor::PeerId, message: OM, on_ack: M::Event) {
        let to_libp2p: libp2p::PeerId = (&to.0).into();
        if self.swarm.local_peer_id() == &to_libp2p {
            // we need special case send to self
            // this is because dialing to self will fail
            self.self_events.push_back(message.into().event(*to));
            self.self_events.push_back(on_ack);
            return;
        }
        let id = message.as_ref().id();
        let message = Arc::new(message);
        let request_id = self.swarm.behaviour_mut().request_response.send_request(
            &to_libp2p,
            // FIXME shouldn't need to this type stuff...
            Arc::new((*message).as_ref().clone()),
        );
        self.outbound_messages.insert(request_id, (message, on_ack));
        self.outbound_messages_lookup
            .insert((to_libp2p, id), request_id);
        assert_eq!(
            self.outbound_messages.len(),
            self.outbound_messages_lookup.len()
        );
    }

    pub fn unpublish_message(&mut self, to: &monad_executor::PeerId, message_id: &M::Id) {
        let to: libp2p::PeerId = (&to.0).into();
        if let Some(request_id) = self
            .outbound_messages_lookup
            .remove(&(to, message_id.clone()))
        {
            self.outbound_messages
                .remove(&request_id)
                .expect("outbound_messages out of sync");
        }
        assert_eq!(
            self.outbound_messages.len(),
            self.outbound_messages_lookup.len()
        );
    }
}

impl<M, OM> Stream for Service<M, OM>
where
    M: Message + Deserializable + Send + Sync + 'static,
    <M as Deserializable>::ReadError: 'static,
    OM: AsRef<M> + Serializable + Send + Sync + 'static,
    Self: Unpin,

    M: Serializable, // FIXME
{
    type Item = M::Event;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Some(event) = self.self_events.pop_front() {
            return Poll::Ready(Some(event));
        }
        while let Poll::Ready(Some(event)) = self.swarm.poll_next_unpin(cx) {
            match event {
                libp2p::swarm::SwarmEvent::Behaviour(behavior::BehaviorEvent::RequestResponse(
                    libp2p::request_response::Event::Message { peer, message },
                )) => {
                    let pubkey = match monad_crypto::secp256k1::PubKey::try_from(peer) {
                        Ok(pubkey) => pubkey,
                        Err(_) => {
                            // We don't need to repsond if the peer isn't using a valid secp256k1 key
                            // TODO do we block peer or something?
                            continue;
                        }
                    };
                    let service = self.deref_mut();
                    match message {
                        libp2p::request_response::Message::Request {
                            request_id: _,
                            request,
                            channel,
                        } => {
                            // err doesn't matter - peer will resent message and get acked later
                            // TODO log/inc-counter here?
                            let _ = service
                                .swarm
                                .behaviour_mut()
                                .request_response
                                .send_response(channel, ());
                            return Poll::Ready(Some(
                                Arc::try_unwrap(request)
                                    .unwrap_or_else(|_| {
                                        panic!("more than 1 copies of Arc<Message>")
                                    })
                                    .event(monad_executor::PeerId(pubkey)),
                            ));
                        }
                        libp2p::request_response::Message::Response {
                            request_id,
                            response: (),
                        } => {
                            if let Some((message, on_ack)) =
                                service.outbound_messages.remove(&request_id)
                            {
                                service
                                    .outbound_messages_lookup
                                    .remove(&(peer, (*message).as_ref().id()))
                                    .expect("outbound_messages_lookup out of sync");
                                return Poll::Ready(Some(on_ack));
                            }
                        }
                    }
                }
                libp2p::swarm::SwarmEvent::Behaviour(behavior::BehaviorEvent::RequestResponse(
                    libp2p::request_response::Event::OutboundFailure {
                        peer: _,
                        request_id: _,
                        error: e,
                    },
                )) => {
                    todo!("TODO schedule retry: {:?}", e)
                }
                _ => {}
            }
        }
        // because libp2p::Swarm will never yield Poll::Ready(None), we can safely return Pending
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use std::{array::TryFromSliceError, collections::HashSet};

    use crate::Service;
    use monad_executor::{Deserializable, Message, Serializable};

    use futures::StreamExt;

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct TestMessage(u64);

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    enum TestEvent {
        Message(monad_executor::PeerId, TestMessage),
        Ack(monad_executor::PeerId, <TestMessage as Message>::Id),
    }

    impl Message for TestMessage {
        type Event = TestEvent;

        type Id = u64;

        fn id(&self) -> Self::Id {
            self.0
        }

        fn event(self, from: monad_executor::PeerId) -> Self::Event {
            Self::Event::Message(from, self)
        }
    }
    impl Serializable for TestMessage {
        fn serialize(&self) -> Vec<u8> {
            self.0.to_le_bytes().to_vec()
        }
    }

    impl Deserializable for TestMessage {
        type ReadError = TryFromSliceError;

        fn deserialize(message: &[u8]) -> Result<Self, Self::ReadError> {
            let message: [u8; 8] = message.try_into()?;
            Ok(TestMessage(u64::from_le_bytes(message)))
        }
    }
    impl AsRef<TestMessage> for TestMessage {
        fn as_ref(&self) -> &TestMessage {
            &self
        }
    }

    fn create_random_node() -> (monad_executor::PeerId, Service<TestMessage, TestMessage>) {
        let keypair = libp2p::identity::Keypair::generate_secp256k1();
        let public = keypair.public();
        let service = Service::<TestMessage, TestMessage>::without_executor(keypair);
        let libp2p_peer_id = public.to_peer_id();
        (
            monad_executor::PeerId(libp2p_peer_id.try_into().unwrap()),
            service,
        )
    }

    #[test]
    fn test_self() {
        let (peer_id, mut service) = create_random_node();
        let message = TestMessage(0);
        let on_ack_event = TestEvent::Ack(peer_id, message.id());
        service.publish_message(&peer_id, message.clone(), on_ack_event.clone());

        let mut expected_events: HashSet<_> =
            vec![TestEvent::Message(peer_id, message), on_ack_event]
                .into_iter()
                .collect();

        while !expected_events.is_empty() {
            let event = futures::executor::block_on(service.next()).unwrap();
            expected_events.remove(&event);
        }
    }
}
