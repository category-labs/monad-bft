use std::{collections::HashMap, fmt::Debug, ops::DerefMut, sync::Arc, task::Poll};

use futures::{Stream, StreamExt};
use monad_executor::{Message, Serializable};

use libp2p::{request_response::RequestId, swarm::SwarmBuilder, PeerId, Transport};

mod behavior;
use behavior::Behavior;

pub struct Service<M>
where
    M: Message + Serializable,
{
    swarm: libp2p::Swarm<Behavior<M>>,

    outbound_messages: HashMap<RequestId, (Arc<M>, M::Event)>,
    outbound_messages_lookup: HashMap<(PeerId, M::Id), RequestId>,
}

impl<M> Service<M>
where
    M: Message + Serializable,
{
    pub fn without_executor(identity: &libp2p::identity::Keypair) -> Self {
        let transport = libp2p::core::transport::MemoryTransport::default()
            .upgrade(libp2p::core::upgrade::Version::V1Lazy)
            .authenticate(libp2p::noise::NoiseAuthenticated::xx(identity).unwrap())
            .multiplex(libp2p::mplex::MplexConfig::new())
            .boxed();
        let local_peer_id: libp2p::PeerId = identity.public().into();

        let behavior = Behavior::new(identity);

        let mut swarm = SwarmBuilder::without_executor(transport, behavior, local_peer_id).build();
        swarm.listen_on("/memory/0".parse().unwrap()).unwrap();

        Self {
            swarm,
            outbound_messages: HashMap::new(),
            outbound_messages_lookup: HashMap::new(),
        }
    }

    pub fn publish_message(&mut self, to: &PeerId, message: M, on_ack: M::Event) {
        let message = Arc::new(message);
        let id = message.id();
        let request_id = self
            .swarm
            .behaviour_mut()
            .request_response
            .send_request(to, message.clone());
        self.outbound_messages.insert(request_id, (message, on_ack));
        self.outbound_messages_lookup
            .insert((*to, id.clone()), request_id);
        assert_eq!(
            self.outbound_messages.len(),
            self.outbound_messages_lookup.len()
        );
    }

    pub fn unpublish_message(&mut self, to: &PeerId, message_id: &M::Id) {
        if let Some(request_id) = self
            .outbound_messages_lookup
            .remove(&(*to, message_id.clone()))
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

impl<M> Stream for Service<M>
where
    M: Message + Serializable + Debug,
{
    type Item = M::Event;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        while let Poll::Ready(Some(event)) = self.swarm.poll_next_unpin(cx) {
            match event {
                libp2p::swarm::SwarmEvent::Behaviour(behavior::BehaviorEvent::RequestResponse(
                    libp2p::request_response::Event::Message { peer, message },
                )) => {
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
                                    .expect("more than 1 copies of Arc<Message>")
                                    .event(todo!("libp2p::PeerId -> monad::PeerId")),
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
                                    .remove(&(peer, message.id()))
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
                        error: _,
                    },
                )) => {
                    todo!("schedule retry")
                }
                _ => {}
            }
        }
        // because libp2p::Swarm will never yield Poll::Ready(None), we can safely return Pending
        Poll::Pending
    }
}
