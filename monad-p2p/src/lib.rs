use std::{
    collections::{HashMap, HashSet, VecDeque},
    ops::DerefMut,
    sync::Arc,
    task::Poll,
    time::Duration,
};

use futures::{FutureExt, Stream, StreamExt};
use monad_consensus_types::transaction::TransactionCollection;
use monad_executor::{Executor, Message, RouterCommand, RouterTarget};
use monad_types::{Deserializable, Serializable};

use libp2p::{request_response::RequestId, swarm::SwarmBuilder, Transport};

mod behavior;
use behavior::Behavior;

use crate::behavior::WrappedMessage;

pub type Multiaddr = libp2p::Multiaddr;

pub struct Service<M, OM, TC>
where
    M: Message + Deserializable + Send + Sync + 'static,
    <M as Deserializable>::ReadError: 'static,
    OM: Serializable + Send + Sync + 'static,
    TC: TransactionCollection,
{
    swarm: libp2p::Swarm<Behavior<M, OM>>,

    outbound_messages: HashMap<RequestId, Arc<WrappedMessage<M, OM>>>,
    outbound_messages_lookup: HashMap<(libp2p::PeerId, M::Id), RequestId>,

    self_events: VecDeque<M::Event<TC>>,

    // TODO deprecate this once we have a RouterCommand for setting peers
    peers: HashSet<monad_executor::PeerId>,
}

impl<M, OM, TC> Service<M, OM, TC>
where
    M: Message + Deserializable + Send + Sync + 'static,
    <M as Deserializable>::ReadError: 'static,
    OM: Serializable + Send + Sync + 'static,
    TC: TransactionCollection,

    OM: Into<M> + AsRef<M>,
{
    pub fn without_executor(identity: libp2p::identity::Keypair) -> Self {
        // TODO most of the stuff in here can be factored out
        let transport = libp2p::core::transport::MemoryTransport::default()
            .upgrade(libp2p::core::upgrade::Version::V1Lazy)
            .authenticate(libp2p::noise::NoiseAuthenticated::xx(&identity).unwrap())
            .multiplex(libp2p::mplex::MplexConfig::new())
            .boxed();

        let pubkey = identity.public();
        let behavior = Behavior::new(&pubkey, Duration::from_secs(1), Duration::from_secs(10));

        let local_peer_id: libp2p::PeerId = pubkey.into();
        let mut swarm = SwarmBuilder::without_executor(transport, behavior, local_peer_id).build();
        swarm.listen_on("/memory/0".parse().unwrap()).unwrap();

        // wait for address
        // TODO should we make this a future instead?
        while !matches!(
            swarm.next().now_or_never(),
            Some(Some(libp2p::swarm::SwarmEvent::NewListenAddr { .. }))
        ) {}

        Self {
            swarm,
            outbound_messages: HashMap::new(),
            outbound_messages_lookup: HashMap::new(),

            self_events: VecDeque::new(),

            peers: {
                let mut peers = HashSet::new();
                peers.insert(monad_executor::PeerId(local_peer_id.try_into().unwrap()));
                peers
            },
        }
    }

    #[cfg(feature = "tokio")]
    pub async fn with_tokio_executor(
        identity: libp2p::identity::Keypair,
        address: Multiaddr,
        timeout: Duration,
        keepalive: Duration,
    ) -> Self {
        use libp2p::core::muxing::StreamMuxerBox;
        let transport = libp2p::quic::tokio::Transport::new(libp2p::quic::Config::new(&identity))
            .map(|(peer_id, muxer), _| (peer_id, StreamMuxerBox::new(muxer)))
            .boxed();

        let pubkey = identity.public();
        let behavior = Behavior::new(&pubkey, timeout, keepalive);

        let local_peer_id: libp2p::PeerId = pubkey.into();
        let mut swarm = SwarmBuilder::with_tokio_executor(transport, behavior, local_peer_id)
            .substream_upgrade_protocol_override(libp2p::core::upgrade::Version::V1Lazy)
            .build();
        swarm.listen_on(address).unwrap();

        // TODO is it ok to discard these events until NewListenAddr
        while !matches!(
            swarm.next().await,
            Some(libp2p::swarm::SwarmEvent::NewListenAddr { .. })
        ) {}

        Self {
            swarm,
            outbound_messages: HashMap::new(),
            outbound_messages_lookup: HashMap::new(),

            self_events: VecDeque::new(),

            peers: {
                let mut peers = HashSet::new();
                peers.insert(monad_executor::PeerId(local_peer_id.try_into().unwrap()));
                peers
            },
        }
    }

    pub fn add_peer(&mut self, peer: &libp2p::PeerId, address: Multiaddr) {
        self.peers
            .insert(monad_executor::PeerId((*peer).try_into().unwrap()));

        self.swarm
            .behaviour_mut()
            .request_response
            .add_address(peer, address)
    }
    pub fn local_peer_id(&self) -> &libp2p::PeerId {
        self.swarm.local_peer_id()
    }
    pub fn listeners(&self) -> impl Iterator<Item = &Multiaddr> {
        self.swarm.listeners()
    }

    pub fn publish_message(&mut self, to: &monad_executor::PeerId, message: OM) {
        let to_libp2p: libp2p::PeerId = (&to.0).into();
        if self.swarm.local_peer_id() == &to_libp2p {
            // we need special case send to self
            // this is because dialing to self will fail
            self.self_events.push_back(message.into().event(*to));
            return;
        }
        let id = message.as_ref().id();
        let message = Arc::new(WrappedMessage::Send(message));
        let request_id = self
            .swarm
            .behaviour_mut()
            .request_response
            .send_request(&to_libp2p, message.clone());
        self.outbound_messages.insert(request_id, message);
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

impl<M, OM, TC> Executor for Service<M, OM, TC>
where
    M: Message + Deserializable + Send + Sync + 'static,
    <M as Deserializable>::ReadError: 'static,
    OM: Serializable + Send + Sync + 'static,
    TC: TransactionCollection,

    OM: Into<M> + AsRef<M> + Clone,
{
    type Command = RouterCommand<M, OM>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                RouterCommand::Publish { target, message } => {
                    let peers = match target {
                        RouterTarget::Broadcast => self.peers.iter().copied().collect(),
                        RouterTarget::PointToPoint(peer) => vec![peer],
                    };
                    for to in peers {
                        self.publish_message(&to, message.clone())
                    }
                }
                RouterCommand::Unpublish { target, id } => {
                    let peers = match target {
                        RouterTarget::Broadcast => self.peers.iter().copied().collect(),
                        RouterTarget::PointToPoint(peer) => vec![peer],
                    };
                    for to in peers {
                        self.unpublish_message(&to, &id)
                    }
                }
            }
        }
    }
}

impl<M, OM, TC> Stream for Service<M, OM, TC>
where
    M: Message + Deserializable + Send + Sync + 'static,
    <M as Deserializable>::ReadError: 'static,
    OM: AsRef<M> + Serializable + Send + Sync + 'static,
    TC: TransactionCollection,

    OM: Into<M> + AsRef<M>,
    Self: Unpin,
{
    type Item = M::Event<TC>;

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
                            // We don't need to respond if the peer isn't using a valid secp256k1 key
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
                            if let Some(message) = service.outbound_messages.remove(&request_id) {
                                service
                                    .outbound_messages_lookup
                                    .remove(&(peer, message.id()))
                                    .expect("outbound_messages_lookup out of sync");
                            }
                        }
                    }
                }
                libp2p::swarm::SwarmEvent::Behaviour(behavior::BehaviorEvent::RequestResponse(
                    libp2p::request_response::Event::OutboundFailure {
                        peer,
                        request_id: _,
                        error: e,
                    },
                )) => {
                    todo!(
                        "TODO ({:?}) schedule retry to: {:?}, err: {:?}",
                        self.swarm.local_peer_id(),
                        peer,
                        e
                    )
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
    use std::{
        array::TryFromSliceError,
        collections::{BTreeMap, HashSet},
    };

    use crate::Service;
    use monad_consensus_types::transaction::{MockTransactions, TransactionCollection};
    use monad_executor::Message;
    use monad_types::{Deserializable, Serializable};

    use futures::StreamExt;

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct TestMessage(u64);

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    enum TestEvent {
        Message(monad_executor::PeerId, TestMessage),
    }

    impl Message for TestMessage {
        type Event<TC: TransactionCollection> = TestEvent;

        type Id = u64;

        fn id(&self) -> Self::Id {
            self.0
        }

        fn event<TC: TransactionCollection>(self, from: monad_executor::PeerId) -> Self::Event<TC> {
            Self::Event::<TC>::Message(from, self)
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
            self
        }
    }

    fn create_random_node() -> (
        monad_executor::PeerId,
        Service<TestMessage, TestMessage, MockTransactions>,
    ) {
        let keypair = libp2p::identity::Keypair::generate_secp256k1();
        let public = keypair.public();
        let service =
            Service::<TestMessage, TestMessage, MockTransactions>::without_executor(keypair);
        let libp2p_peer_id = public.to_peer_id();
        (
            monad_executor::PeerId(libp2p_peer_id.try_into().unwrap()),
            service,
        )
    }

    fn create_random_nodes(
        num: usize,
    ) -> BTreeMap<monad_executor::PeerId, Service<TestMessage, TestMessage, MockTransactions>> {
        let mut nodes: BTreeMap<_, _> = std::iter::repeat_with(create_random_node)
            .take(num)
            .collect();

        let peers: Vec<_> = nodes.keys().copied().collect();

        for peer_1 in &peers {
            let mut node_1 = nodes.remove(peer_1).unwrap();
            for peer_2 in &peers {
                if peer_1 == peer_2 {
                    continue;
                }
                let node_2 = nodes.get(peer_2).unwrap();

                for address in node_2.swarm.listeners().cloned() {
                    node_1.add_peer(node_2.local_peer_id(), address)
                }
            }
            nodes.insert(*peer_1, node_1);
        }

        nodes
    }

    #[test]
    fn test_send_self() {
        let (peer_id, mut service) = create_random_node();
        let message = TestMessage(0);
        service.publish_message(&peer_id, message.clone());

        let mut expected_events: HashSet<_> = vec![TestEvent::Message(peer_id, message)]
            .into_iter()
            .collect();

        while !expected_events.is_empty() {
            let event = futures::executor::block_on(service.next()).unwrap();
            expected_events.remove(&event);
        }
    }

    #[test]
    fn test_send_other() {
        let mut nodes = create_random_nodes(2);
        let (peer_id_1, peer_id_2) = {
            let mut peer_ids = nodes.keys().copied();
            (peer_ids.next().unwrap(), peer_ids.next().unwrap())
        };
        let message = TestMessage(0);
        {
            let service_1 = nodes.get_mut(&peer_id_1).unwrap();
            service_1.publish_message(&peer_id_2, message.clone());
        }

        let mut expected_events: HashSet<_> =
            vec![(peer_id_2, TestEvent::Message(peer_id_1, message))]
                .into_iter()
                .collect();

        while !expected_events.is_empty() {
            // this future resolves to the next available event (across all nodes)
            let fut = futures::future::select_all(nodes.iter_mut().map(|(peer_id, node)| {
                let fut = async { (*peer_id, node.next().await.unwrap()) };
                Box::pin(fut)
            }));
            let ((peer_id, event), _, _) = futures::executor::block_on(fut);

            expected_events.remove(&(peer_id, event));
        }
    }
}
