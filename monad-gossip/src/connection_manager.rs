use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};

use bytes::{Buf, Bytes};
use bytes_utils::SegmentedBuf;
use monad_crypto::certificate_signature::PubKey;
use monad_types::{NodeId, RouterTarget};

use crate::{AppMessage, Gossip, GossipEvent, GossipMessage};

pub enum ConnectionManagerEvent<PT: PubKey> {
    GossipEvent(GossipEvent<PT>),

    /// Ask executor to connect to given node
    /// Executor must call Gossip::connect after connection complete
    /// RequestConnect is idempotent
    RequestConnect(NodeId<PT>),
}

pub struct ConnectionManager<G: Gossip> {
    max_message_size: u32,

    current_tick: Duration,
    connections: HashMap<NodeId<G::NodeIdPubKey>, Connection>,
    gossip: G,
}

type MessageLenType = u32;
const MESSAGE_HEADER_LEN: usize = std::mem::size_of::<MessageLenType>();

/// Connection is used to provide a framing abstraction on top of the underlying transport. This is
/// because the QUIC stream abstraction is a continuous byte stream. Messages are framed by
/// prefixing each message with its length.
struct Connection {
    maybe_message_len: Option<MessageLenType>,
    buffer: SegmentedBuf<Bytes>,
}

impl Connection {
    fn new() -> Self {
        Self {
            maybe_message_len: None,
            buffer: SegmentedBuf::new(),
        }
    }
}

impl<G: Gossip> ConnectionManager<G> {
    pub fn new(gossip: G) -> Self {
        Self {
            max_message_size: u32::MAX,
            current_tick: Duration::ZERO,
            connections: HashMap::new(),
            gossip,
        }
    }

    /// Tell Gossip implementation that a connection to a peer has been opened
    pub fn connected(&mut self, time: Duration, peer: NodeId<G::NodeIdPubKey>) {
        assert!(time >= self.current_tick);
        self.current_tick = time;
        let removed = self.connections.insert(peer, Connection::new());
        assert!(removed.is_none());
    }

    /// Tell Gossip implementation that the given peer has been disconnected
    pub fn disconnected(&mut self, time: Duration, peer: NodeId<G::NodeIdPubKey>) {
        assert!(time >= self.current_tick);
        self.current_tick = time;
        let removed = self.connections.remove(&peer);
        assert!(removed.is_some());
    }

    /// Ask Gossip implementation to send given application message
    /// There are no delivery guarantees; the message may be dropped. A RequestConnect event may be
    /// emitted instead.
    pub fn send(&mut self, time: Duration, to: RouterTarget<G::NodeIdPubKey>, message: AppMessage) {
        assert!(time >= self.current_tick);
        self.current_tick = time;
        self.gossip.send(time, to, message)
    }

    /// Handle unframed gossip_message received from peer
    pub fn handle_unframed_gossip_message(
        &mut self,
        time: Duration,
        from: NodeId<G::NodeIdPubKey>,
        unframed_gossip_message: Bytes,
    ) {
        assert!(time >= self.current_tick);
        self.current_tick = time;

        let connection = self
            .connections
            .get_mut(&from)
            .expect("invariant: Gossip::connected must have been called before");
        connection.buffer.push(unframed_gossip_message);
        loop {
            if connection.maybe_message_len.is_none()
                && connection.buffer.remaining() >= MESSAGE_HEADER_LEN
            {
                connection.maybe_message_len = Some(MessageLenType::from_le_bytes(
                    connection
                        .buffer
                        .copy_to_bytes(MESSAGE_HEADER_LEN)
                        .to_vec()
                        .try_into()
                        .unwrap(),
                ));
            }
            if let Some(message_len) = connection.maybe_message_len {
                if message_len > self.max_message_size {
                    // clear buffer, message too big
                    connection.buffer = SegmentedBuf::new();

                    tracing::warn!(
                        "discarding message from={:?}, message_len={} too big",
                        from,
                        message_len
                    );
                    // TODO request disconnect
                    break;
                }
                if connection.buffer.remaining() >= message_len as usize {
                    // this copy could be avoided here, but deserialization down the line requires
                    // a contiguous bytes::Bytes anyways, so doesn't really matter
                    let gossip_message: GossipMessage =
                        connection.buffer.copy_to_bytes(message_len as usize);
                    self.gossip
                        .handle_gossip_message(time, from, gossip_message);
                    connection.maybe_message_len = None;
                    continue;
                }
            }
            break;
        }
    }

    pub fn peek_tick(&self) -> Option<Duration> {
        self.gossip.peek_tick()
    }
    pub fn poll(&mut self, time: Duration) -> Option<ConnectionManagerEvent<G::NodeIdPubKey>> {
        assert!(time >= self.current_tick);
        self.current_tick = time;

        let gossip_event = self.gossip.poll(time)?;
        let connection_manager_event = match gossip_event {
            GossipEvent::Send(to, _) if !self.connections.contains_key(&to) => {
                tracing::warn!(
                    "discarding message to={:?}, requesting connect",
                    to,
                );
                ConnectionManagerEvent::RequestConnect(to)
            }
            GossipEvent::Send(to, gossip_message) => {
                let gossip_message_header: Bytes =
                    Vec::from((gossip_message.remaining() as MessageLenType).to_le_bytes()).into();
                let mut framed_gossip_message = VecDeque::from(gossip_message);
                framed_gossip_message.push_front(gossip_message_header);
                ConnectionManagerEvent::GossipEvent(GossipEvent::Send(
                    to,
                    framed_gossip_message.into(),
                ))
            }
            gossip_event @ GossipEvent::Emit(_, _) => {
                ConnectionManagerEvent::GossipEvent(gossip_event)
            }
        };
        Some(connection_manager_event)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::{Buf, Bytes};
    use monad_crypto::{
        certificate_signature::{
            CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
        },
        hasher::{Hasher, HasherType},
        NopSignature,
    };
    use monad_types::NodeId;

    use crate::{
        mock::{MockGossip, MockGossipConfig},
        ConnectionManager, ConnectionManagerEvent, GossipEvent,
    };

    type SignatureType = NopSignature;

    struct Node {
        connection_manager:
            ConnectionManager<MockGossip<CertificateSignaturePubKey<SignatureType>>>,
        me: NodeId<CertificateSignaturePubKey<SignatureType>>,
    }

    impl Node {
        fn create_swarm() -> Vec<Self> {
            const NUM_NODES: usize = 2;
            let all_peers: Vec<_> = (1_u32..)
                .take(NUM_NODES)
                .map(|idx| {
                    let mut secret = {
                        let mut hasher = HasherType::new();
                        hasher.update(idx.to_le_bytes());
                        hasher.hash().0
                    };
                    let keypair = <SignatureType as CertificateSignature>::KeyPairType::from_bytes(
                        &mut secret,
                    )
                    .unwrap();
                    NodeId::new(keypair.pubkey())
                })
                .collect();

            all_peers
                .iter()
                .copied()
                .map(|me| {
                    let gossip = MockGossipConfig {
                        all_peers: all_peers.clone(),
                        me,
                    }
                    .build();
                    let connection_manager = ConnectionManager::new(gossip);

                    Self {
                        connection_manager,
                        me,
                    }
                })
                .collect()
        }
    }

    #[test]
    fn test_send_disconnected() {
        let mut swarm = Node::create_swarm();
        let mut node_1 = swarm.pop().unwrap();
        let node_2 = swarm.pop().unwrap();
        node_1.connection_manager.send(
            Duration::ZERO,
            monad_types::RouterTarget::PointToPoint(node_2.me),
            Bytes::new(),
        );

        assert!(
            matches!(node_1.connection_manager.poll(Duration::ZERO), Some(ConnectionManagerEvent::RequestConnect(other)) if other == node_2.me)
        );
        // is not connected, so nothing else should happen (no sends)
        assert!(node_1.connection_manager.poll(Duration::ZERO).is_none());

        // now we connect
        node_1
            .connection_manager
            .connected(Duration::ZERO, node_2.me);
        // retry same send
        node_1.connection_manager.send(
            Duration::ZERO,
            monad_types::RouterTarget::PointToPoint(node_2.me),
            Bytes::new(),
        );
        // should emit a Send event
        assert!(
            matches!(node_1.connection_manager.poll(Duration::ZERO), Some(ConnectionManagerEvent::GossipEvent(GossipEvent::Send(other, _))) if other == node_2.me)
        );
        // no RequestConnect event
        assert!(node_1.connection_manager.poll(Duration::ZERO).is_none());
    }

    #[test]
    fn test_disconnected_clears_sends() {
        let mut swarm = Node::create_swarm();
        let mut node_1 = swarm.pop().unwrap();
        let node_2 = swarm.pop().unwrap();

        node_1
            .connection_manager
            .connected(Duration::ZERO, node_2.me);
        node_1.connection_manager.send(
            Duration::ZERO,
            monad_types::RouterTarget::PointToPoint(node_2.me),
            Bytes::new(),
        );
        // we disconnect before we poll the Send event
        node_1
            .connection_manager
            .disconnected(Duration::ZERO, node_2.me);

        // Send event got replaced with RequestConnect
        assert!(
            matches!(node_1.connection_manager.poll(Duration::ZERO), Some(ConnectionManagerEvent::RequestConnect(other)) if other == node_2.me)
        );
        assert!(node_1.connection_manager.poll(Duration::ZERO).is_none());
    }

    #[test]
    #[should_panic]
    fn test_receive_disconnected() {
        let mut swarm = Node::create_swarm();
        let mut node_1 = swarm.pop().unwrap();
        let node_2 = swarm.pop().unwrap();
        // we didn't call handle_unframed_gossip_message first
        node_1.connection_manager.handle_unframed_gossip_message(
            Duration::ZERO,
            node_2.me,
            Bytes::new(),
        )
    }

    #[test]
    fn test_split_message() {
        let mut swarm = Node::create_swarm();
        let mut node_1 = swarm.pop().unwrap();
        let mut node_2 = swarm.pop().unwrap();

        let app_message_1_bytes: Bytes = [1, 2, 3].as_ref().into();
        let app_message_2_bytes: Bytes = [3, 4, 5].as_ref().into();

        // tx side
        node_1
            .connection_manager
            .connected(Duration::ZERO, node_2.me);
        node_1.connection_manager.send(
            Duration::ZERO,
            monad_types::RouterTarget::PointToPoint(node_2.me),
            app_message_1_bytes.clone(),
        );
        node_1.connection_manager.send(
            Duration::ZERO,
            monad_types::RouterTarget::PointToPoint(node_2.me),
            app_message_2_bytes.clone(),
        );
        let Some(ConnectionManagerEvent::GossipEvent(GossipEvent::Send(
            message_1_to,
            mut message_1,
        ))) = node_1.connection_manager.poll(Duration::ZERO)
        else {
            unreachable!("unexpected event");
        };
        assert_eq!(message_1_to, node_2.me);
        let Some(ConnectionManagerEvent::GossipEvent(GossipEvent::Send(message_2_to, message_2))) =
            node_1.connection_manager.poll(Duration::ZERO)
        else {
            unreachable!("unexpected event");
        };
        assert_eq!(message_2_to, node_2.me);
        assert!(node_1.connection_manager.poll(Duration::ZERO).is_none());

        // rx side
        node_2
            .connection_manager
            .connected(Duration::ZERO, node_1.me);
        node_2.connection_manager.handle_unframed_gossip_message(
            Duration::ZERO,
            node_1.me,
            // send half of first message
            message_1.copy_to_bytes(message_1.remaining() / 2),
        );
        // doesn't emit on incomplete message
        assert!(node_2.connection_manager.poll(Duration::ZERO).is_none());

        message_1.extend(message_2.into_inner());

        node_2.connection_manager.handle_unframed_gossip_message(
            Duration::ZERO,
            node_1.me,
            // send second half of first message + second message
            message_1.copy_to_bytes(message_1.remaining()),
        );

        let Some(ConnectionManagerEvent::GossipEvent(GossipEvent::Emit(
            message_1_from,
            app_message_1,
        ))) = node_2.connection_manager.poll(Duration::ZERO)
        else {
            unreachable!("unexpected event");
        };
        assert_eq!(message_1_from, node_1.me);
        let Some(ConnectionManagerEvent::GossipEvent(GossipEvent::Emit(
            message_2_from,
            app_message_2,
        ))) = node_2.connection_manager.poll(Duration::ZERO)
        else {
            unreachable!("unexpected event");
        };
        assert_eq!(message_2_from, node_1.me);
        assert!(node_2.connection_manager.poll(Duration::ZERO).is_none());

        assert_eq!(app_message_1, app_message_1_bytes);
        assert_eq!(app_message_2, app_message_2_bytes);
    }
}
