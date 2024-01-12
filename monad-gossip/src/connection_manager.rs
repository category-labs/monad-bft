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

    use bytes::Bytes;
    use monad_crypto::{
        certificate_signature::{CertificateSignature, CertificateSignaturePubKey},
        hasher::{Hasher, HasherType},
        NopSignature,
    };
    use monad_types::NodeId;

    use crate::{
        mock::{MockGossip, MockGossipConfig},
        ConnectionManager, ConnectionManagerEvent, GossipEvent,
    };

    type SignatureType = NopSignature;

    struct Swarm {
        connection_manager:
            ConnectionManager<MockGossip<CertificateSignaturePubKey<SignatureType>>>,
        me: NodeId<CertificateSignaturePubKey<SignatureType>>,
        other: NodeId<CertificateSignaturePubKey<SignatureType>>,
    }

    impl Swarm {
        fn create() -> Self {
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
            let me = all_peers[0];
            let other = all_peers[1];
            let gossip = MockGossipConfig { all_peers, me }.build();
            let connection_manager = ConnectionManager::new(gossip);

            Self {
                connection_manager,
                me,
                other,
            }
        }
    }

    #[test]
    fn test_send_disconnected() {
        let mut swarm = Swarm::create();
        swarm.connection_manager.send(
            Duration::ZERO,
            monad_types::RouterTarget::PointToPoint(swarm.other),
            Bytes::new(),
        );

        assert!(
            matches!(swarm.connection_manager.poll(Duration::ZERO), Some(ConnectionManagerEvent::RequestConnect(other)) if other == swarm.other)
        );
        // is not connected, so nothing else should happen (no sends)
        assert!(swarm.connection_manager.poll(Duration::ZERO).is_none());

        // now we connect
        swarm
            .connection_manager
            .connected(Duration::ZERO, swarm.other);
        // retry same send
        swarm.connection_manager.send(
            Duration::ZERO,
            monad_types::RouterTarget::PointToPoint(swarm.other),
            Bytes::new(),
        );
        // should emit a Send event
        assert!(
            matches!(swarm.connection_manager.poll(Duration::ZERO), Some(ConnectionManagerEvent::GossipEvent(GossipEvent::Send(other, _))) if other == swarm.other)
        );
        // no RequestConnect event
        assert!(swarm.connection_manager.poll(Duration::ZERO).is_none());
    }

    #[test]
    fn test_disconnected_clears_sends() {
        let mut swarm = Swarm::create();

        swarm
            .connection_manager
            .connected(Duration::ZERO, swarm.other);
        swarm.connection_manager.send(
            Duration::ZERO,
            monad_types::RouterTarget::PointToPoint(swarm.other),
            Bytes::new(),
        );
        // we disconnect before we poll the Send event
        swarm
            .connection_manager
            .disconnected(Duration::ZERO, swarm.other);

        // Send event got replaced with RequestConnect
        assert!(
            matches!(swarm.connection_manager.poll(Duration::ZERO), Some(ConnectionManagerEvent::RequestConnect(other)) if other == swarm.other)
        );
        assert!(swarm.connection_manager.poll(Duration::ZERO).is_none());
    }

    #[test]
    #[should_panic]
    fn test_receive_disconnected() {
        let mut swarm = Swarm::create();
        // we didn't call handle_unframed_gossip_message first
        swarm.connection_manager.handle_unframed_gossip_message(
            Duration::ZERO,
            swarm.other,
            Bytes::new(),
        )
    }
}
