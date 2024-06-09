use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::Duration,
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use monad_crypto::{
    certificate_signature::PubKey,
    hasher::{Hasher, HasherType},
};
use monad_types::{NodeId, RouterTarget};
use rand::{seq::SliceRandom, SeedableRng};
use rand_chacha::ChaCha8Rng;
use serde::{Deserialize, Serialize};

use super::{Gossip, GossipEvent};
use crate::{AppMessage, FragmentedGossipMessage, GossipMessage};

type MsgId = [u8; 32];
type PartIndex = u16;

#[derive(Clone, Deserialize, Serialize)]
struct Header {
    message_len: u32,
    id: MsgId,
    root: Vec<u8>,
    part: PartIndex,
    direct: bool,
}

#[derive(Deserialize, Serialize)]
struct OuterHeader(u32);
const OUTER_HEADER_SIZE: usize = std::mem::size_of::<OuterHeader>();

pub struct BroadcastTreeConfig<PT: PubKey> {
    pub all_peers: Vec<NodeId<PT>>,
    pub my_id: NodeId<PT>,
    pub tree_arity: usize,
    pub num_routes: usize,
}

impl<PT: PubKey> BroadcastTreeConfig<PT> {
    pub fn build(mut self) -> BroadcastTree<PT> {
        assert!(self.num_routes > 0);
        assert!(self.tree_arity > 1);
        self.all_peers.sort();
        BroadcastTree {
            config: self,
            msg_cache: HashMap::new(),
            completed_msgs: HashSet::new(),
            events: VecDeque::default(),
            current_tick: Duration::ZERO,
        }
    }
}

// BroadcastTree implements message broadcasting by dividing application messages
// into multiple parts, and send those parts over different trees where each node
// in the tree is responsible for delivering data to its children.
//
// TODO
// if message parts arrive, is there a way to verify if its valid? if msg is corrupt,
// but the header is fine, it will mess up the message reconstruction
pub struct BroadcastTree<PT: PubKey> {
    config: BroadcastTreeConfig<PT>,

    msg_cache: HashMap<MsgId, MsgCache<PT>>,
    // TODO garbage collect complete_msgs
    completed_msgs: HashSet<MsgId>,

    events: VecDeque<GossipEvent<PT>>,
    current_tick: Duration,
}

struct MsgCache<PT: PubKey> {
    route: Vec<Vec<NodeId<PT>>>,
    parts: HashMap<PartIndex, Bytes>,
}

impl<PT: PubKey> MsgCache<PT> {
    fn new(route: Vec<Vec<NodeId<PT>>>) -> Self {
        Self {
            route,
            parts: Default::default(),
        }
    }
}

impl<PT: PubKey> BroadcastTree<PT> {
    // The broadcast is done through a k-arity tree for a given root node.
    // msgid is used to seed a random order of the non-root peers, and from
    // that ordering, a k-arity tree can be formed.
    // A node only needs to know about the child nodes it is responsible for
    // messaging
    // children of a node at index i are at indices (i*k)+1 through (i*k)+k
    fn calculate_route(&self, root: NodeId<PT>, msgid: MsgId) -> Vec<Vec<NodeId<PT>>> {
        let mut rng = ChaCha8Rng::from_seed(msgid);
        let mut peerlist = self.config.all_peers.clone();

        let mut result = Vec::new();
        let k = self.config.tree_arity;
        for _ in 0..self.config.num_routes {
            peerlist.shuffle(&mut rng);
            let root_idx = peerlist
                .iter()
                .position(|&x| x == root)
                .expect("root must be in the peerlist");
            peerlist.swap(0, root_idx);

            let my_idx = peerlist
                .iter()
                .position(|&x| x == self.config.my_id)
                .expect("self must be in the peerlist");

            let a = my_idx * k;
            if a >= peerlist.len() {
                // explicitly set empty vec for no children so that we use
                // the correct route for a given part index
                result.push(vec![]);
                continue;
            }

            let b = if a + k >= peerlist.len() {
                peerlist.len() - 1
            } else {
                a + k
            };

            let children = &peerlist[a + 1..=b];
            result.push(children.to_vec());
        }

        result
    }

    fn handle_message_part(
        &mut self,
        from: NodeId<PT>,
        message_header: Header,
        message_part: GossipMessage,
    ) {
        if message_header.direct {
            // direct message should be emitted to application and does not
            // need to be sent anywhere else
            self.events.push_back(GossipEvent::Emit(from, message_part));
            return;
        }

        // send the part to the designated routes
        // calculate the route based on the root sender of the part
        let root = match PT::from_bytes(&message_header.root) {
            Ok(k) => k,
            Err(_) => return, // TODO, someone sending a bad gossip_header, evidence?
        };
        if !self.msg_cache.contains_key(&message_header.id) {
            let route = self.calculate_route(NodeId::new(root), message_header.id);
            self.msg_cache
                .insert(message_header.id, MsgCache::new(route));
        }
        let cache = self
            .msg_cache
            .get(&message_header.id)
            .expect("msg_cache must have been initialized");
        let routes = &cache.route;
        let route_idx = message_header.part as usize;

        let children = &routes[route_idx];
        for node in children {
            let gossip_messages = Self::create_gossip_message(&message_header, &message_part);
            self.events
                .push_back(GossipEvent::Send(*node, gossip_messages));
        }

        if !self.completed_msgs.contains(&message_header.id) {
            self.try_combine_message_part(NodeId::new(root), message_header, message_part);
        }
    }

    // TODO this is assuming no erasure encoding and one part per route
    fn try_combine_message_part(
        &mut self,
        from_peer: NodeId<PT>,
        message_header: Header,
        message_part: GossipMessage,
    ) {
        let cache = self
            .msg_cache
            .get_mut(&message_header.id)
            .expect("handle_message_part should have initialized the msg_cache");
        let part_list = &mut cache.parts;
        part_list.insert(message_header.part, message_part);

        if part_list.len() >= self.config.num_routes {
            let mut combined_msg = BytesMut::new();
            for i in 0..self.config.num_routes as PartIndex {
                combined_msg.extend_from_slice(part_list.get_mut(&i).unwrap());
            }

            self.completed_msgs.insert(message_header.id);
            self.msg_cache.remove(&message_header.id);
            self.events
                .push_back(GossipEvent::Emit(from_peer, combined_msg.into()));
        }
    }

    fn create_gossip_message(header: &Header, message: &GossipMessage) -> FragmentedGossipMessage {
        let mut inner_header_buf = BytesMut::new().writer();
        bincode::serialize_into(&mut inner_header_buf, header)
            .expect("serializing gossip header should succeed");
        let inner_header_buf: Bytes = inner_header_buf.into_inner().into();

        let outer_header = OuterHeader(inner_header_buf.len() as u32);
        let mut outer_header_buf = BytesMut::new().writer();
        bincode::serialize_into(&mut outer_header_buf, &outer_header)
            .expect("serializing outer header should succeed");
        let outer_header_buf: Bytes = outer_header_buf.into_inner().into();

        std::iter::once(outer_header_buf)
            .chain(std::iter::once(inner_header_buf))
            .chain(std::iter::once(message.clone()))
            .collect()
    }

    fn create_gossip_chunk(
        &self,
        message: Bytes,
        msgid: MsgId,
        root: Vec<u8>,
        part: PartIndex,
        direct: bool,
    ) -> FragmentedGossipMessage {
        let header = Header {
            message_len: message.len() as u32,
            id: msgid,
            root,
            part,
            direct,
        };
        Self::create_gossip_message(&header, &message)
    }

    // Create a vector of gossip messages from the original app message
    fn create_broadcast_gossip_message(
        &self,
        msgid: MsgId,
        root_id: Vec<u8>,
        mut message: AppMessage,
    ) -> Vec<FragmentedGossipMessage> {
        let chunk_size = message.len().div_ceil(self.config.num_routes);

        let mut chunks = Vec::new();
        let mut i = 0;
        while !message.is_empty() {
            let app_chunk = message.copy_to_bytes(chunk_size.min(message.remaining()));
            let chunk =
                self.create_gossip_chunk(app_chunk, msgid, root_id.clone(), i as PartIndex, false);
            chunks.push(chunk);

            i += 1;
        }
        chunks
    }

    // to our designated children for the tree routes calculated from the
    // msgid
    fn send_broadcast_gossip_messages(
        &mut self,
        gossip_messages: Vec<FragmentedGossipMessage>,
        root: NodeId<PT>,
        msgid: MsgId,
    ) {
        let routes = self.calculate_route(root, msgid);
        // for each chunk, use one of the routes -- if the route is empty, do nothing
        // FIXME this assertion breaks for small messages
        assert_eq!(routes.len(), gossip_messages.len());
        for (route, gossip_message) in routes.iter().zip(gossip_messages.into_iter()) {
            for child in route {
                self.events
                    .push_back(GossipEvent::Send(*child, gossip_message.clone()));
            }
        }
    }
}

// application level message -> gossip level conversion
// if we break a message into parts, need to have a msg_id and part index
// also, need to know the root sender of a message so we can figure out the correct root bcast tree to use
impl<PT: PubKey> Gossip for BroadcastTree<PT> {
    type NodeIdPubKey = PT;
    fn send(&mut self, time: std::time::Duration, to: RouterTarget<PT>, message: AppMessage) {
        self.current_tick = time;

        let msgid = {
            let _hash_span =
                tracing::debug_span!("broadcast_tree_hash_span", message_len = message.len())
                    .entered();
            let mut hasher = HasherType::new();
            hasher.update(&message);
            hasher.hash().0
        };

        let root_id_bytes = self.config.my_id.pubkey().bytes();

        match to {
            RouterTarget::Broadcast(_, _) | RouterTarget::Raptorcast(_, _) => {
                // emit message to self, obviously no need to break into chunks
                self.events
                    .push_back(GossipEvent::Emit(self.config.my_id, message.clone()));

                let gossip_messages =
                    self.create_broadcast_gossip_message(msgid, root_id_bytes, message);
                self.send_broadcast_gossip_messages(gossip_messages, self.config.my_id, msgid);
            }
            // direct messages will not be sent as chunks
            RouterTarget::PointToPoint(to) => {
                if to == self.config.my_id {
                    self.events
                        .push_back(GossipEvent::Emit(self.config.my_id, message))
                } else {
                    let gossip_message =
                        self.create_gossip_chunk(message, msgid, root_id_bytes, 0, true);
                    self.events.push_back(GossipEvent::Send(to, gossip_message));
                }
            }
        }
    }

    // raw data is a stream of bytes and is not guaranteed to end on the boundary of a
    // gossip message, so we have to parse through the stream to find a gossip message
    // and store any leftover bytes
    fn handle_gossip_message(
        &mut self,
        time: Duration,
        from: NodeId<PT>,
        mut gossip_message: GossipMessage,
    ) {
        self.current_tick = time;

        let outer_header: OuterHeader =
            bincode::deserialize(&gossip_message.copy_to_bytes(OUTER_HEADER_SIZE)).unwrap();
        let header: Header =
            bincode::deserialize(&gossip_message.copy_to_bytes(outer_header.0 as usize)).unwrap();
        let message_part = gossip_message.copy_to_bytes(header.message_len as usize);
        self.handle_message_part(from, header, message_part)
    }

    fn peek_tick(&self) -> Option<Duration> {
        if !self.events.is_empty() {
            Some(self.current_tick)
        } else {
            None
        }
    }

    fn poll(&mut self, time: Duration) -> Option<GossipEvent<PT>> {
        assert!(time >= self.current_tick);
        self.events.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::{Buf, Bytes};
    use monad_crypto::{
        certificate_signature::CertificateKeyPair,
        hasher::{Hasher, HasherType},
        NopKeyPair, NopSignature,
    };
    use monad_transformer::{BytesTransformer, LatencyTransformer};
    use monad_types::{Epoch, NodeId, Round, RouterTarget};
    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;
    use test_case::test_case;

    use super::BroadcastTreeConfig;
    use crate::{
        testutil::{make_swarm, test_broadcast, test_direct},
        Gossip, GossipEvent,
    };

    #[test]
    fn test_gossip_split() {
        let peers: Vec<_> = (1..=2)
            .map(|idx| {
                let mut key = [idx; 32];
                let keypair = NopKeyPair::from_bytes(&mut key).unwrap();
                NodeId::new(keypair.pubkey())
            })
            .collect();
        let mut g = BroadcastTreeConfig {
            all_peers: peers.clone(),
            my_id: peers[0],
            tree_arity: 2,
            num_routes: 2,
        }
        .build();
        let app_message = "appmessagepayload".into();
        g.send(
            Duration::from_secs(1),
            RouterTarget::Broadcast(Epoch(0), Round(0)),
            app_message,
        );

        // with 2 nodes in the peerlist, peers[1] will always be a child of
        // peers[0] in the any broadcast tree route. There are 2 routes, so
        // we expect 3 Send events to peers[1], 1 for each route and 1 emit
        // to self
        assert_eq!(3, g.events.len());
        for e in g.events {
            match e {
                GossipEvent::Send(p, _) => {
                    assert_eq!(p, peers[1]);
                }
                GossipEvent::Emit(p, _) => {
                    assert_eq!(p, peers[0])
                }
            }
        }
    }

    #[test_case(1; "no split")]
    #[test_case(2; "even split")]
    #[test_case(5; "arbitrary split")]
    fn test_gossip_recombine(num_routes: usize) {
        let peers: Vec<_> = (1..=2)
            .map(|idx| {
                let mut key = [idx; 32];
                let keypair = NopKeyPair::from_bytes(&mut key).unwrap();
                NodeId::new(keypair.pubkey())
            })
            .collect();
        let mut g = BroadcastTreeConfig {
            all_peers: peers.clone(),
            my_id: peers[0],
            tree_arity: 2,
            num_routes,
        }
        .build();
        let app_message = "appmessagepayload".into();
        g.send(
            Duration::from_secs(1),
            RouterTarget::Broadcast(Epoch(0), Round(0)),
            app_message,
        );
        // expecting num_routes number of sends plus 1 event for emit to self
        assert_eq!(num_routes + 1, g.events.len());

        let events = g.events.drain(..).collect::<Vec<_>>();

        let mut receiver = BroadcastTreeConfig {
            all_peers: peers.clone(),
            my_id: peers[1],
            tree_arity: 2,
            num_routes,
        }
        .build();

        for e in events {
            match e {
                GossipEvent::Emit(_from, _msg) => continue,
                GossipEvent::Send(_from, mut msg) => {
                    receiver.handle_gossip_message(
                        Duration::ZERO,
                        g.config.my_id,
                        msg.copy_to_bytes(msg.remaining()),
                    );
                }
            }
        }

        assert_eq!(1, receiver.events.len());
        match receiver
            .events
            .pop_front()
            .expect("there should be an emit event")
        {
            GossipEvent::Send(_, _) => (),
            GossipEvent::Emit(_from, msg) => {
                assert_eq!(
                    "appmessagepayload",
                    String::from_utf8(msg.to_vec()).expect("bytes cannot have been corrupted")
                );
            }
        }
    }

    #[test_case(0)]
    #[test_case(1)]
    #[test_case(2)]
    #[test_case(3)]
    #[test_case(4)]
    fn test_gossip_recombine_multiple_messages(seed: u8) {
        let num_routes = 4;
        let expected_messages = [
            "appmessagepayload",
            "anotherappmessage",
            "differentappmessagepayload",
        ];
        let peers: Vec<_> = (1..=2)
            .map(|idx| {
                let mut key = [idx; 32];
                let keypair = NopKeyPair::from_bytes(&mut key).unwrap();
                NodeId::new(keypair.pubkey())
            })
            .collect();
        let mut g = BroadcastTreeConfig {
            all_peers: peers.clone(),
            my_id: peers[0],
            tree_arity: 2,
            num_routes,
        }
        .build();
        let app_messages = expected_messages
            .iter()
            .map(|s| s.as_bytes().into())
            .collect::<Vec<Bytes>>();
        for m in app_messages {
            g.send(
                Duration::ZERO,
                RouterTarget::Broadcast(Epoch(0), Round(0)),
                m,
            );
        }
        // plus 1 for the emit to self
        assert_eq!((num_routes + 1) * expected_messages.len(), g.events.len());

        let mut receiver = BroadcastTreeConfig {
            all_peers: peers.clone(),
            my_id: peers[1],
            tree_arity: 2,
            num_routes,
        }
        .build();

        // handle each individual message part
        for e in g.events {
            match e {
                GossipEvent::Emit(_from, _msg) => continue,
                GossipEvent::Send(_from, mut msg) => {
                    receiver.handle_gossip_message(
                        Duration::ZERO,
                        g.config.my_id,
                        msg.copy_to_bytes(msg.remaining()),
                    );
                }
            }
        }

        // check for the reconstructed expected messages
        let mut emit_cnt = 0;
        for e in receiver.events {
            match e {
                GossipEvent::Send(_, _) => (),
                GossipEvent::Emit(_from, msg) => {
                    assert!(expected_messages.contains(
                        &String::from_utf8(msg.to_vec())
                            .expect("bytes cannot have been corrupted")
                            .as_str()
                    ));
                    emit_cnt += 1;
                }
            }
        }
        assert_eq!(emit_cnt, expected_messages.len());
    }

    #[test]
    fn test_direct_send() {
        let peers: Vec<_> = (1..=1)
            .map(|idx| {
                let mut key = [idx; 32];
                let keypair = NopKeyPair::from_bytes(&mut key).unwrap();
                NodeId::new(keypair.pubkey())
            })
            .collect();
        let mut g = BroadcastTreeConfig {
            all_peers: peers.clone(),
            my_id: peers[0],
            tree_arity: 2,
            num_routes: 2,
        }
        .build();
        let app_message = "appmessagepayload".into();
        g.send(
            Duration::from_secs(1),
            RouterTarget::PointToPoint(peers[0]),
            app_message,
        );

        // Only one event - for send-to-self Emit
        assert_eq!(1, g.events.len());

        match g.events.pop_front().unwrap() {
            GossipEvent::Send(_from, _msg) => {
                panic!("direct send should not result in any gossiped sends")
            }
            GossipEvent::Emit(_from, msg) => {
                assert_eq!(
                    "appmessagepayload",
                    String::from_utf8(msg.to_vec()).expect("bytes cannot have been corrupted")
                );
            }
        }
    }

    #[test_case(2)]
    #[test_case(3)]
    #[test_case(4)]
    fn test_calculate_route(tree_arity: usize) {
        let mut peers: Vec<_> = (1..=20)
            .map(|idx| {
                let mut key = [idx; 32];
                let keypair = NopKeyPair::from_bytes(&mut key).unwrap();
                NodeId::new(keypair.pubkey())
            })
            .collect();
        peers.sort();
        let root_peer = peers[0];

        let mut all_children = Vec::new();
        for peer in peers.clone() {
            let g = BroadcastTreeConfig {
                all_peers: peers.clone(),
                my_id: peer,
                tree_arity,
                num_routes: 1,
            }
            .build();

            let app_message = "appmessagepayload".as_bytes();
            let mut hasher = HasherType::new();
            hasher.update(app_message);
            let msgid = hasher.hash().0;
            let routes = g.calculate_route(root_peer, msgid);

            let mut children = routes.into_iter().flatten().collect::<Vec<_>>();
            all_children.append(&mut children);
        }
        all_children.sort();
        // for given msgid from a fixed root, every peer should show
        // up in a child route somewhere
        assert_eq!(&all_children[..], &peers[1..]);
    }

    #[test]
    fn test_calculate_multiple_routes() {
        let num_routes = 5;
        let mut peers: Vec<_> = (1..=15)
            .map(|idx| {
                let mut key = [idx; 32];
                let keypair = NopKeyPair::from_bytes(&mut key).unwrap();
                NodeId::new(keypair.pubkey())
            })
            .collect();
        peers.sort();
        let root_peer = peers[0];
        let g = BroadcastTreeConfig {
            all_peers: peers.clone(),
            my_id: peers[1],
            tree_arity: 2,
            num_routes,
        }
        .build();

        let app_message = "appmessagepayload".as_bytes();
        let mut hasher = HasherType::new();
        hasher.update(app_message);
        let msgid = hasher.hash().0;
        let routes = g.calculate_route(root_peer, msgid);
        assert_eq!(num_routes, routes.len());

        // with 15 nodes total, any node in binary tree will have either
        // exactly 2 children or 0
        for r in routes {
            assert!(2 == r.len() || r.is_empty());
        }
    }

    #[test_case(1, 2)]
    #[test_case(3, 2)]
    #[test_case(3, 3)]
    #[test_case(3, 5)]
    fn test_framed_messages(num_routes: usize, tree_arity: usize) {
        let mut swarm = make_swarm::<NopSignature, _>(
            53,
            |all_peers, me| {
                BroadcastTreeConfig {
                    all_peers: all_peers.to_vec(),
                    my_id: *me,
                    tree_arity,
                    num_routes,
                }
                .build()
            },
            |_all_peers, _me| {
                vec![BytesTransformer::Latency(LatencyTransformer::new(
                    Duration::from_millis(5),
                ))]
            },
        );

        let mut rng = ChaCha8Rng::from_seed([0; 32]);
        test_broadcast(
            &mut rng,
            &mut swarm,
            Duration::from_secs(1),
            1024,
            usize::MAX,
            1.0,
        );
        test_direct(&mut rng, &mut swarm, Duration::from_secs(1), 1024);
    }
}
