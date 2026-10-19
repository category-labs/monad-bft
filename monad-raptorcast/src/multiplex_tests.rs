// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

//! These tests use real IPv4 sockets and the production discovery and Wireauth
//! state machines. Shared-port cases configure no dedicated Direct UDP port;
//! the dedicated case also verifies that the sending switch controls that path.

use std::{
    collections::BTreeMap,
    net::{Ipv4Addr, SocketAddr},
    pin::Pin,
    sync::{Arc, Mutex},
    task::Poll,
    time::Duration,
};

use alloy_rlp::{RlpDecodable, RlpEncodable};
use bytes::Bytes;
use futures::{future::poll_fn, Stream};
use monad_executor::Executor;
use monad_executor_glue::{Message, RouterCommand};
use monad_peer_discovery::{
    discovery::{PeerDiscovery, PeerDiscoveryBuilder},
    driver::PeerDiscoveryDriver,
    Capability, MonadNameRecord, NameRecord,
};
use monad_secp::{KeyPair, PubKey as SecpPubKey, SecpSignature};
use monad_types::{Epoch, NodeId, Round, RouterTarget, Stake, UdpPriority};
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

use crate::{
    auth::{NopScore, Protocol, WireAuthProtocol},
    create_dataplane_for_tests,
    metrics::*,
    raptorcast_secondary::SecondaryRaptorCastModeConfig,
    RaptorCast, RaptorCastEvent,
};

const DEFAULT_SIG_VERIFICATION_RATE_LIMIT: u32 = 10_000;
#[derive(Clone, RlpEncodable, RlpDecodable)]
struct Payload {
    bytes: Bytes,
}

impl Message for Payload {
    type NodeIdPubKey = SecpPubKey;
    type Event = (NodeId<SecpPubKey>, Bytes);
    fn event(self, from: NodeId<SecpPubKey>) -> Self::Event {
        (from, self.bytes)
    }
}

type Router = RaptorCast<
    SecpSignature,
    Payload,
    Payload,
    RaptorCastEvent<(NodeId<SecpPubKey>, Bytes), SecpSignature>,
    PeerDiscovery<SecpSignature>,
    WireAuthProtocol,
    NopScore<NodeId<SecpPubKey>>,
>;

fn create_raptorcast_config(
    keypair: Arc<KeyPair>,
    sig_verification_rate_limit: u32,
) -> crate::config::RaptorCastConfig<SecpSignature> {
    crate::config::RaptorCastConfig {
        shared_key: keypair,
        mtu: monad_dataplane::udp::DEFAULT_MTU,
        udp_message_max_age_ms: u64::MAX,
        sig_verification_rate_limit,
        primary_instance: Default::default(),
        secondary_instance: monad_node_config::FullNodeRaptorCastConfig {
            enable_publisher: false,
            enable_client: false,
            raptor10_fullnode_redundancy_factor: 2f32,
            full_nodes_prioritized: monad_node_config::FullNodeConfig { identities: vec![] },
            round_span: monad_types::Round(10),
            invite_lookahead: monad_types::Round(5),
            max_invite_wait: monad_types::Round(3),
            deadline_round_dist: monad_types::Round(3),
            init_empty_round_span: monad_types::Round(1),
            max_group_size: 10,
            max_num_group: 5,
            invite_future_dist_min: monad_types::Round(1),
            invite_future_dist_max: monad_types::Round(5),
            invite_accept_heartbeat_ms: 100,
        },
        deterministic_protocol_rollout: crate::v1_rollout::CURRENT_STAGE,
    }
}

struct Pair {
    routers: [Router; 2],
    ids: [NodeId<SecpPubKey>; 2],
    records: [MonadNameRecord<SecpSignature>; 2],
    events: [Vec<(NodeId<SecpPubKey>, Bytes)>; 2],
}

impl Pair {
    fn new(enabled: [bool; 2]) -> Self {
        Self::with_transport(enabled, false)
    }

    fn with_transport(direct_udp: [bool; 2], dedicated: bool) -> Self {
        let keys = [1u8, 2].map(|seed| Arc::new(KeyPair::from_bytes(&mut [seed; 32]).unwrap()));
        let ids = keys.each_ref().map(|key| NodeId::new(key.pubkey()));
        let dataplanes = [
            create_dataplane_for_tests(dedicated),
            create_dataplane_for_tests(dedicated),
        ];
        let records = std::array::from_fn(|i| {
            let mut record = NameRecord::new_with_ports(
                Ipv4Addr::LOCALHOST,
                dataplanes[i].tcp_addr.port(),
                None,
                dataplanes[i].auth_addr.port(),
                dataplanes[i].direct_udp_addr.map(|addr| addr.port()),
                None,
                1,
            );
            if direct_udp[i] {
                record.set_capability(Capability::DirectUdp);
            }
            MonadNameRecord::new(record, &*keys[i])
        });
        let mut index = 0;
        let routers = dataplanes.map(|dataplane| {
            let i = index;
            index += 1;
            let builder = PeerDiscoveryBuilder {
                self_id: ids[i],
                self_record: records[i].clone(),
                current_round: Round(0),
                current_epoch: Epoch(0),
                epoch_validators: BTreeMap::from([(Epoch(0), ids.into())]),
                pinned_full_nodes: Default::default(),
                prioritized_full_nodes: Default::default(),
                bootstrap_peers: BTreeMap::from([(ids[1 - i], records[1 - i].clone())]),
                refresh_period: Duration::from_millis(100),
                request_timeout: Duration::from_secs(2),
                unresponsive_prune_threshold: 10,
                last_participation_prune_threshold: Round(100),
                min_num_peers: 0,
                max_num_peers: 10,
                max_group_size: 10,
                enable_publisher: false,
                enable_client: false,
                rng: ChaCha8Rng::seed_from_u64(i as u64),
                persisted_peers_path: Default::default(),
                ping_rate_limit_per_second: 1_000,
            };
            let config =
                create_raptorcast_config(keys[i].clone(), DEFAULT_SIG_VERIFICATION_RATE_LIMIT);
            let protocol = WireAuthProtocol::new(
                &crate::auth::metrics::UDP_METRICS,
                Default::default(),
                keys[i].clone(),
            );
            let dedicated_transport = dataplane.direct_udp_socket.map(|socket| {
                (
                    socket,
                    WireAuthProtocol::new(
                        &crate::auth::metrics::DIRECT_UDP_METRICS,
                        Default::default(),
                        keys[i].clone(),
                    ),
                )
            });
            let mut router = Router::new(
                config,
                SecondaryRaptorCastModeConfig::None,
                dataplane.tcp_socket,
                (dataplane.authenticated_socket, protocol),
                dedicated_transport,
                NopScore::new(),
                None,
                dataplane.control,
                Arc::new(Mutex::new(PeerDiscoveryDriver::new(builder))),
                Epoch(0),
                crate::dummy_proposer_schedule(),
            );
            router.exec(vec![RouterCommand::AddEpochValidatorSet {
                epoch: Epoch(0),
                epoch_start: Round(0),
                validator_set: ids.map(|id| (id, Stake::ONE)).into(),
            }]);
            router
        });
        Self {
            routers,
            ids,
            records,
            events: Default::default(),
        }
    }

    async fn until(&mut self, mut done: impl FnMut(&Self) -> bool) {
        tokio::time::timeout(
            Duration::from_secs(10),
            poll_fn(|cx| {
                for i in 0..2 {
                    for _ in 0..32 {
                        match Pin::new(&mut self.routers[i]).poll_next(cx) {
                            Poll::Ready(Some(RaptorCastEvent::Message(event))) => {
                                self.events[i].push(event)
                            }
                            Poll::Ready(Some(_)) => {}
                            Poll::Ready(None) => panic!("router stopped"),
                            Poll::Pending => break,
                        }
                    }
                }
                if done(self) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "real-socket test timed out: events {:?}, capabilities {:?}, connected {:?}",
                self.events,
                [
                    self.peer_supports_direct_udp(0),
                    self.peer_supports_direct_udp(1)
                ],
                (0..2)
                    .map(|i| self.routers[i].is_connected_to(
                        &SocketAddr::V4(self.records[1 - i].name_record.authenticated_udp_socket()),
                        &self.ids[1 - i].pubkey()
                    ))
                    .collect::<Vec<_>>()
            )
        });
    }

    async fn ready(&mut self) {
        self.until(|pair| {
            (0..2).all(|i| {
                pair.routers[i]
                    .peer_discovery_driver
                    .lock()
                    .unwrap()
                    .get_name_record(&pair.ids[1 - i])
                    .is_some()
                    && pair.routers[i].is_connected_to(
                        &SocketAddr::V4(pair.records[1 - i].name_record.authenticated_udp_socket()),
                        &pair.ids[1 - i].pubkey(),
                    )
            })
        })
        .await;
    }

    fn peer_supports_direct_udp(&self, i: usize) -> bool {
        self.routers[i]
            .peer_discovery_driver
            .lock()
            .unwrap()
            .get_name_record(&self.ids[1 - i])
            .is_some_and(|record| record.name_record.check_capability(Capability::DirectUdp))
    }

    fn send(&mut self, i: usize, target: RouterTarget<SecpPubKey>, data: Bytes) {
        self.routers[i].exec(vec![RouterCommand::Publish {
            target,
            message: Payload { bytes: data },
        }]);
    }

    fn metric(&mut self, i: usize, metric: &'static monad_executor::MetricDef) -> u64 {
        self.routers[i].metrics.gauge(metric).get()
    }
}

#[tokio::test]
async fn prepared_receiver_dispatches_tags_and_drops_unknown_or_malformed_frames() {
    let mut pair = Pair::new([false, false]);
    pair.ready().await;
    let public_key = pair.ids[1].pubkey();
    let socket = pair.routers[0].dual_socket.authenticated_mut();
    socket
        .write_with_protocol(
            &public_key,
            255u8,
            Bytes::from_static(b"unknown"),
            7,
            UdpPriority::Regular,
        )
        .unwrap();
    socket
        .write_with_protocol(
            &public_key,
            Protocol::DirectUdp,
            Bytes::from_static(b"bad"),
            3,
            UdpPriority::Regular,
        )
        .unwrap();
    // Bypass the explicit opt-out only in this test to exercise prepared readers.
    let payload =
        crate::message::OutboundRouterMessage::<Payload, SecpSignature>::AppMessage(Payload {
            bytes: Bytes::from_static(b"prepared decoder"),
        })
        .try_serialize()
        .unwrap();
    let router = &mut pair.routers[0];
    let packets: Vec<_> = <crate::auth::LeanUdpFramer<
        NodeId<SecpPubKey>,
        NopScore<NodeId<SecpPubKey>>,
    > as crate::auth::AuthPacketFramer<SecpPubKey>>::frame(
        &mut router.multiplexed_direct_udp,
        payload,
    )
    .unwrap()
    .collect();
    for packet in packets {
        let stride = packet.len() as u16;
        router
            .dual_socket
            .authenticated_mut()
            .write_with_protocol(
                &public_key,
                Protocol::DirectUdp,
                packet,
                stride,
                UdpPriority::Regular,
            )
            .unwrap();
    }
    pair.send(
        0,
        RouterTarget::PointToPoint(pair.ids[1]),
        Bytes::from_static(b"legacy still works"),
    );
    pair.until(|pair| pair.events[1].len() == 2).await;
    assert_eq!(
        pair.metric(1, COUNTER_RAPTORCAST_MULTIPLEX_UNKNOWN_PROTOCOL),
        1
    );
    assert_eq!(
        pair.metric(1, COUNTER_RAPTORCAST_MULTIPLEX_DIRECT_UDP_RECEIVED),
        1
    );
    assert!(pair.events[1].contains(&(pair.ids[0], Bytes::from_static(b"prepared decoder"))));
    assert!(pair.events[1].contains(&(pair.ids[0], Bytes::from_static(b"legacy still works"))));
}
