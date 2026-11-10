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

use std::{future::poll_fn, num::NonZeroU32};

use alloy_rlp::{RlpDecodable, RlpEncodable};
use governor::Quota;
use monad_message_rate_limit::Protocol;
use monad_secp::{KeyPair, SecpSignature};
use monad_testutil::signing::get_key;
use rstest::rstest;

use super::{
    auth::{AuthenticationProtocol, WireAuthProtocol},
    *,
};

type TestNodeId = NodeId<CertificateSignaturePubKey<SecpSignature>>;
type TestEvent = RaptorCastEvent<(TestNodeId, TestMessage), SecpSignature>;
type TestRouter = RaptorCast<
    SecpSignature,
    TestMessage,
    TestMessage,
    TestEvent,
    NopDiscovery<SecpSignature>,
    WireAuthProtocol,
    NopScore<TestNodeId>,
>;

#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable)]
struct TestMessage {
    id: u32,
}

impl Message for TestMessage {
    type NodeIdPubKey = CertificateSignaturePubKey<SecpSignature>;
    type Event = (TestNodeId, Self);

    fn event(self, from: TestNodeId) -> Self::Event {
        (from, self)
    }
}

fn routers() -> Vec<TestRouter> {
    let keys: Vec<Arc<KeyPair>> = (1..=3)
        .map(|seed| Arc::new(get_key::<SecpSignature>(seed)))
        .collect();
    let dataplanes: Vec<_> = keys
        .iter()
        .map(|_| create_dataplane_for_tests(true))
        .collect();
    let name_records: HashMap<_, _> = keys
        .iter()
        .zip(&dataplanes)
        .map(|(key, dp)| {
            (
                NodeId::new(key.pubkey()),
                MonadNameRecord::new(
                    NameRecord::new_with_ports(
                        *dp.auth_addr.ip(),
                        dp.tcp_addr.port(),
                        None, // Exercise WireAuth rather than unauthenticated UDP.
                        dp.auth_addr.port(),
                        dp.direct_udp_addr.map(|addr| addr.port()),
                        None,
                        1,
                    ),
                    key.as_ref(),
                ),
            )
        })
        .collect();
    let known_addresses: HashMap<_, _> = keys
        .iter()
        .zip(&dataplanes)
        .map(|(key, dp)| (NodeId::new(key.pubkey()), dp.auth_addr))
        .collect();
    keys.into_iter()
        .zip(dataplanes)
        .map(|(key, mut dp)| {
            let direct_socket = dp.direct_udp_socket.take().unwrap();
            let mut router = new_wireauth_raptorcast_for_tests(
                dp,
                known_addresses.clone(),
                key.clone(),
                Epoch(0),
            );
            router.peer_discovery_driver =
                Arc::new(Mutex::new(PeerDiscoveryDriver::new(NopDiscoveryBuilder {
                    known_addresses: known_addresses.clone(),
                    name_records: name_records.clone(),
                    ..Default::default()
                })));
            let protocol = WireAuthProtocol::new(
                &auth::metrics::DIRECT_UDP_METRICS,
                monad_wireauth::Config::default(),
                key,
            );
            router.direct_udp_transport = Some(auth::FramedAuthenticatedSocketHandle::new(
                auth::AuthenticatedSocketHandle::new(direct_socket, protocol),
                auth::LeanUdpFramer::new(
                    NopScore::new(),
                    monad_leanudp::Config {
                        max_message_size: TX_FORWARD_DIRECT_UDP_MAX_MESSAGE_SIZE_BYTES,
                        max_fragment_payload: usize::from(
                            segment_size_for_mtu(DEFAULT_MTU)
                                .saturating_sub(WireAuthProtocol::HEADER_SIZE),
                        ),
                        ..Default::default()
                    },
                ),
            ));
            router
        })
        .collect()
}

async fn next_message(routers: &mut [TestRouter]) -> (TestNodeId, TestMessage) {
    tokio::time::timeout(
        Duration::from_secs(5),
        poll_fn(|cx| {
            for (index, router) in routers.iter_mut().enumerate() {
                match router.poll_next_unpin(cx) {
                    Poll::Ready(Some(RaptorCastEvent::Message(event))) => {
                        assert_eq!(index, 0, "only the receiver should get messages");
                        return Poll::Ready(event);
                    }
                    Poll::Pending => {}
                    _ => panic!("unexpected router event"),
                }
            }
            Poll::Pending
        }),
    )
    .await
    .expect("timed out waiting for an admitted message")
}

async fn wait_for_rejections(routers: &mut [TestRouter], expected: u64) {
    tokio::time::timeout(
        Duration::from_secs(5),
        poll_fn(|cx| {
            for router in routers.iter_mut() {
                assert!(
                    router.poll_next_unpin(cx).is_pending(),
                    "a message that should be rate limited was delivered",
                );
            }
            if routers[0]
                .metrics
                .gauge(COUNTER_RAPTORCAST_MESSAGES_RATE_LIMITED)
                .get()
                == expected
            {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }),
    )
    .await
    .expect("timed out waiting for a UDP rate-limit rejection");
}

#[rstest]
#[case::raptorcast_udp(false)]
#[case::direct_udp(true)]
#[tokio::test(flavor = "current_thread")]
async fn udp_limits_are_per_identity_and_tcp_does_not_use_them(#[case] direct: bool) {
    let mut routers = routers();
    let receiver = routers[0].self_id;
    let sender_a = routers[1].self_id;
    let sender_b = routers[2].self_id;
    // One token per hour keeps this test independent of runtime speed.
    routers[0].message_rate_limiter = RateLimiter::new(20_000);
    routers[0].message_rate_limiter.register(
        Protocol::Application,
        Quota::per_hour(NonZeroU32::new(1).unwrap()),
    );
    let udp_target = || {
        if direct {
            RouterTarget::DirectPointToPoint(receiver)
        } else {
            RouterTarget::PointToPoint(receiver)
        }
    };

    // TCP accepts more than the one-message UDP budget and does not spend it.
    for id in 100..102 {
        routers[1].exec(vec![RouterCommand::Publish {
            target: RouterTarget::TcpPointToPoint {
                to: receiver,
                completion: None,
            },
            message: TestMessage { id },
        }]);
        assert_eq!(
            next_message(&mut routers).await,
            (sender_a, TestMessage { id }),
        );
    }

    routers[1].exec(vec![RouterCommand::Publish {
        target: udp_target(),
        message: TestMessage { id: 1 },
    }]);
    assert_eq!(
        next_message(&mut routers).await,
        (sender_a, TestMessage { id: 1 }),
    );
    routers[1].exec(vec![RouterCommand::Publish {
        target: udp_target(),
        message: TestMessage { id: 2 },
    }]);
    wait_for_rejections(&mut routers, 1).await;

    // An exhausted UDP budget does not block TCP from the same identity.
    routers[1].exec(vec![RouterCommand::Publish {
        target: RouterTarget::TcpPointToPoint {
            to: receiver,
            completion: None,
        },
        message: TestMessage { id: 102 },
    }]);
    assert_eq!(
        next_message(&mut routers).await,
        (sender_a, TestMessage { id: 102 }),
    );

    // Another authenticated identity has its own allowance.
    routers[2].exec(vec![RouterCommand::Publish {
        target: udp_target(),
        message: TestMessage { id: 3 },
    }]);
    assert_eq!(
        next_message(&mut routers).await,
        (sender_b, TestMessage { id: 3 }),
    );
    routers[2].exec(vec![RouterCommand::Publish {
        target: udp_target(),
        message: TestMessage { id: 4 },
    }]);
    wait_for_rejections(&mut routers, 2).await;
}
