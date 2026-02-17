use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use alloy_eips::eip2718::Encodable2718;
use futures::StreamExt;
use monad_dataplane::{udp::DEFAULT_MTU, DataplaneBuilder, TcpSocketId, UdpSocketId};
use monad_eth_testutil::{make_eip1559_tx, secret_to_eth_address};
use monad_executor::Executor;
use monad_executor_glue::{OutboundForwardTxs, RouterCommand};
use monad_node_config::{fullnode_raptorcast::FullNodeRaptorCastConfig, FullNodeConfig};
use monad_peer_discovery::driver::PeerDiscoveryDriver;
use monad_peer_score::{ema, StdClock};
use monad_raptorcast::{
    auth::{AuthenticatedSocketHandle, LeanUdpSocketHandle, WireAuthProtocol},
    config::{RaptorCastConfig, RaptorCastConfigPrimary},
    raptorcast_secondary::SecondaryRaptorCastModeConfig,
    RaptorCast, RaptorCastEvent,
};
use monad_secp::KeyPair;
use monad_tfm::base_fee::MIN_BASE_FEE;
use monad_types::{Epoch, NodeId, Round, RouterTarget, Stake, UdpPriority};
use monad_wireauth::Config;

use crate::{
    message::{InboundMessage, OutboundMessage, SignatureType, WireEvent},
    transport::{NodeEndpointsV4, Transport},
    MultiSubmitArgs,
};

struct MultiSubmitRaptorCastEvent(RaptorCastEvent<WireEvent, SignatureType>);

impl From<RaptorCastEvent<WireEvent, SignatureType>> for MultiSubmitRaptorCastEvent {
    fn from(value: RaptorCastEvent<WireEvent, SignatureType>) -> Self {
        Self(value)
    }
}

fn create_raptorcast_config(keypair: Arc<KeyPair>) -> RaptorCastConfig<SignatureType> {
    RaptorCastConfig {
        shared_key: keypair,
        mtu: DEFAULT_MTU,
        udp_message_max_age_ms: 5000,
        sig_verification_rate_limit: 4_000,
        primary_instance: RaptorCastConfigPrimary::default(),
        secondary_instance: FullNodeRaptorCastConfig {
            enable_publisher: false,
            enable_client: false,
            full_nodes_prioritized: FullNodeConfig { identities: vec![] },
            raptor10_fullnode_redundancy_factor: 2.0,
            round_span: Round(10),
            invite_lookahead: Round(5),
            max_invite_wait: Round(3),
            deadline_round_dist: Round(3),
            init_empty_round_span: Round(1),
            max_group_size: 10,
            max_num_group: 5,
            invite_future_dist_min: Round(1),
            invite_future_dist_max: Round(5),
            invite_accept_heartbeat_ms: 100,
        },
    }
}

pub async fn run(args: MultiSubmitArgs) {
    let num = args.num_identities;
    let base = args.sender_index_base;
    tracing::info!(
        num_identities = num,
        sender_index_base = base,
        tps_per_identity = args.tps_per_identity,
        "multi-submit starting"
    );

    let args = Arc::new(args);
    let mut thread_handles = Vec::with_capacity(num);

    for i in 0..num {
        let args = args.clone();
        let identity_index = base + i;
        let stagger = Duration::from_millis(50 * i as u64);
        let handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to create runtime");
            rt.block_on(async {
                tokio::time::sleep(stagger).await;
                run_identity(args, identity_index).await;
            });
        });
        thread_handles.push(handle);
    }

    for h in thread_handles {
        let _ = h.join();
    }

    tracing::info!("multi-submit done");
}

async fn run_identity(args: Arc<MultiSubmitArgs>, identity_index: usize) {
    let bind_addr: std::net::SocketAddr = "0.0.0.0:0".parse().unwrap();
    let endpoints = NodeEndpointsV4 {
        rc_tcp_addr: args.rc_tcp_addr,
        rc_udp_addr: args.rc_udp_addr,
        rc_auth_udp_addr: args.rc_auth_udp_addr,
        leanudp_addr: args.leanudp_addr,
    };

    let keypair = crate::submit::submitter_keypair(identity_index);
    let keypair_arc = Arc::new(keypair);
    let self_node_id = NodeId::new(keypair_arc.pubkey());
    let wireauth_config = Config::default();
    let handshake_timeout = wireauth_config.session_timeout
        + wireauth_config.session_timeout_jitter
        + Duration::from_secs(5);

    let node_pubkey = crate::node::node_keypair().pubkey();
    let node_id = NodeId::new(node_pubkey);

    let nop_builder = endpoints.nop_discovery_builder_for_node(node_id, &keypair_arc);
    let pd = PeerDiscoveryDriver::new(nop_builder);

    let mut rc_udp_sockets = vec![(UdpSocketId::Raptorcast, bind_addr)];
    if args.transport == Transport::RaptorcastAuth {
        rc_udp_sockets.push((UdpSocketId::AuthenticatedRaptorcast, bind_addr));
    }

    let mut dp_rc = DataplaneBuilder::new(1000)
        .with_udp_multishot(true)
        .with_tcp_sockets([(TcpSocketId::Raptorcast, bind_addr)])
        .with_udp_sockets(rc_udp_sockets)
        .build();

    assert!(dp_rc.block_until_ready(Duration::from_secs(5)));

    let tcp_socket = dp_rc
        .tcp_sockets
        .take(TcpSocketId::Raptorcast)
        .expect("tcp socket");

    let raptorcast_auth_socket = if args.transport == Transport::RaptorcastAuth {
        Some(
            dp_rc
                .udp_sockets
                .take(UdpSocketId::AuthenticatedRaptorcast)
                .expect("raptorcast authenticated socket"),
        )
    } else {
        None
    };

    let non_auth_socket = dp_rc
        .udp_sockets
        .take(UdpSocketId::Raptorcast)
        .expect("non-authenticated socket");

    let raptorcast_auth_protocol =
        WireAuthProtocol::new(wireauth_config.clone(), keypair_arc.clone());
    let mut raptorcast = RaptorCast::<
        SignatureType,
        InboundMessage,
        OutboundMessage,
        MultiSubmitRaptorCastEvent,
        monad_peer_discovery::mock::NopDiscovery<SignatureType>,
        WireAuthProtocol,
    >::new(
        create_raptorcast_config(keypair_arc.clone()),
        SecondaryRaptorCastModeConfig::None,
        tcp_socket,
        raptorcast_auth_socket,
        non_auth_socket,
        dp_rc.control.clone(),
        Arc::new(Mutex::new(pd)),
        Epoch(0),
        raptorcast_auth_protocol,
    );

    raptorcast.exec(vec![
        monad_executor_glue::RouterCommand::AddEpochValidatorSet {
            epoch: Epoch(0),
            validator_set: vec![(self_node_id, Stake::ONE)],
        },
    ]);

    let (_, score_reader) =
        ema::create::<NodeId<monad_secp::PubKey>, StdClock>(ema::ScoreConfig::default(), StdClock);

    if args.transport == Transport::Leanudp {
        let mut dp_lean = DataplaneBuilder::new(1000)
            .with_udp_multishot(true)
            .with_udp_sockets([(UdpSocketId::AuthenticatedRaptorcast, bind_addr)])
            .build();

        assert!(dp_lean.block_until_ready(Duration::from_secs(5)));

        let lean_udp_socket = dp_lean
            .udp_sockets
            .take(UdpSocketId::AuthenticatedRaptorcast)
            .expect("lean udp socket");
        let local_addr = lean_udp_socket.local_addr();

        let lean_auth_protocol = WireAuthProtocol::new(wireauth_config, keypair_arc.clone());
        let lean_auth_socket = AuthenticatedSocketHandle::new(lean_udp_socket, lean_auth_protocol);
        let lean_socket = LeanUdpSocketHandle::new(
            lean_auth_socket,
            score_reader,
            monad_leanudp::Config::default(),
        );
        raptorcast.set_lean_udp_socket(lean_socket);

        tracing::info!(%local_addr, identity_index, "leanudp local socket");
    }

    tracing::info!(
        transport = ?args.transport,
        rc_tcp_addr = %endpoints.rc_tcp_addr,
        rc_udp_addr = %endpoints.rc_udp_addr,
        rc_auth_udp_addr = %endpoints.rc_auth_udp_addr,
        leanudp_addr = %endpoints.leanudp_addr,
        identity_index,
        "identity starting"
    );

    if args.transport == Transport::Leanudp {
        let node_leanudp_addr = SocketAddr::V4(args.leanudp_addr);
        {
            let lean_socket = raptorcast.lean_udp_socket_mut().expect("lean socket");
            lean_socket
                .connect(&node_pubkey, node_leanudp_addr, 3)
                .expect("connect failed");
            lean_socket.flush();
        }

        let handshake_start = std::time::Instant::now();

        while handshake_start.elapsed() < handshake_timeout {
            tokio::select! {
                _ = raptorcast.next() => {}
                _ = tokio::time::sleep(Duration::from_millis(10)) => {
                    let lean_socket = raptorcast.lean_udp_socket_mut().expect("lean socket");
                    if lean_socket.is_connected_socket_and_public_key(&node_leanudp_addr, &node_pubkey) {
                        break;
                    }
                }
            }
            if let Some(lean_socket) = raptorcast.lean_udp_socket_mut() {
                lean_socket.flush();
            }
        }

        let connected = raptorcast
            .lean_udp_socket_mut()
            .expect("lean socket")
            .is_connected_socket_and_public_key(&node_leanudp_addr, &node_pubkey);

        if !connected {
            tracing::warn!(identity_index, "leanudp handshake failed");
            return;
        }

        tracing::info!(identity_index, "leanudp handshake complete");
    }

    let secret = crate::submit::sender_secret(identity_index);
    let _sender_addr = secret_to_eth_address(secret);
    let max_fee = if args.max_fee_multiplier > 0 {
        MIN_BASE_FEE * args.max_fee_multiplier
    } else {
        MIN_BASE_FEE * 2
    };
    let priority_fee: u128 = if args.priority_fee_multiplier > 0 {
        (MIN_BASE_FEE * args.priority_fee_multiplier).into()
    } else {
        (MIN_BASE_FEE / 2).into()
    };
    let batch_size = args.batch_size.max(1);

    let interval = if args.tps_per_identity > 0 {
        Duration::from_micros(1_000_000 / args.tps_per_identity)
    } else {
        Duration::from_millis(10)
    };
    let mut ticker = tokio::time::interval(interval);

    let mut nonce: u64 = 0;
    let mut sent: u64 = 0;

    let run_duration = Duration::from_secs(args.duration_secs);
    let send_start = std::time::Instant::now();

    let mut pending_batch: Vec<bytes::Bytes> = Vec::with_capacity(batch_size);

    loop {
        if send_start.elapsed() >= run_duration {
            break;
        }

        tokio::select! {
            _ = ticker.tick() => {
                let tx = make_eip1559_tx(
                    secret,
                    max_fee.into(),
                    priority_fee,
                    21_000,
                    nonce,
                    0,
                );

                let tx_bytes: bytes::Bytes = tx.encoded_2718().into();
                pending_batch.push(tx_bytes);
                nonce += 1;
                sent += 1;

                if pending_batch.len() >= batch_size {
                    let txs = std::mem::take(&mut pending_batch);
                    let message = OutboundMessage::forward_txs(txs);
                    let cmd = match args.transport {
                        Transport::Leanudp => RouterCommand::LeanPointToPoint {
                            target: node_id,
                            message,
                            priority: UdpPriority::Regular,
                        },
                        Transport::Raptorcast | Transport::RaptorcastAuth => {
                            RouterCommand::PublishWithPriority {
                                target: RouterTarget::PointToPoint(node_id),
                                message,
                                priority: UdpPriority::Regular,
                            }
                        }
                    };
                    raptorcast.exec(vec![cmd]);
                }
            }

            _ = raptorcast.next() => {}
        }

        if args.transport == Transport::Leanudp {
            if let Some(lean_socket) = raptorcast.lean_udp_socket_mut() {
                lean_socket.flush();
            }
        }
    }

    if !pending_batch.is_empty() {
        let txs = std::mem::take(&mut pending_batch);
        let message = OutboundMessage::forward_txs(txs);
        let cmd = match args.transport {
            Transport::Leanudp => RouterCommand::LeanPointToPoint {
                target: node_id,
                message,
                priority: UdpPriority::Regular,
            },
            Transport::Raptorcast | Transport::RaptorcastAuth => {
                RouterCommand::PublishWithPriority {
                    target: RouterTarget::PointToPoint(node_id),
                    message,
                    priority: UdpPriority::Regular,
                }
            }
        };
        raptorcast.exec(vec![cmd]);
        if args.transport == Transport::Leanudp {
            if let Some(lean_socket) = raptorcast.lean_udp_socket_mut() {
                lean_socket.flush();
            }
        }
    }

    tracing::info!(
        identity_index,
        total_sent = sent,
        elapsed_secs = format!("{:.1}", send_start.elapsed().as_secs_f64()),
        "identity done"
    );
}
