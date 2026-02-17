use std::{
    collections::HashMap,
    marker::PhantomData,
    net::SocketAddrV4,
    sync::{Arc, Mutex},
    time::Duration,
};

use alloy_eips::eip2718::Encodable2718;
use futures::StreamExt;
use monad_dataplane::{udp::DEFAULT_MTU, DataplaneBuilder, TcpSocketId, UdpSocketId};
use monad_eth_testutil::{make_eip1559_tx, secret_to_eth_address};
use monad_executor::Executor;
use monad_executor_glue::OutboundForwardTxs;
use monad_node_config::{fullnode_raptorcast::FullNodeRaptorCastConfig, FullNodeConfig};
use monad_peer_discovery::{driver::PeerDiscoveryDriver, mock::NopDiscoveryBuilder};
use monad_peer_score::{ema, StdClock};
use monad_raptorcast::{
    auth::{AuthenticatedSocketHandle, LeanUdpSocketHandle, WireAuthProtocol},
    config::{RaptorCastConfig, RaptorCastConfigPrimary},
    message::OutboundRouterMessage,
    raptorcast_secondary::SecondaryRaptorCastModeConfig,
    RaptorCast, RaptorCastEvent,
};
use monad_secp::KeyPair;
use monad_tfm::base_fee::MIN_BASE_FEE;
use monad_types::{Epoch, NodeId, Round, Stake, UdpPriority};
use monad_wireauth::Config;

use crate::{
    message::{InboundMessage, OutboundMessage, SignatureType, WireEvent},
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

    let mut dp = DataplaneBuilder::new(1000)
        .with_udp_multishot(true)
        .with_tcp_sockets([(TcpSocketId::Raptorcast, bind_addr)])
        .with_udp_sockets([
            (UdpSocketId::AuthenticatedRaptorcast, bind_addr),
            (UdpSocketId::Raptorcast, bind_addr),
        ])
        .build();

    let mut dp2 = DataplaneBuilder::new(1000)
        .with_udp_multishot(true)
        .with_udp_sockets([(UdpSocketId::AuthenticatedRaptorcast, bind_addr)])
        .build();

    assert!(dp.block_until_ready(Duration::from_secs(5)));
    assert!(dp2.block_until_ready(Duration::from_secs(5)));

    let tcp_socket = dp
        .tcp_sockets
        .take(TcpSocketId::Raptorcast)
        .expect("tcp socket");

    let raptorcast_auth_socket = dp
        .udp_sockets
        .take(UdpSocketId::AuthenticatedRaptorcast)
        .expect("raptorcast authenticated socket");

    let non_auth_socket = dp
        .udp_sockets
        .take(UdpSocketId::Raptorcast)
        .expect("non-authenticated socket");

    let lean_udp_socket = dp2
        .udp_sockets
        .take(UdpSocketId::AuthenticatedRaptorcast)
        .expect("lean udp socket");

    let local_addr = lean_udp_socket.local_addr();

    let keypair = crate::submit::submitter_keypair(identity_index);
    let keypair_arc = Arc::new(keypair);
    let self_node_id = NodeId::new(keypair_arc.pubkey());
    let wireauth_config = Config::default();
    let handshake_timeout = wireauth_config.session_timeout
        + wireauth_config.session_timeout_jitter
        + Duration::from_secs(5);
    let raptorcast_auth_protocol =
        WireAuthProtocol::new(wireauth_config.clone(), keypair_arc.clone());

    let known_addresses: HashMap<NodeId<monad_secp::PubKey>, SocketAddrV4> = HashMap::new();
    let nop_builder = NopDiscoveryBuilder {
        known_addresses,
        name_records: HashMap::new(),
        pd: PhantomData,
    };
    let pd = PeerDiscoveryDriver::new(nop_builder);

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
        Some(raptorcast_auth_socket),
        non_auth_socket,
        dp.control.clone(),
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

    let lean_auth_protocol = WireAuthProtocol::new(wireauth_config, keypair_arc);
    let lean_auth_socket = AuthenticatedSocketHandle::new(lean_udp_socket, lean_auth_protocol);
    let lean_socket = LeanUdpSocketHandle::new(
        lean_auth_socket,
        score_reader,
        monad_leanudp::Config::default(),
    );
    raptorcast.set_lean_udp_socket(lean_socket);

    tracing::info!(
        %local_addr,
        identity_index,
        "identity starting"
    );

    let node_pubkey = crate::node::node_keypair().pubkey();
    {
        let lean_socket = raptorcast.lean_udp_socket_mut().expect("lean socket");
        lean_socket
            .connect(&node_pubkey, args.node_addr, 3)
            .expect("connect failed");
        lean_socket.flush();
    }

    let handshake_start = std::time::Instant::now();

    while handshake_start.elapsed() < handshake_timeout {
        tokio::select! {
            _ = raptorcast.next() => {}
            _ = tokio::time::sleep(Duration::from_millis(10)) => {
                let lean_socket = raptorcast.lean_udp_socket_mut().expect("lean socket");
                if lean_socket.is_connected_socket_and_public_key(&args.node_addr, &node_pubkey) {
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
        .is_connected_socket_and_public_key(&args.node_addr, &node_pubkey);

    if !connected {
        tracing::warn!(identity_index, "handshake failed");
        return;
    }

    tracing::info!(identity_index, "handshake complete");

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
                        let payload =
                        OutboundRouterMessage::<OutboundMessage, SignatureType>::AppMessage(
                            OutboundMessage::forward_txs(txs),
                        )
                        .try_serialize()
                        .expect("serialize tx integration message");
                        let lean_socket = raptorcast.lean_udp_socket_mut().expect("lean socket");
                        lean_socket.send(args.node_addr, payload, UdpPriority::Regular);
                    lean_socket.flush();
                }
            }

            _ = raptorcast.next() => {}
        }

        if let Some(lean_socket) = raptorcast.lean_udp_socket_mut() {
            lean_socket.flush();
        }
    }

    if !pending_batch.is_empty() {
        let txs = std::mem::take(&mut pending_batch);
        let payload = OutboundRouterMessage::<OutboundMessage, SignatureType>::AppMessage(
            OutboundMessage::forward_txs(txs),
        )
        .try_serialize()
        .expect("serialize tx integration message");
        let lean_socket = raptorcast.lean_udp_socket_mut().expect("lean socket");
        lean_socket.send(args.node_addr, payload, UdpPriority::Regular);
        lean_socket.flush();
    }

    tracing::info!(
        identity_index,
        total_sent = sent,
        elapsed_secs = format!("{:.1}", send_start.elapsed().as_secs_f64()),
        "identity done"
    );
}
