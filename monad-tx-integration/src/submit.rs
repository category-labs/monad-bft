use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::Address;
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
use serde::Serialize;

use crate::{
    message::{InboundMessage, OutboundMessage, SignatureType, WireEvent},
    rpc,
    transport::{NodeEndpointsV4, Transport},
    SubmitArgs,
};

struct SubmitRaptorCastEvent(RaptorCastEvent<WireEvent, SignatureType>);

impl From<RaptorCastEvent<WireEvent, SignatureType>> for SubmitRaptorCastEvent {
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

#[derive(Serialize)]
struct SubmitStats {
    sender_index: usize,
    tps_target: u64,
    total_sent: u64,
    elapsed_secs: f64,
    actual_tps: f64,
    max_fee_per_gas: u64,
    priority_fee_per_gas: u64,
}

pub fn submitter_keypair(index: usize) -> KeyPair {
    let mut seed = [0u8; 32];
    seed[0] = 100 + index as u8;
    KeyPair::from_bytes(&mut seed).unwrap()
}

pub fn sender_secret(index: usize) -> alloy_primitives::B256 {
    use alloy_primitives::keccak256;
    let mut preimage = [0u8; 40];
    preimage[..8].copy_from_slice(b"txsender");
    preimage[8..16].copy_from_slice(&(index as u64).to_le_bytes());
    keccak256(preimage)
}

pub async fn run(args: SubmitArgs) {
    let bind_addr: std::net::SocketAddr = "0.0.0.0:0".parse().unwrap();

    let endpoints = NodeEndpointsV4 {
        rc_tcp_addr: args.rc_tcp_addr,
        rc_udp_addr: args.rc_udp_addr,
        rc_auth_udp_addr: args.rc_auth_udp_addr,
        leanudp_addr: args.leanudp_addr,
    };

    let keypair = submitter_keypair(args.sender_index);
    let keypair_arc = Arc::new(keypair);
    let self_node_id = NodeId::new(keypair_arc.pubkey());
    let wireauth_config = Config::default();
    let handshake_timeout = wireauth_config.session_timeout
        + wireauth_config.session_timeout_jitter
        + Duration::from_secs(5);
    let raptorcast_auth_protocol =
        WireAuthProtocol::new(wireauth_config.clone(), keypair_arc.clone());

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

    let mut raptorcast = RaptorCast::<
        SignatureType,
        InboundMessage,
        OutboundMessage,
        SubmitRaptorCastEvent,
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

        tracing::info!(%local_addr, "leanudp local socket");
    }

    tracing::info!(
        transport = ?args.transport,
        rc_tcp_addr = %endpoints.rc_tcp_addr,
        rc_udp_addr = %endpoints.rc_udp_addr,
        rc_auth_udp_addr = %endpoints.rc_auth_udp_addr,
        leanudp_addr = %endpoints.leanudp_addr,
        sender_index = args.sender_index,
        tps = args.tps,
        count = args.count,
        "submit agent starting"
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
            tracing::error!("leanudp handshake failed");
            return;
        }

        tracing::info!("leanudp handshake complete");
    }

    let secret = sender_secret(args.sender_index);
    let sender_addr: Address = secret_to_eth_address(secret);
    let max_fee = if args.max_fee_multiplier > 0 {
        MIN_BASE_FEE * args.max_fee_multiplier
    } else {
        MIN_BASE_FEE * 2
    };
    let priority_fee = MIN_BASE_FEE.saturating_mul(args.priority_fee_multiplier);
    let batch_size = args.batch_size.max(1);
    tracing::info!(max_fee, priority_fee, batch_size, "gas price config");

    let interval = if args.tps > 0 {
        Duration::from_micros(1_000_000 / args.tps)
    } else {
        Duration::from_millis(10)
    };
    let mut ticker = tokio::time::interval(interval);

    let mut nonce: u64 = 0;
    let mut sent: u64 = 0;
    let mut resent: u64 = 0;
    let max_count = if args.count == 0 {
        u64::MAX
    } else {
        args.count
    };

    let run_duration = if args.duration_secs > 0 {
        Some(Duration::from_secs(args.duration_secs))
    } else {
        None
    };

    let send_start = std::time::Instant::now();
    let report_interval = Duration::from_secs(5);
    let mut last_report = std::time::Instant::now();
    let mut last_report_sent: u64 = 0;

    // Nonce sync interval (only if RPC available)
    let nonce_sync_interval = Duration::from_millis(args.nonce_sync_ms);
    let mut last_nonce_sync = std::time::Instant::now();
    let mut last_observed_committed_nonce: Option<u64> = None;
    let mut last_committed_nonce_change_at = std::time::Instant::now();
    let mut last_nonce_stall_warning_at: Option<std::time::Instant> = None;

    // Replacement mode state
    let mut current_priority_fee = priority_fee as u128;
    let replacement_fee_increment = MIN_BASE_FEE as u128 / 10;

    // Batch accumulator for ForwardTxs
    let mut pending_batch: Vec<bytes::Bytes> = Vec::with_capacity(batch_size);

    if let Some(ref rpc_addr) = args.rpc_addr {
        match rpc::query_nonce(rpc_addr, &sender_addr).await {
            Ok(committed_nonce) => {
                nonce = committed_nonce;
                last_observed_committed_nonce = Some(committed_nonce);
                tracing::info!(committed_nonce, "initialized nonce from RPC");
            }
            Err(e) => {
                tracing::warn!(?e, "failed to initialize nonce from RPC");
            }
        }
    }

    while sent < max_count {
        if let Some(d) = run_duration {
            if send_start.elapsed() >= d {
                break;
            }
        }

        // Periodic nonce sync via RPC
        if let Some(ref rpc_addr) = args.rpc_addr {
            if last_nonce_sync.elapsed() >= nonce_sync_interval {
                last_nonce_sync = std::time::Instant::now();
                match rpc::query_nonce(rpc_addr, &sender_addr).await {
                    Ok(committed_nonce) => {
                        let now = std::time::Instant::now();
                        match last_observed_committed_nonce {
                            Some(previous_nonce) if previous_nonce == committed_nonce => {
                                let unchanged_for =
                                    now.saturating_duration_since(last_committed_nonce_change_at);
                                if unchanged_for >= Duration::from_secs(10)
                                    && last_nonce_stall_warning_at.is_none_or(|last_warned| {
                                        now.saturating_duration_since(last_warned)
                                            >= Duration::from_secs(10)
                                    })
                                {
                                    tracing::warn!(
                                        committed_nonce,
                                        local_next_nonce = nonce,
                                        unchanged_for_secs = unchanged_for.as_secs(),
                                        "committed account nonce unchanged for >=10s"
                                    );
                                    last_nonce_stall_warning_at = Some(now);
                                }
                            }
                            _ => {
                                last_observed_committed_nonce = Some(committed_nonce);
                                last_committed_nonce_change_at = now;
                                last_nonce_stall_warning_at = None;
                            }
                        }

                        if committed_nonce < nonce {
                            // Gap detected: we sent up to `nonce-1`, but only `committed_nonce-1` is committed
                            // Need to resend from committed_nonce
                            let gap = nonce - committed_nonce;
                            if gap > 1 {
                                tracing::debug!(
                                    committed_nonce,
                                    next_nonce = nonce,
                                    gap,
                                    "nonce gap detected, resending"
                                );
                            }
                            nonce = committed_nonce;
                            resent += gap;
                        } else if committed_nonce > nonce {
                            tracing::info!(
                                committed_nonce,
                                local_next_nonce = nonce,
                                "local nonce behind committed state, fast-forwarding"
                            );
                            nonce = committed_nonce;
                        }
                    }
                    Err(e) => {
                        tracing::debug!(?e, "nonce query failed");
                    }
                }
            }
        }

        tokio::select! {
            _ = ticker.tick() => {
                let tx = make_eip1559_tx(
                    secret,
                    max_fee.into(),
                    if args.replacement_mode {
                        current_priority_fee
                    } else {
                        priority_fee.into()
                    },
                    21_000,
                    if args.replacement_mode { 0 } else { nonce },
                    0,
                );

                // Add to batch
                let tx_bytes: bytes::Bytes = tx.encoded_2718().into();
                let tx_bytes = if args.invalid_sig_pct > 0
                    && (sent % 100) < args.invalid_sig_pct as u64
                {
                    let mut corrupted = tx_bytes.to_vec();
                    if corrupted.len() > 10 {
                        let idx = corrupted.len() - 5;
                        corrupted[idx] ^= 0xFF;
                    }
                    bytes::Bytes::from(corrupted)
                } else {
                    tx_bytes
                };
                pending_batch.push(tx_bytes);
                if args.replacement_mode {
                    current_priority_fee += replacement_fee_increment;
                } else if args.nonce_gap_interval > 0
                    && sent > 0
                    && sent % args.nonce_gap_interval == 0
                {
                    nonce += 2;
                } else {
                    nonce += 1;
                }
                sent += 1;

                // Send when batch is full
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

        if last_report.elapsed() >= report_interval {
            let delta = sent - last_report_sent;
            let actual_tps = delta as f64 / last_report.elapsed().as_secs_f64();
            tracing::info!(
                sent,
                resent,
                actual_tps = format!("{actual_tps:.1}"),
                "progress"
            );
            last_report = std::time::Instant::now();
            last_report_sent = sent;
        }
    }

    // Flush any remaining transactions in the batch
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

    let elapsed = send_start.elapsed();
    tracing::info!(
        total_sent = sent,
        elapsed_secs = format!("{:.1}", elapsed.as_secs_f64()),
        "submit agent done"
    );

    if let Some(stats_path) = &args.stats_file {
        let stats = SubmitStats {
            sender_index: args.sender_index,
            tps_target: args.tps,
            total_sent: sent,
            elapsed_secs: elapsed.as_secs_f64(),
            actual_tps: sent as f64 / elapsed.as_secs_f64(),
            max_fee_per_gas: max_fee,
            priority_fee_per_gas: priority_fee,
        };
        if let Ok(json) = serde_json::to_string_pretty(&stats) {
            let _ = std::fs::write(stats_path, json);
        }
    }
}
