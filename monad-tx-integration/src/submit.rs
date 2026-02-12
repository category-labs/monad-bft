use std::{
    collections::HashMap,
    marker::PhantomData,
    net::SocketAddrV4,
    sync::{Arc, Mutex},
    time::Duration,
};

use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::Address;
use alloy_rlp::Encodable;
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
    raptorcast_secondary::SecondaryRaptorCastModeConfig,
    RaptorCast, RaptorCastEvent,
};
use monad_secp::{KeyPair, SecpSignature};
use monad_tfm::base_fee::MIN_BASE_FEE;
use monad_types::{Epoch, NodeId, Round, Stake, UdpPriority};
use monad_wireauth::Config;
use serde::Serialize;

use crate::{rpc, SubmitArgs};

type SignatureType = SecpSignature;

#[derive(Clone, Debug)]
struct TxIntegrationMessage;

impl monad_executor_glue::Message for TxIntegrationMessage {
    type NodeIdPubKey = monad_secp::PubKey;
    type Event = TxIntegrationEvent;

    fn event(self, from: NodeId<Self::NodeIdPubKey>) -> Self::Event {
        TxIntegrationEvent { _from: from }
    }
}

impl monad_types::Serializable<bytes::Bytes> for TxIntegrationMessage {
    fn serialize(&self) -> bytes::Bytes {
        bytes::Bytes::new()
    }
}

impl monad_types::Deserializable<bytes::Bytes> for TxIntegrationMessage {
    type ReadError = std::io::Error;

    fn deserialize(_message: &bytes::Bytes) -> Result<Self, Self::ReadError> {
        Ok(TxIntegrationMessage)
    }
}

impl alloy_rlp::Encodable for TxIntegrationMessage {
    fn encode(&self, _out: &mut dyn alloy_rlp::BufMut) {}
}

impl alloy_rlp::Decodable for TxIntegrationMessage {
    fn decode(_buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        Ok(TxIntegrationMessage)
    }
}

impl OutboundForwardTxs for TxIntegrationMessage {
    fn forward_txs(_txs: Vec<bytes::Bytes>) -> Self {
        TxIntegrationMessage
    }
}

#[derive(Clone, Debug)]
struct TxIntegrationEvent {
    _from: NodeId<monad_secp::PubKey>,
}

impl<ST: monad_crypto::certificate_signature::CertificateSignatureRecoverable>
    From<RaptorCastEvent<TxIntegrationEvent, ST>> for TxIntegrationEvent
{
    fn from(value: RaptorCastEvent<TxIntegrationEvent, ST>) -> Self {
        match value {
            RaptorCastEvent::Message(event) => event,
            RaptorCastEvent::PeerManagerResponse(_) => {
                panic!("unexpected PeerManagerResponse")
            }
            RaptorCastEvent::SecondaryRaptorcastPeersUpdate(_, _) => {
                panic!("unexpected SecondaryRaptorcastPeersUpdate")
            }
            RaptorCastEvent::LeanUdpTx { .. } => {
                panic!("unexpected LeanUdpTx")
            }
            RaptorCastEvent::LeanUdpForwardTxs { .. } => {
                panic!("unexpected LeanUdpForwardTxs")
            }
        }
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
    let bind_addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();

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

    let keypair = submitter_keypair(args.sender_index);
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
        TxIntegrationMessage,
        TxIntegrationMessage,
        TxIntegrationEvent,
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
        node_addr = %args.node_addr,
        sender_index = args.sender_index,
        tps = args.tps,
        count = args.count,
        "submit agent starting"
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
        tracing::error!("handshake failed");
        return;
    }

    tracing::info!("handshake complete");

    let secret = sender_secret(args.sender_index);
    let sender_addr: Address = secret_to_eth_address(secret);
    let max_fee = if args.max_fee_multiplier > 0 {
        MIN_BASE_FEE * args.max_fee_multiplier
    } else {
        MIN_BASE_FEE * 2
    };
    let priority_fee = if args.priority_fee_multiplier > 0 {
        MIN_BASE_FEE * args.priority_fee_multiplier
    } else {
        MIN_BASE_FEE / 2
    };
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

    // Replacement mode state
    let mut current_priority_fee = priority_fee as u128;
    let replacement_fee_increment = MIN_BASE_FEE as u128 / 10;

    // Batch accumulator for ForwardTxs
    let mut pending_batch: Vec<bytes::Bytes> = Vec::with_capacity(batch_size);

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
                    let mut buf = bytes::BytesMut::new();
                    pending_batch.encode(&mut buf);
                    let lean_socket = raptorcast.lean_udp_socket_mut().expect("lean socket");
                    lean_socket.send(args.node_addr, buf.freeze(), UdpPriority::Regular);
                    lean_socket.flush();
                    pending_batch.clear();
                }
            }

            _ = raptorcast.next() => {}
        }

        if let Some(lean_socket) = raptorcast.lean_udp_socket_mut() {
            lean_socket.flush();
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
        let mut buf = bytes::BytesMut::new();
        pending_batch.encode(&mut buf);
        let lean_socket = raptorcast.lean_udp_socket_mut().expect("lean socket");
        lean_socket.send(args.node_addr, buf.freeze(), UdpPriority::Regular);
        lean_socket.flush();
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
