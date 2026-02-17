use std::{
    net::{SocketAddr, SocketAddrV4},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope};
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{keccak256, Address, TxKind, B256, U256};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use clap::{Args, Subcommand};
use futures::StreamExt;
use monad_dataplane::{udp::DEFAULT_MTU, DataplaneBuilder, TcpSocketId, UdpSocketId};
use monad_eth_testutil::secret_to_eth_address;
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
use monad_secp::{KeyPair, PubKey};
use monad_tfm::base_fee::MIN_BASE_FEE;
use monad_types::{Epoch, NodeId, Round, RouterTarget, Stake, UdpPriority};
use monad_wireauth::Config;
use secp256k1::rand::{rngs::OsRng, TryRngCore};
use serde::Serialize;

use crate::{
    message::{InboundMessage, OutboundMessage, SignatureType, WireEvent},
    transport::{NodeEndpointsV4, Transport},
};

#[derive(Args, Clone)]
pub struct ScenarioArgs {
    /// Which transport to use when sending forwarded tx batches.
    #[arg(long, value_enum, default_value = "leanudp")]
    pub transport: Transport,

    /// Node RaptorCast TCP address (advertised).
    #[arg(long)]
    pub rc_tcp_addr: SocketAddrV4,

    /// Node RaptorCast UDP address (advertised).
    #[arg(long)]
    pub rc_udp_addr: SocketAddrV4,

    /// Node authenticated RaptorCast UDP address (advertised).
    #[arg(long)]
    pub rc_auth_udp_addr: SocketAddrV4,

    /// Node LeanUDP tx-ingestion address (advertised).
    #[arg(long)]
    pub leanudp_addr: SocketAddrV4,

    /// Sender's WireAuth private key (32-byte hex, with or without 0x prefix).
    /// If omitted, a random key is generated (recommended for spam testing).
    #[arg(long)]
    pub auth_key: Option<String>,

    /// Node public key in compressed secp256k1 form (33-byte hex). If omitted,
    /// defaults to the test node key used by `monad-tx-integration node`.
    #[arg(long)]
    pub node_pubkey: Option<String>,

    #[arg(long, default_value = "100")]
    pub tps: u64,

    #[arg(long, default_value = "0")]
    pub count: u64,

    #[arg(long, default_value = "0")]
    pub duration_secs: u64,

    #[arg(long, default_value = "32")]
    pub batch_size: usize,

    #[arg(long)]
    pub stats_file: Option<PathBuf>,

    #[command(subcommand)]
    pub mode: ScenarioMode,
}

#[derive(Subcommand, Clone)]
pub enum ScenarioMode {
    /// Sends correctly signed txs from random unfunded senders.
    InvalidNoBalance(InvalidNoBalanceArgs),
    /// Sends correctly signed txs from a single sender with low valid gas fee.
    ValidLowFee(ValidLowFeeArgs),
}

#[derive(Args, Clone)]
pub struct InvalidNoBalanceArgs {
    #[arg(long, default_value = "1")]
    pub seed: u64,

    #[arg(long, default_value = "1337")]
    pub chain_id: u64,

    #[arg(long, default_value = "21000")]
    pub gas_limit: u64,

    #[arg(long, default_value = "1")]
    pub max_fee_multiplier: u64,

    #[arg(long, default_value = "0")]
    pub priority_fee_multiplier: u64,

    #[arg(long, default_value = "0")]
    pub value_wei: u128,
}

#[derive(Args, Clone)]
pub struct ValidLowFeeArgs {
    /// Sender private key (32-byte hex). If omitted, uses fixture key from sender-index.
    #[arg(long)]
    pub sender_key: Option<String>,

    /// Fixture sender index used when `--sender-key` is not provided.
    #[arg(long, default_value = "0")]
    pub sender_index: usize,

    #[arg(long, default_value = "0")]
    pub start_nonce: u64,

    /// Destination address as 20-byte hex. If omitted, uses 0x1111...11.
    #[arg(long)]
    pub to: Option<String>,

    #[arg(long, default_value = "1337")]
    pub chain_id: u64,

    #[arg(long, default_value = "21000")]
    pub gas_limit: u64,

    #[arg(long, default_value = "1")]
    pub max_fee_multiplier: u64,

    #[arg(long, default_value = "0")]
    pub priority_fee_multiplier: u64,

    #[arg(long, default_value = "0")]
    pub value_wei: u128,
}

struct ScenarioRaptorCastEvent(RaptorCastEvent<WireEvent, SignatureType>);

impl From<RaptorCastEvent<WireEvent, SignatureType>> for ScenarioRaptorCastEvent {
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
struct ScenarioStats {
    mode: String,
    total_sent: u64,
    batches_sent: u64,
    elapsed_secs: f64,
    actual_tps: f64,
    handshake_ms: u128,
    sender_address: Option<String>,
    seed: Option<u64>,
}

fn decode_hex(input: &str) -> Result<Vec<u8>, String> {
    let input = input.strip_prefix("0x").unwrap_or(input);
    hex::decode(input).map_err(|e| format!("invalid hex: {e}"))
}

fn parse_secret_key(input: &str) -> Result<[u8; 32], String> {
    let bytes = decode_hex(input)?;
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| "secret key must be exactly 32 bytes".to_owned())?;
    Ok(arr)
}

fn parse_pubkey(input: &str) -> Result<PubKey, String> {
    let bytes = decode_hex(input)?;
    PubKey::from_slice(&bytes).map_err(|e| format!("invalid node pubkey: {e}"))
}

fn parse_address(input: &str) -> Result<Address, String> {
    let bytes = decode_hex(input)?;
    if bytes.len() != 20 {
        return Err("address must be exactly 20 bytes".to_owned());
    }
    Ok(Address::from_slice(&bytes))
}

fn max_fee_from_multiplier(multiplier: u64) -> u128 {
    let base = u128::from(MIN_BASE_FEE);
    if multiplier == 0 {
        base
    } else {
        base * u128::from(multiplier)
    }
}

fn priority_fee_from_multiplier(multiplier: u64) -> u128 {
    u128::from(MIN_BASE_FEE) * u128::from(multiplier)
}

fn random_sender_secret(seed: u64, counter: u64) -> B256 {
    let mut attempt = 0u64;
    loop {
        let mut preimage = [0u8; 32];
        preimage[0..8].copy_from_slice(&seed.to_le_bytes());
        preimage[8..16].copy_from_slice(&counter.to_le_bytes());
        preimage[16..24].copy_from_slice(&attempt.to_le_bytes());
        preimage[24..32].copy_from_slice(b"invldtxs");
        let candidate = keccak256(preimage);
        if PrivateKeySigner::from_bytes(&candidate).is_ok() {
            return candidate;
        }
        attempt = attempt.wrapping_add(1);
    }
}

fn random_address(seed: u64, counter: u64) -> Address {
    let mut preimage = [0u8; 24];
    preimage[0..8].copy_from_slice(&seed.to_le_bytes());
    preimage[8..16].copy_from_slice(&counter.to_le_bytes());
    preimage[16..24].copy_from_slice(b"addrseed");
    let hash = keccak256(preimage);
    Address::from_slice(&hash.as_slice()[12..32])
}

fn make_signed_eip1559_tx(
    sender_secret: B256,
    to: Address,
    nonce: u64,
    value: u128,
    gas_limit: u64,
    max_fee_per_gas: u128,
    max_priority_fee_per_gas: u128,
    chain_id: u64,
) -> Result<TxEnvelope, String> {
    let transaction = TxEip1559 {
        chain_id,
        nonce,
        gas_limit,
        max_fee_per_gas,
        max_priority_fee_per_gas,
        to: TxKind::Call(to),
        value: U256::from(value),
        access_list: Default::default(),
        input: Default::default(),
    };

    let signer = PrivateKeySigner::from_bytes(&sender_secret)
        .map_err(|e| format!("invalid sender key for signing: {e}"))?;
    let signature = signer
        .sign_hash_sync(&transaction.signature_hash())
        .map_err(|e| format!("failed to sign tx: {e}"))?;
    Ok(transaction.into_signed(signature).into())
}

pub async fn run(args: ScenarioArgs) {
    let auth_keypair = match args.auth_key.as_deref() {
        Some(key) => {
            let mut auth_secret = match parse_secret_key(key) {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!(%e, "invalid --auth-key");
                    return;
                }
            };
            match KeyPair::from_bytes(&mut auth_secret) {
                Ok(v) => Arc::new(v),
                Err(e) => {
                    tracing::error!(%e, "failed to build auth keypair from --auth-key");
                    return;
                }
            }
        }
        None => {
            let mut rng = OsRng.unwrap_err();
            Arc::new(KeyPair::generate(&mut rng))
        }
    };

    let node_pubkey = match args.node_pubkey.as_deref() {
        Some(pk) => match parse_pubkey(pk) {
            Ok(v) => v,
            Err(e) => {
                tracing::error!(%e, "invalid --node-pubkey");
                return;
            }
        },
        None => crate::node::node_keypair().pubkey(),
    };

    let endpoints = NodeEndpointsV4 {
        rc_tcp_addr: args.rc_tcp_addr,
        rc_udp_addr: args.rc_udp_addr,
        rc_auth_udp_addr: args.rc_auth_udp_addr,
        leanudp_addr: args.leanudp_addr,
    };

    let bind_addr: SocketAddr = "0.0.0.0:0".parse().expect("valid bind address");
    let self_node_id = NodeId::new(auth_keypair.pubkey());
    let wireauth_config = Config::default();
    let handshake_timeout = wireauth_config.session_timeout
        + wireauth_config.session_timeout_jitter
        + Duration::from_secs(5);
    let raptorcast_auth_protocol =
        WireAuthProtocol::new(wireauth_config.clone(), auth_keypair.clone());

    let node_id = NodeId::new(node_pubkey);
    let nop_builder = endpoints.nop_discovery_builder_for_node(node_id, &auth_keypair);
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
        ScenarioRaptorCastEvent,
        monad_peer_discovery::mock::NopDiscovery<SignatureType>,
        WireAuthProtocol,
    >::new(
        create_raptorcast_config(auth_keypair.clone()),
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

        let lean_auth_protocol = WireAuthProtocol::new(wireauth_config, auth_keypair.clone());
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
        tps = args.tps,
        count = args.count,
        duration_secs = args.duration_secs,
        "scenario sender starting"
    );

    let handshake_ms = if args.transport == Transport::Leanudp {
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

        let ms = handshake_start.elapsed().as_millis();
        tracing::info!(handshake_ms = ms, "leanudp handshake complete");
        ms
    } else {
        0
    };

    let mut sent: u64 = 0;
    let mut batches_sent: u64 = 0;
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
    let interval = if args.tps > 0 {
        Duration::from_micros(1_000_000 / args.tps)
    } else {
        Duration::from_millis(10)
    };
    let mut ticker = tokio::time::interval(interval);
    let mut pending_batch: Vec<bytes::Bytes> = Vec::with_capacity(args.batch_size.max(1));
    let send_start = std::time::Instant::now();
    let batch_size = args.batch_size.max(1);

    let mut valid_nonce = 0u64;
    let mut valid_sender: Option<B256> = None;
    let mut valid_sender_addr: Option<Address> = None;
    let mut valid_to: Option<Address> = None;

    if let ScenarioMode::ValidLowFee(valid) = &args.mode {
        let sender_secret = match &valid.sender_key {
            Some(key) => match parse_secret_key(key) {
                Ok(v) => B256::from(v),
                Err(e) => {
                    tracing::error!(%e, "invalid --sender-key");
                    return;
                }
            },
            None => crate::submit::sender_secret(valid.sender_index),
        };
        valid_nonce = valid.start_nonce;
        valid_sender_addr = Some(secret_to_eth_address(sender_secret));
        valid_sender = Some(sender_secret);
        valid_to = Some(match &valid.to {
            Some(addr) => match parse_address(addr) {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!(%e, "invalid --to address");
                    return;
                }
            },
            None => Address::repeat_byte(0x11),
        });
    }

    while sent < max_count {
        if let Some(d) = run_duration {
            if send_start.elapsed() >= d {
                break;
            }
        }

        tokio::select! {
            _ = ticker.tick() => {
                let tx = match &args.mode {
                    ScenarioMode::InvalidNoBalance(cfg) => {
                        let sender_secret = random_sender_secret(cfg.seed, sent);
                        let to = random_address(cfg.seed, sent);
                        make_signed_eip1559_tx(
                            sender_secret,
                            to,
                            0,
                            cfg.value_wei,
                            cfg.gas_limit,
                            max_fee_from_multiplier(cfg.max_fee_multiplier),
                            priority_fee_from_multiplier(cfg.priority_fee_multiplier),
                            cfg.chain_id,
                        )
                    }
                    ScenarioMode::ValidLowFee(cfg) => {
                        make_signed_eip1559_tx(
                            *valid_sender.as_ref().expect("valid sender initialized"),
                            *valid_to.as_ref().expect("valid recipient initialized"),
                            valid_nonce,
                            cfg.value_wei,
                            cfg.gas_limit,
                            max_fee_from_multiplier(cfg.max_fee_multiplier),
                            priority_fee_from_multiplier(cfg.priority_fee_multiplier),
                            cfg.chain_id,
                        )
                    }
                };

                let tx = match tx {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::error!(%e, "failed to build tx");
                        break;
                    }
                };
                pending_batch.push(tx.encoded_2718().into());
                sent += 1;
                if matches!(&args.mode, ScenarioMode::ValidLowFee(_)) {
                    valid_nonce = valid_nonce.saturating_add(1);
                }

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
                    batches_sent += 1;
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
        batches_sent += 1;
    }

    let elapsed = send_start.elapsed();
    tracing::info!(
        total_sent = sent,
        batches_sent,
        elapsed_secs = format!("{:.1}", elapsed.as_secs_f64()),
        "scenario sender done"
    );

    if let Some(path) = &args.stats_file {
        let stats = ScenarioStats {
            mode: match &args.mode {
                ScenarioMode::InvalidNoBalance(_) => "invalid-no-balance",
                ScenarioMode::ValidLowFee(_) => "valid-low-fee",
            }
            .to_owned(),
            total_sent: sent,
            batches_sent,
            elapsed_secs: elapsed.as_secs_f64(),
            actual_tps: if elapsed.as_secs_f64() > 0.0 {
                sent as f64 / elapsed.as_secs_f64()
            } else {
                0.0
            },
            handshake_ms,
            sender_address: valid_sender_addr.map(|a| format!("{a:#x}")),
            seed: match &args.mode {
                ScenarioMode::InvalidNoBalance(ref cfg) => Some(cfg.seed),
                ScenarioMode::ValidLowFee(_) => None,
            },
        };
        match serde_json::to_string_pretty(&stats) {
            Ok(json) => {
                if let Err(e) = std::fs::write(path, json) {
                    tracing::error!(?e, "failed writing stats file");
                }
            }
            Err(e) => tracing::error!(?e, "failed to serialize stats"),
        }
    }
}
