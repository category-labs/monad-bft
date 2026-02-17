use std::{
    collections::{BTreeMap, HashMap},
    marker::PhantomData,
    net::SocketAddrV4,
    sync::{Arc, Mutex},
    time::Duration,
};

use alloy_rlp::Decodable;
use futures::StreamExt;
use monad_chain_config::MockChainConfig;
use monad_consensus_types::block::GENESIS_TIMESTAMP;
use monad_dataplane::{udp::DEFAULT_MTU, DataplaneBuilder, TcpSocketId, UdpSocketId};
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_testutil::{generate_block_with_txs, secret_to_eth_address};
use monad_eth_txpool_executor::{EthTxPoolExecutor, ForwardedIngressFairQueueConfig};
use monad_executor::Executor;
use monad_executor_glue::{MempoolEvent, MonadEvent, RouterCommand, TxPoolCommand};
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
use monad_state_backend::{InMemoryBlockState, InMemoryState, InMemoryStateInner};
use monad_testutil::signing::MockSignatures;
use monad_tfm::base_fee::MIN_BASE_FEE;
use monad_types::{Balance, Epoch, NodeId, Round, SeqNum, Stake, GENESIS_ROUND, GENESIS_SEQ_NUM};
use monad_wireauth::Config;

use crate::{
    channel_input::ChannelInputStream,
    committer::BlockCommitter,
    message::{InboundMessage, OutboundMessage, SignatureType, WireEvent},
    rpc,
    stats::StatsCollector,
    NodeArgs,
};

struct NodeRaptorCastEvent(RaptorCastEvent<WireEvent, SignatureType>);

impl From<RaptorCastEvent<WireEvent, SignatureType>> for NodeRaptorCastEvent {
    fn from(value: RaptorCastEvent<WireEvent, SignatureType>) -> Self {
        NodeRaptorCastEvent(value)
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

fn fast_score_config(ema_half_life_secs: u64, promotion_threshold: f64) -> ema::ScoreConfig {
    ema::ScoreConfig {
        promoted_capacity: 90_000,
        newcomer_capacity: 10_000,
        max_time_weight: 1.0,
        time_weight_unit: Duration::from_secs(5),
        ema_half_life: Duration::from_secs(ema_half_life_secs),
        block_time: Duration::from_millis(400),
        promotion_threshold,
    }
}

pub fn node_keypair() -> KeyPair {
    KeyPair::from_bytes(&mut [42u8; 32]).unwrap()
}

pub async fn run(args: NodeArgs) {
    let genesis_accounts: BTreeMap<alloy_primitives::Address, u64> = (0..args.num_accounts)
        .map(|i| {
            let secret = crate::submit::sender_secret(i);
            (secret_to_eth_address(secret), 0)
        })
        .collect();

    let genesis_block = generate_block_with_txs(
        GENESIS_ROUND,
        GENESIS_SEQ_NUM,
        MIN_BASE_FEE,
        &MockChainConfig::DEFAULT,
        vec![],
    );
    let genesis_block_state = InMemoryBlockState::genesis(genesis_accounts);

    let state: InMemoryState<SecpSignature, MockSignatures<SecpSignature>> =
        InMemoryStateInner::new(Balance::MAX, SeqNum::MAX, genesis_block_state);

    let rpc_addr = rpc::start_rpc_server(state.clone(), &args.rpc_listen)
        .await
        .expect("failed to start RPC server");
    tracing::info!(%rpc_addr, "RPC server listening");
    println!("RPC_ADDR={rpc_addr}");

    let committer_state = state.clone();

    let score_config = fast_score_config(args.ema_half_life_secs, args.promotion_threshold);
    let (score_provider, score_reader) =
        ema::create::<NodeId<monad_secp::PubKey>, StdClock>(score_config.clone(), StdClock);
    let score_reader_log = score_reader.clone();

    let (_channel_tx, channel_input) = ChannelInputStream::new();

    let eth_block_policy = EthBlockPolicy::new(GENESIS_SEQ_NUM, 3);

    let mut client = EthTxPoolExecutor::start_with_input_stream(
        Box::pin(channel_input),
        eth_block_policy,
        state,
        Duration::from_secs(args.soft_tx_expiry_secs),
        Duration::from_secs(args.hard_tx_expiry_secs),
        MockChainConfig::DEFAULT,
        GENESIS_ROUND,
        GENESIS_TIMESTAMP as u64,
        true,
        score_provider,
        score_reader.clone(),
        ForwardedIngressFairQueueConfig::default(),
    );

    client.exec(vec![TxPoolCommand::Reset {
        last_delay_committed_blocks: vec![genesis_block],
    }]);

    let mut dp_rc = DataplaneBuilder::new(1000)
        .with_udp_multishot(true)
        .with_tcp_sockets([(TcpSocketId::Raptorcast, args.rc_tcp_listen)])
        .with_udp_sockets([
            (
                UdpSocketId::AuthenticatedRaptorcast,
                args.rc_auth_udp_listen,
            ),
            (UdpSocketId::Raptorcast, args.rc_udp_listen),
        ])
        .build();

    let mut dp_lean = DataplaneBuilder::new(1000)
        .with_udp_multishot(true)
        .with_udp_sockets([(UdpSocketId::AuthenticatedRaptorcast, args.listen)])
        .build();

    assert!(dp_rc.block_until_ready(Duration::from_secs(5)));
    assert!(dp_lean.block_until_ready(Duration::from_secs(5)));

    let tcp_socket = dp_rc
        .tcp_sockets
        .take(TcpSocketId::Raptorcast)
        .expect("tcp socket");

    let raptorcast_auth_socket = dp_rc
        .udp_sockets
        .take(UdpSocketId::AuthenticatedRaptorcast)
        .expect("raptorcast authenticated udp socket");

    let non_auth_udp_socket = dp_rc
        .udp_sockets
        .take(UdpSocketId::Raptorcast)
        .expect("non-authenticated udp socket");

    let lean_udp_socket = dp_lean
        .udp_sockets
        .take(UdpSocketId::AuthenticatedRaptorcast)
        .expect("lean udp socket");

    let rc_tcp_bind_addr = tcp_socket.local_addr();
    let rc_udp_bind_addr = non_auth_udp_socket.local_addr();
    let rc_auth_udp_bind_addr = raptorcast_auth_socket.local_addr();
    let leanudp_bind_addr = lean_udp_socket.local_addr();

    // Binding to 0.0.0.0/:: is correct for listening, but it is not a connectable
    // destination address. Print connectable addresses using --advertise-ip.
    let rc_tcp_addr_for_clients = SocketAddrV4::new(args.advertise_ip, rc_tcp_bind_addr.port());
    let rc_udp_addr_for_clients = SocketAddrV4::new(args.advertise_ip, rc_udp_bind_addr.port());
    let rc_auth_udp_addr_for_clients =
        SocketAddrV4::new(args.advertise_ip, rc_auth_udp_bind_addr.port());
    let leanudp_addr_for_clients = SocketAddrV4::new(args.advertise_ip, leanudp_bind_addr.port());

    tracing::info!(
        rc_tcp_bind = %rc_tcp_bind_addr,
        rc_udp_bind = %rc_udp_bind_addr,
        rc_auth_udp_bind = %rc_auth_udp_bind_addr,
        leanudp_bind = %leanudp_bind_addr,
        rc_tcp = %rc_tcp_addr_for_clients,
        rc_udp = %rc_udp_addr_for_clients,
        rc_auth_udp = %rc_auth_udp_addr_for_clients,
        leanudp = %leanudp_addr_for_clients,
        "node listening"
    );

    println!("RC_TCP_BIND_ADDR={rc_tcp_bind_addr}");
    println!("RC_UDP_BIND_ADDR={rc_udp_bind_addr}");
    println!("RC_AUTH_UDP_BIND_ADDR={rc_auth_udp_bind_addr}");
    println!("LEANUDP_BIND_ADDR={leanudp_bind_addr}");
    println!("RC_TCP_ADDR={rc_tcp_addr_for_clients}");
    println!("RC_UDP_ADDR={rc_udp_addr_for_clients}");
    println!("RC_AUTH_UDP_ADDR={rc_auth_udp_addr_for_clients}");
    println!("LEANUDP_ADDR={leanudp_addr_for_clients}");
    // Backwards compatibility for existing scripts: CONNECT_ADDR is the address
    // submitters should connect to (LeanUDP).
    println!("CONNECT_ADDR={leanudp_addr_for_clients}");
    println!("LISTEN_ADDR={leanudp_bind_addr}");

    let keypair = node_keypair();
    let keypair_arc = Arc::new(keypair);
    let node_id = NodeId::new(keypair_arc.pubkey());

    let wireauth_config = Config {
        max_sessions_per_ip: args.max_sessions_per_ip,
        low_watermark_sessions: args.low_watermark_sessions,
        ip_rate_limit_window: Duration::from_millis(args.ip_rate_limit_ms),
        ..Config::default()
    };
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
        NodeRaptorCastEvent,
        monad_peer_discovery::mock::NopDiscovery<SignatureType>,
        WireAuthProtocol,
    >::new(
        create_raptorcast_config(keypair_arc.clone()),
        SecondaryRaptorCastModeConfig::None,
        tcp_socket,
        Some(raptorcast_auth_socket),
        non_auth_udp_socket,
        dp_rc.control.clone(),
        Arc::new(Mutex::new(pd)),
        Epoch(0),
        raptorcast_auth_protocol,
    );

    raptorcast.exec(vec![RouterCommand::AddEpochValidatorSet {
        epoch: Epoch(0),
        validator_set: vec![(node_id, Stake::ONE)],
    }]);

    let lean_auth_protocol = WireAuthProtocol::new(wireauth_config, keypair_arc);
    let lean_auth_socket = AuthenticatedSocketHandle::new(lean_udp_socket, lean_auth_protocol);
    let lean_socket = LeanUdpSocketHandle::new(
        lean_auth_socket,
        score_reader.clone(),
        monad_leanudp::Config::default(),
    );
    raptorcast.set_lean_udp_socket(lean_socket);

    let mut stats = StatsCollector::new(&args.stats_file).expect("failed to create stats file");
    let mut committer = BlockCommitter::new(
        committer_state,
        node_keypair(),
        args.proposal_tx_limit,
        args.proposal_gas_limit,
        args.proposal_byte_limit,
    );

    let commit_interval = Duration::from_millis(args.commit_interval_ms);
    let use_timer = commit_interval > Duration::ZERO;

    let mut commit_timer = tokio::time::interval(if use_timer {
        commit_interval
    } else {
        Duration::from_secs(3600)
    });
    commit_timer.tick().await;

    let mut per_peer_received: BTreeMap<NodeId<monad_secp::PubKey>, u64> = BTreeMap::new();
    let mut tx_peer_map: HashMap<alloy_primitives::B256, NodeId<monad_secp::PubKey>> =
        HashMap::new();

    let mut metrics_timer = tokio::time::interval(Duration::from_secs(1));
    metrics_timer.tick().await;
    let mut prev_metrics: HashMap<&'static str, u64> = HashMap::new();
    let mut udp_recv_count: u64 = 0;

    let run_duration = if args.duration_secs > 0 {
        Some(Duration::from_secs(args.duration_secs))
    } else {
        None
    };
    let start_time = std::time::Instant::now();

    tracing::info!(
        commit_interval_ms = args.commit_interval_ms,
        num_accounts = args.num_accounts,
        "node started"
    );

    loop {
        tokio::select! {
            biased;

            raptorcast_event = raptorcast.next() => {
                match raptorcast_event {
                    Some(NodeRaptorCastEvent(RaptorCastEvent::Message(MonadEvent::MempoolEvent(
                        MempoolEvent::ForwardedTxs { sender, txs }
                    )))) => {
                        let tx_count = txs.len();
                        *per_peer_received
                            .entry(sender)
                            .or_default() += tx_count as u64;

                        for tx_bytes in &txs {
                            if let Ok(tx) = alloy_consensus::TxEnvelope::decode(&mut tx_bytes.as_ref()) {
                                tx_peer_map.insert(*tx.tx_hash(), sender);
                            }
                        }

                        client.exec(vec![TxPoolCommand::InsertForwardedTxs { sender, txs }]);
                        stats.record_received(tx_count as u64);
                        udp_recv_count += tx_count as u64;
                        tracing::debug!(tx_count, "received forward_txs via raptorcast");
                    }
                    Some(NodeRaptorCastEvent(RaptorCastEvent::PeerManagerResponse(_))) => {
                        tracing::trace!("received peer manager response (ignored)");
                    }
                    Some(NodeRaptorCastEvent(RaptorCastEvent::SecondaryRaptorcastPeersUpdate(_, _))) => {
                        tracing::trace!("received secondary raptorcast peers update (ignored)");
                    }
                    Some(NodeRaptorCastEvent(RaptorCastEvent::LeanUdpTx { .. })) => {
                        tracing::trace!("received LeanUdpTx (ignored)");
                    }
                    Some(NodeRaptorCastEvent(RaptorCastEvent::LeanUdpForwardTxs { .. })) => {
                        tracing::trace!("received LeanUdpForwardTxs (ignored)");
                    }
                    Some(NodeRaptorCastEvent(RaptorCastEvent::Message(other))) => {
                        tracing::trace!(?other, "ignoring non-forwarded-txs router message");
                    }
                    None => {
                        tracing::error!("raptorcast stream ended");
                        break;
                    }
                }
            }

            event = client.next() => {
                match event {
                    Some(MonadEvent::MempoolEvent(MempoolEvent::ForwardTxs(txs))) => {
                        tracing::debug!(count = txs.len(), "forward txs event");
                    }
                    Some(other) => {
                        tracing::trace!(?other, "executor event");
                    }
                    None => {
                        tracing::error!("executor stream ended");
                        break;
                    }
                }
            }

            _ = metrics_timer.tick() => {
                let chain = client.metrics();
                let current: HashMap<&'static str, u64> = chain.into_inner().into_iter().collect();
                let keys = [
                    "monad.bft.txpool.pool.insert_forwarded_txs",
                    "monad.bft.txpool.pool.drop_not_well_formed",
                    "monad.bft.txpool.pool.drop_invalid_signature",
                    "monad.bft.txpool.pool.drop_nonce_too_low",
                    "monad.bft.txpool.pool.drop_fee_too_low",
                    "monad.bft.txpool.pool.drop_insufficient_balance",
                    "monad.bft.txpool.pool.drop_pool_full",
                    "monad.bft.txpool.pool.drop_pool_not_ready",
                    "monad.bft.txpool.pool.tracked.txs",
                    "monad.bft.txpool.pool.tracked.addresses",
                    "monad.bft.txpool.pool.create_proposal_txs",
                    "monad.bft.txpool.reject_forwarded_invalid_bytes",
                ];
                let mut parts = vec![format!("udp_recv={udp_recv_count}")];
                let always_show = [
                    "monad.bft.txpool.pool.tracked.txs",
                    "monad.bft.txpool.pool.tracked.addresses",
                ];
                for key in keys {
                    let cur = current.get(key).copied().unwrap_or(0);
                    let prev = prev_metrics.get(key).copied().unwrap_or(0);
                    let short = key.rsplit('.').next().unwrap_or(key);
                    let delta = cur.saturating_sub(prev);
                    if delta > 0 || cur > 0 || always_show.contains(&key) {
                        parts.push(format!("{short}={cur}(+{delta})"));
                    }
                }
                tracing::info!("{}", parts.join(" | "));
                prev_metrics = current;
            }

            _ = commit_timer.tick(), if use_timer => {
                let result = committer.commit(&mut client, &tx_peer_map).await;
                stats.record_commit(
                    result.block_number,
                    result.txs_included,
                    result.commit_latency_us,
                    &per_peer_received,
                    &result.per_peer_included,
                    &score_reader_log,
                );
            }
        }

        if let Some(lean_socket) = raptorcast.lean_udp_socket_mut() {
            lean_socket.flush();
        }

        if let Some(d) = run_duration {
            if start_time.elapsed() >= d {
                tracing::info!("duration elapsed, shutting down");
                break;
            }
        }
    }
}
