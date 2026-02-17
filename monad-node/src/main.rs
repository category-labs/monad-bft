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

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    marker::PhantomData,
    net::{IpAddr, SocketAddr, ToSocketAddrs},
    path::PathBuf,
    process,
    sync::Arc,
    time::{Duration, Instant},
};

use alloy_rlp::{Decodable, Encodable};
use chrono::Utc;
use clap::CommandFactory;
use futures_util::{FutureExt, StreamExt};
use monad_chain_config::ChainConfig;
use monad_consensus_state::ConsensusConfig;
use monad_consensus_types::{metrics::Metrics, validator_data::ValidatorSetDataWithEpoch};
use monad_control_panel::ipc::ControlPanelIpcReceiver;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_dataplane::{DataplaneBuilder, TcpSocketId, UdpSocketId};
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_block_validator::EthBlockValidator;
use monad_eth_txpool_executor::{EthTxPoolExecutor, EthTxPoolIpcConfig};
use monad_executor::{Executor, ExecutorMetricsChain};
use monad_executor_glue::{LogFriendlyMonadEvent, Message, MonadEvent};
use monad_ledger::MonadBlockFileLedger;
use monad_node_config::{
    ExecutionProtocolType, FullNodeIdentityConfig, NodeBootstrapConfig, NodeConfig,
    PeerDiscoveryConfig, SignatureCollectionType, SignatureType,
};
use monad_peer_discovery::{
    discovery::{PeerDiscovery, PeerDiscoveryBuilder},
    MonadNameRecord, NameRecord,
};
use monad_pprof::start_pprof_server;
use monad_raptorcast::config::{RaptorCastConfig, RaptorCastConfigPrimary};
use monad_router_multi::MultiRouter;
use monad_state::{MonadMessage, MonadStateBuilder, VerifiedMonadMessage};
use monad_state_backend::StateBackendThreadClient;
use monad_state_backend_cache::StateBackendCache;
use monad_statesync::StateSync;
use monad_triedb_utils::TriedbReader;
use monad_types::{DropTimer, Epoch, NodeId, Round, SeqNum, GENESIS_SEQ_NUM};
use monad_updaters::{
    config_file::ConfigFile, config_loader::ConfigLoader, loopback::LoopbackExecutor,
    parent::ParentExecutor, timer::TokioTimer, tokio_timestamp::TokioTimestamp,
    triedb_val_set::ValSetUpdater,
};
use monad_validator::{
    signature_collection::SignatureCollection, validator_set::ValidatorSetFactory,
    weighted_round_robin::WeightedRoundRobin,
};
use monad_wal::wal::WALoggerConfig;
use opentelemetry::metrics::MeterProvider;
use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use rand_chacha::{rand_core::SeedableRng, ChaCha8Rng};
use tokio::signal::unix::{signal, SignalKind};
use tracing::{error, event, info, warn, Instrument, Level};

use self::{cli::Cli, error::NodeSetupError, state::NodeState};

mod cli;
mod error;
mod state;

#[cfg(all(not(target_env = "msvc"), feature = "jemallocator"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(feature = "jemallocator")]
#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:19\0";

const MONAD_NODE_VERSION: Option<&str> = option_env!("MONAD_VERSION");
const STATESYNC_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

const EXECUTION_DELAY: u64 = 3;

fn main() {
    let mut cmd = Cli::command();

    let node_state = NodeState::setup(&mut cmd).unwrap_or_else(|e| cmd.error(e.kind(), e).exit());

    rayon::ThreadPoolBuilder::new()
        .num_threads(8)
        .thread_name(|i| format!("monad-bft-rn-{}", i))
        .build_global()
        .map_err(Into::into)
        .unwrap_or_else(|e: NodeSetupError| cmd.error(e.kind(), e).exit());

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(Into::into)
        .unwrap_or_else(|e: NodeSetupError| cmd.error(e.kind(), e).exit());

    drop(cmd);

    MONAD_NODE_VERSION.map(|v| info!("starting monad-bft with version {}", v));

    if !node_state.pprof.is_empty() {
        runtime.spawn({
            let pprof = node_state.pprof.clone();
            async {
                let server = match start_pprof_server(pprof) {
                    Ok(server) => server,
                    Err(err) => {
                        error!("failed to start pprof server: {}", err);
                        return;
                    }
                };
                if let Err(err) = server.await {
                    error!("pprof server failed: {}", err);
                }
            }
        });
    }

    if let Err(e) = runtime.block_on(run(node_state)) {
        tracing::error!("monad consensus node crashed: {:?}", e);
    }
}

async fn run(node_state: NodeState) -> Result<(), ()> {
    let locked_epoch_validators = node_state
        .validators_config
        .get_locked_validator_sets(&node_state.forkpoint_config);

    let current_epoch = node_state
        .forkpoint_config
        .high_certificate
        .qc()
        .get_epoch();
    let current_round = node_state
        .forkpoint_config
        .high_certificate
        .qc()
        .get_round()
        + Round(1);
    let router = build_raptorcast_router::<
        SignatureType,
        SignatureCollectionType,
        MonadMessage<SignatureType, SignatureCollectionType, ExecutionProtocolType>,
        VerifiedMonadMessage<SignatureType, SignatureCollectionType, ExecutionProtocolType>,
    >(
        node_state.node_config.clone(),
        node_state.node_config.peer_discovery,
        node_state.router_identity,
        node_state.node_config.bootstrap.clone(),
        &node_state.node_config.fullnode_dedicated.identities,
        locked_epoch_validators.clone(),
        current_epoch,
        current_round,
        node_state.persisted_peers_path,
    );

    let statesync_threshold: usize = node_state.node_config.statesync_threshold.into();

    _ = std::fs::remove_file(node_state.mempool_ipc_path.as_path());
    _ = std::fs::remove_file(node_state.control_panel_ipc_path.as_path());
    _ = std::fs::remove_file(node_state.statesync_ipc_path.as_path());

    // FIXME this is super jank... we should always just pass the 1 file in monad-node
    let mut statesync_triedb_path = node_state.triedb_path.clone();
    if let Ok(files) = std::fs::read_dir(&statesync_triedb_path) {
        let mut files: Vec<_> = files.collect();
        assert_eq!(files.len(), 1, "nothing in triedb path");
        statesync_triedb_path = files
            .pop()
            .unwrap()
            .expect("failed to read triedb path")
            .path();
    }

    let mut bootstrap_nodes = Vec::new();
    for peer_config in &node_state.node_config.bootstrap.peers {
        let peer_id = NodeId::new(peer_config.secp256k1_pubkey);
        bootstrap_nodes.push(peer_id);
    }

    let state_sync_init_peers = node_state
        .node_config
        .statesync
        .init_peers
        .into_iter()
        .map(|p| NodeId::new(p.secp256k1_pubkey))
        .collect();

    // TODO: use PassThruBlockPolicy and NopStateBackend for consensus only mode
    let create_block_policy = || {
        EthBlockPolicy::new(
            GENESIS_SEQ_NUM, // FIXME: MonadStateBuilder is responsible for updating this to forkpoint root if necessary
            EXECUTION_DELAY,
        )
    };

    let state_backend = StateBackendThreadClient::new({
        let triedb_path = node_state.triedb_path.clone();

        move || {
            let triedb_handle =
                TriedbReader::try_new(triedb_path.as_path()).expect("triedb should exist in path");

            StateBackendCache::new(triedb_handle, SeqNum(EXECUTION_DELAY))
        }
    });

    let mut executor = ParentExecutor {
        metrics: Default::default(),
        router,
        timer: TokioTimer::default(),
        ledger: MonadBlockFileLedger::new(node_state.ledger_path),
        config_file: ConfigFile::new(
            node_state.forkpoint_path,
            node_state.validators_path.clone(),
            node_state.chain_config,
        ),
        val_set: ValSetUpdater::new(
            node_state.validators_path,
            node_state.chain_config.get_epoch_length(),
            node_state.chain_config.get_staking_activation(),
            state_backend.clone(),
        ),
        timestamp: TokioTimestamp::new(Duration::from_millis(5), 100, 10001),
        txpool: EthTxPoolExecutor::start(
            create_block_policy(),
            state_backend.clone(),
            EthTxPoolIpcConfig {
                bind_path: node_state.mempool_ipc_path,
                tx_batch_size: node_state.node_config.ipc_tx_batch_size as usize,
                max_queued_batches: node_state.node_config.ipc_max_queued_batches as usize,
                queued_batches_watermark: node_state.node_config.ipc_queued_batches_watermark
                    as usize,
            },
            // TODO(andr-dev): Add tx_expiry to node config
            Duration::from_secs(15),
            Duration::from_secs(5 * 60),
            node_state.chain_config,
            node_state
                .forkpoint_config
                .high_certificate
                .qc()
                .get_round(),
            // TODO(andr-dev): Use timestamp from last commit in ledger
            0,
            true,
        )
        .expect("txpool ipc succeeds"),
        control_panel: ControlPanelIpcReceiver::new(
            node_state.control_panel_ipc_path,
            node_state.reload_handle,
            1000,
        )
        .expect("uds bind failed"),
        loopback: LoopbackExecutor::default(),
        state_sync: StateSync::<SignatureType, SignatureCollectionType>::new(
            vec![statesync_triedb_path.to_string_lossy().to_string()],
            node_state.statesync_sq_thread_cpu,
            state_sync_init_peers,
            node_state
                .node_config
                .statesync_max_concurrent_requests
                .into(),
            STATESYNC_REQUEST_TIMEOUT,
            STATESYNC_REQUEST_TIMEOUT,
            node_state
                .statesync_ipc_path
                .to_str()
                .expect("invalid file name")
                .to_owned(),
        ),
        config_loader: ConfigLoader::new(node_state.node_config_path),
    };

    let logger_config: WALoggerConfig<LogFriendlyMonadEvent<_, _, _>> = WALoggerConfig::new(
        node_state.wal_path.clone(), // output wal path
        false,                       // flush on every write
    );
    let Ok(mut wal) = logger_config.build() else {
        event!(
            Level::ERROR,
            path = node_state.wal_path.as_path().display().to_string(),
            "failed to initialize wal",
        );
        return Err(());
    };

    let block_sync_override_peers = node_state
        .node_config
        .blocksync_override
        .peers
        .into_iter()
        .map(|p| NodeId::new(p.secp256k1_pubkey))
        .collect();

    let whitelisted_statesync_nodes = node_state
        .node_config
        .fullnode_dedicated
        .identities
        .into_iter()
        .map(|p| NodeId::new(p.secp256k1_pubkey))
        .chain(
            node_state
                .node_config
                .fullnode_raptorcast
                .full_nodes_prioritized
                .identities
                .into_iter()
                .map(|p| NodeId::new(p.secp256k1_pubkey)),
        )
        .collect();

    let mut last_ledger_tip: Option<SeqNum> = None;

    let builder = MonadStateBuilder {
        validator_set_factory: ValidatorSetFactory::default(),
        leader_election: WeightedRoundRobin::default(),
        block_validator: EthBlockValidator::default(),
        block_policy: create_block_policy(),
        state_backend,
        key: node_state.secp256k1_identity,
        certkey: node_state.bls12_381_identity,
        beneficiary: node_state.node_config.beneficiary.into(),
        forkpoint: node_state.forkpoint_config.into(),
        locked_epoch_validators,
        block_sync_override_peers,
        maybe_blocksync_rng_seed: None,
        consensus_config: ConsensusConfig {
            execution_delay: SeqNum(EXECUTION_DELAY),
            delta: Duration::from_millis(100),
            // StateSync -> Live transition happens here
            statesync_to_live_threshold: SeqNum(statesync_threshold as u64),
            // Live -> StateSync transition happens here
            live_to_statesync_threshold: SeqNum(statesync_threshold as u64 * 3 / 2),
            // Live starts execution here
            start_execution_threshold: SeqNum(statesync_threshold as u64 / 2),
            chain_config: node_state.chain_config,
            timestamp_latency_estimate_ns: 20_000_000,
            _phantom: Default::default(),
        },
        whitelisted_statesync_nodes,
        statesync_expand_to_group: node_state.node_config.statesync.expand_to_group,
        _phantom: PhantomData,
    };

    let (mut state, init_commands) = builder.build();
    executor.exec(init_commands);

    let mut ledger_span = tracing::info_span!(
        "ledger_span",
        last_ledger_tip = last_ledger_tip.map(|s| s.as_u64())
    );

    let (maybe_otel_meter_provider, mut maybe_metrics_ticker) = node_state
        .otel_endpoint_interval
        .map(|(otel_endpoint, record_metrics_interval)| {
            let provider = build_otel_meter_provider(
                &otel_endpoint,
                format!(
                    "{network_name}_{node_name}",
                    network_name = node_state.node_config.network_name,
                    node_name = node_state.node_config.node_name
                ),
                node_state.node_config.network_name.clone(),
                record_metrics_interval,
            )
            .expect("failed to build otel monad-node");

            let mut timer = tokio::time::interval(record_metrics_interval);

            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            (provider, timer)
        })
        .unzip();

    let maybe_otel_meter = maybe_otel_meter_provider
        .as_ref()
        .map(|provider| provider.meter("opentelemetry"));

    let mut gauge_cache = HashMap::new();
    let process_start = Instant::now();
    let mut total_state_update_elapsed = Duration::ZERO;

    let mut sigterm = signal(SignalKind::terminate()).expect("in tokio rt");
    let mut sigint = signal(SignalKind::interrupt()).expect("in tokio rt");

    loop {
        tokio::select! {
            biased; // events are in order of priority

            result = sigterm.recv() => {
                info!(?result, "received SIGTERM, exiting...");
                break;
            }
            result = sigint.recv() => {
                info!(?result, "received SIGINT, exiting...");
                break;
            }
            _ = match &mut maybe_metrics_ticker {
                Some(ticker) => ticker.tick().boxed(),
                None => futures_util::future::pending().boxed(),
            } => {
                let otel_meter = maybe_otel_meter.as_ref().expect("otel_endpoint must have been set");
                let state_metrics = state.metrics();
                let executor_metrics = executor.metrics();
                send_metrics(otel_meter, &mut gauge_cache, state_metrics, executor_metrics, &process_start, &total_state_update_elapsed);
            }
            event = executor.next().instrument(ledger_span.clone()) => {
                let Some(event) = event else {
                    event!(Level::ERROR, "parent executor returned none!");
                    return Err(());
                };
                let event_debug = {
                    let _timer = DropTimer::start(Duration::from_millis(1), |elapsed| {
                        warn!(
                            ?elapsed,
                            ?event,
                            "long time to format event"
                        )
                    });
                    format!("{:?}", event)
                };

                let event = LogFriendlyMonadEvent {
                    timestamp: Utc::now(),
                    event,
                };

                {
                    let _ledger_span = ledger_span.enter();
                    let _wal_event_span = tracing::trace_span!("wal_event_span").entered();
                    if let Err(err) = wal.push(&event) {
                        event!(Level::ERROR, ?err, "failed to push to wal",);
                        return Err(());
                    }
                };

                let commands = {
                    let _timer = DropTimer::start(Duration::from_millis(50), |elapsed| {
                        warn!(
                            ?elapsed,
                            event =? event_debug,
                            "long time to update event"
                        )
                    });
                    let _ledger_span = ledger_span.enter();
                    let _event_span = tracing::trace_span!("event_span", ?event.event).entered();
                    let start = Instant::now();
                    let cmds = state.update(event.event);
                    total_state_update_elapsed += start.elapsed();
                    cmds
                };

                if !commands.is_empty() {
                    let num_commands = commands.len();
                    let _timer = DropTimer::start(Duration::from_millis(50), |elapsed| {
                        warn!(
                            ?elapsed,
                            event =? event_debug,
                            num_commands,
                            "long time to execute commands"
                        )
                    });
                    let _ledger_span = ledger_span.enter();
                    let _exec_span = tracing::trace_span!("exec_span", num_commands).entered();
                    executor.exec(commands);
                }

                if let Some(ledger_tip) = executor.ledger.last_commit() {
                    if last_ledger_tip.is_none_or(|last_ledger_tip| ledger_tip > last_ledger_tip) {
                        last_ledger_tip = Some(ledger_tip);
                        ledger_span = tracing::info_span!("ledger_span", last_ledger_tip = last_ledger_tip.map(|s| s.as_u64()));
                    }
                }
            }
        }
    }

    Ok(())
}

fn build_raptorcast_router<ST, SCT, M, OM>(
    node_config: NodeConfig<ST>,
    peer_discovery_config: PeerDiscoveryConfig<ST>,
    identity: ST::KeyPairType,
    bootstrap_nodes: NodeBootstrapConfig<ST>,
    full_nodes: &[FullNodeIdentityConfig<CertificateSignaturePubKey<ST>>],
    locked_epoch_validators: Vec<ValidatorSetDataWithEpoch<SCT>>,
    current_epoch: Epoch,
    current_round: Round,
    persisted_peers_path: PathBuf,
) -> MultiRouter<
    ST,
    M,
    OM,
    MonadEvent<ST, SCT, ExecutionProtocolType>,
    PeerDiscovery<ST>,
    monad_raptorcast::auth::WireAuthProtocol,
>
where
    ST: CertificateSignatureRecoverable<KeyPairType = monad_secp::KeyPair>,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>>
        + Decodable
        + From<OM>
        + Send
        + Sync
        + 'static,
    OM: Encodable + Clone + Send + Sync + 'static,
{
    let bind_address = SocketAddr::new(
        IpAddr::V4(node_config.network.bind_address_host),
        node_config.network.bind_address_port,
    );
    let authenticated_bind_address = node_config
        .network
        .authenticated_bind_address_port
        .map(|port| SocketAddr::new(IpAddr::V4(node_config.network.bind_address_host), port));
    let Some(SocketAddr::V4(name_record_address)) = resolve_domain_v4(
        &NodeId::new(identity.pubkey()),
        &peer_discovery_config.self_address,
    ) else {
        panic!(
            "Unable to resolve self address: {:?}",
            peer_discovery_config.self_address
        );
    };

    tracing::debug!(
        ?bind_address,
        ?authenticated_bind_address,
        ?name_record_address,
        "Monad-node starting, pid: {}",
        process::id()
    );

    let network_config = node_config.network;

    let mut dp_builder = DataplaneBuilder::new(network_config.max_mbps.into())
        .with_udp_multishot(network_config.enable_udp_multishot);
    if let Some(buffer_size) = network_config.buffer_size {
        dp_builder = dp_builder.with_udp_buffer_size(buffer_size);
    }
    dp_builder = dp_builder
        .with_tcp_connections_limit(
            network_config.tcp_connections_limit,
            network_config.tcp_per_ip_connections_limit,
        )
        .with_tcp_rps_burst(
            network_config.tcp_rate_limit_rps,
            network_config.tcp_rate_limit_burst,
        );

    let mut udp_sockets: Vec<(UdpSocketId, std::net::SocketAddr)> =
        vec![(UdpSocketId::Raptorcast, bind_address)];
    if let Some(auth_addr) = authenticated_bind_address {
        udp_sockets.push((UdpSocketId::AuthenticatedRaptorcast, auth_addr));
    }
    dp_builder = dp_builder
        .with_udp_sockets(udp_sockets)
        .with_tcp_sockets([(TcpSocketId::Raptorcast, bind_address)]);

    // auth port in peer discovery config and network config should be set and unset simultaneously
    assert_eq!(
        peer_discovery_config.self_auth_port.is_some(),
        network_config.authenticated_bind_address_port.is_some()
    );

    let self_id = NodeId::new(identity.pubkey());
    let self_record = match peer_discovery_config.self_auth_port {
        Some(auth_port) => NameRecord::new_with_authentication(
            *name_record_address.ip(),
            name_record_address.port(),
            name_record_address.port(),
            auth_port,
            peer_discovery_config.self_record_seq_num,
        ),
        None => NameRecord::new(
            *name_record_address.ip(),
            name_record_address.port(),
            peer_discovery_config.self_record_seq_num,
        ),
    };
    let self_record = MonadNameRecord::new(self_record, &identity);
    info!(?self_id, ?self_record, "self name record");
    assert!(
        self_record.signature == peer_discovery_config.self_name_record_sig,
        "self name record signature mismatch"
    );

    // initial set of peers
    let bootstrap_peers: BTreeMap<_, _> = bootstrap_nodes
        .peers
        .iter()
        .filter_map(|peer| {
            let node_id = NodeId::new(peer.secp256k1_pubkey);
            if node_id == self_id {
                return None;
            }
            let address = match resolve_domain_v4(&node_id, &peer.address) {
                Some(SocketAddr::V4(addr)) => addr,
                _ => {
                    warn!(?node_id, ?peer.address, "Unable to resolve");
                    return None;
                }
            };

            let peer_entry = monad_executor_glue::PeerEntry {
                pubkey: peer.secp256k1_pubkey,
                addr: address,
                signature: peer.name_record_sig,
                record_seq_num: peer.record_seq_num,
                auth_port: peer.auth_port,
            };

            match MonadNameRecord::try_from(&peer_entry) {
                Ok(monad_name_record) => Some((node_id, monad_name_record)),
                Err(_) => {
                    warn!(?node_id, "invalid name record signature in config file");
                    None
                }
            }
        })
        .collect();

    let epoch_validators: BTreeMap<Epoch, BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>> =
        locked_epoch_validators
            .iter()
            .map(|epoch_validators| {
                (
                    epoch_validators.epoch,
                    epoch_validators
                        .validators
                        .0
                        .iter()
                        .map(|validator| validator.node_id)
                        .collect(),
                )
            })
            .collect();
    let prioritized_full_nodes: BTreeSet<_> = node_config
        .fullnode_raptorcast
        .full_nodes_prioritized
        .identities
        .iter()
        .map(|id| NodeId::new(id.secp256k1_pubkey))
        .collect();
    let pinned_full_nodes: BTreeSet<_> = full_nodes
        .iter()
        .map(|full_node| NodeId::new(full_node.secp256k1_pubkey))
        .chain(prioritized_full_nodes.clone())
        .chain(bootstrap_peers.keys().cloned())
        .collect();

    let peer_discovery_builder = PeerDiscoveryBuilder {
        self_id,
        self_record,
        current_round,
        current_epoch,
        epoch_validators: epoch_validators.clone(),
        pinned_full_nodes,
        prioritized_full_nodes,
        bootstrap_peers,
        refresh_period: Duration::from_secs(peer_discovery_config.refresh_period),
        request_timeout: Duration::from_secs(peer_discovery_config.request_timeout),
        unresponsive_prune_threshold: peer_discovery_config.unresponsive_prune_threshold,
        last_participation_prune_threshold: peer_discovery_config
            .last_participation_prune_threshold,
        min_num_peers: peer_discovery_config.min_num_peers,
        max_num_peers: peer_discovery_config.max_num_peers,
        max_group_size: node_config.fullnode_raptorcast.max_group_size,
        enable_publisher: node_config.fullnode_raptorcast.enable_publisher,
        enable_client: node_config.fullnode_raptorcast.enable_client,
        rng: ChaCha8Rng::from_entropy(),
        persisted_peers_path,
        ping_rate_limit_per_second: peer_discovery_config.ping_rate_limit_per_second,
    };

    let shared_key = Arc::new(identity);
    let wireauth_config = monad_wireauth::Config::default();
    let auth_protocol =
        monad_raptorcast::auth::WireAuthProtocol::new(wireauth_config, shared_key.clone());

    MultiRouter::new(
        self_id,
        RaptorCastConfig {
            shared_key,
            mtu: network_config.mtu,
            udp_message_max_age_ms: network_config.udp_message_max_age_ms,
            sig_verification_rate_limit: network_config.signature_verifications_per_second,
            primary_instance: RaptorCastConfigPrimary {
                raptor10_redundancy: 2.5f32,
                fullnode_dedicated: full_nodes
                    .iter()
                    .map(|full_node| NodeId::new(full_node.secp256k1_pubkey))
                    .collect(),
            },
            secondary_instance: node_config.fullnode_raptorcast,
        },
        dp_builder,
        peer_discovery_builder,
        current_epoch,
        epoch_validators,
        auth_protocol,
    )
}

fn resolve_domain_v4<P: PubKey>(node_id: &NodeId<P>, domain: &String) -> Option<SocketAddr> {
    let resolved = match domain.to_socket_addrs() {
        Ok(resolved) => resolved,
        Err(err) => {
            warn!(?node_id, ?domain, ?err, "Unable to resolve");
            return None;
        }
    };

    for entry in resolved {
        match entry {
            SocketAddr::V4(_) => return Some(entry),
            SocketAddr::V6(_) => continue,
        }
    }

    warn!(?node_id, ?domain, "No IPv4 DNS record");
    None
}

const GAUGE_TOTAL_UPTIME_US: &str = "monad.total_uptime_us";
const GAUGE_STATE_TOTAL_UPDATE_US: &str = "monad.state.total_update_us";
const GAUGE_NODE_INFO: &str = "monad_node_info";

/// Returns the HELP description for a metric, used for Prometheus/OTEL.
fn get_metric_description(name: &str) -> &'static str {
    match name {
        // Node metrics
        "monad.total_uptime_us" => "Total node uptime in microseconds",
        "monad.state.total_update_us" => "Total time spent updating state in microseconds",
        "monad_node_info" => "Node info indicator (always 1)",
        // Ledger metrics
        "monad.execution_ledger.num_commits" => "Blocks committed to the execution ledger",
        "monad.execution_ledger.num_tx_commits" => "Transactions committed to the execution ledger",
        "monad.execution_ledger.block_num" => "Current block number in the execution ledger",
        // StateSync metrics
        "monad.statesync.syncing" => "Whether state sync is active (1) or not (0)",
        "monad.statesync.progress_estimate" => "Estimated progress of state sync operation",
        "monad.statesync.last_target" => "Last target block number for state sync",
        "monad.statesync.server_pending_requests" => "Pending state sync server requests",
        "monad.statesync.server_num_syncdone_success" => "Successful sync completions",
        "monad.statesync.server_num_syncdone_failed" => "Failed sync completions",
        "monad.statesync.server_total_service_time_us" => {
            "Total state sync service time in microseconds"
        }
        // TxPool metrics
        "monad.bft.txpool.pool.insert_owned_txs" => "Owned transactions inserted into the pool",
        "monad.bft.txpool.pool.insert_forwarded_txs" => {
            "Forwarded transactions inserted into the pool"
        }
        "monad.bft.txpool.pool.drop_not_well_formed" => {
            "Transactions dropped due to malformed data"
        }
        "monad.bft.txpool.pool.drop_invalid_signature" => {
            "Transactions dropped due to invalid signature"
        }
        "monad.bft.txpool.pool.drop_nonce_too_low" => "Transactions dropped due to nonce too low",
        "monad.bft.txpool.pool.drop_fee_too_low" => "Transactions dropped due to fee too low",
        "monad.bft.txpool.pool.drop_insufficient_balance" => {
            "Transactions dropped due to insufficient balance"
        }
        "monad.bft.txpool.pool.drop_existing_higher_priority" => {
            "Transactions dropped - existing tx has higher priority"
        }
        "monad.bft.txpool.pool.drop_replaced_by_higher_priority" => {
            "Transactions replaced by higher priority"
        }
        "monad.bft.txpool.pool.drop_pool_full" => "Transactions dropped because pool is full",
        "monad.bft.txpool.pool.drop_pool_not_ready" => {
            "Transactions dropped because pool is not ready"
        }
        "monad.bft.txpool.pool.drop_internal_state_backend_error" => {
            "Transactions dropped due to backend error"
        }
        "monad.bft.txpool.pool.drop_internal_not_ready" => {
            "Transactions dropped due to internal not ready"
        }
        "monad.bft.txpool.pool.create_proposal" => "Proposals created from txpool",
        "monad.bft.txpool.pool.create_proposal_txs" => "Transactions included in proposals",
        "monad.bft.txpool.pool.create_proposal_tracked_addresses" => {
            "Tracked addresses during proposal creation"
        }
        "monad.bft.txpool.pool.create_proposal_available_addresses" => {
            "Available addresses during proposal creation"
        }
        "monad.bft.txpool.pool.create_proposal_backend_lookups" => {
            "Backend lookups during proposal creation"
        }
        "monad.bft.txpool.pool.tracked.addresses" => "Addresses being tracked in the pool",
        "monad.bft.txpool.pool.tracked.txs" => "Transactions being tracked in the pool",
        "monad.bft.txpool.pool.tracked.evict_expired_addresses" => {
            "Addresses evicted due to expiration"
        }
        "monad.bft.txpool.pool.tracked.evict_expired_txs" => {
            "Transactions evicted due to expiration"
        }
        "monad.bft.txpool.pool.tracked.remove_committed_addresses" => {
            "Addresses removed after commitment"
        }
        "monad.bft.txpool.pool.tracked.remove_committed_txs" => {
            "Transactions removed after commitment"
        }
        "monad.bft.txpool.reject_forwarded_invalid_bytes" => {
            "Forwarded txs rejected due to invalid bytes"
        }
        "monad.bft.txpool.create_proposal_elapsed_ns" => {
            "Time spent creating proposals in nanoseconds"
        }
        "monad.bft.txpool.preload_backend_lookups" => "Preload backend lookups",
        "monad.bft.txpool.preload_backend_requests" => "Preload backend requests",
        // RaptorCast metrics
        "monad.raptorcast.total_messages_received" => "Total raptorcast messages received",
        "monad.raptorcast.total_recv_errors" => "Total raptorcast receive errors",
        "monad.raptorcast.decoding_cache.signature_verifications_rate_limited" => {
            "Signature verifications rate limited"
        }
        "monad.raptorcast.decoding_cache.decoded_hit" => "Hits on recently decoded messages",
        "monad.raptorcast.decoding_cache.pending_hit" => "Hits on pending messages in cache",
        "monad.raptorcast.decoding_cache.new_entry" => "New entries added to decoding cache",
        "monad.raptorcast.decoding_cache.decoded" => "Messages successfully decoded",
        "monad.bft.raptorcast.udp.primary_broadcast_latency_p99_ms" => {
            "P99 primary UDP broadcast latency in ms (30s rolling window)"
        }
        "monad.bft.raptorcast.udp.primary_broadcast_latency_count" => {
            "Primary broadcast latency measurement count (30s rolling window)"
        }
        "monad.bft.raptorcast.udp.secondary_broadcast_latency_p99_ms" => {
            "P99 secondary UDP broadcast latency in ms (30s rolling window)"
        }
        "monad.bft.raptorcast.udp.secondary_broadcast_latency_count" => {
            "Secondary broadcast latency measurement count (30s rolling window)"
        }
        "monad.raptorcast.auth.authenticated_udp_bytes_written" => {
            "Bytes written via authenticated UDP"
        }
        "monad.raptorcast.auth.non_authenticated_udp_bytes_written" => {
            "Bytes written via non-authenticated UDP"
        }
        "monad.raptorcast.auth.authenticated_udp_bytes_read" => "Bytes read via authenticated UDP",
        "monad.raptorcast.auth.non_authenticated_udp_bytes_read" => {
            "Bytes read via non-authenticated UDP"
        }
        // Peer Discovery metrics
        "monad.peer_disc.send_ping" => "Ping messages sent",
        "monad.peer_disc.recv_ping" => "Ping messages received",
        "monad.peer_disc.drop_ping" => "Ping messages dropped",
        "monad.peer_disc.ping_timeout" => "Ping timeouts",
        "monad.peer_disc.send_pong" => "Pong messages sent",
        "monad.peer_disc.recv_pong" => "Pong messages received",
        "monad.peer_disc.drop_pong" => "Pong messages dropped",
        "monad.peer_disc.rate_limited" => "Peer discovery operations rate limited",
        "monad.peer_disc.send_lookup_request" => "Lookup requests sent",
        "monad.peer_disc.recv_lookup_request" => "Lookup requests received",
        "monad.peer_disc.recv_open_lookup_request" => "Open lookup requests received",
        "monad.peer_disc.recv_targeted_lookup_request" => "Targeted lookup requests received",
        "monad.peer_disc.retry_lookup_request" => "Lookup request retries",
        "monad.peer_disc.send_lookup_response" => "Lookup responses sent",
        "monad.peer_disc.recv_lookup_response" => "Lookup responses received",
        "monad.peer_disc.drop_lookup_response" => "Lookup responses dropped",
        "monad.peer_disc.lookup_timeout" => "Lookup timeouts",
        "monad.peer_disc.send_raptorcast_request" => "Raptorcast requests sent",
        "monad.peer_disc.recv_raptorcast_request" => "Raptorcast requests received",
        "monad.peer_disc.send_raptorcast_response" => "Raptorcast responses sent",
        "monad.peer_disc.recv_raptorcast_response" => "Raptorcast responses received",
        "monad.peer_disc.refresh" => "Peer discovery refresh operations",
        "monad.peer_disc.num_peers" => "Current number of known peers",
        "monad.peer_disc.num_pending_peers" => "Current number of pending peers",
        "monad.peer_disc.num_upstream_validators" => "Current number of upstream validators",
        "monad.peer_disc.num_downstream_fullnodes" => "Current number of downstream full nodes",
        "monad.peer_disc.socket_collisions" => "Socket address collisions",
        // WireAuth state metrics
        "monad.wireauth.state.initiating_sessions" => "Sessions in initiating state",
        "monad.wireauth.state.responding_sessions" => "Sessions in responding state",
        "monad.wireauth.state.transport_sessions" => "Sessions in transport state",
        "monad.wireauth.state.total_sessions" => "Current total wireauth sessions",
        "monad.wireauth.state.allocated_indices" => "Allocated session indices",
        "monad.wireauth.state.sessions_by_public_key" => "Sessions indexed by public key",
        "monad.wireauth.state.sessions_by_socket" => "Sessions indexed by socket",
        "monad.wireauth.state.session_index_allocated" => "Session indices allocated",
        "monad.wireauth.state.session_established_initiator" => "Sessions established as initiator",
        "monad.wireauth.state.session_established_responder" => "Sessions established as responder",
        "monad.wireauth.state.session_terminated" => "Sessions terminated",
        "monad.wireauth.state.timers_size" => "Active timers count",
        "monad.wireauth.state.packet_queue_size" => "Packet queue size",
        "monad.wireauth.state.initiated_session_by_peer_size" => "Initiated sessions by peer",
        "monad.wireauth.state.accepted_sessions_by_peer_size" => "Accepted sessions by peer",
        "monad.wireauth.state.ip_session_counts_size" => "IP session counts map size",
        // WireAuth filter metrics
        "monad.wireauth.filter.ip_request_history_size" => "IP request history size",
        "monad.wireauth.filter.pass" => "Packets that passed the filter",
        "monad.wireauth.filter.send_cookie" => "Cookie replies sent by filter",
        "monad.wireauth.filter.drop" => "Packets dropped by filter",
        // WireAuth API metrics
        "monad.wireauth.api.connect" => "Connect API calls",
        "monad.wireauth.api.decrypt" => "Decrypt API calls",
        "monad.wireauth.api.encrypt_by_public_key" => "Encrypt by public key API calls",
        "monad.wireauth.api.encrypt_by_socket" => "Encrypt by socket API calls",
        "monad.wireauth.api.disconnect" => "Disconnect API calls",
        "monad.wireauth.api.dispatch_control" => "Dispatch control API calls",
        "monad.wireauth.api.next_packet" => "Next packet API calls",
        "monad.wireauth.api.tick" => "Tick API calls",
        // WireAuth dispatch metrics
        "monad.wireauth.dispatch.handshake_initiation" => {
            "Handshake initiation messages dispatched"
        }
        "monad.wireauth.dispatch.handshake_response" => "Handshake response messages dispatched",
        "monad.wireauth.dispatch.cookie_reply" => "Cookie reply messages dispatched",
        "monad.wireauth.dispatch.keepalive" => "Keepalive messages dispatched",
        // WireAuth error metrics
        "monad.wireauth.error.connect" => "Connect errors",
        "monad.wireauth.error.decrypt" => "Decrypt errors",
        "monad.wireauth.error.decrypt.nonce_outside_window" => {
            "Decrypt errors - nonce outside window"
        }
        "monad.wireauth.error.decrypt.nonce_duplicate" => "Decrypt errors - duplicate nonce",
        "monad.wireauth.error.decrypt.mac" => "Decrypt errors - MAC verification failed",
        "monad.wireauth.error.encrypt_by_public_key" => "Encrypt by public key errors",
        "monad.wireauth.error.encrypt_by_socket" => "Encrypt by socket errors",
        "monad.wireauth.error.dispatch_control" => "Dispatch control errors",
        "monad.wireauth.error.session_exhausted" => "Session exhausted errors",
        "monad.wireauth.error.mac1_verification_failed" => "MAC1 verification failures",
        "monad.wireauth.error.timestamp_replay" => "Timestamp replay errors",
        "monad.wireauth.error.session_not_found" => "Session not found errors",
        "monad.wireauth.error.session_index_not_found" => "Session index not found errors",
        "monad.wireauth.error.handshake_init_validation" => "Handshake init validation errors",
        "monad.wireauth.error.cookie_reply" => "Cookie reply errors",
        "monad.wireauth.error.handshake_response_validation" => {
            "Handshake response validation errors"
        }
        // WireAuth enqueued metrics
        "monad.wireauth.enqueued.handshake_init" => "Handshake init messages enqueued",
        "monad.wireauth.enqueued.handshake_response" => "Handshake response messages enqueued",
        "monad.wireauth.enqueued.cookie_reply" => "Cookie reply messages enqueued",
        "monad.wireauth.enqueued.keepalive" => "Keepalive messages enqueued",
        "monad.wireauth.rate_limit.drop" => "Packets dropped due to rate limiting",
        // Executor exec metrics
        "monad.executor.parent.total_exec_us" => {
            "Total parent executor execution time in microseconds"
        }
        "monad.executor.ledger.total_exec_us" => {
            "Total ledger executor execution time in microseconds"
        }
        "monad.executor.config_file.total_exec_us" => {
            "Total config file executor execution time in microseconds"
        }
        "monad.executor.txpool.total_exec_us" => {
            "Total TxPool executor execution time in microseconds"
        }
        "monad.executor.router.total_exec_us" => {
            "Total router executor execution time in microseconds"
        }
        "monad.executor.statesync.total_exec_us" => {
            "Total StateSync executor execution time in microseconds"
        }
        // Executor poll metrics
        "monad.executor.parent.total_poll_us" => "Total parent executor poll time in microseconds",
        "monad.executor.ledger.total_poll_us" => "Total ledger executor poll time in microseconds",
        "monad.executor.txpool.total_poll_us" => "Total TxPool executor poll time in microseconds",
        "monad.executor.router.total_poll_us" => "Total router executor poll time in microseconds",
        "monad.executor.statesync.total_poll_us" => {
            "Total StateSync executor poll time in microseconds"
        }
        // Consensus - NodeState
        "monad.state.node_state.self_stake_bps" => "Self stake in basis points",
        // Consensus - ValidationErrors
        "monad.state.validation_errors.invalid_author" => "Validation errors - invalid author",
        "monad.state.validation_errors.not_well_formed_sig" => {
            "Validation errors - malformed signature"
        }
        "monad.state.validation_errors.invalid_signature" => {
            "Validation errors - invalid signature"
        }
        "monad.state.validation_errors.invalid_tc_round" => "Validation errors - invalid TC round",
        "monad.state.validation_errors.duplicate_tc_tip_round" => {
            "Validation errors - duplicate TC tip round"
        }
        "monad.state.validation_errors.empty_signers_tc_tip_round" => {
            "Validation errors - empty signers TC tip"
        }
        "monad.state.validation_errors.too_many_tc_tip_round" => {
            "Validation errors - too many TC tip round"
        }
        "monad.state.validation_errors.insufficient_stake" => {
            "Validation errors - insufficient stake"
        }
        "monad.state.validation_errors.invalid_seq_num" => {
            "Validation errors - invalid sequence number"
        }
        "monad.state.validation_errors.val_data_unavailable" => {
            "Validation errors - validator data unavailable"
        }
        "monad.state.validation_errors.signatures_duplicate_node" => {
            "Validation errors - duplicate node signatures"
        }
        "monad.state.validation_errors.invalid_vote_message" => {
            "Validation errors - invalid vote message"
        }
        "monad.state.validation_errors.invalid_version" => "Validation errors - invalid version",
        "monad.state.validation_errors.invalid_epoch" => "Validation errors - invalid epoch",
        // Consensus - ConsensusEvents
        "monad.state.consensus_events.local_timeout" => "Local timeout events",
        "monad.state.consensus_events.handle_proposal" => "Proposal handling events",
        "monad.state.consensus_events.failed_txn_validation" => "Failed transaction validation",
        "monad.state.consensus_events.failed_ts_validation" => "Failed timestamp validation",
        "monad.state.consensus_events.invalid_proposal_round_leader" => {
            "Invalid proposal round leader"
        }
        "monad.state.consensus_events.out_of_order_proposals" => "Out of order proposals",
        "monad.state.consensus_events.created_vote" => "Votes created",
        "monad.state.consensus_events.old_vote_received" => "Old votes received",
        "monad.state.consensus_events.future_vote_received" => "Future votes received",
        "monad.state.consensus_events.vote_received" => "Votes received",
        "monad.state.consensus_events.created_qc" => "Quorum certificates created",
        "monad.state.consensus_events.old_remote_timeout" => "Old remote timeout events",
        "monad.state.consensus_events.remote_timeout_msg" => "Remote timeout messages received",
        "monad.state.consensus_events.remote_timeout_msg_with_tc" => {
            "Remote timeout messages with TC"
        }
        "monad.state.consensus_events.remote_timeout_msg_with_future_tc" => {
            "Remote timeout messages with future TC"
        }
        "monad.state.consensus_events.created_tc" => "Timeout certificates created",
        "monad.state.consensus_events.process_old_qc" => "Old QC processing events",
        "monad.state.consensus_events.process_qc" => "QC processing events",
        "monad.state.consensus_events.process_old_tc" => "Old TC processing events",
        "monad.state.consensus_events.process_tc" => "TC processing events",
        "monad.state.consensus_events.creating_proposal" => "Proposal creation events",
        "monad.state.consensus_events.rx_execution_lagging" => "Execution lagging events",
        "monad.state.consensus_events.rx_bad_state_root" => "Bad state root events",
        "monad.state.consensus_events.rx_base_fee_error" => "Base fee error events",
        "monad.state.consensus_events.proposal_with_tc" => "Proposals with TC",
        "monad.state.consensus_events.failed_verify_randao_reveal_sig" => {
            "Failed RANDAO reveal signature verifications"
        }
        "monad.state.consensus_events.commit_block" => "Block commit events",
        "monad.state.consensus_events.enter_new_round_qc" => "New round entries via QC",
        "monad.state.consensus_events.enter_new_round_tc" => "New round entries via TC",
        "monad.state.consensus_events.trigger_state_sync" => "State sync trigger events",
        "monad.state.consensus_events.handle_round_recovery" => "Round recovery handling events",
        "monad.state.consensus_events.invalid_round_recovery_leader" => {
            "Invalid round recovery leader"
        }
        "monad.state.consensus_events.handle_no_endorsement" => "No endorsement handling events",
        "monad.state.consensus_events.old_no_endorsement_received" => {
            "Old no endorsement messages received"
        }
        "monad.state.consensus_events.future_no_endorsement_received" => {
            "Future no endorsement messages received"
        }
        "monad.state.consensus_events.created_nec" => "No endorsement certificates created",
        "monad.state.consensus_events.handle_advance_round" => "Advance round handling events",
        // Consensus - BlocktreeEvents
        "monad.state.blocktree_events.prune_success" => "Successful blocktree prune operations",
        "monad.state.blocktree_events.add_success" => "Successful blocktree add operations",
        "monad.state.blocktree_events.add_dup" => "Duplicate blocktree add operations",
        // Consensus - BlocksyncEvents
        "monad.state.blocksync_events.self_headers_request" => "Self headers requests",
        "monad.state.blocksync_events.self_payload_request" => "Self payload requests",
        "monad.state.blocksync_events.self_payload_requests_in_flight" => {
            "Self payload requests in flight"
        }
        "monad.state.blocksync_events.headers_response_successful" => {
            "Successful headers responses"
        }
        "monad.state.blocksync_events.headers_response_failed" => "Failed headers responses",
        "monad.state.blocksync_events.headers_response_unexpected" => {
            "Unexpected headers responses"
        }
        "monad.state.blocksync_events.headers_validation_failed" => "Failed headers validations",
        "monad.state.blocksync_events.self_headers_response_successful" => {
            "Successful self headers responses"
        }
        "monad.state.blocksync_events.self_headers_response_failed" => {
            "Failed self headers responses"
        }
        "monad.state.blocksync_events.num_headers_received" => "Headers received",
        "monad.state.blocksync_events.payload_response_successful" => {
            "Successful payload responses"
        }
        "monad.state.blocksync_events.payload_response_failed" => "Failed payload responses",
        "monad.state.blocksync_events.payload_response_unexpected" => {
            "Unexpected payload responses"
        }
        "monad.state.blocksync_events.self_payload_response_successful" => {
            "Successful self payload responses"
        }
        "monad.state.blocksync_events.self_payload_response_failed" => {
            "Failed self payload responses"
        }
        "monad.state.blocksync_events.request_timeout" => "Block sync request timeouts",
        "monad.state.blocksync_events.request_failed_no_peers" => {
            "Block sync requests failed - no peers"
        }
        "monad.state.blocksync_events.peer_headers_request" => "Peer headers requests",
        "monad.state.blocksync_events.peer_headers_request_successful" => {
            "Successful peer headers requests"
        }
        "monad.state.blocksync_events.peer_headers_request_failed" => {
            "Failed peer headers requests"
        }
        "monad.state.blocksync_events.peer_payload_request" => "Peer payload requests",
        "monad.state.blocksync_events.peer_payload_request_successful" => {
            "Successful peer payload requests"
        }
        "monad.state.blocksync_events.peer_payload_request_failed" => {
            "Failed peer payload requests"
        }
        _ => "",
    }
}

fn send_metrics(
    meter: &opentelemetry::metrics::Meter,
    gauge_cache: &mut HashMap<&'static str, opentelemetry::metrics::Gauge<u64>>,
    state_metrics: &Metrics,
    executor_metrics: ExecutorMetricsChain,
    process_start: &Instant,
    total_state_update_elapsed: &Duration,
) {
    let node_info_gauge = gauge_cache.entry(GAUGE_NODE_INFO).or_insert_with(|| {
        let desc = get_metric_description(GAUGE_NODE_INFO);
        if desc.is_empty() {
            meter.u64_gauge(GAUGE_NODE_INFO).build()
        } else {
            meter
                .u64_gauge(GAUGE_NODE_INFO)
                .with_description(desc)
                .build()
        }
    });
    node_info_gauge.record(1, &[]);

    for (k, v) in state_metrics
        .metrics()
        .into_iter()
        .chain(executor_metrics.into_inner())
        .chain(std::iter::once((
            GAUGE_TOTAL_UPTIME_US,
            process_start.elapsed().as_micros() as u64,
        )))
        .chain(std::iter::once((
            GAUGE_STATE_TOTAL_UPDATE_US,
            total_state_update_elapsed.as_micros() as u64,
        )))
    {
        let gauge = gauge_cache.entry(k).or_insert_with(|| {
            let desc = get_metric_description(k);
            if desc.is_empty() {
                meter.u64_gauge(k).build()
            } else {
                meter.u64_gauge(k).with_description(desc).build()
            }
        });
        gauge.record(v, &[]);
    }
}

fn build_otel_meter_provider(
    otel_endpoint: &str,
    service_name: String,
    network_name: String,
    interval: Duration,
) -> Result<opentelemetry_sdk::metrics::SdkMeterProvider, NodeSetupError> {
    let exporter = MetricExporter::builder()
        .with_tonic()
        .with_timeout(interval * 2)
        .with_endpoint(otel_endpoint)
        .build()?;

    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter)
        .with_interval(interval / 2)
        .build();

    let mut attrs = vec![
        opentelemetry::KeyValue::new(
            opentelemetry_semantic_conventions::resource::SERVICE_NAME,
            service_name,
        ),
        opentelemetry::KeyValue::new("network", network_name),
    ];
    if let Some(version) = MONAD_NODE_VERSION {
        attrs.push(opentelemetry::KeyValue::new(
            opentelemetry_semantic_conventions::resource::SERVICE_VERSION,
            version,
        ));
    }

    let provider_builder = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(
            opentelemetry_sdk::Resource::builder_empty()
                .with_attributes(attrs)
                .build(),
        );

    Ok(provider_builder.build())
}
