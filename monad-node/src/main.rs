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
    CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_dataplane::{
    DataplaneBuilder, TcpSocketConfig, TcpSocketType, UdpSocketConfig, UdpSocketType,
};
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
use monad_statesync::StateSync;
use monad_triedb_cache::StateBackendCache;
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
        leader_election: WeightedRoundRobin::new(node_state.chain_config.get_staking_activation()),
        block_validator: EthBlockValidator::default(),
        block_policy: create_block_policy(),
        state_backend,
        key: node_state.secp256k1_identity,
        certkey: node_state.bls12_381_identity,
        beneficiary: node_state.node_config.beneficiary.into(),
        forkpoint: node_state.forkpoint_config.into(),
        locked_epoch_validators,
        block_sync_override_peers,
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
) -> MultiRouter<ST, M, OM, MonadEvent<ST, SCT, ExecutionProtocolType>, PeerDiscovery<ST>>
where
    ST: CertificateSignatureRecoverable,
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
        ?name_record_address,
        "Monad-node starting, pid: {}",
        process::id()
    );

    let network_config = node_config.network;

    let mut dp_builder = DataplaneBuilder::new(network_config.max_mbps.into());
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
        )
        .extend_udp_sockets(vec![UdpSocketConfig {
            socket_addr: bind_address,
            socket: UdpSocketType::Original,
        }])
        .extend_tcp_sockets(vec![TcpSocketConfig {
            socket_addr: bind_address,
            socket: TcpSocketType::Original,
        }]);

    let self_id = NodeId::new(identity.pubkey());
    let self_record = NameRecord::new(
        *name_record_address.ip(),
        name_record_address.port(),
        peer_discovery_config.self_record_seq_num,
    );
    let self_record = MonadNameRecord::new(self_record, &identity);
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
    };

    MultiRouter::new(
        self_id,
        RaptorCastConfig {
            shared_key: Arc::new(identity),
            mtu: network_config.mtu,
            udp_message_max_age_ms: network_config.udp_message_max_age_ms,
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

fn send_metrics(
    meter: &opentelemetry::metrics::Meter,
    gauge_cache: &mut HashMap<&'static str, opentelemetry::metrics::Gauge<u64>>,
    state_metrics: &Metrics,
    executor_metrics: ExecutorMetricsChain,
    process_start: &Instant,
    total_state_update_elapsed: &Duration,
) {
    let node_info_gauge = gauge_cache
        .entry(GAUGE_NODE_INFO)
        .or_insert_with(|| meter.u64_gauge(GAUGE_NODE_INFO).build());
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
        let gauge = gauge_cache
            .entry(k)
            .or_insert_with(|| meter.u64_gauge(k).build());
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
