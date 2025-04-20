use std::{
    collections::{BTreeSet, HashMap},
    marker::PhantomData,
    net::{SocketAddr, SocketAddrV4, ToSocketAddrs},
    time::Duration,
};

use bytes::Bytes;
use chrono::Utc;
use clap::CommandFactory;
use futures_util::{FutureExt, StreamExt};
use monad_chain_config::{revision::ChainRevision, ChainConfig};
use monad_compress::{nop::NopCompression, CompressionAlgo};
use monad_consensus_state::ConsensusConfig;
use monad_consensus_types::{metrics::Metrics, signature_collection::SignatureCollection};
use monad_control_panel::ipc::ControlPanelIpcReceiver;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_block_validator::EthValidator;
use monad_eth_txpool_executor::{EthTxPoolExecutor, EthTxPoolIpcConfig};
use monad_executor::{Executor, ExecutorMetricsChain};
use monad_executor_glue::{LogFriendlyMonadEvent, Message, MonadEvent};
use monad_ledger::MonadBlockFileLedger;
use monad_node_config::{
    ExecutionProtocolType, FullNodeIdentityConfig, NodeNetworkConfig, SignatureCollectionType,
    SignatureType,
};
use monad_raptorcast::{RaptorCast, RaptorCastConfig};
use monad_state::{MonadMessage, MonadStateBuilder, VerifiedMonadMessage};
use monad_statesync::StateSync;
use monad_triedb_cache::StateBackendCache;
use monad_triedb_utils::TriedbReader;
use monad_types::{
    Deserializable, DropTimer, NodeId, Round, SeqNum, Serializable, GENESIS_SEQ_NUM,
};
use monad_updaters::{
    checkpoint::FileCheckpoint, config_loader::ConfigLoader, loopback::LoopbackExecutor,
    parent::ParentExecutor, timer::TokioTimer, tokio_timestamp::TokioTimestamp,
    triedb_state_root_hash::StateRootHashTriedbPoll, BoxUpdater, Updater,
};
use monad_validator::{
    validator_set::ValidatorSetFactory, weighted_round_robin::WeightedRoundRobin,
};
use monad_wal::{wal::WALoggerConfig, PersistenceLoggerBuilder};
use opentelemetry::metrics::MeterProvider;
use opentelemetry_otlp::WithExportConfig;
use tokio::signal::unix::{signal, SignalKind};
use tracing::{event, info, warn, Instrument, Level};
use tracing_subscriber::{
    fmt::{format::FmtSpan, Layer as FmtLayer},
    layer::SubscriberExt,
};

use self::{cli::Cli, error::NodeSetupError, state::NodeState};

mod cli;
mod error;
mod state;

type ReloadHandle =
    tracing_subscriber::reload::Handle<tracing_subscriber::EnvFilter, tracing_subscriber::Registry>;

const CLIENT_VERSION: &str = env!("VERGEN_GIT_DESCRIBE");
const STATESYNC_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

fn main() {
    let mut cmd = Cli::command();

    let node_state = NodeState::setup(&mut cmd).unwrap_or_else(|e| cmd.error(e.kind(), e).exit());

    rayon::ThreadPoolBuilder::new()
        .num_threads(8)
        .build_global()
        .map_err(Into::into)
        .unwrap_or_else(|e: NodeSetupError| cmd.error(e.kind(), e).exit());

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(Into::into)
        .unwrap_or_else(|e: NodeSetupError| cmd.error(e.kind(), e).exit());

    let reload_handle =
        setup_tracing().unwrap_or_else(|e: NodeSetupError| cmd.error(e.kind(), e).exit());

    drop(cmd);

    if let Err(e) = runtime.block_on(run(node_state, reload_handle)) {
        tracing::error!("monad consensus node crashed: {:?}", e);
    }
}

fn setup_tracing() -> Result<ReloadHandle, NodeSetupError> {
    let (filter, reload_handle) =
        tracing_subscriber::reload::Layer::new(tracing_subscriber::EnvFilter::from_default_env());

    let subscriber = tracing_subscriber::Registry::default().with(filter).with(
        FmtLayer::default()
            .json()
            .with_span_events(FmtSpan::NONE)
            .with_current_span(false)
            .with_span_list(false)
            .with_writer(std::io::stdout)
            .with_ansi(false),
    );

    tracing::subscriber::set_global_default(subscriber)?;

    Ok(reload_handle)
}

async fn run(node_state: NodeState, reload_handle: ReloadHandle) -> Result<(), ()> {
    let checkpoint_validators_first = node_state
        .forkpoint_config
        .validator_sets
        .first()
        .expect("no validator sets")
        .clone();

    let checkpoint_validators_last = node_state
        .forkpoint_config
        .validator_sets
        .last()
        .expect("no validator sets")
        .clone();

    {
        let mut bootstrap_peers_seen = BTreeSet::new();
        for peer in node_state.node_config.bootstrap.peers.iter() {
            if !bootstrap_peers_seen.insert(peer.secp256k1_pubkey) {
                panic!(
                    "Multiple bootstrap entries for pubkey={}",
                    peer.secp256k1_pubkey
                );
            }
        }
    }

    let known_addresses: Vec<_> = node_state
        .node_config
        .bootstrap
        .peers
        .iter()
        .map(|peer| {
            let node_id = NodeId::new(peer.secp256k1_pubkey);
            let maybe_addr = resolve_domain_v4(&node_id, &peer.address);
            (node_id, maybe_addr)
        })
        .collect();

    let nop_compression = NopCompression::new(0, 0, Vec::new());
    let router: BoxUpdater<_, _> = {
        let raptor_router = build_raptorcast_router::<
            SignatureType,
            SignatureCollectionType,
            MonadMessage<SignatureType, SignatureCollectionType, ExecutionProtocolType>,
            VerifiedMonadMessage<SignatureType, SignatureCollectionType, ExecutionProtocolType>,
            NopCompression,
        >(
            node_state.node_config.network.clone(),
            node_state.router_identity,
            known_addresses
                .iter()
                .cloned()
                .filter_map(|(node_id, maybe_addr)| Some((node_id, maybe_addr?)))
                .collect(),
            &node_state.node_config.fullnode.identities,
            nop_compression,
        )
        .await;

        #[cfg(feature = "full-node")]
        let raptor_router = monad_router_filter::FullNodeRouterFilter::new(raptor_router);

        <_ as Updater<_>>::boxed(raptor_router)
    };

    let val_set_update_interval = SeqNum(50_000); // TODO configurable

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

    let mut bootstrap_validators = Vec::new();
    let validator_set = checkpoint_validators_first
        .clone()
        .validators
        .0
        .into_iter()
        .map(|data| data.node_id)
        .collect::<BTreeSet<_>>();
    for peer_config in &node_state.node_config.bootstrap.peers {
        let peer_id = NodeId::new(peer_config.secp256k1_pubkey);
        if validator_set.contains(&peer_id) {
            bootstrap_validators.push(peer_id);
        }
    }

    // default statesync peers to bootstrap validators if none is specified
    let state_sync_peers = if node_state.node_config.statesync.peers.is_empty() {
        bootstrap_validators
    } else {
        node_state
            .node_config
            .statesync
            .peers
            .into_iter()
            .map(|p| NodeId::new(p.secp256k1_pubkey))
            .collect()
    };

    // TODO: use PassThruBlockPolicy and NopStateBackend for consensus only mode
    let create_block_policy = || {
        EthBlockPolicy::new(
            GENESIS_SEQ_NUM, // FIXME: MonadStateBuilder is responsible for updating this to forkpoint root if necessary
            node_state.node_config.consensus.execution_delay,
            node_state.node_config.chain_id,
        )
    };

    let triedb_handle = TriedbReader::try_new(node_state.triedb_path.as_path())
        .expect("triedb should exist in path");

    let mut executor = ParentExecutor {
        router,
        timer: TokioTimer::default(),
        ledger: MonadBlockFileLedger::new(node_state.ledger_path),
        checkpoint: FileCheckpoint::new(node_state.forkpoint_path),
        state_root_hash: StateRootHashTriedbPoll::new(
            &node_state.triedb_path,
            // Use the more recent of the 2 checkpoint validator sets for seeding the default e+2
            // validator set. This allows us to manually change validator set e+1 without it
            // getting rolled back in e+2.
            // FIXME this should be deleted post staking module
            checkpoint_validators_last.validators,
            val_set_update_interval,
        ),
        timestamp: TokioTimestamp::new(Duration::from_millis(5), 100, 10001),
        txpool: EthTxPoolExecutor::new(
            create_block_policy(),
            StateBackendCache::new(
                triedb_handle.clone(),
                SeqNum(node_state.node_config.consensus.execution_delay),
            ),
            EthTxPoolIpcConfig {
                bind_path: node_state.mempool_ipc_path,
                tx_batch_size: node_state.node_config.ipc_tx_batch_size as usize,
                max_queued_batches: node_state.node_config.ipc_max_queued_batches as usize,
                queued_batches_watermark: node_state.node_config.ipc_queued_batches_watermark
                    as usize,
            },
            true,
            // TODO(andr-dev): Add tx_expiry to node config
            Duration::from_secs(15),
            Duration::from_secs(5 * 60),
            node_state.chain_config,
            node_state
                .chain_config
                .get_chain_revision(node_state.forkpoint_config.high_qc.get_round())
                .chain_params()
                .proposal_gas_limit,
        )
        .expect("txpool ipc succeeds"),
        control_panel: ControlPanelIpcReceiver::new(
            node_state.control_panel_ipc_path,
            reload_handle,
            1000,
        )
        .expect("uds bind failed"),
        loopback: LoopbackExecutor::default(),
        state_sync: StateSync::<SignatureType, SignatureCollectionType>::new(
            vec![statesync_triedb_path.to_string_lossy().to_string()],
            node_state.genesis_path.to_string_lossy().to_string(),
            node_state.statesync_sq_thread_cpu,
            state_sync_peers,
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
        config_loader: ConfigLoader::new(
            node_state.node_config_path,
            node_state.node_config.bootstrap.peers,
            known_addresses,
        ),
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

    let mut last_ledger_tip = triedb_handle
        .get_latest_finalized_block()
        .unwrap_or(SeqNum(0));

    let builder = MonadStateBuilder {
        validator_set_factory: ValidatorSetFactory::default(),
        leader_election: WeightedRoundRobin::default(),
        block_validator: EthValidator::new(node_state.node_config.chain_id),
        block_policy: create_block_policy(),
        state_backend: StateBackendCache::new(
            triedb_handle,
            SeqNum(node_state.node_config.consensus.execution_delay),
        ),
        key: node_state.secp256k1_identity,
        certkey: node_state.bls12_381_identity,
        val_set_update_interval,
        epoch_start_delay: Round(5000),
        beneficiary: node_state.node_config.beneficiary.into(),
        forkpoint: node_state.forkpoint_config.into(),
        block_sync_override_peers,
        consensus_config: ConsensusConfig {
            execution_delay: SeqNum(node_state.node_config.consensus.execution_delay),
            delta: Duration::from_millis(node_state.node_config.network.max_rtt_ms / 2),
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
        _phantom: PhantomData,
    };

    let (mut state, init_commands) = builder.build();
    executor.exec(init_commands);

    let mut ledger_span = tracing::info_span!("ledger_span", last_ledger_tip = last_ledger_tip.0);

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
                send_metrics(otel_meter, &mut gauge_cache, state_metrics, executor_metrics);
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
                    state.update(event.event)
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

                let ledger_tip = executor.ledger.last_commit().unwrap_or(last_ledger_tip);


                if ledger_tip > last_ledger_tip {
                    last_ledger_tip = ledger_tip;
                    ledger_span = tracing::info_span!("ledger_span", last_ledger_tip = last_ledger_tip.0);
                }
            }
        }
    }

    Ok(())
}

async fn build_raptorcast_router<ST, SCT, M, OM, CA>(
    network_config: NodeNetworkConfig,
    identity: ST::KeyPairType,
    known_addresses: Vec<(NodeId<SCT::NodeIdPubKey>, SocketAddr)>,
    full_nodes: &[FullNodeIdentityConfig<CertificateSignaturePubKey<ST>>],
    compression_algo: CA,
) -> RaptorCast<ST, M, OM, MonadEvent<ST, SCT, ExecutionProtocolType>, CA>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>>
        + Deserializable<Bytes>
        + From<OM>
        + Send
        + Sync
        + 'static,
    <M as Deserializable<Bytes>>::ReadError: 'static,
    OM: Serializable<Bytes> + Clone + Send + Sync + 'static,
    CA: CompressionAlgo,
{
    RaptorCast::new(RaptorCastConfig {
        key: identity,
        known_addresses: known_addresses.into_iter().collect(),
        full_nodes: full_nodes
            .iter()
            .map(|full_node| NodeId::new(full_node.secp256k1_pubkey))
            .collect(),
        redundancy: 3,
        local_addr: SocketAddr::V4(SocketAddrV4::new(
            network_config.bind_address_host,
            network_config.bind_address_port,
        )),
        up_bandwidth_mbps: network_config.max_mbps.into(),
        mtu: network_config.mtu,
        compression_algo,
    })
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

fn send_metrics(
    meter: &opentelemetry::metrics::Meter,
    gauge_cache: &mut HashMap<&'static str, opentelemetry::metrics::Gauge<u64>>,
    state_metrics: &Metrics,
    executor_metrics: ExecutorMetricsChain,
) {
    for (k, v) in state_metrics
        .metrics()
        .into_iter()
        .chain(executor_metrics.into_inner())
    {
        let gauge = gauge_cache
            .entry(k)
            .or_insert_with(|| meter.u64_gauge(k).try_init().unwrap());
        gauge.record(v, &[]);
    }
}

fn build_otel_meter_provider(
    otel_endpoint: &str,
    service_name: String,
    interval: Duration,
) -> Result<opentelemetry_sdk::metrics::SdkMeterProvider, NodeSetupError> {
    let exporter = opentelemetry_otlp::MetricsExporterBuilder::Tonic(
        opentelemetry_otlp::new_exporter()
            .tonic()
            .with_endpoint(otel_endpoint),
    )
    .build_metrics_exporter(
        Box::<opentelemetry_sdk::metrics::reader::DefaultTemporalitySelector>::default(),
        Box::<opentelemetry_sdk::metrics::reader::DefaultAggregationSelector>::default(),
    )?;

    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(
        exporter,
        opentelemetry_sdk::runtime::Tokio,
    )
    .with_interval(interval / 2)
    .with_timeout(interval * 2)
    .build();

    let provider_builder = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(opentelemetry_sdk::Resource::new(vec![
            opentelemetry::KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_NAME,
                service_name,
            ),
            opentelemetry::KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_VERSION,
                CLIENT_VERSION,
            ),
        ]));

    Ok(provider_builder.build())
}
