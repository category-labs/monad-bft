use std::{
    collections::{BTreeMap, HashMap},
    marker::PhantomData,
    net::{SocketAddr, SocketAddrV4, ToSocketAddrs},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use chrono::Utc;
use clap::CommandFactory;
use config::{NodeBootstrapPeerConfig, NodeNetworkConfig};
use futures_util::{FutureExt, StreamExt};
use monad_async_state_verify::PeerAsyncStateVerify;
use monad_blockdb::BlockDbBuilder;
use monad_consensus_state::ConsensusConfig;
use monad_consensus_types::{
    metrics::Metrics,
    payload::{NopStateRoot, StateRoot, StateRootValidator},
    state_root_hash::StateRootHash,
};
use monad_control_panel::ipc::ControlPanelIpcReceiver;
use monad_crypto::{
    certificate_signature::{CertificateSignature, CertificateSignaturePubKey},
    hasher::{Hash, Hasher, HasherType},
};
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_block_validator::EthValidator;
use monad_eth_reserve_balance::ReserveBalanceCacheTrait;
use monad_eth_txpool::EthTxPool;
use monad_executor::{Executor, ExecutorMetricsChain};
use monad_executor_glue::{LogFriendlyMonadEvent, Message};
use monad_gossip::{
    mock::MockGossipConfig,
    seeder::{Raptor, SeederConfig},
    Gossip,
};
use monad_ipc::IpcReceiver;
use monad_ledger::{EthHeaderParam, MonadBlockFileLedger};
use monad_quic::{SafeQuinnConfig, Service, ServiceConfig};
use monad_state::{MonadMessage, MonadStateBuilder, MonadVersion, VerifiedMonadMessage};
use monad_triedb_cache::ReserveBalanceCache;
use monad_types::{Deserializable, NodeId, Round, SeqNum, Serializable, GENESIS_SEQ_NUM};
use monad_updaters::{
    checkpoint::FileCheckpoint, ledger::BoundedLedger, loopback::LoopbackExecutor,
    parent::ParentExecutor, state_root_hash::MockStateRootHashNop, timer::TokioTimer,
    tokio_timestamp::TokioTimestamp,
};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory};
use monad_wal::{wal::WALoggerConfig, PersistenceLoggerBuilder};
use opentelemetry::{
    metrics::MeterProvider,
    trace::{Span, SpanBuilder, TraceContextExt},
    Context,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::TracerProvider;
use rand_chacha::{
    rand_core::{RngCore, SeedableRng},
    ChaChaRng,
};
use sorted_vector_map::SortedVectorMap;
use tokio::signal;
use tracing::{event, Instrument, Level};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::{
    fmt::{format::FmtSpan, Layer as FmtLayer},
    layer::SubscriberExt,
    EnvFilter, Registry,
};

mod cli;
use cli::Cli;

mod config;
use config::{SignatureCollectionType, SignatureType};
mod mode;

mod error;
use error::NodeSetupError;

mod state;
use state::NodeState;

fn main() {
    let mut cmd = Cli::command();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| e.into())
        .unwrap_or_else(|e: NodeSetupError| cmd.error(e.kind(), e).exit());

    if let Err(e) = runtime.block_on(wrapped_run(cmd)) {
        log::error!("monad consensus node crashed: {:?}", e);
    }
}

async fn wrapped_run(mut cmd: clap::Command) -> Result<(), ()> {
    // NodeState::setup needs to be called within a tokio runtime
    let node_state = NodeState::setup(&mut cmd).unwrap_or_else(|e| cmd.error(e.kind(), e).exit());
    drop(cmd);

    // if provider is dropped, then traces stop getting sent silently...
    let maybe_provider = node_state.otel_endpoint.as_ref().map(|endpoint| {
        build_otel_tracer_provider(endpoint, node_state.node_name.clone())
            .expect("failed to build otel monad-node")
    });

    let maybe_telemetry = if let Some(provider) = &maybe_provider {
        use opentelemetry::trace::TracerProvider;
        let tracer = provider.tracer("opentelemetry");
        Some(tracing_opentelemetry::layer().with_tracer(tracer))
    } else {
        None
    };

    let subscriber = Registry::default()
        .with(EnvFilter::from_default_env())
        .with(
            FmtLayer::default()
                .json()
                .with_span_events(FmtSpan::NONE)
                .with_current_span(false)
                .with_span_list(false)
                .with_writer(std::io::stdout)
                .with_ansi(false),
        )
        .with(maybe_telemetry);

    tracing::subscriber::set_global_default(subscriber).expect("failed to set logger");

    // if provider is dropped, then traces stop getting sent silently...
    let maybe_coordinator_provider = node_state.otel_endpoint.as_ref().map(|endpoint| {
        build_otel_tracer_provider(endpoint, node_state.network_name.clone())
            .expect("failed to build otel monad-coordinator")
    });

    run(maybe_coordinator_provider, node_state).await
}

async fn run(
    maybe_coordinator_provider: Option<TracerProvider>,
    node_state: NodeState,
) -> Result<(), ()> {
    let gossip = if node_state.forkpoint_config.validator_sets[0]
        .validators
        .0
        .len()
        > 1
    {
        SeederConfig::<Raptor<SignatureType>> {
            all_peers: node_state.forkpoint_config.validator_sets[0]
                .validators
                .0
                .iter()
                .map(|peer| NodeId::new(peer.node_id.pubkey()))
                .collect(),
            key: {
                // TODO make this less jank
                //
                // This is required right now because Service::new requires a 'static
                // future for spawning the helper task.
                let identity = Box::leak(Box::new(node_state.gossip_identity));
                assert_eq!(identity.pubkey(), node_state.secp256k1_identity.pubkey());
                identity
            },
            timeout: Duration::from_millis(node_state.node_config.network.max_rtt_ms),
            up_bandwidth_Mbps: node_state.node_config.network.max_mbps,
            chunker_poll_interval: Duration::from_millis(10),
        }
        .build()
        .boxed()
    } else {
        MockGossipConfig {
            all_peers: node_state.forkpoint_config.validator_sets[0]
                .validators
                .0
                .iter()
                .map(|peer| NodeId::new(peer.node_id.pubkey()))
                .collect(),
            me: NodeId::new(node_state.secp256k1_identity.pubkey()),
            message_delay: Duration::from_millis(node_state.node_config.network.max_rtt_ms / 2),
        }
        .build()
        .boxed()
    };

    let router = build_router::<
        MonadMessage<SignatureType, SignatureCollectionType>,
        VerifiedMonadMessage<SignatureType, SignatureCollectionType>,
        _,
    >(
        node_state.node_config.network.clone(),
        &node_state.secp256k1_identity,
        &node_state.node_config.bootstrap.peers,
        gossip,
    )
    .await;

    let validators = node_state.forkpoint_config.validator_sets[0].clone();

    let val_set_update_interval = SeqNum(2000);

    let blockdb = BlockDbBuilder::create(&node_state.blockdb_path);
    let state_sync_bound: usize = 100;

    _ = std::fs::remove_file(node_state.mempool_ipc_path.as_path());
    _ = std::fs::remove_file(node_state.control_panel_ipc_path.as_path());
    let mut executor = ParentExecutor {
        router,
        timer: TokioTimer::default(),
        ledger: BoundedLedger::new(state_sync_bound),
        execution_ledger: MonadBlockFileLedger::new(
            node_state.execution_ledger_path,
            blockdb.clone(),
            EthHeaderParam {
                gas_limit: node_state.node_config.consensus.block_gas_limit,
            },
        ),
        checkpoint: FileCheckpoint::new(node_state.forkpoint_path),
        state_root_hash: MockStateRootHashNop::new(
            validators.validators.clone(),
            val_set_update_interval,
        ),
        timestamp: TokioTimestamp::new(Duration::from_millis(25), 100, 10001),
        ipc: IpcReceiver::new(node_state.mempool_ipc_path, 1000).expect("uds bind failed"),
        control_panel: ControlPanelIpcReceiver::new(node_state.control_panel_ipc_path, 1000)
            .expect("uds bind failed"),
        loopback: LoopbackExecutor::default(),
    };

    let logger_config: WALoggerConfig<LogFriendlyMonadEvent<_, _>> =
        WALoggerConfig::new(node_state.wal_path.clone(), true);
    let Ok((mut wal, wal_events)) = logger_config.build() else {
        event!(
            Level::ERROR,
            path = node_state.wal_path.as_path().display().to_string(),
            "failed to initialize wal",
        );
        return Err(());
    };
    assert!(wal_events.is_empty(), "wal must be cleared after restart");

    let mut last_ledger_tip = node_state.forkpoint_config.root.seq_num;
    let mut builder = MonadStateBuilder {
        version: MonadVersion::new("ALPHA"),
        validator_set_factory: ValidatorSetFactory::default(),
        leader_election: SimpleRoundRobin::default(),
        transaction_pool: EthTxPool::default(),
        block_validator: EthValidator {
            tx_limit: node_state.node_config.consensus.block_txn_limit,
            block_gas_limit: node_state.node_config.consensus.block_gas_limit,
        },
        block_policy: EthBlockPolicy {
            account_nonces: BTreeMap::new(),
            // MonadStateBuilder is responsible for updating this to forkpoint root if necessary
            last_commit: GENESIS_SEQ_NUM,
            txn_cache: SortedVectorMap::new(),
            max_reserve_balance: node_state.node_config.consensus.max_reserve_balance.into(),
            execution_delay: node_state.node_config.consensus.execution_delay,
            reserve_balance_check_mode: node_state.node_config.consensus.reserve_balance_check_mode,
        },
        reserve_balance_cache: ReserveBalanceCache::new(
            node_state.triedb_path,
            node_state.node_config.consensus.execution_delay,
        ),
        state_root_validator: Box::new(NopStateRoot {}) as Box<dyn StateRootValidator>,
        async_state_verify: PeerAsyncStateVerify::default(),
        key: node_state.secp256k1_identity,
        certkey: node_state.bls12_381_identity,
        val_set_update_interval,
        epoch_start_delay: Round(50),
        beneficiary: node_state.node_config.beneficiary,
        forkpoint: node_state.forkpoint_config.into(),
        consensus_config: ConsensusConfig {
            proposal_txn_limit: node_state.node_config.consensus.block_txn_limit,
            proposal_gas_limit: node_state.node_config.consensus.block_gas_limit,
            delta: Duration::from_millis(node_state.node_config.network.max_rtt_ms),
            max_blocksync_retries: 5,
            state_sync_threshold: SeqNum(state_sync_bound as u64),
            timestamp_latency_estimate_ms: 50,
        },
        _pd: PhantomData,
    };

    // parse test mode commands
    match node_state.run_mode {
        mode::RunModeCommand::ProdMode => {}
        mode::RunModeCommand::TestMode {
            byzantine_execution,
        } => {
            if byzantine_execution {
                executor
                    .state_root_hash
                    .inject_byzantine_srh(|seq_num: &SeqNum| {
                        let mut gen = ChaChaRng::seed_from_u64(seq_num.0);
                        let mut hash = [0_u8; 32];
                        let mut discard = [0_u8; 32];
                        gen.fill_bytes(&mut discard);
                        gen.fill_bytes(&mut hash);
                        StateRootHash(Hash(hash))
                    });
            }
            // use real StateRootValidator
            builder.state_root_validator = Box::new(StateRoot::new(SeqNum(4)));
        }
    }

    let (mut state, init_commands) = builder.build();
    executor.exec(init_commands);

    let network_name_hash = {
        let mut hasher = HasherType::new();
        hasher.update(&node_state.network_name);
        let hash = hasher.hash();
        u64::from_le_bytes(
            hash.0[..std::mem::size_of::<u64>()]
                .try_into()
                .expect("u64 is 8 bytes"),
        )
    };
    let mut otel_context = maybe_coordinator_provider
        .as_ref()
        .map(|provider| build_otel_context(provider, network_name_hash));

    let mut ledger_span = tracing::info_span!("ledger_span", last_ledger_tip = last_ledger_tip.0);

    if let Some((cx, _expiry)) = &otel_context {
        ledger_span.set_parent(cx.clone());
    }

    // if provider is dropped, then traces stop getting sent silently...
    let maybe_otel_meter_provider = node_state.otel_endpoint.and_then(|otel_endpoint| {
        let record_metrics_interval = node_state.record_metrics_interval?;
        let provider = build_otel_meter_provider(
            &otel_endpoint,
            node_state.node_name.clone(),
            record_metrics_interval,
        )
        .expect("failed to build otel monad-node");
        Some(provider)
    });
    let maybe_otel_meter = maybe_otel_meter_provider
        .as_ref()
        .map(|provider| provider.meter("opentelemetry"));
    let mut gauge_cache = HashMap::new();
    let mut maybe_metrics_ticker =
        node_state
            .record_metrics_interval
            .map(|record_metrics_interval| {
                let mut timer = tokio::time::interval(record_metrics_interval);
                timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                timer
            });

    let mut ctrlc = Box::pin(signal::ctrl_c()).into_stream();

    loop {
        tokio::select! {
            _ = ctrlc.next() => {
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
                    let _ledger_span = ledger_span.enter();
                    let _event_span = tracing::trace_span!("event_span", ?event.event).entered();
                    state.update(event.event)
                };

                if !commands.is_empty() {
                    let _ledger_span = ledger_span.enter();
                    let _exec_span = tracing::trace_span!("exec_span", num_commands = commands.len()).entered();
                    executor.exec(commands);
                }

                let ledger_tip = executor.ledger.last_commit().unwrap_or(last_ledger_tip);


                if ledger_tip > last_ledger_tip {
                    last_ledger_tip = ledger_tip;
                    ledger_span = tracing::info_span!("ledger_span", last_ledger_tip = last_ledger_tip.0);

                    if let Some((cx, expiry)) = &mut otel_context {
                        if *expiry < SystemTime::now() {
                            let (new_cx, new_expiry) = build_otel_context(
                                maybe_coordinator_provider
                                    .as_ref()
                                    .expect("coordinator must exist"),
                                    network_name_hash,
                            );
                            *cx = new_cx;
                            *expiry = new_expiry;
                        }
                        ledger_span.set_parent(cx.clone());
                    }
                }
            }
        }
    }

    Ok(())
}

async fn build_router<M, OM, G>(
    network_config: NodeNetworkConfig,
    identity: &<SignatureType as CertificateSignature>::KeyPairType,
    peers: &[NodeBootstrapPeerConfig],
    gossip: G,
) -> Service<SafeQuinnConfig<SignatureType>, G, M, OM>
where
    G: Gossip<NodeIdPubKey = CertificateSignaturePubKey<SignatureType>> + Send + 'static,
    M: Message<NodeIdPubKey = G::NodeIdPubKey> + Deserializable<Bytes> + Send + Sync + 'static,
    <M as Deserializable<Bytes>>::ReadError: 'static,
    OM: Serializable<Bytes> + Send + Sync + 'static,
{
    Service::new(
        ServiceConfig {
            me: NodeId::new(identity.pubkey()),
            server_address: SocketAddr::V4(SocketAddrV4::new(
                network_config.bind_address_host,
                network_config.bind_address_port,
            )),
            quinn_config: SafeQuinnConfig::new(
                identity,
                Duration::from_millis(network_config.max_rtt_ms),
                network_config.max_mbps,
            ),
            known_addresses: peers
                .iter()
                .map(|peer| {
                    let address = peer
                        .address
                        .to_socket_addrs()
                        .unwrap_or_else(|err| {
                            panic!("unable to resolve address={}, err={:?}", peer.address, err)
                        })
                        .next()
                        .unwrap_or_else(|| panic!("couldn't look up address={}", peer.address));
                    (NodeId::new(peer.secp256k1_pubkey.to_owned()), address)
                })
                .collect(),
        },
        gossip,
    )
}

/// Returns (otel_context, expiry)
fn build_otel_context(provider: &TracerProvider, network_name_hash: u64) -> (Context, SystemTime) {
    const ROUND_SECONDS: u64 = 60 * 1; // 1 minute

    let (start_time, start_seconds) = {
        let unix_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("can't compute elapsed time");
        let start_seconds = unix_ts.as_secs() / ROUND_SECONDS * ROUND_SECONDS;
        let rounded_duration = Duration::from_secs(start_seconds);
        (UNIX_EPOCH + rounded_duration, start_seconds)
    };
    let context = {
        let span = SpanBuilder::from_name("exec")
            .with_trace_id(((network_name_hash as u128) << 64 | u128::from(start_seconds)).into())
            .with_span_id(15.into())
            .with_start_time(start_time)
            .with_end_time(start_time + Duration::from_secs(1));
        use opentelemetry::trace::{Tracer, TracerProvider};
        let span = provider
            .tracer("opentelemetry")
            .build_with_context(span, &Context::default());
        span.span_context().clone()
    };

    (
        Context::default().with_remote_span_context(context),
        start_time + Duration::from_secs(ROUND_SECONDS),
    )
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

fn build_otel_tracer_provider(
    otel_endpoint: &str,
    service_name: String,
) -> Result<opentelemetry_sdk::trace::TracerProvider, NodeSetupError> {
    let exporter = opentelemetry_otlp::SpanExporterBuilder::Tonic(
        opentelemetry_otlp::new_exporter()
            .tonic()
            .with_endpoint(otel_endpoint),
    )
    .build_span_exporter()?;

    let provider_builder = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_config(opentelemetry_sdk::trace::Config::default().with_resource(
            opentelemetry_sdk::Resource::new(vec![opentelemetry::KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_NAME,
                service_name,
            )]),
        ))
        .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio);

    Ok(provider_builder.build())
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
        ]));

    Ok(provider_builder.build())
}
