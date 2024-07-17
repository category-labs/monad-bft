use std::{
    collections::BTreeMap,
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
    payload::{NopStateRoot, StateRoot, StateRootValidator},
    state_root_hash::StateRootHash,
};
use monad_control_panel::ipc::ControlPanelIpcReceiver;
use monad_crypto::{
    certificate_signature::{CertificateSignature, CertificateSignaturePubKey},
    hasher::Hash,
};
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_block_validator::EthValidator;
use monad_eth_txpool::EthTxPool;
use monad_executor::Executor;
use monad_executor_glue::{LogFriendlyMonadEvent, Message, MetricsCommand, MonadEvent};
use monad_gossip::{
    mock::MockGossipConfig,
    seeder::{Raptor, SeederConfig},
    Gossip,
};
use monad_ipc::IpcReceiver;
use monad_ledger::{EthHeaderParam, MonadBlockFileLedger};
use monad_quic::{SafeQuinnConfig, Service, ServiceConfig};
use monad_state::{MonadMessage, MonadStateBuilder, MonadVersion, VerifiedMonadMessage};
use monad_types::{Deserializable, NodeId, Round, SeqNum, Serializable, GENESIS_SEQ_NUM};
use monad_updaters::{
    checkpoint::MockCheckpoint, ledger::BoundedLedger, loopback::LoopbackExecutor,
    nop_metrics::NopMetricsExecutor, parent::ParentExecutor, state_root_hash::MockStateRootHashNop,
    timer::TokioTimer, BoxUpdater, Updater,
};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory};
use monad_wal::{wal::WALoggerConfig, PersistenceLoggerBuilder};
use opentelemetry::{
    sdk::trace::TracerProvider,
    trace::{Span, SpanBuilder, TraceContextExt},
    Context,
};
use rand_chacha::{
    rand_core::{RngCore, SeedableRng},
    ChaChaRng,
};
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
use monad_opentelemetry_executor::OpenTelemetryExecutor;

mod state;
use state::{build_otel_provider, NodeState};

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
        build_otel_provider(endpoint, node_state.node_name.clone())
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
                .with_writer(std::io::stdout)
                .with_span_events(FmtSpan::CLOSE)
                .with_ansi(false),
        )
        .with(maybe_telemetry);

    tracing::subscriber::set_global_default(subscriber).expect("failed to set logger");

    // if provider is dropped, then traces stop getting sent silently...
    let maybe_coordinator_provider = node_state.otel_endpoint.as_ref().map(|endpoint| {
        build_otel_provider(endpoint, node_state.network_name.clone())
            .expect("failed to build otel monad-coordinator")
    });

    run(maybe_coordinator_provider, node_state).await
}

async fn run(
    maybe_coordinator_provider: Option<TracerProvider>,
    node_state: NodeState,
) -> Result<(), ()> {
    let gossip = if node_state.forkpoint_config.forkpoint.validator_sets[0]
        .validators
        .0
        .len()
        > 1
    {
        SeederConfig::<Raptor<SignatureType>> {
            all_peers: node_state.forkpoint_config.forkpoint.validator_sets[0]
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
            all_peers: node_state.forkpoint_config.forkpoint.validator_sets[0]
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

    let validators = node_state.forkpoint_config.forkpoint.validator_sets[0].clone();

    let val_set_update_interval = SeqNum(2000);

    let metrics_executor: BoxUpdater<
        'static,
        MetricsCommand,
        MonadEvent<SignatureType, SignatureCollectionType>,
    > = if let Some(record_metrics_interval) = node_state.record_metrics_interval {
        Updater::boxed(OpenTelemetryExecutor::new(
            node_state.otel_endpoint.expect(
                "cannot specify record metrics interval without specifying OpenTelemetry endpoint",
            ),
            node_state.node_name.clone(),
            record_metrics_interval,
            /*enable_grpc_gzip=*/ false,
        ))
    } else {
        Updater::boxed(NopMetricsExecutor::default())
    };

    let blockdb = BlockDbBuilder::create(&node_state.blockdb_path);
    let state_sync_bound: usize = 100;
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
        checkpoint: MockCheckpoint::default(),
        state_root_hash: MockStateRootHashNop::new(
            validators.validators.clone(),
            val_set_update_interval,
        ),
        ipc: IpcReceiver::new(node_state.mempool_ipc_path, 1000).expect("uds bind failed"),
        control_panel: ControlPanelIpcReceiver::new(node_state.control_panel_ipc_path, 1000)
            .expect("uds bind failed"),
        loopback: LoopbackExecutor::default(),
        metrics: metrics_executor,
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
            latest_nonces: BTreeMap::new(),
            // MonadStateBuilder is responsible for updating this to forkpoint root if necessary
            last_commit: GENESIS_SEQ_NUM,
        },
        state_root_validator: Box::new(NopStateRoot {}) as Box<dyn StateRootValidator>,
        async_state_verify: PeerAsyncStateVerify::default(),
        key: node_state.secp256k1_identity,
        certkey: node_state.bls12_381_identity,
        val_set_update_interval,
        epoch_start_delay: Round(50),
        beneficiary: node_state.node_config.beneficiary,
        forkpoint: node_state.forkpoint_config.forkpoint,
        consensus_config: ConsensusConfig {
            proposal_txn_limit: node_state.node_config.consensus.block_txn_limit,
            proposal_gas_limit: node_state.node_config.consensus.block_gas_limit,
            delta: Duration::from_millis(node_state.node_config.network.max_rtt_ms),
            max_blocksync_retries: 5,
            state_sync_threshold: SeqNum(state_sync_bound as u64),
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

    for wal_event in wal_events {
        let cmds = state.update(wal_event.event);
        executor.replay(cmds);
    }

    let mut otel_context = maybe_coordinator_provider.as_ref().map(build_otel_context);

    let mut last_ledger_len = executor.ledger().get_num_commits();
    let mut ledger_span = tracing::info_span!("ledger_span", last_ledger_len);

    if let Some((cx, _expiry)) = &otel_context {
        ledger_span.set_parent(cx.clone());
    }

    let mut ctrlc = Box::pin(signal::ctrl_c()).into_stream();

    loop {
        tokio::select! {
            _ = ctrlc.next() => {
                break;
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

                let ledger_len = executor.ledger().get_num_commits();

                if ledger_len > last_ledger_len {
                    last_ledger_len = ledger_len;
                    ledger_span = tracing::info_span!("ledger_span", last_ledger_len);

                    if let Some((cx, expiry)) = &mut otel_context {
                        if *expiry < SystemTime::now() {
                            let (new_cx, new_expiry) = build_otel_context(
                                maybe_coordinator_provider
                                    .as_ref()
                                    .expect("coordinator must exist"),
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
fn build_otel_context(provider: &TracerProvider) -> (Context, SystemTime) {
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
            .with_trace_id((1_u128 << 64 | u128::from(start_seconds)).into())
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
