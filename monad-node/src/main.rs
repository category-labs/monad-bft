use std::{
    net::{SocketAddr, SocketAddrV4, ToSocketAddrs},
    time::{Duration, Instant},
};

use bytes::Bytes;
use clap::CommandFactory;
use config::{NodeBootstrapPeerConfig, NodeNetworkConfig};
use futures_util::{FutureExt, StreamExt};
use monad_async_state_verify::PeerAsyncStateVerify;
use monad_bls::BlsSignatureCollection;
use monad_consensus_state::ConsensusConfig;
use monad_consensus_types::{
    block_validator::MockValidator, payload::NopStateRoot, validator_data::ValidatorData,
};
use monad_crypto::certificate_signature::CertificateSignaturePubKey;
use monad_eth_txpool::EthTxPool;
use monad_executor::Executor;
use monad_executor_glue::{Message, MetricsCommand, MonadEvent};
use monad_gossip::{mock::MockGossipConfig, Gossip};
use monad_ipc::IpcReceiver;
use monad_ledger::MonadBlockFileLedger;
use monad_quic::{SafeQuinnConfig, Service, ServiceConfig};
use monad_secp::{KeyPair, SecpSignature};
use monad_state::{MonadMessage, MonadStateBuilder, MonadVersion, VerifiedMonadMessage};
use monad_types::{Deserializable, NodeId, Round, SeqNum, Serializable};
use monad_updaters::{
    checkpoint::MockCheckpoint, ledger::MockLedger, loopback::LoopbackExecutor,
    nop_metrics::NopMetricsExecutor, parent::ParentExecutor, state_root_hash::MockStateRootHashNop,
    timer::TokioTimer, BoxUpdater, Updater,
};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory};
use monad_wal::{wal::WALoggerConfig, PersistenceLoggerBuilder};
use tokio::signal;
use tracing::{event, Level};
use tracing_opentelemetry::OpenTelemetrySpanExt;

mod cli;
use cli::Cli;

mod config;

mod error;
use error::NodeSetupError;
use monad_opentelemetry_executor::OpenTelemetryExecutor;

mod state;
use state::{build_otel_provider, NodeState};

type SignatureType = SecpSignature;
type SignatureCollectionType = BlsSignatureCollection<CertificateSignaturePubKey<SignatureType>>;

fn main() {
    let mut cmd = Cli::command();

    env_logger::try_init()
        .map_err(NodeSetupError::EnvLoggerError)
        .unwrap_or_else(|e| cmd.error(e.kind(), e).exit());

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

    let maybe_provider = node_state.otel_endpoint.as_ref().map(|endpoint| {
        build_otel_provider(
            endpoint,
            format!("monad-node-{:?}", &node_state.secp256k1_identity.pubkey()),
        )
        .expect("failed to build otel monad-node")
    });

    let fut = run(node_state);
    if let Some(provider) = &maybe_provider {
        use tracing::instrument::WithSubscriber;
        fut.with_subscriber({
            use opentelemetry::trace::TracerProvider;
            use tracing_subscriber::layer::SubscriberExt;

            let tracer = provider.tracer("opentelemetry");
            let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
            tracing_subscriber::Registry::default().with(telemetry)
        })
        .boxed()
    } else {
        fut.boxed()
    }
    .await
}

async fn run(node_state: NodeState) -> Result<(), ()> {
    let router = build_router::<
        MonadMessage<SignatureType, SignatureCollectionType>,
        VerifiedMonadMessage<SignatureType, SignatureCollectionType>,
        _,
    >(
        node_state.node_config.network.clone(),
        &node_state.secp256k1_identity,
        &node_state.node_config.bootstrap.peers,
        MockGossipConfig {
            all_peers: node_state
                .genesis_config
                .validators
                .iter()
                .map(|peer| NodeId::new(peer.secp256k1_pubkey))
                .collect(),
            me: NodeId::new(node_state.secp256k1_identity.pubkey()),
        }
        .build(),
    )
    .await;

    let validators = ValidatorData(
        node_state
            .genesis_config
            .validators
            .into_iter()
            .map(|peer| {
                (
                    NodeId::new(peer.secp256k1_pubkey),
                    peer.stake,
                    peer.bls12_381_pubkey,
                )
            })
            .collect(),
    );
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
            record_metrics_interval,
            /*enable_grpc_gzip=*/ false,
        ))
    } else {
        Updater::boxed(NopMetricsExecutor::default())
    };

    let mut executor = ParentExecutor {
        router,
        timer: TokioTimer::default(),
        ledger: MockLedger::default(),
        execution_ledger: MonadBlockFileLedger::new(node_state.execution_ledger_path),
        checkpoint: MockCheckpoint::default(),
        state_root_hash: MockStateRootHashNop::new(validators.clone(), val_set_update_interval),
        ipc: IpcReceiver::new(node_state.mempool_ipc_path, 1000).expect("uds bind failed"),
        loopback: LoopbackExecutor::default(),
        metrics: metrics_executor,
    };

    let logger_config = WALoggerConfig::new(node_state.wal_path.clone(), true);
    let Ok((mut wal, wal_events)) = logger_config.build() else {
        event!(
            Level::ERROR,
            path = node_state.wal_path.as_path().display().to_string(),
            "failed to initialize wal",
        );
        return Err(());
    };

    let builder = MonadStateBuilder {
        version: MonadVersion::new("ALPHA"),
        validator_set_factory: ValidatorSetFactory::default(),
        leader_election: SimpleRoundRobin::default(),
        transaction_pool: EthTxPool::default(),
        block_validator: MockValidator,
        state_root_validator: NopStateRoot {},
        async_state_verify: PeerAsyncStateVerify::default(),
        validators,
        key: node_state.secp256k1_identity,
        certkey: node_state.bls12_381_identity,
        val_set_update_interval,
        epoch_start_delay: Round(50),
        beneficiary: node_state.node_config.beneficiary,
        consensus_config: ConsensusConfig {
            proposal_txn_limit: 5000,
            proposal_gas_limit: 800_000_000,
            propose_with_missing_blocks: false,
            delta: Duration::from_secs(1),
        },
    };
    let (mut state, init_commands) = builder.build();
    executor.exec(init_commands);

    for wal_event in wal_events {
        let cmds = state.update(wal_event);
        executor.replay(cmds);
    }
    let total_start = Instant::now();
    let mut start = total_start;
    let mut last_printed_len = 0;

    const BLOCK_INTERVAL: usize = 100;

    let mut last_ledger_len = executor.ledger().get_blocks().len();
    let mut ledger_span = tracing::info_span!("ledger_span", last_ledger_len);

    if let Some(cx) = &node_state.otel_context {
        ledger_span.set_parent(cx.clone());
    }

    let mut ctrlc = Box::pin(signal::ctrl_c()).into_stream();

    loop {
        tokio::select! {
            _ = ctrlc.next() => {
                break;
            }
            event = executor.next() => {
                let Some(event) = event else {
                    event!(
                        Level::ERROR,
                        "parent executor returned none!"
                    );
                    return Err(());
                };

                {
                    let _ledger_span = ledger_span.enter();
                    let _wal_event_span = tracing::info_span!("wal_event_span").entered();
                    if let Err(err) = wal.push(&event) {
                        event!(
                            Level::ERROR,
                            ?err,
                            "failed to push to wal",
                        );
                        return Err(());
                    }
                };

                let commands = {
                    let _ledger_span = ledger_span.enter();
                    let _event_span = tracing::info_span!("event_span", ?event).entered();
                    state.update(event)
                };

                executor.exec(commands);

                let ledger_len = executor.ledger().get_blocks().len();

                if ledger_len > last_ledger_len {
                    last_ledger_len = ledger_len;
                    ledger_span = tracing::info_span!("ledger_span", last_ledger_len);

                    if let Some(cx) = &node_state.otel_context {
                        ledger_span.set_parent(cx.clone());
                    }
                }

                if ledger_len >= last_printed_len + BLOCK_INTERVAL {
                    event!(
                        Level::INFO,
                        ledger_len = ledger_len,
                        elapsed_ms = start.elapsed().as_millis(),
                        "100 blocks"
                    );
                    start = Instant::now();
                    last_printed_len = ledger_len / BLOCK_INTERVAL * BLOCK_INTERVAL;
                }
            }
        }
    }

    Ok(())
}

async fn build_router<M, OM, G: Gossip>(
    network_config: NodeNetworkConfig,
    identity: &KeyPair,
    peers: &[NodeBootstrapPeerConfig],
    gossip: G,
) -> Service<SafeQuinnConfig<SignatureType>, G, M, OM>
where
    G: Gossip<NodeIdPubKey = monad_secp::PubKey> + Send + 'static,
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
