use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    time::{Duration, Instant},
};

use clap::CommandFactory;
use config::{NodeBootstrapPeerConfig, NodeNetworkConfig};
use futures_util::{FutureExt, StreamExt};
use monad_consensus_state::{ConsensusConfig, ConsensusState};
use monad_consensus_types::{
    multi_sig::MultiSig, payload::NopStateRoot, transaction_validator::MockValidator,
};
use monad_crypto::secp256k1::{KeyPair, SecpSignature};
use monad_executor::{Executor, State};
use monad_executor_glue::Message;
use monad_gossip::mock::{MockGossip, MockGossipConfig};
use monad_mempool_controller::ControllerConfig;
use monad_quic::service::{SafeQuinnConfig, ServiceConfig};
use monad_types::{NodeId, SeqNum, Stake};
use monad_updaters::{
    checkpoint::MockCheckpoint, execution_ledger::MonadFileLedger, ledger::MockLedger,
    mempool::MonadMempool, parent::ParentExecutor, timer::TokioTimer,
};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
use tokio::signal;
use tracing::{event, Level};
use tracing_opentelemetry::OpenTelemetrySpanExt;

mod cli;
use cli::Cli;

mod config;

mod error;
use error::NodeSetupError;

mod state;
use state::NodeState;

type SignatureType = SecpSignature;
type SignatureCollectionType = MultiSig<SignatureType>;
type TransactionValidatorType = MockValidator;
type StateRootValidatorType = NopStateRoot;
type MonadState = monad_state::MonadState<
    ConsensusState<SignatureCollectionType, TransactionValidatorType, StateRootValidatorType>,
    SignatureType,
    SignatureCollectionType,
    ValidatorSet,
    SimpleRoundRobin,
>;
type MonadConfig = <MonadState as State>::Config;

fn main() {
    let mut cmd = Cli::command();

    env_logger::try_init()
        .map_err(NodeSetupError::EnvLoggerError)
        .unwrap_or_else(|e| cmd.error(e.kind(), e).exit());

    let state = NodeState::setup(&mut cmd).unwrap_or_else(|e| cmd.error(e.kind(), e).exit());

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| e.into())
        .unwrap_or_else(|e: NodeSetupError| cmd.error(e.kind(), e).exit());

    drop(cmd);

    if let Err(e) = runtime.block_on(run(state)) {
        log::error!("monad consensus node crashed: {:?}", e);
    }
}

async fn run(node_state: NodeState) -> Result<(), ()> {
    let router = build_router(
        node_state.config.network,
        &node_state.identity,
        &node_state.config.bootstrap.peers,
    )
    .await;

    // FIXME-1 hack so all tcp sockets are bound before they try and send mesages
    // we can delete this once we support retry at the monad-p2p executor level
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let mut mempool_controller_config = ControllerConfig::default();

    if let Some(mempool_ipc_path) = node_state.mempool_ipc_path {
        mempool_controller_config =
            mempool_controller_config.with_mempool_ipc_path(mempool_ipc_path);
    }

    let mut executor = ParentExecutor {
        router,
        timer: TokioTimer::default(),
        mempool: MonadMempool::new(mempool_controller_config),
        ledger: MockLedger::default(),
        execution_ledger: MonadFileLedger::new(node_state.execution_ledger_path),
        checkpoint: MockCheckpoint::default(),
    };

    let (mut state, init_commands) = MonadState::init(MonadConfig {
        transaction_validator: MockValidator {},
        validators: node_state
            .config
            .bootstrap
            .peers
            .into_iter()
            .map(|peer| (peer.pubkey, Stake(1), peer.certkey))
            .collect(),
        key: node_state.identity,
        certkey: node_state.certkey,
        beneficiary: node_state.config.beneficiary,
        delta: Duration::from_secs(1),
        consensus_config: ConsensusConfig {
            proposal_size: 5000,
            state_root_delay: SeqNum(0),
            propose_with_missing_blocks: false,
        },
    });

    executor.exec(init_commands);

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

async fn build_router<M, OM>(
    network_config: NodeNetworkConfig,
    identity: &KeyPair,
    peers: &[NodeBootstrapPeerConfig],
) -> monad_quic::service::Service<SafeQuinnConfig, MockGossip, M, OM>
where
    M: Message,
{
    monad_quic::service::Service::new(
        ServiceConfig {
            zero_instant: Instant::now(),
            me: NodeId(identity.pubkey()),
            server_address: generate_bind_address(
                network_config.bind_address_host,
                network_config.bind_address_port,
            ),
            quinn_config: SafeQuinnConfig::new(
                identity,
                Duration::from_millis(network_config.max_rtt_ms),
                network_config.max_mbps,
            ),
            known_addresses: peers
                .iter()
                .map(|peer| {
                    (
                        NodeId(peer.pubkey.to_owned()),
                        generate_bind_address(peer.ip, peer.port),
                    )
                })
                .collect(),
        },
        MockGossipConfig {
            all_peers: peers
                .iter()
                .map(|peer| NodeId(peer.pubkey.to_owned()))
                .collect(),
            me: NodeId(identity.pubkey()),
        }
        .build(),
    )
}

fn generate_bind_address(host: Ipv4Addr, port: u16) -> SocketAddr {
    SocketAddr::V4(SocketAddrV4::new(host, port))
}
