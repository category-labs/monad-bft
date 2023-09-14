use std::{
    net::Ipv4Addr,
    time::{Duration, Instant},
};

use clap::CommandFactory;
use config::{NodeBootstrapPeerConfig, NodeNetworkConfig};
use futures_util::{FutureExt, StreamExt};
use monad_block_sync::BlockSyncState;
use monad_consensus_state::ConsensusState;
use monad_consensus_types::{
    multi_sig::MultiSig, payload::NopStateRoot, transaction_validator::MockValidator,
    validation::Sha256Hash,
};
use monad_crypto::secp256k1::SecpSignature;
use monad_executor::{
    executor::{
        checkpoint::MockCheckpoint, ledger::MockLedger, mempool::MonadMempool,
        parent::ParentExecutor, timer::TokioTimer,
    },
    Executor, State,
};
use monad_mempool_controller::ControllerConfig;
use monad_p2p::Multiaddr;
use monad_state::{MonadMessage, VerifiedMonadMessage};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
use tokio::signal;
use tracing::{event, Level};
use tracing_opentelemetry::OpenTelemetrySpanExt;

mod cli;
use cli::Cli;

mod config;

mod error;
use error::NodeSetupError;

mod genesis;

mod state;
use state::NodeState;

type HasherType = Sha256Hash;
type SignatureType = SecpSignature;
type SignatureCollectionType = MultiSig<SignatureType>;
type TransactionValidatorType = MockValidator;
type StateRootValidatorType = NopStateRoot;
type MonadState = monad_state::MonadState<
    ConsensusState<
        SignatureType,
        SignatureCollectionType,
        TransactionValidatorType,
        StateRootValidatorType,
    >,
    SignatureType,
    SignatureCollectionType,
    ValidatorSet,
    SimpleRoundRobin,
    BlockSyncState,
    HasherType,
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
        node_state.identity_libp2p,
        node_state.config.network,
        &node_state.config.bootstrap.peers,
    )
    .await;

    // FIXME hack so all tcp sockets are bound before they try and send mesages
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
        checkpoint: MockCheckpoint::default(),
    };

    let (mut state, init_commands) = MonadState::init(MonadConfig {
        transaction_validator: MockValidator {},
        validators: node_state
            .config
            .bootstrap
            .peers
            .into_iter()
            .map(|peer| (peer.pubkey, peer.certkey))
            .collect(),
        key: node_state.identity,
        certkey: node_state.certkey,
        delta: Duration::from_secs(1),
        state_root_delay: 0,
        genesis_block: node_state.genesis.genesis_block,
        genesis_vote_info: node_state.genesis.genesis_vote_info,
        genesis_signatures: node_state.genesis.genesis_signatures,
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

async fn build_router(
    identity_libp2p: libp2p_identity::secp256k1::Keypair,
    network_config: NodeNetworkConfig,
    peers: &Vec<NodeBootstrapPeerConfig>,
) -> monad_p2p::Service<
    MonadMessage<SignatureType, SignatureCollectionType>,
    VerifiedMonadMessage<SignatureType, SignatureCollectionType>,
> {
    let mut router = monad_p2p::Service::with_tokio_executor(
        identity_libp2p.into(),
        generate_bind_address(
            network_config.bind_address_host,
            network_config.bind_address_port,
        ),
        Duration::from_millis(network_config.libp2p_timeout_ms),
        Duration::from_millis(network_config.libp2p_keepalive_ms),
    )
    .await;

    for peer in peers {
        router.add_peer(
            &(&peer.pubkey).into(),
            generate_bind_address(peer.ip, peer.port),
        );
    }

    router
}

fn generate_bind_address(host: Ipv4Addr, port: u16) -> Multiaddr {
    format!("/ip4/{}/udp/{}/quic-v1", host, port)
        .parse()
        .unwrap()
}
