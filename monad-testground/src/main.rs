use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use clap::Parser;
use executor::{LedgerConfig, StateRootHashConfig};
use futures_util::{FutureExt, StreamExt};
use monad_bls::BlsSignatureCollection;
use monad_chain_config::{revision::ChainParams, MockChainConfig};
use monad_consensus_state::ConsensusConfig;
use monad_consensus_types::{
    block::MockExecutionProtocol,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    validator_data::{ValidatorData, ValidatorSetData},
};
use monad_crypto::certificate_signature::{
    CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
    CertificateSignatureRecoverable,
};
use monad_dataplane::network::DEFAULT_MTU;
use monad_executor::Executor;
use monad_raptorcast::RaptorCastConfig;
use monad_secp::SecpSignature;
use monad_state_backend::InMemoryStateInner;
use monad_types::{NodeId, Round, SeqNum, Stake};
use monad_updaters::{ledger::MockableLedger, local_router::LocalRouterConfig};
use opentelemetry::trace::{Span, TraceContextExt, Tracer};
use opentelemetry_otlp::WithExportConfig;
use tracing::{event, instrument::WithSubscriber, Instrument, Level};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::{
    fmt::{format::FmtSpan, Layer as FmtLayer},
    EnvFilter, Registry,
};

use crate::executor::{
    make_monad_executor, make_monad_state, ExecutorConfig, RouterConfig, StateConfig,
};

mod executor;

pub struct Config<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub num_nodes: usize,
    pub simulation_length: Duration,
    pub executor_config: ExecutorConfig<ST, SCT, MockExecutionProtocol>,
    pub state_config: StateConfig<ST, SCT>,
}

#[derive(Parser, Debug)]
struct Args {
    /// otel endpoint
    #[arg(short, long)]
    otel_endpoint: Option<String>,
    /// addresses
    #[arg(short, long, required=true, num_args=1..)]
    addresses: Vec<String>,
}

struct TestgroundArgs {
    simulation_length_s: u64,
    delta_ms: u64,
    proposal_size: usize,
    val_set_update_interval: u64,
    epoch_start_delay: u64,

    router: RouterArgs,
    ledger: LedgerArgs,
}

enum RouterArgs {
    Local { external_latency_ms: u64 },
    RaptorCast,
}

enum GossipArgs {
    Simple,
    Gossipsub {
        fanout: usize,
    },
    Raptor {
        timeout: Duration,
        up_bandwidth_Mbps: u16,
    },
}

pub enum LedgerArgs {
    Mock,
}

static CHAIN_PARAMS: ChainParams = ChainParams {
    tx_limit: 10_000,
    proposal_gas_limit: 300_000_000,
    proposal_byte_limit: 4_000_000,
    vote_pace: Duration::from_millis(0),
};

fn make_provider(
    otel_endpoint: String,
    service_name: String,
) -> opentelemetry_sdk::trace::TracerProvider {
    let exporter = opentelemetry_otlp::SpanExporterBuilder::Tonic(
        opentelemetry_otlp::new_exporter()
            .tonic()
            .with_endpoint(otel_endpoint),
    )
    .build_span_exporter()
    .unwrap();
    let rt = opentelemetry_sdk::runtime::Tokio;
    let provider_builder = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_config(opentelemetry_sdk::trace::Config::default().with_resource(
            opentelemetry_sdk::Resource::new(vec![opentelemetry::KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_NAME,
                service_name,
            )]),
        ))
        .with_batch_exporter(exporter, rt);
    provider_builder.build()
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let context = args.otel_endpoint.as_ref().map(|endpoint| {
        let provider = make_provider(endpoint.to_owned(), "monad-coordinator".to_owned());
        use opentelemetry::trace::TracerProvider;

        let context = {
            let tracer = provider.tracer("opentelemetry");
            let span = tracer.start("exec");
            span.span_context().clone()
        };

        opentelemetry::Context::default().with_remote_span_context(context)
    });

    type SignatureTypeConfig = SecpSignature;
    type SignatureCollectionTypeConfig =
        BlsSignatureCollection<CertificateSignaturePubKey<SignatureTypeConfig>>;
    // TODO parse this from CLI args
    let testground_args = TestgroundArgs {
        simulation_length_s: 3600,
        delta_ms: 4000,
        proposal_size: 5_000,
        val_set_update_interval: 2_000,
        epoch_start_delay: 50,

        router: RouterArgs::RaptorCast,
        ledger: LedgerArgs::Mock,
    };

    let (wg_tx, _) = tokio::sync::broadcast::channel::<()>(args.addresses.len());
    futures_util::future::join_all(
        testnet::<SignatureTypeConfig, SignatureCollectionTypeConfig>(
            &args.addresses,
            &testground_args,
        )
        .into_iter()
        .enumerate()
        .map(|(index, config)| {
            let maybe_provider = args.otel_endpoint.as_ref().map(|endpoint| {
                make_provider(
                    endpoint.to_owned(),
                    format!("monad-testground-{:?}", &config.state_config.key.pubkey()),
                )
            });

            let context = context.clone();
            let (wg_tx, wg_rx) = (wg_tx.clone(), wg_tx.subscribe());
            let fut = async move {
                let fut = run(index, context, wg_tx, wg_rx, config);
                if let Some(provider) = &maybe_provider {
                    fut.with_subscriber({
                        use opentelemetry::trace::TracerProvider;
                        use tracing_subscriber::layer::SubscriberExt;

                        let tracer = provider.tracer("opentelemetry");
                        let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

                        Registry::default()
                            .with(EnvFilter::from_default_env())
                            .with(
                                FmtLayer::default()
                                    .with_writer(std::io::stdout)
                                    .with_span_events(FmtSpan::CLOSE)
                                    .with_ansi(false),
                            )
                            .with(telemetry)
                    })
                    .boxed()
                } else {
                    fut.boxed()
                }
                .await;

                // sleep to flush remaining traces
                tokio::time::sleep(Duration::from_secs(2)).await;
                // destructor is blocking forever for some reason...
                std::mem::forget(maybe_provider);
            };
            Box::pin(fut)
        })
        .map(tokio::spawn),
    )
    .await
    .into_iter()
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
}

fn testnet<ST, SCT>(addresses: &[String], args: &TestgroundArgs) -> Vec<Config<ST, SCT>>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    let configs = std::iter::repeat_with(|| {
        // RaptorCast wants to own the specified keypair, and we can't clone these later,
        // so we have to instantiate two copies here.
        let keypair_bytes = rand::random::<[u8; 32]>();
        let keypair = ST::KeyPairType::from_bytes(keypair_bytes.clone().as_mut_slice()).unwrap();
        let keypair2 = ST::KeyPairType::from_bytes(keypair_bytes.clone().as_mut_slice()).unwrap();

        let cert_keypair = <SignatureCollectionKeyPairType<SCT> as CertificateKeyPair>::from_bytes(
            rand::random::<[u8; 32]>().as_mut_slice(),
        )
        .unwrap();

        (keypair, keypair2, cert_keypair)
    })
    .take(addresses.len())
    .collect::<Vec<_>>();

    let validators = ValidatorSetData(
        configs
            .iter()
            .map(|(keypair, _, cert_keypair)| ValidatorData {
                node_id: NodeId::new(keypair.pubkey()),
                stake: Stake(1),
                cert_pubkey: cert_keypair.pubkey(),
            })
            .collect::<Vec<_>>(),
    );

    let all_peers: Vec<NodeId<_>> = validators.0.iter().map(|data| data.node_id).collect();

    let mut maybe_local_routers = match args.router {
        RouterArgs::Local {
            external_latency_ms,
        } => {
            let local_routers = LocalRouterConfig {
                all_peers,
                external_latency: Duration::from_millis(external_latency_ms),
            }
            .build();
            Some(local_routers)
        }
        _ => None,
    };

    let known_addresses: HashMap<_, _> = addresses
        .iter()
        .zip(configs.iter())
        .map(|(address, (keypair, _, _))| (NodeId::new(keypair.pubkey()), address.parse().unwrap()))
        .collect();

    addresses
        .iter()
        .zip(configs)
        .map(|(address, (keypair, keypair2, cert_keypair))| {
            let me = NodeId::new(keypair.pubkey());
            Config {
                num_nodes: addresses.len(),
                simulation_length: Duration::from_secs(args.simulation_length_s),
                executor_config: ExecutorConfig {
                    router_config: match &args.router {
                        RouterArgs::Local { .. } => RouterConfig::Local(
                            maybe_local_routers.as_mut().unwrap().remove(&me).unwrap(),
                        ),
                        RouterArgs::RaptorCast => RouterConfig::RaptorCast(RaptorCastConfig {
                            key: keypair,
                            known_addresses: known_addresses.clone(),
                            full_nodes: Default::default(),
                            redundancy: 3,
                            local_addr: address.parse().unwrap(),
                            up_bandwidth_mbps: 1_000,
                            mtu: DEFAULT_MTU,
                        }),
                    },
                    ledger_config: match args.ledger {
                        LedgerArgs::Mock => LedgerConfig::Mock,
                    },
                    state_root_hash_config: StateRootHashConfig::Mock {
                        genesis_validator_data: validators.clone(),
                        val_set_update_interval: SeqNum(args.val_set_update_interval),
                    },
                    nodeid: me,
                },
                state_config: StateConfig {
                    key: keypair2,
                    cert_key: cert_keypair,
                    val_set_update_interval: SeqNum(args.val_set_update_interval),
                    epoch_start_delay: Round(args.epoch_start_delay),
                    validators: validators.clone(),
                    consensus_config: ConsensusConfig {
                        execution_delay: SeqNum::MAX,
                        proposal_txn_limit: args.proposal_size,
                        delta: Duration::from_millis(args.delta_ms),
                        statesync_to_live_threshold: SeqNum(600),
                        live_to_statesync_threshold: SeqNum(900),
                        start_execution_threshold: SeqNum(300),
                        chain_config: MockChainConfig::new(&CHAIN_PARAMS),
                        timestamp_latency_estimate_ns: 10_000_000,
                        _phantom: Default::default(),
                    },
                },
            }
        })
        .collect()
}

async fn run<ST, SCT>(
    index: usize,
    cx: Option<opentelemetry::Context>,
    wg_tx: tokio::sync::broadcast::Sender<()>,
    mut wg_rx: tokio::sync::broadcast::Receiver<()>,
    config: Config<ST, SCT>,
) where
    ST: CertificateSignatureRecoverable + Unpin,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Unpin,
    <ST as CertificateSignature>::KeyPairType: Unpin,
    <SCT as SignatureCollection>::SignatureType: Unpin,
{
    let state_backend = InMemoryStateInner::genesis(u128::MAX, SeqNum(4));
    let nodeid = config.executor_config.nodeid;
    let (mut state, init_commands) = make_monad_state(state_backend.clone(), config.state_config);
    let mut executor = make_monad_executor(index, state_backend, config.executor_config);

    executor.exec(init_commands);

    let total_start = Instant::now();
    let mut start = total_start;
    let mut last_printed_len = 0;
    const BLOCK_INTERVAL: usize = 100;

    let mut last_ledger_len = executor.ledger().get_finalized_blocks().len();
    let mut ledger_span = tracing::info_span!("ledger_span", last_ledger_len, ?nodeid);
    if let Some(cx) = &cx {
        ledger_span.set_parent(cx.clone());
    }

    while let Some(event) = executor.next().instrument(ledger_span.clone()).await {
        {
            let _ledger_span = ledger_span.enter();
            let commands = state.update(event);
            executor.exec(commands);
        }
        let ledger_len = executor.ledger().get_finalized_blocks().len();
        if ledger_len > last_ledger_len {
            last_ledger_len = ledger_len;
            ledger_span = tracing::info_span!("ledger_span", last_ledger_len, ?nodeid);
            if let Some(cx) = &cx {
                ledger_span.set_parent(cx.clone());
            }
        }
        if ledger_len >= last_printed_len + BLOCK_INTERVAL {
            event!(
                Level::INFO,
                instance = index,
                ledger_len = ledger_len,
                elapsed_ms = start.elapsed().as_millis(),
                "{}",
                format!("{} blocks", BLOCK_INTERVAL),
            );
            start = Instant::now();
            last_printed_len = ledger_len / BLOCK_INTERVAL * BLOCK_INTERVAL;
        }
        if total_start.elapsed() > config.simulation_length {
            break;
        }
    }
    wg_tx.send(()).unwrap();
    let mut num_done = 0;
    while num_done < config.num_nodes {
        if let futures_util::future::Either::Left((result, _)) =
            futures_util::future::select(wg_rx.recv().boxed(), executor.next().boxed()).await
        {
            result.unwrap();
            num_done += 1;
        }
    }
    eprintln!("exited runloop");
}
