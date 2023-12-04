use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use clap::Parser;
use executor::MonadP2PGossipConfig;
use futures_util::{FutureExt, StreamExt};
use monad_consensus_state::ConsensusConfig;
use monad_consensus_types::{
    block::{Block, BlockType, FullBlock},
    certificate_signature::{CertificateKeyPair, CertificateSignature},
    ledger::LedgerCommitInfo,
    message_signature::MessageSignature,
    multi_sig::MultiSig,
    payload::{
        ExecutionArtifacts, FullTransactionList, Payload, RandaoReveal, TransactionHashList,
    },
    quorum_certificate::{genesis_vote_info, QuorumCertificate},
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    transaction_validator::MockValidator,
    voting::ValidatorMapping,
};
use monad_crypto::{
    hasher::{Hasher, HasherType},
    secp256k1::{KeyPair, SecpSignature},
};
use monad_eth_types::EthAddress;
use monad_executor::{Executor, State};
use monad_gossip::{gossipsub::UnsafeGossipsubConfig, mock::MockGossipConfig};
use monad_quic::service::{SafeQuinnConfig, ServiceConfig};
use monad_types::{NodeId, Round, SeqNum};
use monad_updaters::local_router::LocalRouterConfig;
use opentelemetry::trace::{Span, TraceContextExt, Tracer};
use opentelemetry_otlp::WithExportConfig;
use tracing::{event, instrument::WithSubscriber, Instrument, Level};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::executor::{
    make_monad_executor, make_monad_state, ExecutionLedgerConfig, ExecutorConfig, MempoolConfig,
    RouterConfig, StateConfig,
};

mod executor;

pub struct Config<MessageSignatureType, SignatureCollectionType>
where
    MessageSignatureType: MessageSignature,
    SignatureCollectionType: SignatureCollection,
{
    pub num_nodes: usize,
    pub simulation_length: Duration,
    pub executor_config: ExecutorConfig<MessageSignatureType, SignatureCollectionType>,
    pub state_config: StateConfig<SignatureCollectionType>,
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
    state_root_delay: u64,    // default 0
    simulation_length_s: u64, // default 10
    delta_ms: u64,            // default 1000
    proposal_size: usize,     // default 5000

    router: RouterArgs,
    mempool: MempoolArgs,
    execution_ledger: ExecutionLedgerArgs,
}

enum RouterArgs {
    Local {
        external_latency_ms: u64,
    },
    MonadP2P {
        max_rtt_ms: u64,
        /// bandwidth_Mbps is in Megabit/s
        bandwidth_Mbps: u16,
        gossip: GossipArgs,
    },
}

enum GossipArgs {
    Simple,
    Gossipsub { fanout: usize },
}

enum MempoolArgs {
    Mock,
    LibP2P,
}

pub enum ExecutionLedgerArgs {
    Mock,
    File,
}

fn make_provider(
    otel_endpoint: String,
    service_name: String,
) -> opentelemetry::sdk::trace::TracerProvider {
    let exporter = opentelemetry_otlp::SpanExporterBuilder::Tonic(
        opentelemetry_otlp::new_exporter()
            .tonic()
            .with_endpoint(otel_endpoint),
    )
    .build_span_exporter()
    .unwrap();
    let rt = opentelemetry::runtime::Tokio;
    let provider_builder = opentelemetry::sdk::trace::TracerProvider::builder()
        .with_config(opentelemetry::sdk::trace::config().with_resource(
            opentelemetry::sdk::Resource::new(vec![opentelemetry::KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_NAME,
                service_name,
            )]),
        ))
        .with_batch_exporter(exporter, rt);
    provider_builder.build()
}

#[tokio::main]
async fn main() {
    // tracing_subscriber::fmt::init();
    env_logger::init();

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
    type SignatureCollectionTypeConfig = MultiSig<SignatureTypeConfig>;
    // TODO parse this from CLI args
    let testground_args = TestgroundArgs {
        state_root_delay: 0,
        simulation_length_s: 10,
        delta_ms: 75,
        proposal_size: 5_000,

        router: RouterArgs::MonadP2P {
            max_rtt_ms: 150,
            bandwidth_Mbps: 1_000,
            gossip: GossipArgs::Simple,
        },
        mempool: MempoolArgs::Mock,
        execution_ledger: ExecutionLedgerArgs::Mock,
    };

    let (wg_tx, _) = tokio::sync::broadcast::channel::<()>(args.addresses.len());
    futures_util::future::join_all(
        testnet::<SignatureTypeConfig, SignatureCollectionTypeConfig>(
            &args.addresses,
            &testground_args,
        )
        .into_iter()
        .map(|config| {
            let maybe_provider = args.otel_endpoint.as_ref().map(|endpoint| {
                make_provider(
                    endpoint.to_owned(),
                    format!("monad-testground-{:?}", &config.state_config.key.pubkey()),
                )
            });

            let context = context.clone();
            let (wg_tx, wg_rx) = (wg_tx.clone(), wg_tx.subscribe());
            let fut = async move {
                let fut = run(context, wg_tx, wg_rx, config);
                if let Some(provider) = &maybe_provider {
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

fn testnet<MessageSignatureType, SignatureCollectionType>(
    addresses: &Vec<String>,
    args: &TestgroundArgs,
) -> Vec<Config<MessageSignatureType, SignatureCollectionType>>
where
    MessageSignatureType: MessageSignature,
    SignatureCollectionType: SignatureCollection,
{
    let configs = std::iter::repeat_with(|| {
        let keypair = KeyPair::from_bytes(rand::random::<[u8; 32]>().as_mut_slice()).unwrap();
        let cert_keypair = <SignatureCollectionKeyPairType<SignatureCollectionType> as CertificateKeyPair>::from_bytes(rand::random::<[u8; 32]>().as_mut_slice()).unwrap();
        (keypair, cert_keypair)
    })
    .take(addresses.len())
    .collect::<Vec<_>>();

    let validator_mapping = ValidatorMapping::new(
        configs
            .iter()
            .map(|(keypair, cert_keypair)| (NodeId(keypair.pubkey()), cert_keypair.pubkey()))
            .collect::<Vec<_>>(),
    );

    let genesis_peers = configs
        .iter()
        .map(|(keypair, cert_keypair)| (keypair.pubkey(), cert_keypair.pubkey()))
        .collect::<Vec<_>>();
    let genesis_block = {
        let genesis_txn = TransactionHashList::empty();
        let genesis_prime_qc = QuorumCertificate::genesis_prime_qc::<HasherType>();
        let genesis_execution_header = ExecutionArtifacts::zero();
        FullBlock::from_block(
            Block::new::<HasherType>(
                NodeId(KeyPair::from_bytes(&mut [0xBE_u8; 32]).unwrap().pubkey()),
                Round(0),
                &Payload {
                    txns: genesis_txn,
                    header: genesis_execution_header,
                    seq_num: SeqNum(0),
                    beneficiary: EthAddress::default(),
                    randao_reveal: RandaoReveal::default(),
                },
                &genesis_prime_qc,
            ),
            FullTransactionList::empty(),
            &MockValidator,
        )
        .unwrap()
    };
    let gen_vote_info = genesis_vote_info(genesis_block.get_id());
    let genesis_signatures = {
        let genesis_lci = LedgerCommitInfo::new::<HasherType>(None, &gen_vote_info);
        let msg = HasherType::hash_object(&genesis_lci);

        let mut sigs = Vec::new();
        for (key, cert_key) in &configs {
            let node_id = NodeId(key.pubkey());
            let sig = <SignatureCollectionType as SignatureCollection>::SignatureType::sign(
                msg.as_ref(),
                cert_key,
            );
            sigs.push((node_id, sig));
        }

        let signatures =
            SignatureCollectionType::new(sigs, &validator_mapping, msg.as_ref()).unwrap();

        signatures
    };

    let mut maybe_local_routers = match args.router {
        RouterArgs::Local {
            external_latency_ms,
        } => {
            let local_routers = LocalRouterConfig {
                all_peers: genesis_peers
                    .iter()
                    .map(|(peer_id, _)| NodeId(*peer_id))
                    .collect(),
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
        .map(|(address, (keypair, _))| (NodeId(keypair.pubkey()), address.parse().unwrap()))
        .collect();

    addresses
        .iter()
        .zip(configs)
        .map(|(address, (keypair, cert_keypair))| {
            let me = NodeId(keypair.pubkey());
            Config {
                num_nodes: addresses.len(),
                simulation_length: Duration::from_secs(args.simulation_length_s),
                executor_config: ExecutorConfig {
                    router_config: match &args.router {
                        RouterArgs::Local { .. } => RouterConfig::Local(
                            maybe_local_routers.as_mut().unwrap().remove(&me).unwrap(),
                        ),
                        RouterArgs::MonadP2P {
                            max_rtt_ms,
                            bandwidth_Mbps,
                            gossip,
                        } => RouterConfig::MonadP2P {
                            config: ServiceConfig {
                                zero_instant: Instant::now(),
                                me,
                                server_address: address.parse().unwrap(),
                                quinn_config: SafeQuinnConfig::new(
                                    &keypair,
                                    Duration::from_millis(*max_rtt_ms),
                                    *bandwidth_Mbps,
                                ),
                                known_addresses: known_addresses.clone(),
                            },
                            gossip_config: match gossip {
                                GossipArgs::Simple => {
                                    MonadP2PGossipConfig::Simple(MockGossipConfig {
                                        all_peers: genesis_peers
                                            .iter()
                                            .map(|(pubkey, _)| NodeId(*pubkey))
                                            .collect(),
                                        me,
                                    })
                                }
                                GossipArgs::Gossipsub { fanout } => {
                                    MonadP2PGossipConfig::Gossipsub(UnsafeGossipsubConfig {
                                        seed: rand::random(),
                                        me,
                                        all_peers: genesis_peers
                                            .iter()
                                            .map(|(pubkey, _)| NodeId(*pubkey))
                                            .collect(),
                                        fanout: *fanout,
                                    })
                                }
                            },
                        },
                    },
                    mempool_config: match args.mempool {
                        MempoolArgs::Mock => MempoolConfig::Mock,
                        MempoolArgs::LibP2P => MempoolConfig::LibP2P,
                    },
                    execution_ledger_config: match args.execution_ledger {
                        ExecutionLedgerArgs::Mock => ExecutionLedgerConfig::Mock,
                        ExecutionLedgerArgs::File => ExecutionLedgerConfig::File,
                    },
                },
                state_config: StateConfig {
                    key: keypair,
                    cert_key: cert_keypair,
                    genesis_peers: genesis_peers.clone(),
                    delta: Duration::from_millis(args.delta_ms),
                    consensus_config: ConsensusConfig {
                        proposal_size: args.proposal_size,
                        state_root_delay: SeqNum(args.state_root_delay),
                        propose_with_missing_blocks: false,
                    },
                    genesis_block: genesis_block.clone(),
                    genesis_vote_info: gen_vote_info,
                    genesis_signatures: genesis_signatures.clone(),
                },
            }
        })
        .collect()
}

async fn run<MessageSignatureType, SignatureCollectionType>(
    cx: Option<opentelemetry_api::Context>,
    wg_tx: tokio::sync::broadcast::Sender<()>,
    mut wg_rx: tokio::sync::broadcast::Receiver<()>,
    config: Config<MessageSignatureType, SignatureCollectionType>,
) where
    MessageSignatureType: MessageSignature + Unpin,
    SignatureCollectionType: SignatureCollection + Unpin,
    <SignatureCollectionType as SignatureCollection>::SignatureType: Unpin,
{
    let mut executor = make_monad_executor(config.executor_config).await;
    let (mut state, init_commands) = make_monad_state(config.state_config);

    executor.exec(init_commands);

    let total_start = Instant::now();
    let mut start = total_start;
    let mut last_printed_len = 0;
    const BLOCK_INTERVAL: usize = 100;

    let mut last_ledger_len = executor.ledger().get_blocks().len();
    let mut ledger_span = tracing::info_span!("ledger_span", last_ledger_len);
    if let Some(cx) = &cx {
        ledger_span.set_parent(cx.clone());
    }

    while let Some(event) = executor.next().instrument(ledger_span.clone()).await {
        {
            let _ledger_span = ledger_span.enter();
            let commands = state.update(event);
            executor.exec(commands);
        }
        let ledger_len = executor.ledger().get_blocks().len();
        if ledger_len > last_ledger_len {
            last_ledger_len = ledger_len;
            ledger_span = tracing::info_span!("ledger_span", last_ledger_len);
            if let Some(cx) = &cx {
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
