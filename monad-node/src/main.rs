use std::time::{Duration, Instant};

use clap::Parser;
use futures_util::FutureExt;
use futures_util::StreamExt;
use opentelemetry::trace::Span;
use opentelemetry::trace::TraceContextExt;
use opentelemetry::trace::Tracer;
use opentelemetry_otlp::WithExportConfig;
use tracing::event;
use tracing::Level;

use monad_consensus_types::{
    block::{Block, TransactionList},
    ledger::LedgerCommitInfo,
    multi_sig::MultiSig,
    quorum_certificate::{genesis_vote_info, QuorumCertificate},
    signature::SignatureCollection,
    validation::{Hasher, Sha256Hash},
    voting::VoteInfo,
};
use monad_crypto::secp256k1::{KeyPair, PubKey, SecpSignature};
use monad_crypto::Signature;
use monad_executor::{
    executor::{
        ledger::MockLedger, mempool::MockMempool, parent::ParentExecutor, timer::TokioTimer,
    },
    Executor, State,
};
use monad_p2p::Multiaddr;
use monad_types::{NodeId, Round};
use tracing::instrument::WithSubscriber;
use tracing_opentelemetry::OpenTelemetrySpanExt;

type HasherType = Sha256Hash;
type SignatureType = SecpSignature;
type SignatureCollectionType = MultiSig<SignatureType>;
type MonadState = monad_state::MonadState<SignatureType, SignatureCollectionType>;
type MonadConfig = <MonadState as State>::Config;

pub struct Config {
    // boxed for no implicit copies
    pub secret_key: Box<[u8; 32]>,
    pub pubkey: PubKey,
    pub simulation_length: Duration,

    pub bind_address: Multiaddr,
    pub libp2p_timeout: Duration,
    pub libp2p_keepalive: Duration,

    pub genesis_peers: Vec<(Multiaddr, PubKey)>,
    pub delta: Duration,
    pub genesis_block: Block<SignatureCollectionType>,
    pub genesis_vote_info: VoteInfo,
    pub genesis_signatures: SignatureCollectionType,
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

    let (wg_tx, _) = tokio::sync::broadcast::channel::<()>(args.addresses.len());
    futures_util::future::join_all(
        testnet(
            args.addresses
                .into_iter()
                .map(|address| {
                    let (host, port) = address.split_once(':').expect("expected <host>:<port>");
                    (host.to_owned(), port.parse().unwrap())
                })
                .collect(),
            Duration::from_secs(10),
            Duration::from_secs(1),
            Duration::from_secs(60),
        )
        .map(|config| {
            let maybe_provider = args.otel_endpoint.as_ref().map(|endpoint| {
                make_provider(
                    endpoint.to_owned(),
                    format!("monad-node-{:?}", &config.pubkey),
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

fn testnet(
    addresses: Vec<(String, u16)>,
    simulation_length: Duration,
    delta: Duration,
    keepalive: Duration,
) -> impl Iterator<Item = Config> {
    let secrets = std::iter::repeat_with(rand::random::<[u8; 32]>)
        .map(Box::new)
        .take(addresses.len())
        .collect::<Vec<_>>();
    let keys: Vec<_> = secrets
        .iter()
        .cloned() // It's ok to copy these secrets because this is just used for testnet config gen
        .map(|mut secret| KeyPair::libp2p_from_bytes(secret.as_mut_slice()).unwrap())
        .collect();

    let addresses = addresses
        .into_iter()
        .map(|(host, port)| format!("/ip4/{host}/udp/{port}/quic-v1").parse().unwrap())
        .collect::<Vec<Multiaddr>>();

    let peers = addresses
        .iter()
        .cloned()
        .zip(keys.iter().map(|(keypair, _)| keypair.pubkey()))
        .collect::<Vec<_>>();

    let genesis_block = {
        let genesis_txn = TransactionList::default();
        let genesis_prime_qc = QuorumCertificate::genesis_prime_qc::<HasherType>();
        Block::new::<HasherType>(
            // FIXME init from genesis config, don't use random key
            NodeId(KeyPair::from_bytes(&mut [0xBE_u8; 32]).unwrap().pubkey()),
            Round(0),
            &genesis_txn,
            &genesis_prime_qc,
        )
    };
    let genesis_signatures = {
        let genesis_lci =
            LedgerCommitInfo::new::<HasherType>(None, &genesis_vote_info(genesis_block.get_id()));
        let msg = HasherType::hash_object(&genesis_lci);
        let mut signatures = SignatureCollectionType::new();
        for signature in keys
            .iter()
            .map(|(key, _)| SignatureType::sign(msg.as_ref(), key))
        {
            signatures.add_signature(signature)
        }
        signatures
    };

    addresses
        .into_iter()
        .zip(secrets.into_iter())
        .zip(
            peers
                .iter()
                .map(|(_, pubkey)| pubkey)
                .copied()
                .collect::<Vec<_>>(),
        )
        .map(move |((bind_address, secret_key), pubkey)| Config {
            secret_key,
            pubkey,
            simulation_length,
            bind_address,
            libp2p_timeout: delta,
            libp2p_keepalive: keepalive,
            genesis_peers: peers.clone(),
            delta,
            genesis_block: genesis_block.clone(),
            genesis_vote_info: genesis_vote_info(genesis_block.get_id()),
            genesis_signatures: genesis_signatures.clone(),
        })
}

async fn run(
    cx: Option<opentelemetry_api::Context>,
    wg_tx: tokio::sync::broadcast::Sender<()>,
    mut wg_rx: tokio::sync::broadcast::Receiver<()>,
    mut config: Config,
) {
    let (keypair, libp2p_keypair) =
        KeyPair::libp2p_from_bytes(config.secret_key.as_mut_slice()).expect("invalid key");

    let mut router = monad_p2p::Service::with_tokio_executor(
        libp2p_keypair.into(),
        config.bind_address,
        config.libp2p_timeout,
        config.libp2p_keepalive,
    )
    .await;
    let num_nodes = config.genesis_peers.len();
    for (address, peer) in &config.genesis_peers {
        router.add_peer(&peer.into(), address.clone())
    }

    // FIXME hack so all tcp sockets are bound before they try and send mesages
    // we can delete this once we support retry at the monad-p2p executor level
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let mut executor = ParentExecutor {
        router,
        timer: TokioTimer::default(),
        mempool: MockMempool::default(),
        ledger: MockLedger::default(),
    };

    let (mut state, init_commands) = MonadState::init(MonadConfig {
        validators: config
            .genesis_peers
            .into_iter()
            .map(|(_, peer)| peer)
            .collect(),
        key: keypair,
        delta: config.delta,
        genesis_block: config.genesis_block,
        genesis_vote_info: config.genesis_vote_info,
        genesis_signatures: config.genesis_signatures,
    });
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

    while let Some(event) = executor.next().await {
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
    while num_done < num_nodes {
        if let futures_util::future::Either::Left((result, _)) =
            futures_util::future::select(wg_rx.recv().boxed(), executor.next().boxed()).await
        {
            result.unwrap();
            num_done += 1;
        }
    }
    eprintln!("exited runloop");
}
