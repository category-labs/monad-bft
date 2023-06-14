use std::time::{Duration, Instant};

use clap::Parser;
use futures_util::StreamExt;
use tracing::event;
use tracing::Level;

use monad_consensus::{
    signatures::aggregate_signature::AggregateSignatures,
    types::{
        block::{Block, TransactionList},
        ledger::LedgerCommitInfo,
        quorum_certificate::{genesis_vote_info, QuorumCertificate},
        signature::SignatureCollection,
        voting::VoteInfo,
    },
    validation::hashing::{Hasher, Sha256Hash},
};
use monad_crypto::secp256k1::{KeyPair, PubKey, SecpSignature};
use monad_crypto::Signature;
use monad_executor::{
    executor::{
        ledger::MockLedger, mempool::MockMempool, parent::ParentExecutor, timer::TokioTimer,
    },
    Executor, State,
};
use monad_p2p::{Auth, Multiaddr};
use monad_types::{NodeId, Round};

type HasherType = Sha256Hash;
type SignatureType = SecpSignature;
type SignatureCollectionType = AggregateSignatures<SignatureType>;
type MonadState = monad_state::MonadState<SignatureType, SignatureCollectionType>;
type MonadConfig = <MonadState as State>::Config;

pub struct Config {
    // boxed for no implicit copies
    pub secret_key: Box<[u8; 32]>,
    pub simulation_length: Duration,

    pub bind_address: Multiaddr,
    pub libp2p_timeout: Duration,
    pub libp2p_keepalive: Duration,
    pub libp2p_auth: Auth,

    pub genesis_peers: Vec<(Multiaddr, PubKey)>,
    pub delta: Duration,
    pub genesis_block: Block<SignatureCollectionType>,
    pub genesis_vote_info: VoteInfo,
    pub genesis_signatures: SignatureCollectionType,
}

#[derive(Parser, Debug)]
struct Args {
    /// addresses
    #[arg(short, long, required=true, num_args=1..)]
    addresses: Vec<String>,
}

#[tokio::main]
async fn main() {
    // tracing_subscriber::fmt::init();

    use tracing_subscriber::layer::SubscriberExt;
    let tracer = opentelemetry_contrib::trace::exporter::jaeger_json::JaegerJsonExporter::new(
        "/tmp/jaeger".into(),
        "jaeger".to_owned(),
        "monad-node".to_owned(),
        opentelemetry::runtime::Tokio,
    )
    // ID generator needs to be passed into TracerProvider::builder() inside install_batch
    .install_batch();

    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    let subscriber = tracing_subscriber::Registry::default().with(telemetry);
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let args = Args::parse();

    {
        let all_nodes_span = tracing::info_span!("all_nodes");
        let _entered = all_nodes_span.enter();
        let id = all_nodes_span.id().unwrap();
        futures_util::future::join_all(
            testnet(
                args.addresses,
                Duration::from_secs(10),
                Duration::from_secs(5),
                Duration::from_secs(60),
            )
            .map(|config| Box::pin(run(id.clone(), config)))
            .map(tokio::spawn),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    }
    tokio::time::sleep(Duration::from_secs(3)).await;
}

fn testnet(
    addresses: Vec<String>,
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
        .map(|address| format!("/ip4/{}/tcp/4700", address).parse().unwrap())
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
        .map(move |(bind_address, secret_key)| Config {
            secret_key,
            simulation_length,
            bind_address,
            libp2p_timeout: delta,
            libp2p_keepalive: keepalive,
            libp2p_auth: Auth::None,
            genesis_peers: peers.clone(),
            delta,
            genesis_block: genesis_block.clone(),
            genesis_vote_info: genesis_vote_info(genesis_block.get_id()),
            genesis_signatures: genesis_signatures.clone(),
        })
}

async fn run(cx: tracing::span::Id, mut config: Config) {
    let (keypair, libp2p_keypair) =
        KeyPair::libp2p_from_bytes(config.secret_key.as_mut_slice()).expect("invalid key");

    let mut router = monad_p2p::Service::with_tokio_executor(
        libp2p_keypair.into(),
        config.bind_address,
        config.libp2p_timeout,
        config.libp2p_keepalive,
        config.libp2p_auth,
    )
    .await;
    for (address, peer) in &config.genesis_peers {
        router.add_peer(&peer.into(), address.clone())
    }

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

    // FIXME hack so all tcp sockets are bound before they try and send mesages
    // we can delete this once we support retry at the monad-p2p executor level
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    let total_start = Instant::now();
    let mut start = total_start;
    let mut last_printed_len = 0;
    const BLOCK_INTERVAL: usize = 100;

    let mut last_ledger_len = executor.ledger().get_blocks().len();
    let mut ledger_span = tracing::info_span!(parent: cx.clone(), "ledger_span", last_ledger_len);

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
            ledger_span = tracing::info_span!(parent: cx.clone(), "ledger_span", last_ledger_len);
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
    eprintln!("exited runloop");
    tokio::time::sleep(Duration::from_secs(1)).await;
    eprintln!("exited runloop, done sleeping");
}
