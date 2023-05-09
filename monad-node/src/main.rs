use std::time::Duration;

use clap::Parser;
use futures_util::StreamExt;
use tracing::instrument::Instrument;

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
    executor::{parent::ParentExecutor, timer::TokioTimer},
    Executor, State,
};
use monad_p2p::Multiaddr;
use monad_types::{NodeId, Round};

type HasherType = Sha256Hash;
type SignatureType = SecpSignature;
type SignatureCollectionType = AggregateSignatures<SignatureType>;
type MonadState = monad_state::MonadState<SignatureType, SignatureCollectionType>;
type MonadConfig = <MonadState as State>::Config;

pub struct Config {
    // boxed for no implicit copies
    pub secret_key: Box<[u8; 32]>,

    pub bind_address: Multiaddr,

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
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    futures_util::future::select_all(testnet(args.addresses, Duration::from_secs(1)).map(
        |config| {
            let fut = {
                let bind_address = config.bind_address.clone();
                run(config).instrument(tracing::info_span!("node", ?bind_address))
            };
            Box::pin(fut)
        },
    ))
    .await;
}

fn testnet(addresses: Vec<String>, delta: Duration) -> impl Iterator<Item = Config> {
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
            bind_address,
            genesis_peers: peers.clone(),
            delta,
            genesis_block: genesis_block.clone(),
            genesis_vote_info: genesis_vote_info(genesis_block.get_id()),
            genesis_signatures: genesis_signatures.clone(),
        })
}

async fn run(mut config: Config) {
    let (keypair, libp2p_keypair) =
        KeyPair::libp2p_from_bytes(config.secret_key.as_mut_slice()).expect("invalid key");

    let mut router =
        monad_p2p::Service::with_tokio_executor(libp2p_keypair.into(), config.bind_address).await;
    for (address, peer) in &config.genesis_peers {
        router.add_peer(&peer.into(), address.clone())
    }

    let mut executor = ParentExecutor {
        router,
        timer: TokioTimer::default(),
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

    while let Some(event) = executor.next().await {
        let commands = state.update(event);
        executor.exec(commands);
    }
}
