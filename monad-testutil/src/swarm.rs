use std::time::Duration;

use monad_consensus_types::{
    block::BlockType, message_signature::MessageSignature, quorum_certificate::genesis_vote_info,
    signature_collection::SignatureCollection, validation::Sha256Hash,
};
use monad_crypto::secp256k1::{KeyPair, PubKey};
use monad_executor::{
    executor::mock::{MockExecutor, MockableExecutor, RouterScheduler},
    mock_swarm::Nodes,
    timed_event::TimedEvent,
    transformer::Pipeline,
    PeerId, State,
};
use monad_state::MonadConfig;
use monad_types::{Deserializable, NodeId, Serializable};
use monad_wal::PersistenceLogger;

use crate::{signing::get_genesis_config, validators::create_keys_w_validators};

pub fn get_configs<ST: MessageSignature, SCT: SignatureCollection, TVT: Clone>(
    tvt: TVT,
    num_nodes: u16,
    delta: Duration,
) -> (Vec<PubKey>, Vec<MonadConfig<SCT, TVT>>) {
    let (keys, cert_keys, _validators, validator_mapping) =
        create_keys_w_validators::<SCT>(num_nodes as u32);
    let pubkeys = keys.iter().map(KeyPair::pubkey).collect::<Vec<_>>();
    let voting_keys = keys
        .iter()
        .map(|k| NodeId(k.pubkey()))
        .zip(cert_keys.iter())
        .collect::<Vec<_>>();

    let (genesis_block, genesis_sigs) =
        get_genesis_config::<Sha256Hash, SCT>(voting_keys.iter(), &validator_mapping);

    let state_configs = keys
        .into_iter()
        .zip(cert_keys)
        .map(|(key, certkey)| MonadConfig {
            transaction_validator: tvt.clone(),
            key,
            certkey,
            validators: validator_mapping
                .map
                .iter()
                .map(|(node_id, sctpubkey)| (node_id.0, *sctpubkey))
                .collect::<Vec<_>>(),
            delta,
            state_root_delay: 0,
            genesis_block: genesis_block.clone(),
            genesis_vote_info: genesis_vote_info(genesis_block.get_id()),
            genesis_signatures: genesis_sigs.clone(),
        })
        .collect::<Vec<_>>();

    (pubkeys, state_configs)
}

pub fn node_ledger_verification<O: BlockType + PartialEq>(ledgers: &Vec<Vec<O>>) {
    let (max_ledger_idx, max_b) = ledgers
        .iter()
        .map(Vec::len)
        .enumerate()
        .max_by_key(|(_idx, num_b)| *num_b)
        .unwrap();

    for ledger in ledgers {
        let ledger_len = ledger.len();
        assert_ne!(ledger_len, 0);
        assert!(
            ledger.iter().collect::<Vec<_>>()
                == ledgers[max_ledger_idx]
                    .iter()
                    .take(ledger_len)
                    .collect::<Vec<_>>()
        );
        assert!(max_b - ledger.len() <= 5); // this 5 block bound is arbitrary... is there a better way to do this?
    }
}

pub fn run_nodes<S, ST, SCT, RS, RSC, LGR, P, TVT, ME>(
    tvt: TVT,
    router_scheduler_config: RSC,
    logger_config: LGR::Config,

    pipeline: P,
    num_nodes: u16,
    num_blocks: usize,
    delta: Duration,
) where
    S: State<Config = MonadConfig<SCT, TVT>>,
    ST: MessageSignature,
    SCT: SignatureCollection,

    RS: RouterScheduler<M = S::Message>,
    S::Message: Deserializable<RS::M>,
    S::OutboundMessage: Serializable<RS::M>,
    RS::Serialized: Eq,

    LGR: PersistenceLogger<Event = TimedEvent<S::Event>>,
    P: Pipeline<RS::Serialized>,
    ME: MockableExecutor<Event = S::Event>,

    MockExecutor<S, RS, ME>: Unpin,
    S::Event: Unpin,
    S::Block: PartialEq + Unpin,

    RSC: Fn(Vec<PeerId>, PeerId) -> RS::Config,

    LGR::Config: Clone,
    TVT: Clone,
{
    let (pubkeys, state_configs) = get_configs::<ST, SCT, TVT>(tvt, num_nodes, delta);
    let peers = pubkeys
        .iter()
        .copied()
        .zip(state_configs)
        .map(|(pubkey, b)| {
            (
                pubkey,
                b,
                logger_config.clone(),
                router_scheduler_config(
                    pubkeys.iter().copied().map(PeerId).collect(),
                    PeerId(pubkey),
                ),
            )
        })
        .collect::<Vec<_>>();
    let mut nodes = Nodes::<S, RS, P, LGR, ME>::new(peers, pipeline);

    while let Some((duration, id, event)) = nodes.step() {
        if nodes
            .states()
            .values()
            .next()
            .unwrap()
            .executor
            .ledger()
            .get_blocks()
            .len()
            > num_blocks
        {
            break;
        }
    }

    node_ledger_verification(
        &nodes
            .states()
            .values()
            .map(|node| node.executor.ledger().get_blocks().clone())
            .collect(),
    );
}

pub fn run_nodes_until<S, ST, SCT, RS, RSC, LGR, P, TVT, ME>(
    pubkeys: Vec<PubKey>,
    state_configs: Vec<MonadConfig<SCT, TVT>>,
    router_scheduler_config: RSC,
    logger_config: LGR::Config,

    pipeline: P,
    until: Duration,
) where
    S: State<Config = MonadConfig<SCT, TVT>>,
    ST: MessageSignature,
    SCT: SignatureCollection,

    RS: RouterScheduler<M = S::Message>,
    S::Message: Deserializable<RS::M>,
    S::OutboundMessage: Serializable<RS::M>,
    RS::Serialized: Eq,

    LGR: PersistenceLogger<Event = TimedEvent<S::Event>>,
    P: Pipeline<RS::Serialized>,
    ME: MockableExecutor<Event = S::Event>,

    MockExecutor<S, RS, ME>: Unpin,
    S::Event: Unpin,
    S::Block: PartialEq + Unpin,

    RSC: Fn(Vec<PeerId>, PeerId) -> RS::Config,

    LGR::Config: Clone,
    TVT: Clone,
{
    let mut nodes = Nodes::<S, RS, P, LGR, ME>::new(
        pubkeys
            .iter()
            .copied()
            .zip(state_configs)
            .map(|(pubkey, b)| {
                (
                    pubkey,
                    b,
                    logger_config.clone(),
                    router_scheduler_config(
                        pubkeys.iter().copied().map(PeerId).collect(),
                        PeerId(pubkey),
                    ),
                )
            })
            .collect(),
        pipeline,
    );

    while nodes.step_until(until).is_some() {}

    node_ledger_verification(
        &nodes
            .states()
            .values()
            .map(|node| node.executor.ledger().get_blocks().clone())
            .collect(),
    );
}
