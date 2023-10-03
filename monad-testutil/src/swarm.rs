use std::time::Duration;

use monad_consensus_state::ConsensusConfig;
use monad_consensus_types::{
    block::BlockType, message_signature::MessageSignature, quorum_certificate::genesis_vote_info,
    signature_collection::SignatureCollection, transaction_validator::TransactionValidator,
    validation::Sha256Hash,
};
use monad_crypto::secp256k1::{KeyPair, PubKey};
use monad_executor::{timed_event::TimedEvent, State};
use monad_executor_glue::{MonadEvent, PeerId};
use monad_mock_swarm::{
    mock::{MockExecutor, MockableExecutor, RouterScheduler},
    mock_swarm::{Node, Nodes},
    transformer::Pipeline,
};
use monad_state::MonadConfig;
use monad_types::{Deserializable, NodeId, Serializable};
use monad_wal::PersistenceLogger;

use crate::{signing::get_genesis_config, validators::create_keys_w_validators};

pub struct SwarmTestConfig {
    pub num_nodes: u16,
    pub consensus_delta: Duration,

    pub parallelize: bool,
    pub until: Duration,
    pub until_block: usize,
    pub expected_block: usize,
}

pub fn get_configs<ST: MessageSignature, SCT: SignatureCollection, TVT: TransactionValidator>(
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
        get_genesis_config::<Sha256Hash, SCT, TVT>(voting_keys.iter(), &validator_mapping, &tvt);

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
            consensus_config: ConsensusConfig {
                proposal_size: 5000,
                state_root_delay: 4,
                propose_with_missing_blocks: false,
            },
            genesis_block: genesis_block.clone(),
            genesis_vote_info: genesis_vote_info(genesis_block.get_id()),
            genesis_signatures: genesis_sigs.clone(),
        })
        .collect::<Vec<_>>();

    (pubkeys, state_configs)
}

pub fn node_ledger_verification<O: BlockType + PartialEq>(
    ledgers: &Vec<Vec<O>>,
    min_ledger_len: usize,
) {
    let (max_ledger_idx, max_b) = ledgers
        .iter()
        .map(Vec::len)
        .enumerate()
        .max_by_key(|(_idx, num_b)| *num_b)
        .unwrap();

    for ledger in ledgers {
        let ledger_len = ledger.len();
        assert!(ledger_len >= min_ledger_len);
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

pub fn create_and_run_nodes<S, ST, SCT, RS, RSC, LGR, P, TVT, ME>(
    tvt: TVT,
    router_scheduler_config: RSC,
    logger_config: LGR::Config,
    pipeline: P,
    swarm_config: SwarmTestConfig,
) -> Duration
where
    S: State<
        Config = MonadConfig<SCT, TVT>,
        Event = MonadEvent<ST, SCT>,
        SignatureCollection = SCT,
    >,
    ST: MessageSignature + Unpin,
    SCT: SignatureCollection + Unpin,

    RS: RouterScheduler,
    S::Message: Deserializable<RS::M>,
    S::OutboundMessage: Serializable<RS::M>,
    RS::Serialized: Eq,

    LGR: PersistenceLogger<Event = TimedEvent<S::Event>>,
    P: Pipeline<RS::Serialized> + Clone,
    ME: MockableExecutor<Event = S::Event, SignatureCollection = SCT>,

    MockExecutor<S, RS, ME, ST, SCT>: Unpin,
    S::Block: PartialEq + Unpin,
    Node<S, RS, P, LGR, ME, ST, SCT>: Send,
    RS::Serialized: Send,

    RSC: Fn(Vec<PeerId>, PeerId) -> RS::Config,

    LGR::Config: Clone,
    TVT: TransactionValidator,
{
    let (peers, state_configs) =
        get_configs::<ST, SCT, TVT>(tvt, swarm_config.num_nodes, swarm_config.consensus_delta);
    run_nodes_until::<S, ST, SCT, RS, RSC, LGR, P, TVT, ME>(
        peers,
        state_configs,
        router_scheduler_config,
        logger_config,
        pipeline,
        swarm_config.parallelize,
        swarm_config.until,
        swarm_config.until_block,
        swarm_config.expected_block,
    )
}

pub fn run_nodes_until<S, ST, SCT, RS, RSC, LGR, P, TVT, ME>(
    pubkeys: Vec<PubKey>,
    state_configs: Vec<MonadConfig<SCT, TVT>>,
    router_scheduler_config: RSC,
    logger_config: LGR::Config,
    pipeline: P,
    parallelize: bool,

    until: Duration,
    until_block: usize,
    min_ledger_len: usize,
) -> Duration
where
    S: State<
        Config = MonadConfig<SCT, TVT>,
        Event = MonadEvent<ST, SCT>,
        SignatureCollection = SCT,
    >,
    ST: MessageSignature + Unpin,
    SCT: SignatureCollection + Unpin,

    RS: RouterScheduler,
    S::Message: Deserializable<RS::M>,
    S::OutboundMessage: Serializable<RS::M>,
    RS::Serialized: Eq,

    LGR: PersistenceLogger<Event = TimedEvent<S::Event>>,
    P: Pipeline<RS::Serialized> + Clone,
    ME: MockableExecutor<Event = S::Event, SignatureCollection = SCT>,

    MockExecutor<S, RS, ME, ST, SCT>: Unpin,
    S::Block: PartialEq + Unpin,
    Node<S, RS, P, LGR, ME, ST, SCT>: Send,
    RS::Serialized: Send,

    RSC: Fn(Vec<PeerId>, PeerId) -> RS::Config,

    LGR::Config: Clone,
    TVT: Clone,
{
    let mut nodes = Nodes::<S, RS, P, LGR, ME, ST, SCT>::new(
        pubkeys
            .iter()
            .copied()
            .zip(state_configs)
            .map(|(pubkey, state_config)| {
                (
                    pubkey,
                    state_config,
                    logger_config.clone(),
                    router_scheduler_config(
                        pubkeys.iter().copied().map(PeerId).collect(),
                        PeerId(pubkey),
                    ),
                    pipeline.clone(),
                )
            })
            .collect(),
    );

    let mut max_tick = Duration::from_nanos(0);
    if parallelize {
        max_tick = nodes.batch_step_until(until, until_block);
    } else {
        while let Some((tick, _, _)) = nodes.step_until(until, until_block) {
            assert!(tick >= max_tick);
            max_tick = tick;
        }
    }

    node_ledger_verification(
        &nodes
            .states()
            .values()
            .map(|node| node.executor.ledger().get_blocks().clone())
            .collect(),
        min_ledger_len,
    );

    max_tick
}
