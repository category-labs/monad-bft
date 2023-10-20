use std::time::Duration;

use monad_consensus_state::ConsensusConfig;
use monad_consensus_types::{
    block::BlockType, message_signature::MessageSignature, quorum_certificate::genesis_vote_info,
    signature_collection::SignatureCollection, transaction_validator::TransactionValidator,
    validation::Sha256Hash,
};
use monad_crypto::secp256k1::{KeyPair, PubKey};
use monad_eth_types::EthAddress;
use monad_executor::{timed_event::TimedEvent, State};
use monad_executor_glue::{MonadEvent, PeerId};
use monad_mock_swarm::{
    mock::{MockExecutor, MockableExecutor, RouterScheduler},
    mock_swarm::{Node, Nodes, NodesTerminator},
    transformer::{Pipeline, ID},
};
use monad_state::MonadConfig;
use monad_types::{Deserializable, NodeId, Serializable};
use monad_wal::PersistenceLogger;

use crate::{signing::get_genesis_config, validators::create_keys_w_validators};

pub struct SwarmTestConfig {
    pub num_nodes: u16,
    pub consensus_delta: Duration,

    pub parallelize: bool,
    pub expected_block: usize,
    pub state_root_delay: u64,
    pub seed: u64,
}

pub fn get_configs<ST: MessageSignature, SCT: SignatureCollection, TVT: TransactionValidator>(
    tvt: TVT,
    num_nodes: u16,
    delta: Duration,
    state_root_delay: u64,
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
            beneficiary: EthAddress::default(),
            validators: validator_mapping
                .map
                .iter()
                .map(|(node_id, sctpubkey)| (node_id.0, *sctpubkey))
                .collect::<Vec<_>>(),
            delta,
            consensus_config: ConsensusConfig {
                proposal_size: 5000,
                state_root_delay,
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
        assert!(
            ledger_len >= min_ledger_len,
            "ledger length expected {:?} actual {:?}",
            min_ledger_len,
            ledger_len
        );
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

pub fn create_and_run_nodes<S, ST, SCT, RS, RSC, LGR, P, TVT, ME, TERM>(
    tvt: TVT,
    router_scheduler_config: RSC,
    logger_config: LGR::Config,
    mock_mempool_config: ME::Config,
    pipeline: P,
    terminator: TERM,
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
    TERM: NodesTerminator<S, RS, P, LGR, ME, ST, SCT>,
{
    let (peers, state_configs) = get_configs::<ST, SCT, TVT>(
        tvt,
        swarm_config.num_nodes,
        swarm_config.consensus_delta,
        swarm_config.state_root_delay,
    );
    run_nodes_until::<_, _, _, _, _, _, _, _, _, _>(
        peers,
        state_configs,
        router_scheduler_config,
        logger_config,
        mock_mempool_config,
        pipeline,
        swarm_config.parallelize,
        terminator,
        swarm_config.expected_block,
        swarm_config.seed,
    )
}

pub fn run_nodes_until<S, ST, SCT, RS, RSC, LGR, P, TVT, ME, TERM>(
    pubkeys: Vec<PubKey>,
    state_configs: Vec<MonadConfig<SCT, TVT>>,
    router_scheduler_config: RSC,
    logger_config: LGR::Config,
    mock_mempool_config: ME::Config,
    pipeline: P,
    parallelize: bool,

    terminator: TERM,
    min_ledger_len: usize,
    seed: u64,
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
    TERM: NodesTerminator<S, RS, P, LGR, ME, ST, SCT>,
{
    let mut nodes = Nodes::<S, RS, P, LGR, ME, ST, SCT>::new(
        pubkeys
            .iter()
            .copied()
            .zip(state_configs)
            .map(|(pubkey, state_config)| {
                (
                    ID::new(PeerId(pubkey)),
                    state_config,
                    logger_config.clone(),
                    router_scheduler_config(
                        pubkeys.iter().copied().map(PeerId).collect(),
                        PeerId(pubkey),
                    ),
                    mock_mempool_config,
                    pipeline.clone(),
                    seed,
                )
            })
            .collect(),
    );

    let mut max_tick = Duration::from_nanos(0);
    if parallelize {
        if let Some(tick) = nodes.batch_step_until(&terminator) {
            max_tick = tick;
        }
    } else {
        while let Some(tick) = nodes.step_until(&terminator) {
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
