use std::time::Duration;

use monad_consensus_state::ConsensusConfig;
use monad_consensus_types::{
    block::BlockType,
    block_validator::BlockValidator,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    voting::ValidatorMapping,
};
use monad_crypto::certificate_signature::{
    CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::EthAddress;
use monad_mock_swarm::{
    mock::MockExecutor,
    mock_swarm::{Node, Nodes, NodesTerminator},
    swarm_relation::SwarmRelation,
};
use monad_state::MonadConfig;
use monad_transformer::ID;
use monad_types::{NodeId, Round, SeqNum, Stake};

use crate::validators::create_keys_w_validators;

#[derive(Debug, Clone, Copy)]
pub struct SwarmTestConfig {
    pub num_nodes: u16,
    pub consensus_delta: Duration,

    pub parallelize: bool,
    pub expected_block: usize,
    pub state_root_delay: u64,
    pub seed: u64,
    pub proposal_size: usize,
    pub val_set_update_interval: SeqNum,
    pub epoch_start_delay: Round,
}

pub fn get_configs<
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BVT: BlockValidator,
>(
    tvt: BVT,
    num_nodes: u16,
    delta: Duration,
    state_root_delay: u64,
    proposal_size: usize,
    val_set_update_interval: SeqNum,
    epoch_start_delay: Round,
) -> (
    Vec<CertificateSignaturePubKey<ST>>,
    Vec<MonadConfig<ST, SCT, BVT>>,
) {
    let (keys, cert_keys, _validators, validator_mapping) =
        create_keys_w_validators::<ST, SCT>(num_nodes as u32);
    complete_config::<ST, SCT, BVT>(
        tvt,
        keys,
        cert_keys,
        validator_mapping,
        delta,
        state_root_delay,
        proposal_size,
        val_set_update_interval,
        epoch_start_delay,
    )
}

pub fn complete_config<ST, SCT, BVT>(
    tvt: BVT,
    keys: Vec<ST::KeyPairType>,
    cert_keys: Vec<SignatureCollectionKeyPairType<SCT>>,
    validator_mapping: ValidatorMapping<
        CertificateSignaturePubKey<ST>,
        SignatureCollectionKeyPairType<SCT>,
    >,
    delta: Duration,
    state_root_delay: u64,
    proposal_txn_limit: usize,
    val_set_update_interval: SeqNum,
    epoch_start_delay: Round,
) -> (
    Vec<CertificateSignaturePubKey<ST>>,
    Vec<MonadConfig<ST, SCT, BVT>>,
)
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BVT: BlockValidator,
{
    let pubkeys = keys
        .iter()
        .map(CertificateKeyPair::pubkey)
        .collect::<Vec<_>>();

    let state_configs = keys
        .into_iter()
        .zip(cert_keys)
        .map(|(key, certkey)| MonadConfig {
            transaction_validator: tvt.clone(),
            key,
            certkey,
            val_set_update_interval,
            epoch_start_delay,
            beneficiary: EthAddress::default(),
            validators: validator_mapping
                .map
                .iter()
                .map(|(node_id, sctpubkey)| (node_id.pubkey(), Stake(1), *sctpubkey))
                .collect::<Vec<_>>(),
            consensus_config: ConsensusConfig {
                proposal_txn_limit,
                proposal_gas_limit: 30_000_000,
                state_root_delay: SeqNum(state_root_delay),
                propose_with_missing_blocks: false,
                delta,
            },
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

pub fn create_and_run_nodes<S, R, T>(
    tvt: S::TransactionValidator,
    router_scheduler_config: R,
    logger_config: S::LoggerConfig,
    pipeline: S::Pipeline,
    terminator: T,
    swarm_config: SwarmTestConfig,
) -> Duration
where
    S: SwarmRelation,

    MockExecutor<S>: Unpin,
    Node<S>: Send,
    R: Fn(
        Vec<NodeId<CertificateSignaturePubKey<S::SignatureType>>>,
        NodeId<CertificateSignaturePubKey<S::SignatureType>>,
    ) -> S::RouterSchedulerConfig,
    T: NodesTerminator<S>,
{
    let (peers, state_configs) =
        get_configs::<S::SignatureType, S::SignatureCollectionType, S::TransactionValidator>(
            tvt,
            swarm_config.num_nodes,
            swarm_config.consensus_delta,
            swarm_config.state_root_delay,
            swarm_config.proposal_size,
            swarm_config.val_set_update_interval,
            swarm_config.epoch_start_delay,
        );
    run_nodes_until::<S, _, _>(
        peers,
        state_configs,
        router_scheduler_config,
        logger_config,
        pipeline,
        swarm_config.parallelize,
        terminator,
        swarm_config.expected_block,
        swarm_config.seed,
    )
}

pub fn run_nodes_until<S, R, T>(
    pubkeys: Vec<CertificateSignaturePubKey<S::SignatureType>>,
    state_configs: Vec<
        MonadConfig<S::SignatureType, S::SignatureCollectionType, S::TransactionValidator>,
    >,
    router_scheduler_config: R,
    logger_config: S::LoggerConfig,
    pipeline: S::Pipeline,
    parallelize: bool,

    terminator: T,
    min_ledger_len: usize,
    seed: u64,
) -> Duration
where
    S: SwarmRelation,

    MockExecutor<S>: Unpin,
    Node<S>: Send,
    R: Fn(
        Vec<NodeId<CertificateSignaturePubKey<S::SignatureType>>>,
        NodeId<CertificateSignaturePubKey<S::SignatureType>>,
    ) -> S::RouterSchedulerConfig,
    T: NodesTerminator<S>,
{
    let mut nodes = Nodes::<S>::new(
        pubkeys
            .iter()
            .copied()
            .zip(state_configs)
            .map(|(pubkey, state_config)| {
                (
                    ID::new(NodeId::new(pubkey)),
                    state_config,
                    logger_config.clone(),
                    router_scheduler_config(
                        pubkeys.iter().copied().map(NodeId::new).collect(),
                        NodeId::new(pubkey),
                    ),
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
