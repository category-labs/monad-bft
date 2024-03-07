use std::time::Duration;

use monad_consensus_state::ConsensusConfig;
use monad_consensus_types::{block::BlockType, validator_data::ValidatorData};
use monad_eth_types::EthAddress;
use monad_mock_swarm::{mock_swarm::Nodes, swarm_relation::SwarmRelation};
use monad_state::{MonadStateBuilder, MonadVersion};
use monad_types::{Round, SeqNum, Stake};
use monad_validator::validator_set::ValidatorSetType;

use crate::validators::create_keys_w_validators;

pub fn make_state_configs<S: SwarmRelation>(
    num_nodes: u16,

    validator_set_factory: impl Fn() -> S::ValidatorSetTypeFactory,
    leader_election: impl Fn() -> S::LeaderElection,
    transaction_pool: impl Fn() -> S::TxPool,
    block_validator: impl Fn() -> S::BlockValidator,
    state_root_validator: impl Fn() -> S::StateRootValidator,
    async_state_verify: impl Fn(fn(Stake) -> Stake) -> S::AsyncStateRootVerify,

    delta: Duration,
    proposal_txn_limit: usize,
    val_set_update_interval: SeqNum,
    epoch_start_delay: Round,
    state_root_quorum_threshold: fn(Stake) -> Stake,
    max_blocksync_retries: usize,
) -> Vec<
    MonadStateBuilder<
        S::SignatureType,
        S::SignatureCollectionType,
        S::ValidatorSetTypeFactory,
        S::LeaderElection,
        S::TxPool,
        S::BlockValidator,
        S::StateRootValidator,
        S::AsyncStateRootVerify,
    >,
> {
    let (keys, cert_keys, validators, validator_mapping) =
        create_keys_w_validators::<S::SignatureType, S::SignatureCollectionType, _>(
            num_nodes as u32,
            validator_set_factory(),
        );

    let validator_data = ValidatorData::new(
        validator_mapping
            .map
            .iter()
            .map(|(node_id, sctpubkey)| {
                (
                    node_id.pubkey(),
                    *validators.get_members().get(node_id).unwrap(),
                    *sctpubkey,
                )
            })
            .collect(),
    );

    keys.into_iter()
        .zip(cert_keys)
        .map(|(key, certkey)| MonadStateBuilder {
            version: MonadVersion::new("MOCK_SWARM"),
            validator_set_factory: validator_set_factory(),
            leader_election: leader_election(),
            transaction_pool: transaction_pool(),
            block_validator: block_validator(),
            state_root_validator: state_root_validator(),
            async_state_verify: async_state_verify(state_root_quorum_threshold),
            validators: validator_data.clone(),

            key,
            certkey,

            val_set_update_interval,
            epoch_start_delay,
            beneficiary: EthAddress::default(),

            consensus_config: ConsensusConfig {
                proposal_txn_limit,
                proposal_gas_limit: 30_000_000,
                propose_with_missing_blocks: false,
                delta,
                max_blocksync_retries,
            },
        })
        .collect()
}

pub fn swarm_ledger_verification<S: SwarmRelation>(swarm: &Nodes<S>, min_ledger_len: usize) {
    let ledgers: Vec<Vec<_>> = swarm
        .states()
        .values()
        .map(|node| node.executor.ledger().get_blocks().clone())
        .collect();
    ledger_verification(&ledgers, min_ledger_len)
}

pub fn ledger_verification<O: BlockType + PartialEq>(ledgers: &Vec<Vec<O>>, min_ledger_len: usize) {
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
        assert!(
            max_b - ledger.len() <= 5, // this 5 block bound is arbitrary... is there a better way to do this?
            "max_b={}, ledger.len()={}",
            max_b,
            ledger.len()
        );
    }
}
