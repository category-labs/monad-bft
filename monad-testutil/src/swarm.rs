use std::{collections::BTreeMap, marker::PhantomData, time::Duration};

use monad_consensus_state::ConsensusConfig;
use monad_consensus_types::{
    block::{ConsensusFullBlock, ExecutionProtocol},
    signature_collection::SignatureCollection,
    validator_data::ValidatorSetData,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::EthAddress;
use monad_mock_swarm::{mock_swarm::Nodes, swarm_relation::SwarmRelation};
use monad_state::{Forkpoint, MonadStateBuilder};
use monad_types::{Round, SeqNum};
use monad_updaters::ledger::MockableLedger;
use monad_validator::validator_set::ValidatorSetType;

use crate::validators::create_keys_w_validators;

pub fn make_state_configs<S: SwarmRelation>(
    num_nodes: u16,

    validator_set_factory: impl Fn() -> S::ValidatorSetTypeFactory,
    leader_election: impl Fn() -> S::LeaderElection,
    transaction_pool: impl Fn() -> S::TxPool,
    block_validator: impl Fn() -> S::BlockValidator,
    block_policy: impl Fn() -> S::BlockPolicyType,
    state_backend: impl Fn() -> S::StateBackendType,

    execution_delay: SeqNum,
    delta: Duration,
    vote_pace: Duration,
    proposal_txn_limit: usize,
    val_set_update_interval: SeqNum,
    epoch_start_delay: Round,
    statesync_threshold: SeqNum,
) -> Vec<
    MonadStateBuilder<
        S::SignatureType,
        S::SignatureCollectionType,
        S::ExecutionProtocolType,
        S::BlockPolicyType,
        S::StateBackendType,
        S::ValidatorSetTypeFactory,
        S::LeaderElection,
        S::TxPool,
        S::BlockValidator,
    >,
> {
    let (keys, cert_keys, validators, validator_mapping) =
        create_keys_w_validators::<S::SignatureType, S::SignatureCollectionType, _>(
            num_nodes as u32,
            validator_set_factory(),
        );

    let validator_data = ValidatorSetData::new(
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
            validator_set_factory: validator_set_factory(),
            leader_election: leader_election(),
            transaction_pool: transaction_pool(),
            block_validator: block_validator(),
            block_policy: block_policy(),
            state_backend: state_backend(),
            forkpoint: Forkpoint::genesis(validator_data.clone()),

            key,
            certkey,

            val_set_update_interval,
            epoch_start_delay,
            beneficiary: EthAddress::default(),

            consensus_config: ConsensusConfig {
                execution_delay,
                proposal_txn_limit,
                delta,
                // StateSync -> Live transition happens here
                statesync_to_live_threshold: statesync_threshold,
                // Live -> StateSync transition happens here
                live_to_statesync_threshold: SeqNum(statesync_threshold.0 * 3 / 2),
                // Live starts execution here
                start_execution_threshold: SeqNum(statesync_threshold.0 / 2),
                vote_pace,
                timestamp_latency_estimate_ns: 10,
            },

            _phantom: PhantomData,
        })
        .collect()
}

pub fn swarm_ledger_verification<S: SwarmRelation>(swarm: &Nodes<S>, min_ledger_len: usize) {
    let ledgers: Vec<_> = swarm
        .states()
        .values()
        .map(|node| node.executor.ledger().get_finalized_blocks().clone())
        .collect();
    ledger_verification(&ledgers, min_ledger_len)
}

pub fn ledger_verification<ST, SCT, EPT>(
    ledgers: &Vec<BTreeMap<SeqNum, ConsensusFullBlock<ST, SCT, EPT>>>,
    min_ledger_len: usize,
) where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    let (max_ledger_idx, max_b) = ledgers
        .iter()
        .map(BTreeMap::len)
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
