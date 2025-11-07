// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{collections::BTreeMap, marker::PhantomData, time::Duration};

use monad_consensus_state::ConsensusConfig;
use monad_consensus_types::{
    block::ConsensusFullBlock,
    validator_data::{ValidatorSetData, ValidatorSetDataWithEpoch},
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_mock_swarm::{mock_swarm::Nodes, swarm_relation::SwarmRelation};
use monad_state::{Forkpoint, MonadStateBuilder};
use monad_types::{ExecutionProtocol, SeqNum};
use monad_updaters::ledger::MockableLedger;
use monad_validator::{signature_collection::SignatureCollection, validator_set::ValidatorSetType};

use crate::validators::create_keys_w_validators;

pub fn make_state_configs<S: SwarmRelation>(
    num_nodes: u16,

    validator_set_factory: impl Fn() -> S::ValidatorSetTypeFactory,
    leader_election: impl Fn() -> S::LeaderElection,
    block_validator: impl Fn() -> S::BlockValidator,
    block_policy: impl Fn() -> S::BlockPolicyType,
    state_backend: impl Fn() -> S::StateBackendType,

    execution_delay: SeqNum,
    delta: Duration,
    chain_config: S::ChainConfigType,
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
        S::BlockValidator,
        S::ChainConfigType,
        S::ChainRevisionType,
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

    let forkpoint = Forkpoint::genesis();
    let locked_epoch_validators: Vec<_> = forkpoint
        .validator_sets
        .iter()
        .map(|locked_epoch| ValidatorSetDataWithEpoch {
            epoch: locked_epoch.epoch,
            validators: validator_data.clone(),
        })
        .collect();

    keys.into_iter()
        .zip(cert_keys)
        .map(|(key, certkey)| MonadStateBuilder {
            validator_set_factory: validator_set_factory(),
            leader_election: leader_election(),
            block_validator: block_validator(),
            block_policy: block_policy(),
            state_backend: state_backend(),
            forkpoint: forkpoint.clone(),
            locked_epoch_validators: locked_epoch_validators.clone(),

            key,
            certkey,

            beneficiary: Default::default(),
            block_sync_override_peers: Default::default(),

            consensus_config: ConsensusConfig {
                execution_delay,
                delta,
                // StateSync -> Live transition happens here
                statesync_to_live_threshold: statesync_threshold,
                // Live -> StateSync transition happens here
                live_to_statesync_threshold: SeqNum(statesync_threshold.0 * 3 / 2),
                // Live starts execution here
                start_execution_threshold: SeqNum(statesync_threshold.0 / 2),
                chain_config,
                timestamp_latency_estimate_ns: 10_000_000,

                _phantom: PhantomData,
            },

            whitelisted_statesync_nodes: Default::default(),
            statesync_expand_to_group: true,

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
