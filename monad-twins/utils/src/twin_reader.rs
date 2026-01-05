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

use core::fmt;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    fs,
    marker::PhantomData,
    time::Duration,
};

use alloy_primitives::U256;
use itertools::{izip, Itertools};
use monad_chain_config::{
    revision::{ChainParams, ChainRevision, MockChainRevision},
    ChainConfig, MockChainConfig,
};
use monad_consensus_state::ConsensusConfig;
use monad_consensus_types::{
    block::BlockPolicy,
    block_validator::BlockValidator,
    validator_data::{ValidatorSetData, ValidatorSetDataWithEpoch},
};
use monad_crypto::{
    certificate_signature::{
        CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
        CertificateSignatureRecoverable,
    },
    hasher::{Hasher, HasherType},
};
use monad_mock_swarm::{swarm_relation::SwarmRelation, terminator::ProgressTerminator};
use monad_state::{Forkpoint, MonadStateBuilder};
use monad_state_backend::{InMemoryState, InMemoryStateInner, StateBackend};
use monad_testutil::validators::complete_keys_w_validators;
use monad_transformer::ID;
use monad_types::{ExecutionProtocol, NodeId, Round, SeqNum};
use monad_validator::{
    leader_election::LeaderElection,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    validator_set::{ValidatorSetFactory, ValidatorSetType, ValidatorSetTypeFactory},
};
use serde::Deserialize;

// following paramters don't matter too much for twins thus kept as constant
pub const TWINS_STATE_ROOT_DELAY: u64 = u32::MAX as u64;
const TWINS_DEFAULT_IDENTIFIER: usize = 1;
const TWINS_DUP_IDENTIFIER: usize = TWINS_DEFAULT_IDENTIFIER + 1;

#[derive(Debug, Deserialize)]
struct TwinsTestCaseRaw {
    // test description
    description: String,
    // vector of nodes with their name (must be unique)
    nodes: BTreeSet<String>,
    // array representing twins, the format must start with a name of honest nodes, follow by _, and at least 1 char after ward
    twins: BTreeSet<String>,
    // expected amount of blocks for honest nodes if not provided by mapping
    expected_block_default: usize,
    // when to timeout
    timeout_ms: u64,
    // delta of protocol
    delta_ms: u64,
    // round partition setting
    partition: Vec<Vec<Vec<String>>>,
    // what's the behaviour of partition outside of defined
    default_partition: Vec<Vec<String>>,

    /// optional flag
    // if the test should allow block-sync
    allow_block_sync: Option<bool>,
    // if liveness should be tested
    liveness: Option<usize>,
    // expected amount of blocks to be observed on particular honest node if diff from default
    expected_block: Option<BTreeMap<String, usize>>,
}

pub struct FullTwinsNodeConfig<ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT, CCT, CRT>,
    SBT: StateBackend<ST, SCT>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BVT: BlockValidator<ST, SCT, EPT, BPT, SBT, CCT, CRT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    id: ID<CertificateSignaturePubKey<ST>>,
    state_config: MonadStateBuilder<ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT>,
    partition: BTreeMap<Round, Vec<ID<CertificateSignaturePubKey<ST>>>>,
    default_partition: Vec<ID<CertificateSignaturePubKey<ST>>>,

    // some redundant info in case its useful for future
    key_secret: [u8; 32],
    certkey_secret: [u8; 32],
    name: String,
    expected_block: usize,
    is_honest: bool,
}

impl<ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT> Clone
    for FullTwinsNodeConfig<ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT, CCT, CRT> + Clone,
    SBT: StateBackend<ST, SCT> + Clone,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Clone,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Clone,
    BVT: BlockValidator<ST, SCT, EPT, BPT, SBT, CCT, CRT> + Clone,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            state_config: MonadStateBuilder {
                validator_set_factory: self.state_config.validator_set_factory.clone(),
                leader_election: self.state_config.leader_election.clone(),
                block_validator: self.state_config.block_validator.clone(),
                block_policy: self.state_config.block_policy.clone(),
                state_backend: self.state_config.state_backend.clone(),

                forkpoint: self.state_config.forkpoint.clone(),
                locked_epoch_validators: self.state_config.locked_epoch_validators.clone(),
                key: CertificateKeyPair::from_bytes(&mut self.key_secret.clone()).unwrap(),
                certkey: SignatureCollectionKeyPairType::<SCT>::from_bytes(
                    &mut self.certkey_secret.clone(),
                )
                .unwrap(),

                beneficiary: self.state_config.beneficiary,
                block_sync_override_peers: self.state_config.block_sync_override_peers.clone(),
                maybe_blocksync_rng_seed: self.state_config.maybe_blocksync_rng_seed,

                consensus_config: self.state_config.consensus_config,

                whitelisted_statesync_nodes: Default::default(),
                statesync_expand_to_group: true,

                _phantom: PhantomData,
            },
            partition: self.partition.clone(),
            default_partition: self.default_partition.clone(),

            key_secret: self.key_secret,
            certkey_secret: self.certkey_secret,
            name: self.name.clone(),
            expected_block: self.expected_block,
            is_honest: self.is_honest,
        }
    }
}

impl<ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT> Debug
    for FullTwinsNodeConfig<ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT, CCT, CRT>,
    SBT: StateBackend<ST, SCT>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BVT: BlockValidator<ST, SCT, EPT, BPT, SBT, CCT, CRT>,
    CCT: ChainConfig<CRT> + Debug,
    CRT: ChainRevision + Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{:?} \n partition: {:?}, \n default_part: {:?} \n",
            self.name, self.partition, self.default_partition
        )
    }
}

pub struct TwinsNodeConfig<ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT, CCT, CRT>,
    SBT: StateBackend<ST, SCT>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BVT: BlockValidator<ST, SCT, EPT, BPT, SBT, CCT, CRT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    pub id: ID<CertificateSignaturePubKey<ST>>,
    pub state_config: MonadStateBuilder<ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT>,
    pub partition: BTreeMap<Round, Vec<ID<CertificateSignaturePubKey<ST>>>>,
    pub default_partition: Vec<ID<CertificateSignaturePubKey<ST>>>,
}

impl<ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT>
    From<FullTwinsNodeConfig<ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT>>
    for TwinsNodeConfig<ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT, CCT, CRT>,
    SBT: StateBackend<ST, SCT>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BVT: BlockValidator<ST, SCT, EPT, BPT, SBT, CCT, CRT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    fn from(value: FullTwinsNodeConfig<ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT>) -> Self {
        let FullTwinsNodeConfig {
            id,
            state_config,
            default_partition,
            partition,
            ..
        } = value;

        Self {
            id,
            state_config,
            default_partition,
            partition,
        }
    }
}

pub struct TwinsTestCase<S>
where
    S: SwarmRelation,
{
    pub description: String,
    pub terminator: ProgressTerminator<CertificateSignaturePubKey<S::SignatureType>>,
    pub delta: u64,
    pub allow_block_sync: bool,
    pub liveness: Option<usize>,
    pub duplicates: BTreeMap<NodeId<CertificateSignaturePubKey<S::SignatureType>>, Vec<usize>>,
    pub nodes: BTreeMap<
        ID<CertificateSignaturePubKey<S::SignatureType>>,
        TwinsNodeConfig<
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
    >,
}

static CHAIN_PARAMS: ChainParams = ChainParams {
    tx_limit: 10_000,
    proposal_gas_limit: 300_000_000,
    proposal_byte_limit: 4_000_000,
    max_reserve_balance: 1_000_000_000_000_000_000,
    vote_pace: Duration::from_millis(5),
};

pub fn read_twins_test<S>(path: &str) -> TwinsTestCase<S>
where
    S: SwarmRelation<
        StateBackendType = InMemoryState<
            <S as SwarmRelation>::SignatureType,
            <S as SwarmRelation>::SignatureCollectionType,
        >,
        ChainConfigType = MockChainConfig,
        ChainRevisionType = MockChainRevision,
    >,
    S::ValidatorSetTypeFactory: Default + Clone,
    S::LeaderElection: Default + Clone,
    S::BlockValidator: Default + Clone,
    S::BlockPolicyType: Default + Clone,
{
    let raw_str = fs::read_to_string(path).expect("unable to read file in twins testing");

    let TwinsTestCaseRaw {
        description,
        nodes: mut names,
        twins,
        expected_block_default,
        expected_block,
        timeout_ms,
        delta_ms,
        allow_block_sync,
        liveness,
        partition,
        default_partition,
    } = serde_json::from_str(&raw_str).expect("twins test case JSON is not formatted correctly");

    let expected_block = expected_block.unwrap_or(BTreeMap::new());
    let mut seeds = (0_u32..).map(|i| {
        let mut h = HasherType::new();
        h.update(i.to_le_bytes());
        h.hash().0
    });
    let key_secrets = seeds.by_ref().take(names.len()).collect_vec();
    let certkey_secrets = seeds.by_ref().take(names.len()).collect_vec();

    let keys = key_secrets
        .iter()
        .copied()
        .map(|mut keypair| CertificateKeyPair::from_bytes(&mut keypair))
        .collect::<Result<Vec<_>, _>>()
        .expect("secp secret invalid");
    let certkeys: Vec<_> = certkey_secrets
        .iter()
        .copied()
        .map(|mut certkey| CertificateKeyPair::from_bytes(&mut certkey))
        .collect::<Result<Vec<_>, _>>()
        .expect("secret is invalid when convert to cert-key");

    let (validators, validator_mapping) = complete_keys_w_validators::<
        S::SignatureType,
        S::SignatureCollectionType,
        _,
    >(&keys, &certkeys, ValidatorSetFactory::default());

    let validator_data = ValidatorSetData::<S::SignatureCollectionType>::new(
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

    let state_configs: Vec<_> = keys
        .into_iter()
        .zip(certkeys)
        .map(|(key, certkey)| MonadStateBuilder::<
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
        > {
            validator_set_factory: S::ValidatorSetTypeFactory::default(),
            leader_election: S::LeaderElection::default(),
            block_validator: S::BlockValidator::default(),
            block_policy: S::BlockPolicyType::default(),
            state_backend: InMemoryStateInner::genesis(
                U256::MAX,
                monad_types::SeqNum(TWINS_STATE_ROOT_DELAY),
            ),
            forkpoint: forkpoint.clone(),
            locked_epoch_validators: locked_epoch_validators.clone(),

            key,
            certkey,

            beneficiary: Default::default(),
            block_sync_override_peers: Default::default(),
            maybe_blocksync_rng_seed: Some(123456),

            consensus_config: ConsensusConfig {
                execution_delay: SeqNum(TWINS_STATE_ROOT_DELAY),
                delta: Duration::from_millis(delta_ms),
                statesync_to_live_threshold: SeqNum(600),
                live_to_statesync_threshold: SeqNum(900),
                start_execution_threshold: SeqNum(300),
                chain_config: MockChainConfig::new(&CHAIN_PARAMS),
                timestamp_latency_estimate_ns: 10_000_000,
                _phantom: PhantomData,
            },

            whitelisted_statesync_nodes: Default::default(),
            statesync_expand_to_group: true,

            _phantom: PhantomData,
        })
        .collect();

    let mut nodes: BTreeMap<String, _> = BTreeMap::new();
    let mut duplicates = BTreeMap::new();

    for (name, key_secret, certkey_secret, state_config) in
        izip!(names.iter(), key_secrets, certkey_secrets, state_configs)
    {
        let key = <S::SignatureType as CertificateSignature>::KeyPairType::from_bytes(
            &mut key_secret.clone(),
        )
        .unwrap();
        let pid = NodeId::new(key.pubkey());
        let id = ID::new(pid).as_non_unique(TWINS_DEFAULT_IDENTIFIER);
        let expected_block = *expected_block.get(name).unwrap_or(&expected_block_default);
        nodes.insert(
            name.clone(),
            FullTwinsNodeConfig {
                id,
                name: name.to_string(),
                state_config,
                key_secret,
                certkey_secret,
                partition: BTreeMap::new(),
                default_partition: vec![],
                is_honest: true,
                expected_block,
            },
        );
        duplicates.insert(pid, vec![TWINS_DEFAULT_IDENTIFIER]);
    }

    // make twins that mimic original
    for (idx, name) in twins.iter().enumerate() {
        let parts: Vec<&str> = name.split("_").collect::<Vec<_>>();
        // format check
        assert_eq!(parts.len(), 2);

        let original = nodes
            .get_mut(parts[0])
            .expect("mimic target doesn't exists when reading test case");
        original.is_honest = false;
        let mut twin = original.clone();
        let identifier = TWINS_DUP_IDENTIFIER + idx;
        twin.id = twin.id.as_non_unique(identifier);
        twin.state_config.state_backend =
            InMemoryStateInner::genesis(U256::MAX, monad_types::SeqNum(TWINS_STATE_ROOT_DELAY));
        duplicates
            .get_mut(twin.id.get_peer_id())
            .expect("mimic target doesn't exists when reading test case")
            .push(identifier);

        nodes.insert(name.clone(), twin);
    }

    names.extend(twins);

    // construct Terminator
    let terminator = ProgressTerminator::new(
        nodes
            .values()
            .filter_map(|config| {
                if config.is_honest {
                    Some((config.id, config.expected_block))
                } else {
                    None
                }
            })
            .collect::<BTreeMap<_, _>>(),
        Duration::from_millis(timeout_ms),
    );

    // used for format check
    let mut round_nodes = BTreeSet::new();
    // insert partitions
    for (r, partition_round) in partition.into_iter().enumerate() {
        let round: Round = Round((r as u64) + 1);

        // in a given round, iterate through all ways of partitioning stuff.
        for partition in partition_round {
            let transformed_partition = partition
                .iter()
                .map(|name| {
                    nodes
                        .get(name)
                        .expect("partition target doesn't exists when reading test case")
                        .id
                })
                .collect::<Vec<_>>();
            for node in partition {
                nodes
                    .get_mut(&node)
                    .expect("partition target doesn't exists when reading test case")
                    .partition
                    .insert(round, transformed_partition.clone());
                assert!(!round_nodes.contains(&node));
                round_nodes.insert(node);
            }
        }
        // format check
        assert_eq!(names, round_nodes);
        round_nodes.clear();
    }

    // insert partition_default
    for partition in default_partition {
        let transformed_partition = partition
            .iter()
            .map(|name| {
                nodes
                    .get(name)
                    .expect("partition target doesn't exists when reading test case")
                    .id
            })
            .collect::<Vec<_>>();
        for node in partition {
            nodes
                .get_mut(&node)
                .expect("partition target doesn't exists when reading test case")
                .default_partition = transformed_partition.clone();
            assert!(!round_nodes.contains(&node));
            round_nodes.insert(node);
        }
    }
    // format check
    assert_eq!(names, round_nodes);

    TwinsTestCase {
        description,
        // optional flags
        allow_block_sync: allow_block_sync.unwrap_or(true),
        liveness,
        // mandatory params
        terminator,
        delta: delta_ms,
        duplicates,
        // name is not useful outside of extractor, so swap name with id in future usage
        nodes: nodes
            .into_values()
            .map(|v| (v.id, v.into()))
            .collect::<BTreeMap<_, _>>(),
    }
}
