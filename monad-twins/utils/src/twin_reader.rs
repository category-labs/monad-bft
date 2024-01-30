use core::fmt;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    fs,
    time::Duration,
};

use itertools::{izip, Itertools};
use monad_async_state_verify::AsyncStateVerifyProcess;
use monad_consensus_state::ConsensusConfig;
use monad_consensus_types::{
    block_validator::BlockValidator,
    payload::{StateRoot, StateRootValidator},
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    txpool::TxPool,
    validator_data::ValidatorData,
};
use monad_crypto::{
    certificate_signature::{
        CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
        CertificateSignatureRecoverable,
    },
    hasher::{Hasher, HasherType},
};
use monad_eth_types::EthAddress;
use monad_mock_swarm::{swarm_relation::SwarmRelation, terminator::ProgressTerminator};
use monad_state::MonadStateBuilder;
use monad_testutil::validators::complete_keys_w_validators;
use monad_transformer::ID;
use monad_types::{NodeId, Round, SeqNum};
use monad_validator::{
    leader_election::LeaderElection,
    validator_set::{ValidatorSetFactory, ValidatorSetType, ValidatorSetTypeFactory},
};
use serde::Deserialize;

// following paramters don't matter too much for twins thus kept as constant
pub const TWINS_STATE_ROOT_DELAY: u64 = u64::MAX;
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

pub struct FullTwinsNodeConfig<ST, SCT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    TT: TxPool,
    BVT: BlockValidator,
    SVT: StateRootValidator,
    ASVT: AsyncStateVerifyProcess<
        SignatureCollectionType = SCT,
        ValidatorSetType = VTF::ValidatorSetType,
    >,
{
    id: ID<CertificateSignaturePubKey<ST>>,
    state_config: MonadStateBuilder<ST, SCT, VTF, LT, TT, BVT, SVT, ASVT>,
    partition: BTreeMap<Round, Vec<ID<CertificateSignaturePubKey<ST>>>>,
    default_partition: Vec<ID<CertificateSignaturePubKey<ST>>>,

    // some redundant info in case its useful for future
    key_secret: [u8; 32],
    certkey_secret: [u8; 32],
    name: String,
    expected_block: usize,
    is_honest: bool,
}

impl<ST, SCT, VTF, LT, TT, BVT, SVT, ASVT> Clone
    for FullTwinsNodeConfig<ST, SCT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Clone,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Clone,
    TT: TxPool + Default,
    BVT: BlockValidator + Clone,
    SVT: StateRootValidator + Clone,
    ASVT: AsyncStateVerifyProcess<
            SignatureCollectionType = SCT,
            ValidatorSetType = VTF::ValidatorSetType,
        > + Clone,
{
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            state_config: MonadStateBuilder {
                validator_set_factory: self.state_config.validator_set_factory.clone(),
                leader_election: self.state_config.leader_election.clone(),
                transaction_pool: TT::default(),
                block_validator: self.state_config.block_validator.clone(),
                state_root_validator: self.state_config.state_root_validator.clone(),
                async_state_verify: self.state_config.async_state_verify.clone(),

                validators: self.state_config.validators.clone(),
                key: CertificateKeyPair::from_bytes(&mut self.key_secret.clone()).unwrap(),
                certkey: SignatureCollectionKeyPairType::<SCT>::from_bytes(
                    &mut self.certkey_secret.clone(),
                )
                .unwrap(),

                val_set_update_interval: self.state_config.val_set_update_interval,
                epoch_start_delay: self.state_config.epoch_start_delay,
                beneficiary: self.state_config.beneficiary,

                consensus_config: self.state_config.consensus_config,
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

impl<ST, SCT, VTF, LT, TT, BVT, SVT, ASVT> Debug
    for FullTwinsNodeConfig<ST, SCT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    TT: TxPool,
    BVT: BlockValidator,
    SVT: StateRootValidator,
    ASVT: AsyncStateVerifyProcess<
        SignatureCollectionType = SCT,
        ValidatorSetType = VTF::ValidatorSetType,
    >,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{:?} \n partition: {:?}, \n default_part: {:?} \n",
            self.name, self.partition, self.default_partition
        )
    }
}

pub struct TwinsNodeConfig<ST, SCT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    TT: TxPool,
    BVT: BlockValidator,
    SVT: StateRootValidator,
    ASVT: AsyncStateVerifyProcess<
        SignatureCollectionType = SCT,
        ValidatorSetType = VTF::ValidatorSetType,
    >,
{
    pub id: ID<CertificateSignaturePubKey<ST>>,
    pub state_config: MonadStateBuilder<ST, SCT, VTF, LT, TT, BVT, SVT, ASVT>,
    pub partition: BTreeMap<Round, Vec<ID<CertificateSignaturePubKey<ST>>>>,
    pub default_partition: Vec<ID<CertificateSignaturePubKey<ST>>>,
}

impl<ST, SCT, VTF, LT, TT, BVT, SVT, ASVT>
    From<FullTwinsNodeConfig<ST, SCT, VTF, LT, TT, BVT, SVT, ASVT>>
    for TwinsNodeConfig<ST, SCT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    TT: TxPool,
    BVT: BlockValidator,
    SVT: StateRootValidator,
    ASVT: AsyncStateVerifyProcess<
        SignatureCollectionType = SCT,
        ValidatorSetType = VTF::ValidatorSetType,
    >,
{
    fn from(value: FullTwinsNodeConfig<ST, SCT, VTF, LT, TT, BVT, SVT, ASVT>) -> Self {
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
            S::ValidatorSetTypeFactory,
            S::LeaderElection,
            S::TxPool,
            S::BlockValidator,
            S::StateRootValidator,
            S::AsyncStateRootVerify,
        >,
    >,
}

pub fn read_twins_test<S>(path: &str) -> TwinsTestCase<S>
where
    S: SwarmRelation<StateRootValidator = StateRoot>,
    S::ValidatorSetTypeFactory: Default + Clone,
    S::LeaderElection: Default + Clone,
    S::TxPool: Default,
    S::BlockValidator: Default + Clone,
    S::AsyncStateRootVerify: Default + Clone,
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

    let validator_data = ValidatorData::<S::SignatureCollectionType>::new(
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

    let state_configs: Vec<_> = keys
        .into_iter()
        .zip(certkeys)
        .map(|(key, certkey)| MonadStateBuilder::<
            S::SignatureType,
            S::SignatureCollectionType,
            S::ValidatorSetTypeFactory,
            S::LeaderElection,
            S::TxPool,
            S::BlockValidator,
            S::StateRootValidator,
            S::AsyncStateRootVerify,
        > {
            validator_set_factory: S::ValidatorSetTypeFactory::default(),
            leader_election: S::LeaderElection::default(),
            transaction_pool: S::TxPool::default(),
            block_validator: S::BlockValidator::default(),
            state_root_validator: StateRoot::new(monad_types::SeqNum(TWINS_STATE_ROOT_DELAY)),
            async_state_verify: S::AsyncStateRootVerify::default(),
            validators: validator_data.clone(),

            key,
            certkey,

            val_set_update_interval: SeqNum(2000),
            epoch_start_delay: Round(50),
            beneficiary: EthAddress::default(),

            consensus_config: ConsensusConfig {
                proposal_txn_limit: 10,
                proposal_gas_limit: 30_000_000,
                propose_with_missing_blocks: false,
                delta: Duration::from_millis(delta_ms),
            },
        })
        .collect();

    let mut nodes = BTreeMap::new();
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
                name: name.clone(),
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
                assert!(round_nodes.get(&node).is_none());
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
            assert!(round_nodes.get(&node).is_none());
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
