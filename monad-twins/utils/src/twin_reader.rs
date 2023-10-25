use core::fmt;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    fs,
    time::Duration,
};

use itertools::izip;
use monad_consensus_types::{
    certificate_signature::CertificateKeyPair, signature_collection::SignatureCollection,
    transaction_validator::TransactionValidator,
};
use monad_executor_glue::PeerId;
use monad_mock_swarm::{
    mock_swarm::ProgressTerminator, swarm_relation::SwarmRelation, transformer::ID,
};
use monad_state::MonadConfig;
use monad_testutil::{
    signing::{create_keys, create_seed_for_certificate_keys, get_certificate_key_secret},
    swarm::complete_config,
    validators::complete_keys_w_validators,
};
use monad_types::Round;
use serde::Deserialize;

// following paramters don't matter too much for twins thus kept as constant
const TWINS_STATE_ROOT_DELAY: u64 = u64::MAX;
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

pub struct FullTwinsNodeConfig<SCT, TVT>
where
    SCT: SignatureCollection,
    TVT: TransactionValidator,
{
    id: ID,
    state_config: MonadConfig<SCT, TVT>,
    partition: BTreeMap<Round, Vec<ID>>,
    default_partition: Vec<ID>,

    // some redundant info in case its useful for future
    secret: [u8; 32],
    name: String,
    expected_block: usize,
    is_honest: bool,
}

impl<SCT, TVT> Clone for FullTwinsNodeConfig<SCT, TVT>
where
    SCT: SignatureCollection,
    TVT: TransactionValidator,
{
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            secret: self.secret,
            state_config: self.state_config.dup(self.secret),
            partition: self.partition.clone(),
            default_partition: self.default_partition.clone(),
            expected_block: self.expected_block,
            is_honest: self.is_honest,
            name: self.name.clone(),
        }
    }
}
impl<SCT, TVT> Debug for FullTwinsNodeConfig<SCT, TVT>
where
    SCT: SignatureCollection,
    TVT: TransactionValidator,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{:?} \n partition: {:?}, \n default_part: {:?} \n",
            self.name, self.partition, self.default_partition
        )
    }
}

pub struct TwinsNodeConfig<SCT, TVT>
where
    SCT: SignatureCollection,
    TVT: TransactionValidator,
{
    pub id: ID,
    pub state_config: MonadConfig<SCT, TVT>,
    pub partition: BTreeMap<Round, Vec<ID>>,
    pub default_partition: Vec<ID>,
}

impl<SCT, TVT> From<FullTwinsNodeConfig<SCT, TVT>> for TwinsNodeConfig<SCT, TVT>
where
    SCT: SignatureCollection,
    TVT: TransactionValidator,
{
    fn from(value: FullTwinsNodeConfig<SCT, TVT>) -> Self {
        let FullTwinsNodeConfig {
            id,
            state_config,
            default_partition,
            partition,

            //extra info
            is_honest: _,
            expected_block: _,
            name: _,
            secret: _,
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
    pub terminator: ProgressTerminator,
    pub delta: u64,
    pub allow_block_sync: bool,
    pub liveness: Option<usize>,
    pub duplicates: BTreeMap<PeerId, Vec<usize>>,
    pub nodes: BTreeMap<ID, TwinsNodeConfig<S::SCT, S::TVT>>,
}

pub fn read_twins_test<S>(tvt: S::TVT, path: &str) -> TwinsTestCase<S>
where
    S: SwarmRelation,
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
    let keys = create_keys(names.len() as u32);
    let cert_key_secrete: Vec<_> = create_seed_for_certificate_keys::<S::SCT>(names.len() as u32)
        .into_iter()
        .map(get_certificate_key_secret)
        .collect();

    let cert_keys: Vec<_> = cert_key_secrete
        .iter()
        .map(|secret| {
            CertificateKeyPair::from_bytes(*secret)
                .expect("secret is invalid when convert to cert-key")
        })
        .collect();

    let (_, validator_mapping) = complete_keys_w_validators::<S::SCT>(&keys, &cert_keys);
    let (pubkeys, state_configs) = complete_config::<S::ST, S::SCT, _>(
        tvt,
        keys,
        cert_keys,
        validator_mapping,
        Duration::from_millis(delta_ms),
        TWINS_STATE_ROOT_DELAY,
    );

    let mut nodes = BTreeMap::new();
    let mut duplicates = BTreeMap::new();

    for (name, pubkey, secret, state_config) in
        izip!(names.iter(), pubkeys, cert_key_secrete, state_configs)
    {
        let pid = PeerId(pubkey);
        let id = ID::new(pid).as_non_unique(TWINS_DEFAULT_IDENTIFIER);
        let expected_block = *expected_block.get(name).unwrap_or(&expected_block_default);
        nodes.insert(
            name.clone(),
            FullTwinsNodeConfig {
                id,
                name: name.clone(),
                state_config,
                secret,
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
