use std::{ops::Deref, time::Duration};

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use monad_consensus::{
    messages::message::{ProposalMessage, TimeoutMessage, VoteMessage},
    validation::signing::Verified,
};
use monad_consensus_state::{
    command::ConsensusCommand, timestamp::BlockTimestamp, ConsensusConfig, ConsensusState,
    ConsensusStateWrapper,
};
use monad_consensus_types::{
    block::{BlockKind, BlockRange, FullBlock},
    checkpoint::RootInfo,
    metrics::Metrics,
    payload::{
        ExecutionProtocol, FullTransactionList, NopStateRoot, StateRootValidator,
        TransactionPayload,
    },
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    state_root_hash::StateRootHash,
    txpool::TxPool,
    voting::ValidatorMapping,
};
use monad_crypto::{
    certificate_signature::{
        CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable,
    },
    NopPubKey, NopSignature,
};
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_txpool::EthTxPool;
use monad_eth_types::EthAddress;
use monad_multi_sig::MultiSig;
use monad_state_backend::{InMemoryState, InMemoryStateInner};
use monad_testutil::{
    proposal::ProposalGen,
    signing::{create_certificate_keys, create_keys},
    validators::create_keys_w_validators,
};
use monad_types::{Epoch, NodeId, Round, RouterTarget, SeqNum, GENESIS_SEQ_NUM};
use monad_validator::{
    epoch_manager::EpochManager,
    leader_election::LeaderElection,
    simple_round_robin::SimpleRoundRobin,
    validator_set::{ValidatorSetFactory, ValidatorSetType, ValidatorSetTypeFactory},
    validators_epoch_mapping::ValidatorsEpochMapping,
};

const NUM_TRANSACTIONS: usize = 1000;
const TRANSACTION_SIZE_BYTES: usize = 400;

type NodeCtx = NodeContext<
    SignatureType,
    MultiSig<SignatureType>,
    ValidatorSetFactory<NopPubKey>,
    NopStateRoot,
    SimpleRoundRobin<NopPubKey>,
    EthTxPool<MultiSig<SignatureType>, InMemoryState>,
>;

type EnvCtx = EnvContext<
    NopSignature,
    MultiSig<NopSignature>,
    ValidatorSetFactory<NopPubKey>,
    SimpleRoundRobin<NopPubKey>,
>;

type BenchTuple = (
    FullTransactionList,
    EnvCtx,
    Vec<NodeCtx>,
    NodeId<NopPubKey>,
    ProposalMessage<MultiSig<NopSignature>>,
);

struct EnvContext<ST, SCT, VTF, LT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Clone,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    proposal_gen: ProposalGen<ST, SCT>,
    malicious_proposal_gen: ProposalGen<ST, SCT>,
    keys: Vec<ST::KeyPairType>,
    cert_keys: Vec<SignatureCollectionKeyPairType<SCT>>,
    epoch_manager: EpochManager,
    val_epoch_map: ValidatorsEpochMapping<VTF, SCT>,
    election: LT,
}

impl<ST, SCT, VTF, LT> EnvContext<ST, SCT, VTF, LT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Clone,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn next_proposal_empty(&mut self) -> Verified<ST, ProposalMessage<SCT>> {
        self.proposal_gen.next_proposal(
            &self.keys,
            &self.cert_keys,
            &self.epoch_manager,
            &self.val_epoch_map,
            &self.election,
            TransactionPayload::Null,
            StateRootHash::default(),
        )
    }

    fn next_proposal(
        &mut self,
        txn_list: FullTransactionList,
        state_root: StateRootHash,
    ) -> Verified<ST, ProposalMessage<SCT>> {
        self.proposal_gen.next_proposal(
            &self.keys,
            &self.cert_keys,
            &self.epoch_manager,
            &self.val_epoch_map,
            &self.election,
            TransactionPayload::List(txn_list),
            state_root,
        )
    }

    // TODO come up with better API for making mal proposals relative to state of proposal_gen
    fn mal_proposal_empty(&mut self) -> Verified<ST, ProposalMessage<SCT>> {
        self.malicious_proposal_gen.next_proposal(
            &self.keys,
            &self.cert_keys,
            &self.epoch_manager,
            &self.val_epoch_map,
            &self.election,
            TransactionPayload::List(FullTransactionList::new(vec![5].into())),
            StateRootHash::default(),
        )
    }

    // TODO come up with better API for making mal proposals relative to state of proposal_gen
    fn branch_proposal(
        &mut self,
        txn_list: FullTransactionList,
        state_root: StateRootHash,
    ) -> Verified<ST, ProposalMessage<SCT>> {
        self.malicious_proposal_gen.next_proposal(
            &self.keys,
            &self.cert_keys,
            &self.epoch_manager,
            &self.val_epoch_map,
            &self.election,
            TransactionPayload::List(txn_list),
            state_root,
        )
    }

    fn next_tc(&mut self, epoch: Epoch) -> Vec<Verified<ST, TimeoutMessage<SCT>>> {
        let valset = self.val_epoch_map.get_val_set(&epoch).unwrap();
        let val_cert_pubkeys = self.val_epoch_map.get_cert_pubkeys(&epoch).unwrap();
        self.proposal_gen.next_tc(
            &self.keys,
            &self.cert_keys,
            valset,
            &self.epoch_manager,
            val_cert_pubkeys,
        )
    }
}

struct NodeContext<ST, SCT, VTF, SVT, LT, TT>
where
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SVT: StateRootValidator,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    TT: TxPool<SCT, EthBlockPolicy, InMemoryState> + Default,
{
    consensus_state: ConsensusState<SCT, EthBlockPolicy, InMemoryState>,

    metrics: Metrics,
    txpool: TT,
    epoch_manager: EpochManager,

    val_epoch_map: ValidatorsEpochMapping<VTF, SCT>,
    election: LT,
    version: &'static str,

    state_root_validator: SVT,
    block_validator: EthValidator,
    block_policy: EthBlockPolicy,
    state_backend: InMemoryState,
    block_timestamp: BlockTimestamp,
    beneficiary: EthAddress,
    nodeid: NodeId<CertificateSignaturePubKey<ST>>,
    consensus_config: ConsensusConfig,

    keypair: ST::KeyPairType,
    cert_keypair: SignatureCollectionKeyPairType<SCT>,
}

impl<ST, SCT, VTF, SVT, LT, TT> NodeContext<ST, SCT, VTF, SVT, LT, TT>
where
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SVT: StateRootValidator,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    TT: TxPool<SCT, EthBlockPolicy, InMemoryState> + Default,
    // BPT: BlockPolicy<SCT, ValidatedBlock = EthValidatedBlock<SCT>>,
{
    fn wrapped_state(
        &mut self,
    ) -> ConsensusStateWrapper<ST, SCT, EthBlockPolicy, InMemoryState, VTF, LT, TT, EthValidator, SVT>
    {
        ConsensusStateWrapper {
            consensus: &mut self.consensus_state,

            metrics: &mut self.metrics,
            tx_pool: &mut self.txpool,
            epoch_manager: &mut self.epoch_manager,

            val_epoch_map: &self.val_epoch_map,
            election: &self.election,
            version: self.version,

            state_root_validator: &self.state_root_validator,
            block_validator: &self.block_validator,
            block_policy: &mut self.block_policy,
            state_backend: &self.state_backend,
            block_timestamp: &mut self.block_timestamp,
            beneficiary: &self.beneficiary,
            nodeid: &self.nodeid,
            config: &self.consensus_config,

            keypair: &self.keypair,
            cert_keypair: &self.cert_keypair,
        }
    }

    fn handle_proposal_message(
        &mut self,
        author: NodeId<SCT::NodeIdPubKey>,
        p: ProposalMessage<SCT>,
    ) -> Vec<ConsensusCommand<ST, SCT>> {
        self.wrapped_state().handle_proposal_message(author, p)
    }

    fn handle_timeout_message(
        &mut self,
        author: NodeId<SCT::NodeIdPubKey>,
        p: TimeoutMessage<SCT>,
    ) -> Vec<ConsensusCommand<ST, SCT>> {
        self.wrapped_state().handle_timeout_message(author, p)
    }

    fn handle_vote_message(
        &mut self,
        author: NodeId<SCT::NodeIdPubKey>,
        p: VoteMessage<SCT>,
    ) -> Vec<ConsensusCommand<ST, SCT>> {
        self.wrapped_state().handle_vote_message(author, p)
    }

    fn handle_block_sync(
        &mut self,
        block_range: BlockRange,
        full_blocks: Vec<FullBlock<SCT>>,
    ) -> Vec<ConsensusCommand<ST, SCT>> {
        self.wrapped_state()
            .handle_block_sync(block_range, full_blocks)
    }
}

fn setup<
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Clone,
    SVT: StateRootValidator,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Clone,
    TT: TxPool<SCT, EthBlockPolicy, InMemoryState> + Default,
>(
    num_states: u32,
    valset_factory: VTF,
    election: LT,
    state_root: impl Fn() -> SVT,
) -> (
    EnvContext<ST, SCT, VTF, LT>,
    Vec<NodeContext<ST, SCT, VTF, SVT, LT, TT>>,
) {
    let (keys, cert_keys, valset, _valmap) =
        create_keys_w_validators::<ST, SCT, _>(num_states, ValidatorSetFactory::default());
    let val_stakes = Vec::from_iter(valset.get_members().clone());
    let val_cert_pubkeys = keys
        .iter()
        .map(|k| NodeId::new(k.pubkey()))
        .zip(cert_keys.iter().map(|k| k.pubkey()))
        .collect::<Vec<_>>();
    let mut dupkeys = create_keys::<ST>(num_states);
    let mut dupcertkeys = create_certificate_keys::<SCT>(num_states);

    let ctxs: Vec<NodeContext<_, _, _, _, _, _>> = (0..num_states)
        .map(|i| {
            let mut val_epoch_map = ValidatorsEpochMapping::new(valset_factory.clone());
            val_epoch_map.insert(
                Epoch(1),
                val_stakes.clone(),
                ValidatorMapping::new(val_cert_pubkeys.clone()),
            );
            val_epoch_map.insert(
                Epoch(2),
                val_stakes.clone(),
                ValidatorMapping::new(val_cert_pubkeys.clone()),
            );
            let epoch_manager = EpochManager::new(SeqNum(100), Round(20), &[(Epoch(1), Round(0))]);

            let default_key =
                <ST::KeyPairType as CertificateKeyPair>::from_bytes(&mut [127; 32]).unwrap();
            let default_cert_key =
                <SignatureCollectionKeyPairType<SCT> as CertificateKeyPair>::from_bytes(
                    &mut [127; 32],
                )
                .unwrap();
            let consensus_config = ConsensusConfig {
                proposal_txn_limit: 5000,
                proposal_gas_limit: 8_000_000,
                delta: Duration::from_secs(1),
                statesync_to_live_threshold: SeqNum(600),
                live_to_statesync_threshold: SeqNum(900),
                start_execution_threshold: SeqNum(300),
                vote_pace: Duration::from_secs(1),
                timestamp_latency_estimate_ms: 10,
            };
            let genesis_qc = QuorumCertificate::genesis_qc();
            let cs = ConsensusState::new(
                &epoch_manager,
                &consensus_config,
                RootInfo {
                    round: genesis_qc.get_round(),
                    seq_num: genesis_qc.get_seq_num(),
                    epoch: genesis_qc.get_epoch(),
                    block_id: genesis_qc.get_block_id(),
                    state_root: StateRootHash(Hash([0xb; 32])),
                },
                genesis_qc,
            );

            NodeContext {
                consensus_state: cs,

                metrics: Metrics::default(),
                txpool: TT::default(),
                epoch_manager,

                val_epoch_map,
                election: election.clone(),
                version: "TEST",

                state_root_validator: state_root(),
                block_validator: EthValidator::new(10_000, u64::MAX, 1337),
                block_policy: EthBlockPolicy::new(GENESIS_SEQ_NUM, 0, 1337),
                state_backend: InMemoryStateInner::genesis(u128::MAX, SeqNum(0)),
                block_timestamp: BlockTimestamp::new(
                    100,
                    consensus_config.timestamp_latency_estimate_ms,
                ),
                beneficiary: EthAddress::default(),
                nodeid: NodeId::new(keys[i as usize].pubkey()),
                consensus_config,

                keypair: std::mem::replace(&mut dupkeys[i as usize], default_key),
                cert_keypair: std::mem::replace(&mut dupcertkeys[i as usize], default_cert_key),
            }
        })
        .collect();

    let mut val_epoch_map = ValidatorsEpochMapping::new(valset_factory);
    val_epoch_map.insert(
        Epoch(1),
        val_stakes,
        ValidatorMapping::new(val_cert_pubkeys),
    );
    let epoch_manager = EpochManager::new(SeqNum(100), Round(20), &[(Epoch(1), Round(0))]);

    let env: EnvContext<ST, SCT, VTF, LT> = EnvContext {
        proposal_gen: ProposalGen::<ST, SCT>::new(),
        malicious_proposal_gen: ProposalGen::<ST, SCT>::new(),
        keys,
        cert_keys,
        epoch_manager,
        val_epoch_map,
        election,
    };

    (env, ctxs)
}

type SignatureType = NopSignature;
type SignatureCollectionType = MultiSig<SignatureType>;
type StateRootValidatorType = NopStateRoot;
type BlockPolicyType = EthBlockPolicy;
type StateBackendType = InMemoryState;

use alloy_consensus::{Transaction, TxEnvelope};
use alloy_primitives::{FixedBytes, U256};
use alloy_rlp::Encodable;
use monad_consensus::messages::consensus_message::ProtocolMessage;
use monad_consensus_types::{
    block::Block, payload::Payload, quorum_certificate::QuorumCertificate,
};
use monad_crypto::{certificate_signature::PubKey, hasher::Hash};
use monad_eth_block_validator::EthValidator;

fn make_tx(input_len: usize) -> TxEnvelope {
    let sender = FixedBytes::from(U256::from(rand::random::<u64>()));
    let nonce = rand::random();
    monad_eth_testutil::make_tx(sender, 1_000, 21_000, nonce, input_len)
}
fn make_txns() -> (Vec<TxEnvelope>, FullTransactionList) {
    let txns = (0..NUM_TRANSACTIONS)
        .map(|_| make_tx(TRANSACTION_SIZE_BYTES))
        .collect::<Vec<_>>();
    let proposal_gas_limit: u64 = txns.iter().map(|txn| txn.gas_limit()).sum::<u64>() + 1;

    let mut txns_encoded: Vec<u8> = vec![];
    txns.encode(&mut txns_encoded);

    (
        txns.clone(),
        FullTransactionList::new(Bytes::from(txns_encoded)),
    )
}
fn init(seed_mempool: bool) -> BenchTuple {
    let (mut env, mut ctx) = setup::<
        SignatureType,
        SignatureCollectionType,
        _,
        _,
        _,
        EthTxPool<MultiSig<SignatureType>, InMemoryState>,
    >(
        4u32,
        ValidatorSetFactory::default(),
        SimpleRoundRobin::default(),
        || NopStateRoot,
    );

    // this guy is the leader
    let wrapped_state = ctx[0].wrapped_state();
    let leader = wrapped_state.election.get_leader(
        Round(1),
        env.val_epoch_map
            .get_val_set(&Epoch(1))
            .unwrap()
            .get_members(),
    );
    assert_eq!(&leader, wrapped_state.nodeid);
    let (raw_txns, encoded_txns) = make_txns();

    if seed_mempool {
        let txns: Vec<Bytes> = raw_txns
            .iter()
            .map(|t| Bytes::from(alloy_rlp::encode(t)))
            .collect();
        EthTxPool::insert_tx(
            wrapped_state.tx_pool,
            txns,
            wrapped_state.block_policy,
            wrapped_state.state_backend,
        );
    }
    let (author, _, proposal_message) = env
        .next_proposal(encoded_txns.clone(), StateRootHash::default())
        .destructure();
    assert_eq!(author, leader);
    (encoded_txns, env, ctx, author, proposal_message)
}
fn make_block<SCT: SignatureCollection<NodeIdPubKey = NopPubKey>>() -> (Block<SCT>, Payload) {
    let txns = (0..NUM_TRANSACTIONS)
        .map(|_| make_tx(TRANSACTION_SIZE_BYTES))
        .collect::<Vec<_>>();

    let mut txns_encoded: Vec<u8> = vec![];
    txns.encode(&mut txns_encoded);

    let txns = TransactionPayload::List(FullTransactionList::new(Bytes::copy_from_slice(
        &txns_encoded,
    )));

    let payload = Payload { txns };
    (
        Block::new(
            NodeId::new(NopPubKey::from_bytes(&[0u8; 32]).unwrap()),
            0,
            Epoch(1),
            Round(1),
            &ExecutionProtocol {
                state_root: StateRootHash::default(),
                seq_num: SeqNum(0),
                beneficiary: Default::default(),
                randao_reveal: Default::default(),
            },
            payload.get_id(),
            BlockKind::Executable,
            &QuorumCertificate::<SCT>::genesis_qc(),
        ),
        payload,
    )
}

fn extract_vote_msgs<ST, SCT>(cmds: Vec<ConsensusCommand<ST, SCT>>) -> Vec<VoteMessage<SCT>>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    cmds.into_iter()
        .filter_map(|c| match c {
            ConsensusCommand::Publish {
                target: RouterTarget::PointToPoint(_),
                message,
            } => match message.deref().deref().message {
                ProtocolMessage::Vote(vote) => Some(vote),
                _ => None,
            },
            _ => None,
        })
        .collect::<Vec<_>>()
}

fn extract_blocksync_requests<ST, SCT>(cmds: Vec<ConsensusCommand<ST, SCT>>) -> Vec<BlockRange>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    cmds.into_iter()
        .filter_map(|c| match c {
            ConsensusCommand::RequestSync(block_range) => Some(block_range),
            _ => None,
        })
        .collect()
}

#[allow(clippy::useless_vec)]
pub fn criterion_benchmark(c: &mut Criterion) {
    // hardware requirement: CPU (16 cores, hyperthreading disabled)
    // assume half of those are allocated to consensus
    rayon::ThreadPoolBuilder::new()
        .num_threads(8)
        .build_global()
        .unwrap();
    let mut group = c.benchmark_group("consensus_state_machine");
    group.bench_function("handle_proposal_message", |b| {
        b.iter_batched_ref(
            || init(true),
            |(_txns, env, ctx, author, proposal_message)| {
                let mut wrapped_state = ctx[0].wrapped_state();
                let cmds = wrapped_state.handle_proposal_message(*author, proposal_message.clone());
            },
            BatchSize::SmallInput,
        );
    });
    group.bench_function("handle_vote_message", |b| {
        b.iter_batched_ref(
            || -> (
                EnvCtx,
                Vec<NodeCtx>,
                Vec<(NodeId<NopPubKey>, VoteMessage<SignatureCollectionType>)>,
            ) {
                let (mut env, mut ctx) =
                    setup::<SignatureType, SignatureCollectionType, _, _, _, EthTxPool<MultiSig<SignatureType>, InMemoryState>>(
                        4u32,
                        ValidatorSetFactory::default(),
                        SimpleRoundRobin::default(),
                        || NopStateRoot,
                    );

                // this guy is the leader
                let wrapped_state = ctx[0].wrapped_state();
                let leader = wrapped_state.election.get_leader(
                    Round(1),
                    env.val_epoch_map
                        .get_val_set(&Epoch(1))
                        .unwrap()
                        .get_members(),
                );
                assert_eq!(&leader, wrapped_state.nodeid);
                let (raw_txns, encoded_txns) = make_txns();

                let txns: Vec<Bytes> = raw_txns
                    .iter()
                    .map(|t| Bytes::from(alloy_rlp::encode(t)))
                    .collect();
                EthTxPool::insert_tx(
                    wrapped_state.tx_pool,
                    txns,
                    wrapped_state.block_policy,
                    wrapped_state.state_backend,
                );
                let (author, _, proposal_message) = env
                    .next_proposal(
                        encoded_txns,
                        StateRootHash::default(),
                    )
                    .destructure();
                assert_eq!(&author, &leader);
                let mut votes = vec![];
                for node in ctx.iter_mut() {
                    let mut wrapped_state = node.wrapped_state();
                    let cmds = wrapped_state.handle_proposal_message(
                        author,
                        proposal_message.clone(),
                    );
                    votes.extend(
                        std::iter::repeat(*wrapped_state.nodeid)
                            .zip(extract_vote_msgs(cmds)),
                    );
                }
                (env, ctx, votes)
            },
            |(env, ctx, votes)| {
                let (n1, _) = ctx.split_first_mut().unwrap();
                for (vote_author, vote) in votes.iter_mut() {
                    let cmds = n1.handle_vote_message(*vote_author, *vote);
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("handle_timeout", |b| {
        b.iter_batched_ref(
            || {
                let (mut env, mut ctx) = setup::<
                    SignatureType,
                    SignatureCollectionType,
                    _,
                    _,
                    _,
                    EthTxPool<MultiSig<SignatureType>, InMemoryState>,
                >(
                    4u32,
                    ValidatorSetFactory::default(),
                    SimpleRoundRobin::default(),
                    || NopStateRoot,
                );

                let (raw_txns, _) = make_txns();
                let txns: Vec<Bytes> = raw_txns
                    .iter()
                    .map(|t| Bytes::from(alloy_rlp::encode(t)))
                    .collect();
                let ctx_3 = &mut ctx[3];
                let _ = EthTxPool::insert_tx(
                    &mut ctx_3.txpool,
                    txns,
                    &ctx_3.block_policy,
                    &ctx_3.state_backend,
                );
                let _ = env.next_tc(Epoch(1));
                let tc = env.next_tc(Epoch(1));
                (ctx, tc)
            },
            |(ctx, tcs)| {
                let node = &mut ctx[3];
                for tc in tcs {
                    let (author, _, message) = tc.clone().destructure();
                    let cmds = node.handle_timeout_message(author, message);
                }
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
