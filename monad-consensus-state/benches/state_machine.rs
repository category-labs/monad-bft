use std::{collections::BTreeMap, ops::Deref, time::Duration};

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use monad_consensus::{
    messages::message::{BlockSyncResponseMessage, ProposalMessage, TimeoutMessage, VoteMessage},
    validation::signing::Verified,
};
use monad_consensus_state::{
    command::ConsensusCommand, ConsensusConfig, ConsensusState, ConsensusStateWrapper,
};
use monad_consensus_types::{
    metrics::Metrics,
    payload::{Bloom, ExecutionArtifacts, FullTransactionList, NopStateRoot, StateRootValidator},
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
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
use monad_testutil::{
    proposal::ProposalGen,
    signing::{create_certificate_keys, create_keys},
    validators::create_keys_w_validators,
};
use monad_types::{BlockId, Epoch, NodeId, Round, RouterTarget, SeqNum, GENESIS_SEQ_NUM};
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
    EthTxPool,
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
            FullTransactionList::empty(),
            ExecutionArtifacts::zero(),
        )
    }

    fn next_proposal(
        &mut self,
        txn_list: FullTransactionList,
        execution_hdr: ExecutionArtifacts,
    ) -> Verified<ST, ProposalMessage<SCT>> {
        self.proposal_gen.next_proposal(
            &self.keys,
            &self.cert_keys,
            &self.epoch_manager,
            &self.val_epoch_map,
            &self.election,
            txn_list,
            execution_hdr,
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
            FullTransactionList::new(vec![5].into()),
            ExecutionArtifacts::zero(),
        )
    }

    // TODO come up with better API for making mal proposals relative to state of proposal_gen
    fn branch_proposal(
        &mut self,
        txn_list: FullTransactionList,
        execution_hdr: ExecutionArtifacts,
    ) -> Verified<ST, ProposalMessage<SCT>> {
        self.malicious_proposal_gen.next_proposal(
            &self.keys,
            &self.cert_keys,
            &self.epoch_manager,
            &self.val_epoch_map,
            &self.election,
            txn_list,
            execution_hdr,
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
    TT: TxPool<SCT, EthBlockPolicy> + Default,
{
    consensus_state: ConsensusState<ST, SCT, EthBlockPolicy>,

    metrics: Metrics,
    txpool: TT,
    epoch_manager: EpochManager,

    val_epoch_map: ValidatorsEpochMapping<VTF, SCT>,
    election: LT,
    version: &'static str,

    state_root_validator: SVT,
    block_validator: EthValidator,
    block_policy: EthBlockPolicy,
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
    TT: TxPool<SCT, EthBlockPolicy> + Default,
    // BPT: BlockPolicy<SCT, ValidatedBlock = EthValidatedBlock<SCT>>,
{
    fn wrapped_state(
        &mut self,
    ) -> ConsensusStateWrapper<ST, SCT, EthBlockPolicy, VTF, LT, TT, EthValidator, SVT> {
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
        author: NodeId<SCT::NodeIdPubKey>,
        p: BlockSyncResponseMessage<SCT>,
    ) -> Vec<ConsensusCommand<ST, SCT>> {
        self.wrapped_state().handle_block_sync(author, p)
    }
}

fn setup<
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Clone,
    SVT: StateRootValidator,
    LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Clone,
    TT: TxPool<SCT, EthBlockPolicy> + Default,
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
                max_blocksync_retries: 5,
                state_sync_threshold: SeqNum(100),
            };
            let cs = ConsensusState::new(
                &consensus_config,
                QuorumCertificate::genesis_qc(),
                QuorumCertificate::genesis_qc(),
                Epoch(1),
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
                block_validator: EthValidator::new(10_000, u64::MAX),
                block_policy: EthBlockPolicy {
                    latest_nonces: BTreeMap::new(),
                    last_commit: GENESIS_SEQ_NUM,
                },
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

use monad_consensus::messages::consensus_message::ProtocolMessage;
use monad_consensus_types::{
    block::{Block, BlockType},
    ledger::CommitResult,
    payload::Payload,
    quorum_certificate::QuorumCertificate,
    voting::{Vote, VoteInfo},
};
use monad_crypto::{certificate_signature::PubKey, hasher::Hash};
use monad_eth_block_validator::EthValidator;
use rand::{Rng, RngCore};
use reth_primitives::{
    alloy_primitives::private::alloy_rlp::Encodable, sign_message, Address, Transaction,
    TransactionKind, TransactionSigned, TxLegacy, B256,
};

fn make_tx(input_len: usize) -> TransactionSigned {
    let mut input = vec![0; input_len];
    rand::thread_rng().fill_bytes(&mut input);
    let transaction = Transaction::Legacy(TxLegacy {
        chain_id: Some(1337),
        nonce: rand::thread_rng().gen_range(10_000..50_000),
        gas_price: 1,
        gas_limit: 6400,
        to: TransactionKind::Call(Address::random()),
        value: 0.into(),
        input: input.into(),
    });

    let hash = transaction.signature_hash();

    let sender_secret_key = B256::random();
    let signature = sign_message(sender_secret_key, hash).expect("signature should always succeed");

    TransactionSigned::from_transaction_and_signature(transaction, signature)
}
fn make_txns() -> (Vec<TransactionSigned>, FullTransactionList) {
    let txns = (0..NUM_TRANSACTIONS)
        .map(|_| make_tx(TRANSACTION_SIZE_BYTES))
        .collect::<Vec<_>>();
    let proposal_gas_limit: u64 = txns
        .iter()
        .map(|txn| txn.transaction.gas_limit())
        .sum::<u64>()
        + 1;

    let mut txns_encoded: Vec<u8> = vec![];
    txns.encode(&mut txns_encoded);

    (
        txns.clone(),
        FullTransactionList::new(Bytes::from(txns_encoded)),
    )
}
fn init(seed_mempool: bool) -> BenchTuple {
    let (mut env, mut ctx) = setup::<SignatureType, SignatureCollectionType, _, _, _, EthTxPool>(
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
        for txn in raw_txns.iter() {
            <EthTxPool as TxPool<SignatureCollectionType, EthBlockPolicy>>::insert_tx(
                wrapped_state.tx_pool,
                Bytes::from(txn.envelope_encoded()),
            );
        }
    }
    let (author, _, proposal_message) = env
        .next_proposal(
            encoded_txns.clone(),
            ExecutionArtifacts {
                parent_hash: Default::default(),
                state_root: Default::default(),
                transactions_root: Default::default(),
                receipts_root: Default::default(),
                logs_bloom: Bloom::zero(),
                gas_used: Default::default(),
            },
        )
        .destructure();
    assert_eq!(author, leader);
    (encoded_txns, env, ctx, author, proposal_message)
}
fn make_block<SCT: SignatureCollection<NodeIdPubKey = NopPubKey>>() -> Block<SCT> {
    let txns = (0..NUM_TRANSACTIONS)
        .map(|_| make_tx(TRANSACTION_SIZE_BYTES))
        .collect::<Vec<_>>();

    let mut txns_encoded: Vec<u8> = vec![];
    txns.encode(&mut txns_encoded);

    let txns = FullTransactionList::new(Bytes::copy_from_slice(&txns_encoded));

    Block::new(
        NodeId::new(NopPubKey::from_bytes(&[0u8; 32]).unwrap()),
        Epoch(1),
        Round(1),
        &Payload {
            txns,
            header: ExecutionArtifacts {
                parent_hash: Default::default(),
                state_root: Default::default(),
                transactions_root: Default::default(),
                receipts_root: Default::default(),
                logs_bloom: Bloom::zero(),
                gas_used: Default::default(),
            },
            seq_num: SeqNum(0),
            beneficiary: Default::default(),
            randao_reveal: Default::default(),
        },
        &QuorumCertificate::<SCT>::genesis_qc(),
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

fn extract_blocksync_requests<ST, SCT>(cmds: Vec<ConsensusCommand<ST, SCT>>) -> Vec<BlockId>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    cmds.into_iter()
        .filter_map(|c| match c {
            ConsensusCommand::RequestSync { peer: _, block_id } => Some(block_id),
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
                    setup::<SignatureType, SignatureCollectionType, _, _, _, EthTxPool>(
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

                for txn in raw_txns.iter() {
                    <EthTxPool as TxPool<SignatureCollectionType, EthBlockPolicy>>::insert_tx(
                        wrapped_state.tx_pool,
                        Bytes::from(txn.envelope_encoded()),
                    );
                }
                let (author, _, proposal_message) = env
                    .next_proposal(
                        encoded_txns,
                        ExecutionArtifacts {
                            parent_hash: Default::default(),
                            state_root: Default::default(),
                            transactions_root: Default::default(),
                            receipts_root: Default::default(),
                            logs_bloom: Bloom::zero(),
                            gas_used: Default::default(),
                        },
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
                let (mut env, mut ctx) =
                    setup::<SignatureType, SignatureCollectionType, _, _, _, EthTxPool>(
                        4u32,
                        ValidatorSetFactory::default(),
                        SimpleRoundRobin::default(),
                        || NopStateRoot,
                    );

                let (raw_txns, _) = make_txns();
                for txn in raw_txns.iter() {
                    <EthTxPool as TxPool<SignatureCollectionType, EthBlockPolicy>>::insert_tx(
                        &mut ctx[3].txpool,
                        Bytes::from(txn.envelope_encoded()),
                    );
                }
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
    group.bench_function("handle_blocksync", |b| {
        b.iter_batched_ref(
            || {
                let (_txns, env, mut ctx, author, proposal_message) = init(false);
                let (n1, remaining) = ctx.split_first_mut().unwrap();
                for (index, peer) in remaining.iter().enumerate() {
                    let peer_id = peer.nodeid;
                    let cmds = n1.handle_vote_message(
                        peer_id,
                        VoteMessage::new(
                            Vote {
                                vote_info: VoteInfo {
                                    id: proposal_message.block.get_id(),
                                    epoch: proposal_message.block.epoch,
                                    round: proposal_message.block.round,
                                    parent_id: BlockId(Hash([0u8; 32])),
                                    parent_round: Round(0),
                                    seq_num: SeqNum(0),
                                },
                                ledger_commit_info: CommitResult::Commit,
                            },
                            &env.cert_keys[index + 1],
                        ),
                    );
                }
                (_txns, env, ctx, author, proposal_message)
            },
            |(_txns, env, ctx, author, proposal_message)| {
                let (n1, remaining) = ctx.split_first_mut().unwrap();
                let res = n1.handle_block_sync(
                    remaining[0].nodeid,
                    BlockSyncResponseMessage::BlockFound(proposal_message.block.clone()),
                );
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
