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

use std::collections::BTreeMap;

use alloy_consensus::{
    constants::EMPTY_WITHDRAWALS, proofs::calculate_transaction_root, transaction::Recovered,
    SignableTransaction, TxEip7702, TxEnvelope, TxLegacy, EMPTY_OMMER_ROOT_HASH,
};
use alloy_eips::eip7702::{Authorization, SignedAuthorization};
use alloy_primitives::{Address, FixedBytes, TxKind, U256};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use monad_chain_config::{
    revision::{ChainParams, ChainRevision, MonadChainRevision},
    ChainConfig, MonadChainConfig, MONAD_DEVNET_CHAIN_ID,
};
use monad_consensus_types::{
    block::{BlockPolicy, ConsensusBlockHeader},
    block_validator::BlockValidator,
    checkpoint::RootInfo,
    metrics::Metrics,
    payload::{ConsensusBlockBody, ConsensusBlockBodyId, ConsensusBlockBodyInner, RoundSignature},
    quorum_certificate::QuorumCertificate,
};
use monad_crypto::{
    certificate_signature::{CertificateKeyPair, PubKey},
    NopKeyPair, NopSignature,
};
use monad_eth_block_policy::{EthBlockPolicy, EthValidatedBlock};
use monad_eth_block_validator::EthBlockValidator;
use monad_eth_testutil::{recover_tx, secret_to_eth_address, S1, S2};
use monad_eth_txpool::{
    EthTxPool, EthTxPoolConfig, EthTxPoolEventTracker, EthTxPoolMetrics, PoolTransactionKind,
    TrackedTxLimitsConfig,
};
use monad_eth_types::{EthBlockBody, EthExecutionProtocol, EthHeader, ProposedEthHeader};
use monad_state_backend::NopStateBackend;
use monad_testutil::signing::MockSignatures;
use monad_types::{Epoch, NodeId, Round, SeqNum, GENESIS_BLOCK_ID, GENESIS_ROUND, GENESIS_SEQ_NUM};
use tracing::info;

type TestBlockPolicy = EthBlockPolicy<
    NopSignature,
    MockSignatures<NopSignature>,
    MonadChainConfig,
    MonadChainRevision,
>;

type TestTxPool = EthTxPool<
    NopSignature,
    MockSignatures<NopSignature>,
    NopStateBackend,
    MonadChainConfig,
    MonadChainRevision,
>;

const ONE_ETHER: u128 = 1_000_000_000_000_000_000;

#[test]
fn sanity_check_coherency() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_line_number(true)
        .with_file(true)
        .with_test_writer()
        .try_init();

    let (_, seq_num, block_policy, chain_config) = genesis_setup();

    let txs = BTreeMap::from([(seq_num, vec![])]);
    let state_backend = NopStateBackend {
        ..Default::default()
    };

    test_runner(chain_config, block_policy, state_backend, txs, true);
}

#[test]
fn test_insufficient_single_emptying_transaction() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = insufficient_single_emptying_transaction_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, false);
}

#[test]
fn test_insufficient_single_emptying_transaction_2() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = insufficient_single_emptying_transaction_inputs_2();

    test_runner(chain_config, block_policy, state_backend, txs, true);
}

#[test]
fn test_sufficient_single_emptying_transaction() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = sufficient_single_emptying_transaction_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, true);
}

#[test]
fn test_insufficient_emptying_transaction() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = insufficient_emptying_transaction_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, false);
}

#[test]
fn test_insufficient_emptying_transaction_2() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = insufficient_emptying_transaction_inputs_2();

    test_runner(chain_config, block_policy, state_backend, txs, false);
}

#[test]
fn test_sufficient_emptying_transaction() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = sufficient_emptying_transaction_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, true);
}

#[test]
fn test_emptying_transaction_different_blocks_insufficient() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = emptying_transaction_different_blocks_insufficient_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, false);
}

#[test]
fn test_emptying_transaction_different_blocks_insufficient_reserve() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = emptying_transaction_different_blocks_insufficient_reserve_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, false);
}

#[test]
fn test_emptying_transaction_different_blocks_sufficient() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = emptying_transaction_different_blocks_sufficient_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, true);
}

#[test]
fn test_non_emptying_transaction_different_blocks_insufficient() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = non_emptying_transaction_different_blocks_insufficient_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, false);
}

#[test]
fn test_non_emptying_transaction_different_blocks_insufficient_reserve() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) =
        non_emptying_transaction_different_blocks_insufficient_reserve_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, false);
}

#[test]
fn test_non_emptying_transaction_different_blocks_sufficient() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = non_emptying_transaction_different_blocks_sufficient_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, true);
}

#[test]
fn test_delegation_non_emptying_same_block_insufficient() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = delegation_non_emptying_same_block_insufficient_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, false);
}

#[test]
fn test_delegation_non_emptying_same_block_insufficient_reserve() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = delegation_non_emptying_same_block_insufficient_reserve_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, false);
}

#[test]
fn test_delegation_non_emptying_same_block_sufficient() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = delegation_non_emptying_same_block_sufficient_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, true);
}

#[test]
fn test_invalid_delegation_non_emptying_same_block() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = invalid_delegation_non_emptying_same_block_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, true);
}

#[test]
fn test_delegation_non_emptying_different_blocks_insufficient() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = delegation_non_emptying_different_blocks_insufficient_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, false);
}

#[test]
fn test_delegation_non_emptying_different_blocks_insufficient_reserve() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) =
        delegation_non_emptying_different_blocks_insufficient_reserve_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, false);
}

#[test]
fn test_delegation_non_emptying_different_blocks_sufficient() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = delegation_non_emptying_different_blocks_sufficient_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, true);
}

#[test]
fn test_invalid_delegation_non_emptying_different_blocks() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = invalid_delegation_non_emptying_different_blocks_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, true);
}

#[test]
fn test_invalid_delegation_non_emptying_different_blocks_2() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = invalid_delegation_non_emptying_different_blocks_inputs_2();

    test_runner(chain_config, block_policy, state_backend, txs, true);
}

#[test]
fn test_emptying_txn_and_delegation_same_block() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = emptying_txn_and_delegation_same_block_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, false);
}

#[test]
fn test_emptying_txn_with_value_and_delegation_same_block() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = emptying_txn_with_value_and_delegation_same_block_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, true);
}

#[test]
fn test_sufficient_balance_emptying_txn_with_value_and_delegation_same_block() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) =
        sufficient_balance_emptying_txn_with_value_and_delegation_same_block_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, true);
}

#[test]
fn test_delegation_and_transfer_same_transaction_insufficient() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = delegation_and_transfer_same_transaction_insufficient_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, false);
}

#[test]
fn test_delegation_and_transfer_same_transaction_insufficient_2() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = delegation_and_transfer_same_transaction_insufficient_inputs_2();

    test_runner(chain_config, block_policy, state_backend, txs, false);
}

#[test]
fn test_delegation_and_transfer_same_transaction_sufficient() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = delegation_and_transfer_same_transaction_sufficient_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, true);
}

#[test]
fn test_prev_block_delegation_insufficient() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = prev_block_delegation_insufficient_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, false);
}

#[test]
fn test_prev_block_delegation_sufficient() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = prev_block_delegation_sufficient_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, true);
}

#[test]
fn test_emptying_non_emptying_delegation_insufficient() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = emptying_non_emptying_delegation_insufficient_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, false);
}

#[test]
fn test_emptying_delegation_sufficient() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = emptying_delegation_sufficient_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, true);
}

#[test]
fn test_delegation_undelegation_insufficient_reserve() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = delegation_undelegation_insufficient_reserve_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, false);
}

#[test]
fn test_delegation_undelegation_sufficient_reserve() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = delegation_undelegation_sufficient_reserve_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, true);
}

#[test]
fn test_emptying_and_delegation_preceding_blocks_insufficient() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = emptying_and_delegation_preceding_blocks_insufficient_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, false);
}

#[test]
fn test_emptying_and_delegation_preceding_blocks_insufficient_reserve() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) =
        emptying_and_delegation_preceding_blocks_insufficient_reserve_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, false);
}

#[test]
fn test_emptying_and_delegation_preceding_blocks_sufficient() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = emptying_and_delegation_preceding_blocks_sufficient_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, true);
}

#[test]
fn test_multiple_non_emptying_same_block_insufficient() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = multiple_non_emptying_same_block_insufficient_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, false);
}

#[test]
fn test_multiple_non_emptying_same_block_sufficient() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = multiple_non_emptying_same_block_sufficient_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, true);
}

#[test]
fn test_multiple_non_emptying_different_blocks_insufficient() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = multiple_non_emptying_different_blocks_insufficient_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, false);
}

#[test]
fn test_multiple_non_emptying_different_blocks_sufficient() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = multiple_non_emptying_different_blocks_sufficient_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, true);
}

#[test]
fn test_invalid_delegation_non_emptying_sufficient() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = invalid_delegation_non_emptying_sufficient_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, true);
}

#[test]
fn test_invalid_delegation_non_emptying_insufficient() {
    let (_round, _seq_num, block_policy, chain_config) = genesis_setup();
    let (txs, state_backend) = invalid_delegation_non_emptying_insufficient_inputs();

    test_runner(chain_config, block_policy, state_backend, txs, false);
}

fn create_test_txpool(chain_config: &MonadChainConfig) -> TestTxPool {
    TestTxPool::new(
        EthTxPoolConfig {
            limits: TrackedTxLimitsConfig::new(
                None,
                None,
                None,
                None,
                std::time::Duration::from_secs(60),
                std::time::Duration::from_secs(60),
            ),
        },
        chain_config.chain_id(),
        chain_config.get_chain_revision(GENESIS_ROUND),
        chain_config.get_execution_chain_revision(0), // genesis timestamp
    )
}

fn check_txpool_coherency(
    chain_config: &MonadChainConfig,
    extending_blocks: &[EthValidatedBlock<NopSignature, MockSignatures<NopSignature>>],
    block_under_test: &EthValidatedBlock<NopSignature, MockSignatures<NopSignature>>,
    state_backend: &NopStateBackend,
    root_info: RootInfo,
) {
    let mut block_policy = TestBlockPolicy::new(GENESIS_SEQ_NUM, 3);

    let mut txpool = create_test_txpool(chain_config);
    let metrics = EthTxPoolMetrics::default();
    let mut ipc_events = BTreeMap::default();
    let mut event_tracker = EthTxPoolEventTracker::new(&metrics, &mut ipc_events);

    // insert extending blocks to txpool
    for extending_block in extending_blocks.iter() {
        txpool.update_committed_block(&mut event_tracker, chain_config, extending_block.clone());
        <TestBlockPolicy as BlockPolicy<
            NopSignature,
            MockSignatures<NopSignature>,
            EthExecutionProtocol,
            NopStateBackend,
            MonadChainConfig,
            MonadChainRevision,
        >>::update_committed_block(&mut block_policy, extending_block, chain_config);
    }

    // insert transactions of the incoming block into txpool
    let block_txs: Vec<Recovered<TxEnvelope>> = block_under_test
        .validated_txns
        .iter()
        .map(|vtx| vtx.tx.clone())
        .collect();
    if !block_txs.is_empty() {
        txpool.insert_txs(
            &mut event_tracker,
            &block_policy,
            state_backend,
            chain_config,
            block_txs
                .into_iter()
                .map(|tx| {
                    (
                        tx,
                        PoolTransactionKind::Forwarded {
                            sender: NodeId::new(
                                <NopKeyPair as CertificateKeyPair>::PubKeyType::from_bytes(
                                    &[0u8; 32],
                                )
                                .unwrap(),
                            ),
                        },
                    )
                })
                .collect(),
            |_tx| {},
        );
    }

    // create proposal
    let base_fee = block_under_test.header().base_fee.unwrap_or(0);
    let timestamp_ns = block_under_test.header().timestamp_ns;
    let beneficiary: [u8; 20] = block_under_test
        .header()
        .execution_inputs
        .beneficiary
        .into();
    let seq_num = block_under_test.header().seq_num;
    let round = block_under_test.header().block_round;
    let proposal = txpool
        .create_proposal(
            &mut event_tracker,
            block_under_test.header().epoch,
            round,
            seq_num,
            base_fee,
            5000,
            200_000_000,
            2_000_000,
            beneficiary,
            timestamp_ns,
            block_under_test.header().author,
            block_under_test.header().round_signature.clone(),
            extending_blocks.to_vec(),
            &block_policy,
            state_backend,
            chain_config,
        )
        .unwrap();

    info!(
        "Txpool created proposal with {} txs for seq_num {:?}",
        proposal.0.body.transactions.len(),
        block_under_test.header().seq_num
    );

    let txs = proposal
        .0
        .body
        .transactions
        .iter()
        .cloned()
        .map(recover_tx)
        .collect::<Vec<Recovered<TxEnvelope>>>();
    let block_policy = TestBlockPolicy::new(GENESIS_SEQ_NUM, 3);
    let created_block = create_test_block_helper(
        chain_config,
        &block_policy,
        round,
        seq_num,
        txs,
        extending_blocks,
    );

    block_policy
        .check_coherency(
            &created_block,
            extending_blocks.iter().collect(),
            root_info,
            state_backend,
            chain_config,
        )
        .expect("Txpool created proposal that failed coherency check");
}

fn test_runner(
    chain_config: MonadChainConfig,
    block_policy: TestBlockPolicy,
    state_backend: NopStateBackend,
    txs: BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    expect_coherent: bool,
) {
    let validated_blocks = create_test_blocks(&chain_config, &block_policy, txs);

    let root_info = RootInfo {
        round: GENESIS_ROUND,
        seq_num: GENESIS_SEQ_NUM,
        epoch: Epoch(0),
        block_id: GENESIS_BLOCK_ID,
        timestamp_ns: 0,
    };

    if let Some((block_under_test, extending)) = validated_blocks.split_last() {
        let result = block_policy.check_coherency(
            block_under_test,
            extending.iter().collect(),
            root_info,
            &state_backend,
            &chain_config,
        );

        if expect_coherent {
            result.unwrap();
        } else {
            assert!(result.is_err());

            // check that the same block produced by txpool is coherent
            check_txpool_coherency(
                &chain_config,
                extending,
                block_under_test,
                &state_backend,
                root_info,
            );
        }
    } else {
        panic!("test did nothing, are inputs correct?");
    }
}

fn genesis_setup() -> (Round, SeqNum, TestBlockPolicy, MonadChainConfig) {
    let round = GENESIS_ROUND + Round(1);
    let seq_num = GENESIS_SEQ_NUM + SeqNum(1);

    let block_policy = TestBlockPolicy::new(GENESIS_SEQ_NUM, 3);

    let chain_config = MonadChainConfig::new(MONAD_DEVNET_CHAIN_ID, None).unwrap();

    (round, seq_num, block_policy, chain_config)
}

fn create_test_blocks(
    chain_config: &MonadChainConfig,
    block_policy: &TestBlockPolicy,
    txs: BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
) -> Vec<EthValidatedBlock<NopSignature, MockSignatures<NopSignature>>> {
    let mut blocks = vec![];

    // create blocks n-2k+2 to block n
    // block 1 to block 2 is block n-2k+2 to block n-k
    // block 3 to block 4 is block n-k+1 to block n-1
    // block 5 is block n
    for i in 1..=5 {
        let round = GENESIS_ROUND + Round(i);
        let seq_num = GENESIS_SEQ_NUM + SeqNum(i);
        let tx = txs.get(&seq_num).cloned().unwrap_or_default();

        let validated_block =
            create_test_block_helper(chain_config, block_policy, round, seq_num, tx, &blocks);

        info!(
            "adding seq_num {:?} : block_id {:?}",
            seq_num,
            validated_block.get_id()
        );

        blocks.push(validated_block);
    }

    blocks
}

fn create_block_body_helper(
    txs: Vec<Recovered<TxEnvelope>>,
) -> ConsensusBlockBody<EthExecutionProtocol> {
    ConsensusBlockBody::new(ConsensusBlockBodyInner {
        execution_body: EthBlockBody {
            transactions: txs.iter().map(|tx| tx.tx().to_owned()).collect(),
            ommers: Vec::default(),
            withdrawals: Vec::default(),
        },
    })
}

fn create_block_header_helper(
    round: Round,
    seq_num: SeqNum,
    timestamp: u128,
    body_id: ConsensusBlockBodyId,
    txns_root: [u8; 32],
    base_fees: (u64, u64, u64),
    chain_params: &ChainParams,
) -> ConsensusBlockHeader<NopSignature, MockSignatures<NopSignature>, EthExecutionProtocol> {
    let keypair = NopKeyPair::from_bytes(rand::random::<[u8; 32]>().as_mut_slice()).unwrap();
    let signature = RoundSignature::new(round, &keypair);

    let (base_fee, base_trend, base_moment) = base_fees;

    let exec_results = if seq_num < SeqNum(3) {
        vec![]
    } else {
        vec![EthHeader(alloy_consensus::Header::default())]
    };

    ConsensusBlockHeader::new(
        NodeId::new(keypair.pubkey()),
        Epoch(1),
        round,
        exec_results, //Default::default(), // delayed_execution_results
        // execution_inputs
        ProposedEthHeader {
            ommers_hash: EMPTY_OMMER_ROOT_HASH.0,
            transactions_root: txns_root,
            number: seq_num.0,
            gas_limit: chain_params.proposal_gas_limit,
            mix_hash: signature.get_hash().0,
            base_fee_per_gas: base_fee,
            withdrawals_root: EMPTY_WITHDRAWALS.0,
            requests_hash: Some([0_u8; 32]),
            ..Default::default()
        },
        body_id,
        QuorumCertificate::genesis_qc(),
        seq_num,
        timestamp,
        signature,
        Some(base_fee),
        Some(base_trend),
        Some(base_moment),
    )
}

fn create_test_block_helper(
    chain_config: &MonadChainConfig,
    block_policy: &TestBlockPolicy,
    round: Round,
    seq_num: SeqNum,
    txs: Vec<Recovered<TxEnvelope>>,
    blocks: &[EthValidatedBlock<NopSignature, MockSignatures<NopSignature>>],
) -> EthValidatedBlock<NopSignature, MockSignatures<NopSignature>> {
    let body = create_block_body_helper(txs);
    let body_id = body.get_id();
    let txns_root = calculate_transaction_root(&body.execution_body.transactions).0;

    let timestamp = seq_num.0 as u128;
    let base_fees = block_policy
        .compute_base_fee::<EthValidatedBlock<NopSignature, MockSignatures<NopSignature>>>(
            blocks,
            chain_config,
            timestamp,
        )
        .unwrap();
    let header = create_block_header_helper(
        round,
        seq_num,
        timestamp,
        body_id,
        txns_root,
        base_fees,
        chain_config.get_chain_revision(round).chain_params(),
    );

    let validator: EthBlockValidator<NopSignature, MockSignatures<NopSignature>> =
        EthBlockValidator::default();
    BlockValidator::<
        NopSignature,
        MockSignatures<NopSignature>,
        EthExecutionProtocol,
        TestBlockPolicy,
        NopStateBackend,
        MonadChainConfig,
        MonadChainRevision,
    >::validate(
        &validator,
        header,
        body,
        None,
        chain_config,
        &mut Metrics::default(),
    )
    .unwrap()
}

fn make_test_tx(
    sender: FixedBytes<32>,
    gas_limit: u64,
    max_fee_per_gas: u128,
    value: u128,
    nonce: u64,
) -> Recovered<TxEnvelope> {
    let transaction = TxLegacy {
        chain_id: Some(MONAD_DEVNET_CHAIN_ID),
        nonce,
        gas_price: max_fee_per_gas,
        gas_limit,
        to: TxKind::Call(Address::repeat_byte(0u8)),
        value: U256::from(value),
        input: vec![].into(),
    };

    let signer = PrivateKeySigner::from_bytes(&sender).unwrap();
    let signature = signer
        .sign_hash_sync(&transaction.signature_hash())
        .unwrap();
    let te: TxEnvelope = transaction.into_signed(signature).into();
    recover_tx(te)
}

pub fn make_eip7702_tx_with_value(
    sender: FixedBytes<32>,
    value: u128,
    max_fee_per_gas: u128,
    max_priority_fee_per_gas: u128,
    gas_limit: u64,
    nonce: u64,
    authorization_list: Vec<SignedAuthorization>,
    input_len: usize,
) -> Recovered<TxEnvelope> {
    let transaction = TxEip7702 {
        chain_id: MONAD_DEVNET_CHAIN_ID,
        nonce,
        gas_limit,
        max_fee_per_gas,
        max_priority_fee_per_gas,
        to: Address::repeat_byte(0u8),
        value: U256::from(value),
        access_list: Default::default(),
        authorization_list,
        input: vec![0; input_len].into(),
    };

    let signer = PrivateKeySigner::from_bytes(&sender).unwrap();
    let signature = signer
        .sign_hash_sync(&transaction.signature_hash())
        .unwrap();
    let te: TxEnvelope = transaction.into_signed(signature).into();
    recover_tx(te)
}

pub fn make_signed_authorization(
    authority: FixedBytes<32>,
    address: Address,
    nonce: u64,
) -> SignedAuthorization {
    let authorization = Authorization {
        chain_id: MONAD_DEVNET_CHAIN_ID,
        address,
        nonce,
    };

    sign_authorization(authority, authorization)
}

pub fn sign_authorization(
    authority: FixedBytes<32>,
    authorization: Authorization,
) -> SignedAuthorization {
    let signer = PrivateKeySigner::from_bytes(&authority).unwrap();
    let signature = signer
        .sign_hash_sync(&authorization.signature_hash())
        .unwrap();
    authorization.into_signed(signature)
}

// 1
fn insufficient_single_emptying_transaction_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    let max_fee_per_gas = (3 * ONE_ETHER) / 50_000;

    let tx1 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 0);
    let sender = tx1.signer();

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx1]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([(sender, U256::from(2 * ONE_ETHER))]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 2
fn insufficient_single_emptying_transaction_inputs_2() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    let max_fee_per_gas = (3 * ONE_ETHER) / 50_000;

    let tx1 = make_test_tx(signer, 50_000, max_fee_per_gas, 3 * ONE_ETHER, 0);
    let sender = tx1.signer();

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx1]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([(sender, U256::from(5 * ONE_ETHER))]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 3
fn sufficient_single_emptying_transaction_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    let max_fee_per_gas = ONE_ETHER / 50_000;

    let tx1 = make_test_tx(signer, 50_000, max_fee_per_gas, 3 * ONE_ETHER, 0);
    let sender = tx1.signer();

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx1]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([(sender, U256::from(5 * ONE_ETHER))]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 4
fn insufficient_emptying_transaction_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    let max_fee_per_gas = (2 * ONE_ETHER) / 50_000;
    let tx1 = make_test_tx(signer, 50_000, max_fee_per_gas, 2 * ONE_ETHER, 0);
    let sender = tx1.signer();

    let tx2 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 1);

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx1, tx2]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([(sender, U256::from(5 * ONE_ETHER))]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 5
fn insufficient_emptying_transaction_inputs_2() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    let max_fee_per_gas = (5 * ONE_ETHER) / 50_000;
    let tx1 = make_test_tx(signer, 50_000, max_fee_per_gas, 2 * ONE_ETHER, 0);
    let sender = tx1.signer();

    let max_fee_per_gas = (11 * ONE_ETHER) / 50_000;
    let tx2 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 1);

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx1, tx2]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([(sender, U256::from(20 * ONE_ETHER))]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 6
fn sufficient_emptying_transaction_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    let max_fee_per_gas = (2 * ONE_ETHER) / 50_000;
    let tx1 = make_test_tx(signer, 50_000, max_fee_per_gas, 2 * ONE_ETHER, 0);
    let sender = tx1.signer();

    let max_fee_per_gas = ONE_ETHER / 50_000;
    let tx2 = make_test_tx(signer, 50_000, max_fee_per_gas, ONE_ETHER, 1);

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx1, tx2]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([(sender, U256::from(5 * ONE_ETHER))]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 7
fn emptying_transaction_different_blocks_insufficient_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    let max_fee_per_gas = (2 * ONE_ETHER) / 50_000;
    let tx1 = make_test_tx(signer, 50_000, max_fee_per_gas, 2 * ONE_ETHER, 0);
    let sender = tx1.signer();

    let tx2 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 1);

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![tx1]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx2]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([(sender, U256::from(5 * ONE_ETHER))]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 8
fn emptying_transaction_different_blocks_insufficient_reserve_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    let max_fee_per_gas = (5 * ONE_ETHER) / 50_000;
    let tx1 = make_test_tx(signer, 50_000, max_fee_per_gas, 2 * ONE_ETHER, 0);
    let sender = tx1.signer();

    let max_fee_per_gas = (11 * ONE_ETHER) / 50_000;
    let tx2 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 1);

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![tx1]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx2]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([(sender, U256::from(20 * ONE_ETHER))]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 9
fn emptying_transaction_different_blocks_sufficient_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    let max_fee_per_gas = (2 * ONE_ETHER) / 50_000;
    let tx1 = make_test_tx(signer, 50_000, max_fee_per_gas, 2 * ONE_ETHER, 0);
    let sender = tx1.signer();

    let max_fee_per_gas = ONE_ETHER / 50_000;
    let tx2 = make_test_tx(signer, 50_000, max_fee_per_gas, ONE_ETHER, 1);

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![tx1]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx2]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([(sender, U256::from(5 * ONE_ETHER))]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 10
fn non_emptying_transaction_different_blocks_insufficient_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    // Tx0 in block n-2k+2 to block n-k
    let tx0 = make_test_tx(signer, 50_000, 100_000_000_000, 0, 0);
    let sender = tx0.signer();

    let max_fee_per_gas = (2 * ONE_ETHER) / 50_000;
    let tx1 = make_test_tx(signer, 50_000, max_fee_per_gas, 2 * ONE_ETHER, 1);

    let max_fee_per_gas = (4 * ONE_ETHER) / 50_000;
    let tx2 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 2);

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![tx0]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![tx1]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx2]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([(sender, U256::from(5 * ONE_ETHER))]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 11
fn non_emptying_transaction_different_blocks_insufficient_reserve_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    // Tx0 in block n-2k+2 to block n-k
    let tx0 = make_test_tx(signer, 50_000, 100_000_000_000, 0, 0);
    let sender = tx0.signer();

    let max_fee_per_gas = (5 * ONE_ETHER) / 50_000;
    let tx1 = make_test_tx(signer, 50_000, max_fee_per_gas, 2 * ONE_ETHER, 1);

    let max_fee_per_gas = (11 * ONE_ETHER) / 50_000;
    let tx2 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 2);

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![tx0]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![tx1]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx2]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([(sender, U256::from(20 * ONE_ETHER))]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 12
fn non_emptying_transaction_different_blocks_sufficient_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    // Tx0 in block n-2k+2 to block n-k
    let tx0 = make_test_tx(signer, 50_000, 100_000_000_000, 0, 0);
    let sender = tx0.signer();

    let max_fee_per_gas = (2 * ONE_ETHER) / 50_000;
    let tx1 = make_test_tx(signer, 50_000, max_fee_per_gas, 2 * ONE_ETHER, 1);

    let tx2 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 2);

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![tx0]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![tx1]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx2]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([(sender, U256::from(5 * ONE_ETHER))]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 13
fn delegation_non_emptying_same_block_insufficient_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    let signed_auth = make_signed_authorization(signer, Address::default(), 0);
    let tx1 =
        make_eip7702_tx_with_value(S2, 0, 100_000_000_000, 0, 50_000, 0, vec![signed_auth], 0);
    let bundler = tx1.signer();

    let max_fee_per_gas = (3 * ONE_ETHER) / 50_000;
    let tx2 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 1);
    let sender = tx2.signer();

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx1, tx2]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([
            (sender, U256::from(2 * ONE_ETHER)),
            (bundler, U256::from(ONE_ETHER)),
        ]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 14
fn delegation_non_emptying_same_block_insufficient_reserve_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    let signed_auth = make_signed_authorization(signer, Address::default(), 0);
    let tx1 =
        make_eip7702_tx_with_value(S2, 0, 100_000_000_000, 0, 50_000, 0, vec![signed_auth], 0);
    let bundler = tx1.signer();

    let max_fee_per_gas = (11 * ONE_ETHER) / 50_000;
    let tx2 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 1);
    let sender = tx2.signer();

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx1, tx2]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([
            (sender, U256::from(15 * ONE_ETHER)),
            (bundler, U256::from(ONE_ETHER)),
        ]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 15
fn delegation_non_emptying_same_block_sufficient_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    let signed_auth = make_signed_authorization(signer, Address::default(), 0);
    let tx1 =
        make_eip7702_tx_with_value(S2, 0, 100_000_000_000, 0, 50_000, 0, vec![signed_auth], 0);
    let bundler = tx1.signer();

    let max_fee_per_gas = ONE_ETHER / 50_000;
    let tx2 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 1);
    let sender = tx2.signer();

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx1, tx2]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([
            (sender, U256::from(2 * ONE_ETHER)),
            (bundler, U256::from(ONE_ETHER)),
        ]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 16
fn invalid_delegation_non_emptying_same_block_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    // Invalid delegation with wrong chain id
    let invalid_auth = Authorization {
        chain_id: MONAD_DEVNET_CHAIN_ID + 999,
        address: Address::default(),
        nonce: 0,
    };
    let signed_invalid_auth = sign_authorization(signer, invalid_auth);
    let tx1 = make_eip7702_tx_with_value(
        S2,
        0,
        100_000_000_000,
        0,
        50_000,
        0,
        vec![signed_invalid_auth],
        0,
    );
    let bundler = tx1.signer();

    let max_fee_per_gas = (2 * ONE_ETHER) / 50_000;
    let tx2 = make_test_tx(signer, 50_000, max_fee_per_gas, 2 * ONE_ETHER, 0);
    let sender = tx2.signer();

    let tx3 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 1);

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx1, tx2, tx3]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([
            (sender, U256::from(5 * ONE_ETHER)),
            (bundler, U256::from(ONE_ETHER)),
        ]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 17
fn delegation_non_emptying_different_blocks_insufficient_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    let signed_auth = make_signed_authorization(signer, Address::default(), 0);
    let tx1 =
        make_eip7702_tx_with_value(S2, 0, 100_000_000_000, 0, 50_000, 0, vec![signed_auth], 0);
    let bundler = tx1.signer();
    let sender = signer;

    let max_fee_per_gas = (3 * ONE_ETHER) / 50_000;
    let tx2 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 1);

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![tx1]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx2]),
    ]);

    let signer_addr = PrivateKeySigner::from_bytes(&sender).unwrap().address();
    let state_backend = NopStateBackend {
        balances: BTreeMap::from([
            (signer_addr, U256::from(2 * ONE_ETHER)),
            (bundler, U256::from(ONE_ETHER)),
        ]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 18
fn delegation_non_emptying_different_blocks_insufficient_reserve_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    let signed_auth = make_signed_authorization(signer, Address::default(), 0);
    let tx1 =
        make_eip7702_tx_with_value(S2, 0, 100_000_000_000, 0, 50_000, 0, vec![signed_auth], 0);
    let bundler = tx1.signer();
    let sender = signer;

    let max_fee_per_gas = (11 * ONE_ETHER) / 50_000;
    let tx2 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 1);

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![tx1]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx2]),
    ]);

    let signer_addr = PrivateKeySigner::from_bytes(&sender).unwrap().address();
    let state_backend = NopStateBackend {
        balances: BTreeMap::from([
            (signer_addr, U256::from(15 * ONE_ETHER)),
            (bundler, U256::from(ONE_ETHER)),
        ]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 19
fn delegation_non_emptying_different_blocks_sufficient_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    let signed_auth = make_signed_authorization(signer, Address::default(), 0);
    let tx1 =
        make_eip7702_tx_with_value(S2, 0, 100_000_000_000, 0, 50_000, 0, vec![signed_auth], 0);
    let bundler = tx1.signer();
    let sender = signer;

    let max_fee_per_gas = ONE_ETHER / 50_000;
    let tx2 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 1);

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![tx1]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx2]),
    ]);

    let signer_addr = PrivateKeySigner::from_bytes(&sender).unwrap().address();
    let state_backend = NopStateBackend {
        balances: BTreeMap::from([
            (signer_addr, U256::from(2 * ONE_ETHER)),
            (bundler, U256::from(ONE_ETHER)),
        ]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 20
fn invalid_delegation_non_emptying_different_blocks_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    // Invalid delegation with wrong chain id
    let invalid_auth = Authorization {
        chain_id: MONAD_DEVNET_CHAIN_ID + 999,
        address: Address::default(),
        nonce: 0,
    };
    let signed_invalid_auth = sign_authorization(signer, invalid_auth);
    let tx1 = make_eip7702_tx_with_value(
        S2,
        0,
        100_000_000_000,
        0,
        50_000,
        0,
        vec![signed_invalid_auth],
        0,
    );
    let bundler = tx1.signer();

    let max_fee_per_gas = (2 * ONE_ETHER) / 50_000;
    let tx2 = make_test_tx(signer, 50_000, max_fee_per_gas, 2 * ONE_ETHER, 0);
    let sender = tx2.signer();

    let tx3 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 1);

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![tx1, tx2]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx3]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([
            (sender, U256::from(5 * ONE_ETHER)),
            (bundler, U256::from(ONE_ETHER)),
        ]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 21
fn invalid_delegation_non_emptying_different_blocks_inputs_2() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    // Invalid delegation with wrong chain id
    let invalid_auth = Authorization {
        chain_id: MONAD_DEVNET_CHAIN_ID + 999,
        address: Address::default(),
        nonce: 0,
    };
    let signed_invalid_auth = sign_authorization(signer, invalid_auth);
    let tx1 = make_eip7702_tx_with_value(
        S2,
        0,
        100_000_000_000,
        0,
        50_000,
        0,
        vec![signed_invalid_auth],
        0,
    );
    let bundler = tx1.signer();
    let sender = signer;

    let max_fee_per_gas = (2 * ONE_ETHER) / 50_000;
    let tx2 = make_test_tx(signer, 50_000, max_fee_per_gas, 2 * ONE_ETHER, 0);

    let tx3 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 1);

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![tx1]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx2, tx3]),
    ]);

    let signer_addr = PrivateKeySigner::from_bytes(&sender).unwrap().address();
    let state_backend = NopStateBackend {
        balances: BTreeMap::from([
            (signer_addr, U256::from(5 * ONE_ETHER)),
            (bundler, U256::from(ONE_ETHER)),
        ]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 22
fn emptying_txn_and_delegation_same_block_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    let max_fee_per_gas = (3 * ONE_ETHER) / 50_000;
    let tx1 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 0);
    let sender = tx1.signer();

    let signed_auth = make_signed_authorization(signer, Address::default(), 1);
    let tx2 =
        make_eip7702_tx_with_value(S2, 0, 100_000_000_000, 0, 50_000, 1, vec![signed_auth], 0);
    let bundler = tx2.signer();

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx1, tx2]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([
            (sender, U256::from(2 * ONE_ETHER)),
            (bundler, U256::from(ONE_ETHER)),
        ]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 23
fn emptying_txn_with_value_and_delegation_same_block_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    let max_fee_per_gas = (3 * ONE_ETHER) / 50_000;
    let tx1 = make_test_tx(signer, 50_000, max_fee_per_gas, 3 * ONE_ETHER, 0);
    let sender = tx1.signer();

    let signed_auth = make_signed_authorization(signer, Address::default(), 1);
    let tx2 =
        make_eip7702_tx_with_value(S2, 0, 100_000_000_000, 0, 50_000, 0, vec![signed_auth], 0);
    let bundler = tx2.signer();

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx1, tx2]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([
            (sender, U256::from(5 * ONE_ETHER)),
            (bundler, U256::from(ONE_ETHER)),
        ]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 24
fn sufficient_balance_emptying_txn_with_value_and_delegation_same_block_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    let max_fee_per_gas = ONE_ETHER / 50_000;
    let tx1 = make_test_tx(signer, 50_000, max_fee_per_gas, 3 * ONE_ETHER, 0);
    let sender = tx1.signer();

    let signed_auth = make_signed_authorization(signer, Address::default(), 1);
    let tx2 =
        make_eip7702_tx_with_value(S2, 0, 100_000_000_000, 0, 50_000, 0, vec![signed_auth], 0);
    let bundler = tx2.signer();

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx1, tx2]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([
            (sender, U256::from(5 * ONE_ETHER)),
            (bundler, U256::from(ONE_ETHER)),
        ]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 25
fn delegation_undelegation_insufficient_reserve_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    // Delegation then undelegation in same transaction
    let signed_auth_1 = make_signed_authorization(signer, secret_to_eth_address(signer), 0);
    let signed_auth_2 = make_signed_authorization(signer, Address::default(), 1);
    let tx1 = make_eip7702_tx_with_value(
        signer,
        0,
        100_000_000_000,
        0,
        100_000,
        0,
        vec![signed_auth_1, signed_auth_2],
        0,
    );
    let bundler = tx1.signer();

    let max_fee_per_gas = (11 * ONE_ETHER) / 50_000;
    let tx2 = make_test_tx(signer, 50_000, max_fee_per_gas, 2 * ONE_ETHER, 2);
    let sender = tx2.signer();

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx1, tx2]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([
            (sender, U256::from(15 * ONE_ETHER)),
            (bundler, U256::from(ONE_ETHER)),
        ]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 26
fn delegation_undelegation_sufficient_reserve_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    // Delegation then undelegation in same transaction
    let signed_auth_1 = make_signed_authorization(signer, secret_to_eth_address(signer), 0);
    let signed_auth_2 = make_signed_authorization(signer, Address::default(), 1);
    let tx1 = make_eip7702_tx_with_value(
        S2,
        0,
        100_000_000_000,
        0,
        100_000,
        0,
        vec![signed_auth_1, signed_auth_2],
        0,
    );
    let bundler = tx1.signer();

    let max_fee_per_gas = (4 * ONE_ETHER) / 50_000;
    let tx2 = make_test_tx(signer, 50_000, max_fee_per_gas, 2 * ONE_ETHER, 2);
    let sender = tx2.signer();

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx1, tx2]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([
            (sender, U256::from(5 * ONE_ETHER)),
            (bundler, U256::from(ONE_ETHER)),
        ]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 27
fn delegation_and_transfer_same_transaction_insufficient_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    let max_fee_per_gas = (3 * ONE_ETHER) / 50_000;
    let signed_auth = make_signed_authorization(signer, Address::default(), 1);
    let tx1 = make_eip7702_tx_with_value(
        signer,
        0,
        max_fee_per_gas,
        max_fee_per_gas,
        50_000,
        0,
        vec![signed_auth],
        0,
    );
    let sender = tx1.signer();

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx1]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([(sender, U256::from(2 * ONE_ETHER))]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 28
fn delegation_and_transfer_same_transaction_insufficient_inputs_2() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    let max_fee_per_gas = (11 * ONE_ETHER) / 50_000;
    let signed_auth = make_signed_authorization(signer, Address::default(), 1);
    let tx1 = make_eip7702_tx_with_value(
        signer,
        0,
        max_fee_per_gas,
        max_fee_per_gas,
        50_000,
        0,
        vec![signed_auth],
        0,
    );
    let sender = tx1.signer();

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx1]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([(sender, U256::from(15 * ONE_ETHER))]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 29
fn delegation_and_transfer_same_transaction_sufficient_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    let max_fee_per_gas = (3 * ONE_ETHER) / 50_000;
    let signed_auth = make_signed_authorization(signer, Address::default(), 1);
    let tx1 = make_eip7702_tx_with_value(
        signer,
        3 * ONE_ETHER,
        max_fee_per_gas,
        max_fee_per_gas,
        50_000,
        0,
        vec![signed_auth],
        0,
    );
    let sender = tx1.signer();

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx1]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([(sender, U256::from(5 * ONE_ETHER))]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 30
fn prev_block_delegation_insufficient_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    let signed_auth = make_signed_authorization(signer, Address::default(), 0);
    let tx1 =
        make_eip7702_tx_with_value(S2, 0, 100_000_000_000, 0, 50_000, 0, vec![signed_auth], 0);
    let bundler = tx1.signer();

    let max_fee_per_gas = (2 * ONE_ETHER) / 50_000;
    let tx2 = make_test_tx(signer, 50_000, max_fee_per_gas, 2 * ONE_ETHER, 1);
    let sender = tx2.signer();

    let max_fee_per_gas = (4 * ONE_ETHER) / 50_000;
    let tx3 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 2);

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![tx1, tx2]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx3]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([
            (sender, U256::from(5 * ONE_ETHER)),
            (bundler, U256::from(ONE_ETHER)),
        ]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 31
fn prev_block_delegation_sufficient_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    let signed_auth = make_signed_authorization(signer, Address::default(), 0);
    let tx1 =
        make_eip7702_tx_with_value(S2, 0, 100_000_000_000, 0, 50_000, 0, vec![signed_auth], 0);
    let bundler = tx1.signer();

    let max_fee_per_gas = (2 * ONE_ETHER) / 50_000;
    let tx2 = make_test_tx(signer, 50_000, max_fee_per_gas, 2 * ONE_ETHER, 1);
    let sender = tx2.signer();

    let max_fee_per_gas = (2 * ONE_ETHER) / 50_000;
    let tx3 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 2);

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![tx1, tx2]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx3]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([
            (sender, U256::from(5 * ONE_ETHER)),
            (bundler, U256::from(ONE_ETHER)),
        ]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 32
fn emptying_and_delegation_preceding_blocks_insufficient_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    let max_fee_per_gas = (2 * ONE_ETHER) / 50_000;
    let tx1 = make_test_tx(signer, 50_000, max_fee_per_gas, 2 * ONE_ETHER, 0);
    let sender = tx1.signer();

    let signed_auth = make_signed_authorization(signer, Address::default(), 1);
    let tx2 =
        make_eip7702_tx_with_value(S2, 0, 100_000_000_000, 0, 50_000, 0, vec![signed_auth], 0);
    let bundler = tx2.signer();

    let tx3 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 2);

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![tx1, tx2]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx3]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([
            (sender, U256::from(5 * ONE_ETHER)),
            (bundler, U256::from(ONE_ETHER)),
        ]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 33
fn emptying_and_delegation_preceding_blocks_insufficient_reserve_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    let max_fee_per_gas = (2 * ONE_ETHER) / 50_000;
    let tx1 = make_test_tx(signer, 50_000, max_fee_per_gas, 2 * ONE_ETHER, 0);
    let sender = tx1.signer();

    let signed_auth = make_signed_authorization(signer, Address::default(), 1);
    let tx2 =
        make_eip7702_tx_with_value(S2, 0, 100_000_000_000, 0, 50_000, 0, vec![signed_auth], 0);
    let bundler = tx2.signer();

    let max_fee_per_gas = (11 * ONE_ETHER) / 50_000;
    let tx3 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 2);

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![tx1, tx2]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx3]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([
            (sender, U256::from(15 * ONE_ETHER)),
            (bundler, U256::from(ONE_ETHER)),
        ]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 34
fn emptying_and_delegation_preceding_blocks_sufficient_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    let max_fee_per_gas = (2 * ONE_ETHER) / 50_000;
    let tx1 = make_test_tx(signer, 50_000, max_fee_per_gas, 2 * ONE_ETHER, 0);
    let sender = tx1.signer();

    let signed_auth = make_signed_authorization(signer, Address::default(), 1);
    let tx2 =
        make_eip7702_tx_with_value(S2, 0, 100_000_000_000, 0, 50_000, 0, vec![signed_auth], 0);
    let bundler = tx2.signer();

    let max_fee_per_gas = ONE_ETHER / 50_000;
    let tx3 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 2);

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![tx1, tx2]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx3]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([
            (sender, U256::from(5 * ONE_ETHER)),
            (bundler, U256::from(ONE_ETHER)),
        ]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 35
fn multiple_non_emptying_same_block_insufficient_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    // Tx0 in block n-k+1 to block n-1
    let tx0 = make_test_tx(signer, 50_000, 100_000_000_000, 0, 0);
    let sender = tx0.signer();

    let max_fee_per_gas = (3 * ONE_ETHER) / 50_000;
    let tx1 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 1);

    let max_fee_per_gas = (9 * ONE_ETHER) / 50_000;
    let tx2 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 2);

    let max_fee_per_gas = (2 * ONE_ETHER) / 50_000;
    let tx3 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 3);

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![tx0]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx1, tx2, tx3]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([(sender, U256::from(15 * ONE_ETHER))]),
        nonces: BTreeMap::from([(sender, 1)]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 36
fn multiple_non_emptying_same_block_sufficient_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    // Tx0 in block n-k+1 to block n-1
    let tx0 = make_test_tx(signer, 50_000, 100_000_000_000, 0, 0);
    let sender = tx0.signer();

    let max_fee_per_gas = (5 * ONE_ETHER) / 50_000;
    let tx1 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 1);

    let max_fee_per_gas = (5 * ONE_ETHER) / 50_000;
    let tx2 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 2);

    let max_fee_per_gas = ONE_ETHER / 50_000;
    let tx3 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 3);

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![tx0]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx1, tx2, tx3]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([(sender, U256::from(15 * ONE_ETHER))]),
        nonces: BTreeMap::from([(sender, 1)]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 37
fn multiple_non_emptying_different_blocks_insufficient_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    // Tx0 in block n-k+1 to block n-1
    let tx0 = make_test_tx(signer, 50_000, 100_000_000_000, 0, 0);
    let sender = tx0.signer();

    let max_fee_per_gas = (5 * ONE_ETHER) / 50_000;
    let tx1 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 1);

    let max_fee_per_gas = (3 * ONE_ETHER) / 50_000;
    let tx2 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 2);

    let max_fee_per_gas = (2 * ONE_ETHER) / 50_000;
    let tx3 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 3);

    let max_fee_per_gas = ONE_ETHER / 50_000;
    let tx4 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 4);

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![tx0]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![tx1, tx2]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx3, tx4]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([(sender, U256::from(15 * ONE_ETHER))]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 38
fn multiple_non_emptying_different_blocks_sufficient_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    // Tx0 in block n-k+1 to block n-1
    let tx0 = make_test_tx(signer, 50_000, 100_000_000_000, 0, 0);
    let sender = tx0.signer();

    let max_fee_per_gas = (5 * ONE_ETHER) / 50_000;
    let tx1 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 1);

    let max_fee_per_gas = (3 * ONE_ETHER) / 50_000;
    let tx2 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 2);

    let max_fee_per_gas = ONE_ETHER / 50_000;
    let tx3 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 3);

    let tx4 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 4);

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![tx0]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![tx1, tx2]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx3, tx4]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([(sender, U256::from(15 * ONE_ETHER))]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 39
fn emptying_non_emptying_delegation_insufficient_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    let max_fee_per_gas = (3 * ONE_ETHER) / 50_000;
    let tx1 = make_test_tx(signer, 50_000, max_fee_per_gas, ONE_ETHER, 0);
    let sender = tx1.signer();

    let max_fee_per_gas = (2 * ONE_ETHER) / 50_000;
    let tx2 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 1);

    let signed_auth = make_signed_authorization(signer, Address::default(), 2);
    let tx3 =
        make_eip7702_tx_with_value(S2, 0, 100_000_000_000, 0, 50_000, 0, vec![signed_auth], 0);
    let bundler = tx3.signer();

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx1, tx2, tx3]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([
            (sender, U256::from(5 * ONE_ETHER)),
            (bundler, U256::from(ONE_ETHER)),
        ]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 40
fn emptying_delegation_sufficient_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    let max_fee_per_gas = (11 * ONE_ETHER) / 50_000;
    let tx1 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 0);
    let sender = tx1.signer();

    let signed_auth = make_signed_authorization(signer, Address::default(), 1);
    let tx2 =
        make_eip7702_tx_with_value(S2, 0, 100_000_000_000, 0, 50_000, 0, vec![signed_auth], 0);
    let bundler = tx2.signer();

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx1, tx2]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([
            (sender, U256::from(15 * ONE_ETHER)),
            (bundler, U256::from(ONE_ETHER)),
        ]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 41
fn invalid_delegation_non_emptying_sufficient_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    // Invalid delegation with wrong chain id
    let invalid_auth = Authorization {
        chain_id: MONAD_DEVNET_CHAIN_ID + 999,
        address: Address::default(),
        nonce: 0,
    };
    let signed_invalid_auth = sign_authorization(signer, invalid_auth);
    let tx0 = make_eip7702_tx_with_value(
        S2,
        0,
        100_000_000_000,
        0,
        50_000,
        0,
        vec![signed_invalid_auth],
        0,
    );
    let bundler = tx0.signer();

    let max_fee_per_gas = (5 * ONE_ETHER) / 50_000;
    let tx1 = make_test_tx(signer, 50_000, max_fee_per_gas, ONE_ETHER, 1);
    let sender = tx1.signer();
    let tx2 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 2);

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![tx0]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![tx1]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx2]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([
            (sender, U256::from(10 * ONE_ETHER)),
            (bundler, U256::from(ONE_ETHER)),
        ]),
        ..Default::default()
    };

    (txs, state_backend)
}

// 42
fn invalid_delegation_non_emptying_insufficient_inputs() -> (
    BTreeMap<SeqNum, Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    // Invalid delegation with wrong chain id
    let invalid_auth = Authorization {
        chain_id: MONAD_DEVNET_CHAIN_ID + 999,
        address: Address::default(),
        nonce: 0,
    };
    let signed_invalid_auth = sign_authorization(signer, invalid_auth);
    let tx0 = make_eip7702_tx_with_value(
        S2,
        0,
        100_000_000_000,
        0,
        50_000,
        0,
        vec![signed_invalid_auth],
        0,
    );
    let bundler = tx0.signer();

    let max_fee_per_gas = (5 * ONE_ETHER) / 50_000;
    let tx1 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 1);
    let sender = tx1.signer();

    let max_fee_per_gas = (6 * ONE_ETHER) / 50_000;
    let tx2 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 2);

    let txs = BTreeMap::from([
        (GENESIS_SEQ_NUM + SeqNum(1), vec![tx0]),
        (GENESIS_SEQ_NUM + SeqNum(3), vec![tx1]),
        (GENESIS_SEQ_NUM + SeqNum(5), vec![tx2]),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([
            (sender, U256::from(15 * ONE_ETHER)),
            (bundler, U256::from(ONE_ETHER)),
        ]),
        ..Default::default()
    };

    (txs, state_backend)
}
