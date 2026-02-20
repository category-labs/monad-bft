#![cfg(feature = "test-scripted-blocks")]
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

use std::time::Duration;

use alloy_consensus::{
    constants::EMPTY_WITHDRAWALS, proofs::calculate_transaction_root, transaction::Recovered,
    TxEnvelope, EMPTY_OMMER_ROOT_HASH,
};
use futures::StreamExt;
use monad_chain_config::{
    revision::{ChainRevision, MonadChainRevision},
    ChainConfig, MonadChainConfig, MONAD_DEVNET_CHAIN_ID,
};
use monad_consensus_types::{
    block::{BlockPolicy, ConsensusBlockHeader},
    block_validator::BlockValidator,
    checkpoint::RootInfo,
    metrics::Metrics,
    payload::{ConsensusBlockBody, ConsensusBlockBodyInner, RoundSignature},
    quorum_certificate::QuorumCertificate,
};
use monad_crypto::{
    certificate_signature::{CertificateKeyPair, CertificateSignaturePubKey},
    NopKeyPair, NopSignature,
};
use monad_eth_block_policy::{timestamp_ns_to_secs, EthBlockPolicy, EthValidatedBlock};
use monad_eth_block_validator::EthBlockValidator;
use monad_eth_testutil::recover_tx;
use monad_eth_txpool_executor::ScriptedTxPoolExecutorClient;
use monad_eth_types::{
    EthBlockBody, EthExecutionProtocol, ExtractEthAddress, ProposedEthHeader,
};
use monad_executor::Executor;
use monad_executor_glue::{MempoolEvent, MonadEvent, TxPoolCommand};
use monad_state_backend::NopStateBackend;
use monad_testutil::signing::MockSignatures;
use monad_types::{
    Epoch, NodeId, Round, SeqNum, GENESIS_BLOCK_ID, GENESIS_ROUND, GENESIS_SEQ_NUM,
};

type SignatureType = NopSignature;
type SignatureCollectionType = MockSignatures<SignatureType>;
type StateBackendType = NopStateBackend;

type TestBlockPolicy = EthBlockPolicy<
    SignatureType,
    SignatureCollectionType,
    MonadChainConfig,
    MonadChainRevision,
>;

fn genesis_root_info() -> RootInfo {
    RootInfo {
        round: GENESIS_ROUND,
        seq_num: GENESIS_SEQ_NUM,
        epoch: Epoch(0),
        block_id: GENESIS_BLOCK_ID,
        timestamp_ns: 0,
    }
}

/// Create a validated block from a proposal's transactions, mirroring what consensus does.
fn create_validated_block(
    chain_config: &MonadChainConfig,
    block_policy: &TestBlockPolicy,
    round: Round,
    seq_num: SeqNum,
    epoch: Epoch,
    txs: Vec<Recovered<TxEnvelope>>,
    extending_blocks: &[EthValidatedBlock<SignatureType, SignatureCollectionType>],
    timestamp_ns: u128,
) -> EthValidatedBlock<SignatureType, SignatureCollectionType> {
    let raw_txs: Vec<TxEnvelope> = txs.iter().map(|tx| tx.inner().to_owned()).collect();
    let txns_root = *calculate_transaction_root(&raw_txs);
    let body = ConsensusBlockBody::new(ConsensusBlockBodyInner {
        execution_body: EthBlockBody {
            transactions: raw_txs,
            ommers: Vec::default(),
            withdrawals: Vec::default(),
        },
    });
    let body_id = body.get_id();

    let base_fees = block_policy
        .compute_base_fee::<EthValidatedBlock<SignatureType, SignatureCollectionType>>(
            extending_blocks,
            chain_config,
            timestamp_ns,
        )
        .unwrap();

    let (base_fee, base_trend, base_moment) = base_fees;
    let chain_params = chain_config.get_chain_revision(round).chain_params();

    let keypair = NopKeyPair::from_bytes(rand::random::<[u8; 32]>().as_mut_slice()).unwrap();
    let signature = RoundSignature::new(round, &keypair);

    let exec_results = if seq_num < SeqNum(3) {
        vec![]
    } else {
        vec![monad_eth_types::EthHeader(
            alloy_consensus::Header::default(),
        )]
    };

    let header = ConsensusBlockHeader::new(
        NodeId::new(keypair.pubkey()),
        epoch,
        round,
        exec_results,
        ProposedEthHeader {
            ommers_hash: EMPTY_OMMER_ROOT_HASH.0,
            transactions_root: txns_root,
            number: seq_num.0,
            gas_limit: chain_params.proposal_gas_limit,
            timestamp: timestamp_ns_to_secs(timestamp_ns),
            mix_hash: signature.get_hash().0,
            base_fee_per_gas: base_fee,
            withdrawals_root: EMPTY_WITHDRAWALS.0,
            requests_hash: Some([0_u8; 32]),
            ..Default::default()
        },
        body_id,
        QuorumCertificate::genesis_qc(),
        seq_num,
        timestamp_ns,
        signature,
        Some(base_fee),
        Some(base_trend),
        Some(base_moment),
    );

    let validator: EthBlockValidator<SignatureType, SignatureCollectionType> =
        EthBlockValidator::default();
    BlockValidator::<
        SignatureType,
        SignatureCollectionType,
        EthExecutionProtocol,
        TestBlockPolicy,
        StateBackendType,
        MonadChainConfig,
        MonadChainRevision,
    >::validate(&validator, header, body, None, chain_config, &mut Metrics::default())
    .unwrap()
}

/// Start a ScriptedTxPoolExecutorClient and return it with its keypair for signing proposals.
async fn setup_scripted_executor() -> (
    ScriptedTxPoolExecutorClient<
        SignatureType,
        SignatureCollectionType,
        StateBackendType,
        MonadChainConfig,
        MonadChainRevision,
    >,
    NopKeyPair,
    MonadChainConfig,
) {
    let block_policy = TestBlockPolicy::new(GENESIS_SEQ_NUM, 3);
    let state_backend = NopStateBackend::default();
    let chain_config = MonadChainConfig::new(MONAD_DEVNET_CHAIN_ID, None).unwrap();

    let tmpdir = tempfile::tempdir().unwrap();
    let socket_path = tmpdir.path().join("scripted_test.sock");

    let executor = ScriptedTxPoolExecutorClient::start(
        socket_path,
        block_policy,
        state_backend,
        chain_config,
    )
    .unwrap();

    let keypair = NopKeyPair::from_bytes(rand::random::<[u8; 32]>().as_mut_slice()).unwrap();

    (executor, keypair, chain_config)
}

/// Helper to send a CreateProposal command and receive the Proposal event.
async fn create_proposal(
    executor: &mut ScriptedTxPoolExecutorClient<
        SignatureType,
        SignatureCollectionType,
        StateBackendType,
        MonadChainConfig,
        MonadChainRevision,
    >,
    keypair: &NopKeyPair,
    seq_num: SeqNum,
    round: Round,
    epoch: Epoch,
    timestamp_ns: u128,
    extending_blocks: Vec<EthValidatedBlock<SignatureType, SignatureCollectionType>>,
) -> MempoolEvent<SignatureType, SignatureCollectionType, EthExecutionProtocol> {
    let chain_config = MonadChainConfig::new(MONAD_DEVNET_CHAIN_ID, None).unwrap();
    let chain_params = chain_config.get_chain_revision(round).chain_params();
    let round_signature = RoundSignature::new(round, keypair);

    let beneficiary: [u8; 20] = CertificateSignaturePubKey::<SignatureType>::from(keypair.pubkey())
        .get_eth_address()
        .into();

    let delayed_execution_results = if seq_num < SeqNum(3) {
        vec![]
    } else {
        vec![monad_eth_types::EthHeader(
            alloy_consensus::Header::default(),
        )]
    };

    executor.exec(vec![TxPoolCommand::CreateProposal {
        node_id: NodeId::new(keypair.pubkey()),
        epoch,
        round,
        seq_num,
        high_qc: QuorumCertificate::genesis_qc(),
        round_signature,
        last_round_tc: None,
        fresh_proposal_certificate: None,
        tx_limit: 5000,
        proposal_gas_limit: chain_params.proposal_gas_limit,
        proposal_byte_limit: 2_000_000,
        beneficiary,
        timestamp_ns,
        delayed_execution_results,
        extending_blocks,
    }]);

    // Receive the proposal event
    let event = tokio::time::timeout(Duration::from_secs(5), executor.next())
        .await
        .expect("should receive proposal within timeout")
        .expect("stream should not end");

    match event {
        MonadEvent::MempoolEvent(mempool_event) => mempool_event,
        _ => panic!("expected MempoolEvent"),
    }
}

/// Test that the scripted executor produces empty blocks (no user txs) that pass coherency.
/// This validates system transaction generation, base fee computation, and header construction.
#[tokio::test]
async fn test_scripted_empty_block_is_coherent() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init();

    let (mut executor, keypair, chain_config) = setup_scripted_executor().await;

    let seq_num = GENESIS_SEQ_NUM + SeqNum(1);
    let round = GENESIS_ROUND + Round(1);
    let epoch = Epoch(1);
    let timestamp_ns: u128 = 1_000_000_000; // 1 second

    let event = create_proposal(
        &mut executor,
        &keypair,
        seq_num,
        round,
        epoch,
        timestamp_ns,
        vec![], // no extending blocks for first block after genesis
    )
    .await;

    let (proposal, base_fee, base_fee_trend, base_fee_moment) = match event {
        MempoolEvent::Proposal {
            proposed_execution_inputs,
            base_fee,
            base_fee_trend,
            base_fee_moment,
            ..
        } => (
            proposed_execution_inputs,
            base_fee,
            base_fee_trend,
            base_fee_moment,
        ),
        _ => panic!("expected Proposal event"),
    };

    // Base fee should be computed (not None, since TFM is enabled)
    assert!(base_fee.is_some(), "base_fee should be Some");
    assert!(base_fee_trend.is_some(), "base_fee_trend should be Some");
    assert!(base_fee_moment.is_some(), "base_fee_moment should be Some");

    // System transactions should be present (staking_activation = Epoch(2))
    // For seq_num=1, epoch=1: we expect EpochChange system tx (epoch 1 >= staking_activation - 1)
    // Also this is genesis+1 with staking_activation=2, so is_genesis_block triggers EpochChange
    assert!(
        !proposal.body.transactions.is_empty(),
        "proposal should contain system transactions"
    );

    // Recover all transactions and create a validated block
    let recovered_txs: Vec<Recovered<TxEnvelope>> = proposal
        .body
        .transactions
        .iter()
        .cloned()
        .map(recover_tx)
        .collect();

    let block_policy = TestBlockPolicy::new(GENESIS_SEQ_NUM, 3);
    let validated_block = create_validated_block(
        &chain_config,
        &block_policy,
        round,
        seq_num,
        epoch,
        recovered_txs,
        &[],
        timestamp_ns,
    );

    // The block should pass coherency check
    let state_backend = NopStateBackend::default();
    block_policy
        .check_coherency(
            &validated_block,
            vec![],
            genesis_root_info(),
            &state_backend,
            &chain_config,
        )
        .expect("scripted proposal should pass coherency check");
}

/// Test that multiple sequential blocks from the scripted executor are coherent.
/// This validates that BlockCommit updates are handled correctly and base fees evolve.
#[tokio::test]
async fn test_scripted_sequential_blocks_are_coherent() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init();

    let (mut executor, keypair, chain_config) = setup_scripted_executor().await;
    let state_backend = NopStateBackend::default();

    let mut committed_blocks: Vec<EthValidatedBlock<SignatureType, SignatureCollectionType>> =
        vec![];
    let mut block_policy_for_validation = TestBlockPolicy::new(GENESIS_SEQ_NUM, 3);

    // Create 5 sequential blocks
    for i in 1..=5u64 {
        let seq_num = GENESIS_SEQ_NUM + SeqNum(i);
        let round = GENESIS_ROUND + Round(i);
        let epoch = Epoch(1);
        let timestamp_ns = i as u128 * 1_000_000_000;

        let event = create_proposal(
            &mut executor,
            &keypair,
            seq_num,
            round,
            epoch,
            timestamp_ns,
            committed_blocks.clone(),
        )
        .await;

        let proposal = match event {
            MempoolEvent::Proposal {
                proposed_execution_inputs,
                ..
            } => proposed_execution_inputs,
            _ => panic!("expected Proposal event for block {i}"),
        };

        let recovered_txs: Vec<Recovered<TxEnvelope>> = proposal
            .body
            .transactions
            .iter()
            .cloned()
            .map(recover_tx)
            .collect();

        let validated_block = create_validated_block(
            &chain_config,
            &block_policy_for_validation,
            round,
            seq_num,
            epoch,
            recovered_txs,
            &committed_blocks,
            timestamp_ns,
        );

        // Verify coherency against extending blocks
        block_policy_for_validation
            .check_coherency(
                &validated_block,
                committed_blocks.iter().collect(),
                genesis_root_info(),
                &state_backend,
                &chain_config,
            )
            .unwrap_or_else(|e| panic!("block {i} should pass coherency check: {e:?}"));

        // Commit the block to both the scripted executor and the validation policy
        executor.exec(vec![TxPoolCommand::BlockCommit(vec![
            validated_block.clone(),
        ])]);
        BlockPolicy::<
            SignatureType,
            SignatureCollectionType,
            EthExecutionProtocol,
            StateBackendType,
            MonadChainConfig,
            MonadChainRevision,
        >::update_committed_block(
            &mut block_policy_for_validation,
            &validated_block,
            &chain_config,
        );

        committed_blocks.push(validated_block);
    }
}
