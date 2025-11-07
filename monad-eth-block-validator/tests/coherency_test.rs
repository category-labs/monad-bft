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
    payload::{ConsensusBlockBody, ConsensusBlockBodyId, ConsensusBlockBodyInner, RoundSignature},
    quorum_certificate::QuorumCertificate,
};
use monad_crypto::{certificate_signature::CertificateKeyPair, NopKeyPair, NopSignature};
use monad_eth_block_policy::{EthBlockPolicy, EthValidatedBlock};
use monad_eth_block_validator::EthBlockValidator;
use monad_eth_testutil::{recover_tx, S1};
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

const ONE_ETHER: u128 = 1_000_000_000_000_000_000;

#[test]
fn sanity_check_coherency() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_line_number(true)
        .with_file(true)
        .with_test_writer()
        .init();

    let (round, seq_num, block_policy, chain_config) = genesis_setup();

    let txs = BTreeMap::from([((round, seq_num), vec![])]);
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

fn test_runner(
    chain_config: MonadChainConfig,
    block_policy: TestBlockPolicy,
    state_backend: NopStateBackend,
    txs: BTreeMap<(Round, SeqNum), Vec<Recovered<TxEnvelope>>>,
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
    txs: BTreeMap<(Round, SeqNum), Vec<Recovered<TxEnvelope>>>,
) -> Vec<EthValidatedBlock<NopSignature, MockSignatures<NopSignature>>> {
    let mut blocks = vec![];

    for ((round, seq_num), tx) in txs {
        let body = create_block_body_helper(tx);
        let body_id = body.get_id();
        let txns_root = calculate_transaction_root(&body.execution_body.transactions).0;

        let timestamp = seq_num.0 as u128;
        let base_fees = block_policy
            .compute_base_fee::<EthValidatedBlock<NopSignature, MockSignatures<NopSignature>>>(
                &blocks,
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
        let validated_block = BlockValidator::<
            NopSignature,
            MockSignatures<NopSignature>,
            EthExecutionProtocol,
            TestBlockPolicy,
            NopStateBackend,
            MonadChainConfig,
            MonadChainRevision,
        >::validate(&validator, header, body, None, chain_config)
        .unwrap();

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
) -> TxEnvelope {
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
    transaction.into_signed(signature).into()
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

fn insufficient_single_emptying_transaction_inputs() -> (
    BTreeMap<(Round, SeqNum), Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    let max_fee_per_gas = (3 * ONE_ETHER) / 50_000;

    let tx1 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 0);
    let sender = tx1.signer();

    let txs = BTreeMap::from([
        (
            (GENESIS_ROUND + Round(1), GENESIS_SEQ_NUM + SeqNum(1)),
            vec![],
        ),
        (
            (GENESIS_ROUND + Round(2), GENESIS_SEQ_NUM + SeqNum(2)),
            vec![],
        ),
        (
            (GENESIS_ROUND + Round(3), GENESIS_SEQ_NUM + SeqNum(3)),
            vec![tx1],
        ),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([(sender, U256::from(2 * ONE_ETHER))]),
        ..Default::default()
    };

    (txs, state_backend)
}

fn insufficient_single_emptying_transaction_inputs_2() -> (
    BTreeMap<(Round, SeqNum), Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    let max_fee_per_gas = (3 * ONE_ETHER) / 50_000;

    let tx1 = make_test_tx(signer, 50_000, max_fee_per_gas, 3 * ONE_ETHER, 0);
    let sender = tx1.signer();

    let txs = BTreeMap::from([
        (
            (GENESIS_ROUND + Round(1), GENESIS_SEQ_NUM + SeqNum(1)),
            vec![],
        ),
        (
            (GENESIS_ROUND + Round(2), GENESIS_SEQ_NUM + SeqNum(2)),
            vec![],
        ),
        (
            (GENESIS_ROUND + Round(3), GENESIS_SEQ_NUM + SeqNum(3)),
            vec![tx1],
        ),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([(sender, U256::from(5 * ONE_ETHER))]),
        ..Default::default()
    };

    (txs, state_backend)
}

fn sufficient_single_emptying_transaction_inputs() -> (
    BTreeMap<(Round, SeqNum), Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    let max_fee_per_gas = (1 * ONE_ETHER) / 50_000;

    let tx1 = make_test_tx(signer, 50_000, max_fee_per_gas, 3 * ONE_ETHER, 0);
    let sender = tx1.signer();

    let txs = BTreeMap::from([
        (
            (GENESIS_ROUND + Round(1), GENESIS_SEQ_NUM + SeqNum(1)),
            vec![],
        ),
        (
            (GENESIS_ROUND + Round(2), GENESIS_SEQ_NUM + SeqNum(2)),
            vec![],
        ),
        (
            (GENESIS_ROUND + Round(3), GENESIS_SEQ_NUM + SeqNum(3)),
            vec![tx1],
        ),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([(sender, U256::from(5 * ONE_ETHER))]),
        ..Default::default()
    };

    (txs, state_backend)
}

fn insufficient_emptying_transaction_inputs() -> (
    BTreeMap<(Round, SeqNum), Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    let max_fee_per_gas = (2 * ONE_ETHER) / 50_000;
    let tx1 = make_test_tx(signer, 50_000, max_fee_per_gas, 2 * ONE_ETHER, 0);
    let sender = tx1.signer();

    let tx2 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 1);

    let txs = BTreeMap::from([
        (
            (GENESIS_ROUND + Round(1), GENESIS_SEQ_NUM + SeqNum(1)),
            vec![],
        ),
        (
            (GENESIS_ROUND + Round(2), GENESIS_SEQ_NUM + SeqNum(2)),
            vec![],
        ),
        (
            (GENESIS_ROUND + Round(3), GENESIS_SEQ_NUM + SeqNum(3)),
            vec![tx1, tx2],
        ),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([(sender, U256::from(5 * ONE_ETHER))]),
        ..Default::default()
    };

    (txs, state_backend)
}

fn insufficient_emptying_transaction_inputs_2() -> (
    BTreeMap<(Round, SeqNum), Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    let max_fee_per_gas = (5 * ONE_ETHER) / 50_000;
    let tx1 = make_test_tx(signer, 50_000, max_fee_per_gas, 2 * ONE_ETHER, 0);
    let sender = tx1.signer();

    let max_fee_per_gas = (11 * ONE_ETHER) / 50_000;
    let tx2 = make_test_tx(signer, 50_000, max_fee_per_gas, 0, 1);

    let txs = BTreeMap::from([
        (
            (GENESIS_ROUND + Round(1), GENESIS_SEQ_NUM + SeqNum(1)),
            vec![],
        ),
        (
            (GENESIS_ROUND + Round(2), GENESIS_SEQ_NUM + SeqNum(2)),
            vec![],
        ),
        (
            (GENESIS_ROUND + Round(3), GENESIS_SEQ_NUM + SeqNum(3)),
            vec![tx1, tx2],
        ),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([(sender, U256::from(20 * ONE_ETHER))]),
        ..Default::default()
    };

    (txs, state_backend)
}

fn sufficient_emptying_transaction_inputs() -> (
    BTreeMap<(Round, SeqNum), Vec<Recovered<TxEnvelope>>>,
    NopStateBackend,
) {
    let signer = S1;

    let max_fee_per_gas = (2 * ONE_ETHER) / 50_000;
    let tx1 = make_test_tx(signer, 50_000, max_fee_per_gas, 2 * ONE_ETHER, 0);
    let sender = tx1.signer();

    let max_fee_per_gas = (1 * ONE_ETHER) / 50_000;
    let tx2 = make_test_tx(signer, 50_000, max_fee_per_gas, ONE_ETHER, 1);

    let txs = BTreeMap::from([
        (
            (GENESIS_ROUND + Round(1), GENESIS_SEQ_NUM + SeqNum(1)),
            vec![],
        ),
        (
            (GENESIS_ROUND + Round(2), GENESIS_SEQ_NUM + SeqNum(2)),
            vec![],
        ),
        (
            (GENESIS_ROUND + Round(3), GENESIS_SEQ_NUM + SeqNum(3)),
            vec![tx1, tx2],
        ),
    ]);

    let state_backend = NopStateBackend {
        balances: BTreeMap::from([(sender, U256::from(5 * ONE_ETHER))]),
        ..Default::default()
    };

    (txs, state_backend)
}
