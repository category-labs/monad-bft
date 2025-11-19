use monad_mock_ledger_machine::MonadMockLedgerMachine;

use monad_chain_config::execution_revision::MonadExecutionRevision;
use monad_chain_config::revision::MonadChainRevision;
use monad_chain_config::{MonadChainConfig, MONAD_DEVNET_CHAIN_ID};

use alloy_consensus::SignableTransaction;
use alloy_consensus::TxEip1559;
use alloy_consensus::TxEnvelope;
use alloy_primitives::Address;
use alloy_primitives::FixedBytes;
use alloy_primitives::TxKind;
use alloy_primitives::U256;
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;

fn get_dummy_tx_eip1559(sender: [u8; 32], nonce: u64) -> TxEnvelope {
    let tx = TxEip1559 {
        chain_id: MONAD_DEVNET_CHAIN_ID,
        nonce,
        gas_limit: 30000,
        max_priority_fee_per_gas: 20,
        max_fee_per_gas: 100_000_000_000,
        to: TxKind::Call(Address::repeat_byte(0u8)),
        value: U256::from(0),
        access_list: Default::default(),
        input: vec![0; 10].into(),
    };
    let tx_sender = FixedBytes(sender);
    let tx_signer = PrivateKeySigner::from_bytes(&tx_sender).unwrap();
    let tx_signature = tx_signer.sign_hash_sync(&tx.signature_hash()).unwrap();
    tx.into_signed(tx_signature).into()
}

fn main() {
    let execution_revision = MonadExecutionRevision::V_FOUR;
    let chain_revision = MonadChainRevision::V_0_11_0;
    let chain_config = MonadChainConfig::new(MONAD_DEVNET_CHAIN_ID, None).unwrap();
    let ledger_path = "/tmp/ledger".into();
    let proposer_private_key = [1u8; 32];

    // Build the mock ledger state machine:
    let mut machine = MonadMockLedgerMachine::new(
        execution_revision,
        chain_revision,
        chain_config,
        ledger_path,
        proposer_private_key,
    );

    // Propose block 1 (without transactions):
    machine.propose(
        /* txs: */ vec![],
        /* base_fee: */ 100_000_000_000,
        /* base_fee_trend: */ 0,
        /* base_fee_moment: */ 0,
        /* tx_limit: */ 128,
        /* beneficiary: */ [0u8; 20],
    );

    // Propose block 2 (with one transaction):
    machine.propose(
        /* txs: */ vec![get_dummy_tx_eip1559([1u8; 32], 0)],
        /* base_fee: */ 100_000_000_000,
        /* base_fee_trend: */ 0,
        /* base_fee_moment: */ 0,
        /* tx_limit: */ 128,
        /* beneficiary: */ [0u8; 20],
    );

    // Finalize block 1:
    machine.finalize();

    // Propose block 3 (with three transaction):
    let tx0 = get_dummy_tx_eip1559([1u8; 32], 1);
    let tx1 = get_dummy_tx_eip1559([2u8; 32], 0);
    let tx2 = get_dummy_tx_eip1559([3u8; 32], 0);
    machine.propose(
        /* txs: */ vec![tx0, tx1, tx2],
        /* base_fee: */ 100_000_000_000,
        /* base_fee_trend: */ 0,
        /* base_fee_moment: */ 0,
        /* tx_limit: */ 128,
        /* beneficiary: */ [0u8; 20],
    );

    // Finalize block 2:
    machine.finalize();

    // Finalize block 3:
    machine.finalize();
}
