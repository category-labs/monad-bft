use monad_mock_ledger_machine::MonadMockLedgerMachine;

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

use std::path::PathBuf;

mod block_generator;
mod faucet;
mod runloop_interface_monad;

use runloop_interface_monad::MonadRunloop;

const CHAIN_ID: u64 = MONAD_DEVNET_CHAIN_ID;

fn get_dummy_tx_eip1559(sender: [u8; 32], nonce: u64) -> TxEnvelope {
    let tx = TxEip1559 {
        chain_id: CHAIN_ID,
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

fn start_balance() -> U256 {
    let mon = U256::from(1_000_000_000_000_000_000u64);
    U256::from(10_000) * mon
}

fn set_balance(runloop: &mut MonadRunloop, user: [u8; 32]) {
    let addr_bytes = FixedBytes(user);
    let addr = PrivateKeySigner::from_bytes(&addr_bytes)
        .unwrap()
        .address();
    runloop.set_balance(addr, start_balance())
}

fn get_balance(runloop: &mut MonadRunloop, user: [u8; 32]) -> U256 {
    let addr_bytes = FixedBytes(user);
    let addr = PrivateKeySigner::from_bytes(&addr_bytes)
        .unwrap()
        .address();
    runloop.get_balance(addr)
}

const USER1: [u8; 32] = [1u8; 32];
const USER2: [u8; 32] = [2u8; 32];
const USER3: [u8; 32] = [3u8; 32];

fn write_blocks(runloop: &mut MonadRunloop, ledger_path: PathBuf) {
    let chain_config = MonadChainConfig::new(CHAIN_ID, None).unwrap();
    let proposer_private_key = [1u8; 32];

    // Build the mock ledger state machine:
    let mut machine = MonadMockLedgerMachine::new(chain_config, ledger_path, proposer_private_key);

    let mut faucet = faucet::Faucet::new();

    set_balance(runloop, USER1);
    set_balance(runloop, USER2);
    set_balance(runloop, USER3);

    assert!(get_balance(runloop, USER1) == start_balance());
    assert!(get_balance(runloop, USER2) == start_balance());
    assert!(get_balance(runloop, USER3) == start_balance());

    // Propose block 1 (without any transactions):
    machine.propose(
        /* txs: */ vec![],
        /* base_fee: */ 100_000_000_000,
        /* base_fee_trend: */ 0,
        /* base_fee_moment: */ 0,
        /* beneficiary: */ [0u8; 20],
    );

    // Propose block 2 (with two transactions from the same sender):
    machine.propose(
        /* txs: */
        vec![
            get_dummy_tx_eip1559(USER1, 0),
            get_dummy_tx_eip1559(USER1, 1),
        ],
        /* base_fee: */ 100_000_000_000,
        /* base_fee_trend: */ 0,
        /* base_fee_moment: */ 0,
        /* beneficiary: */ [0u8; 20],
    );

    // Finalize block 1:
    machine.finalize();

    // Propose block 3 (with three transactions from different senders):
    let tx0 = get_dummy_tx_eip1559(USER1, 2);
    let tx1 = get_dummy_tx_eip1559(USER2, 0);
    let tx2 = get_dummy_tx_eip1559(USER3, 0);
    machine.propose(
        /* txs: */ vec![tx0, tx1, tx2],
        /* base_fee: */ 100_000_000_000,
        /* base_fee_trend: */ 0,
        /* base_fee_moment: */ 0,
        /* beneficiary: */ [0u8; 20],
    );

    // Finalize block 2:
    machine.finalize();

    // Finalize block 3:
    machine.finalize();
}

fn run_finalized_blocks(ledger_path: PathBuf, db_path: PathBuf) {
}

fn main() {
    let ledger_path: PathBuf = "/tmp/ledger".into();
    let db_path: PathBuf = "/dev/triedb".into();

    let mut runloop = MonadRunloop::new(
        CHAIN_ID,
        ledger_path.clone(),
        db_path
    );

    write_blocks(&mut runloop, ledger_path.clone());

    runloop.run(2);

    assert!(get_balance(&mut runloop, USER1) < start_balance());
    assert!(get_balance(&mut runloop, USER2) == start_balance());
    assert!(get_balance(&mut runloop, USER3) == start_balance());

    println!();
    println!("state root after block 2 is\n\t{:x}", runloop.get_state_root());
    println!(
        "balances after block 2 are\n\tuser1: {}\n\tuser2: {}\n\tuser3: {}",
        get_balance(&mut runloop, USER1),
        get_balance(&mut runloop, USER2),
        get_balance(&mut runloop, USER3),
    );

    runloop.run(1);

    assert!(get_balance(&mut runloop, USER1) < start_balance());
    assert!(get_balance(&mut runloop, USER2) < start_balance());
    assert!(get_balance(&mut runloop, USER3) < start_balance());

    println!();
    println!("state root after block 3 is\n\t{:x}", runloop.get_state_root());
    println!(
        "balances after block 2 are\n\tuser1: {}\n\tuser2: {}\n\tuser3: {}",
        get_balance(&mut runloop, USER1),
        get_balance(&mut runloop, USER2),
        get_balance(&mut runloop, USER3),
    );
    println!();
}
