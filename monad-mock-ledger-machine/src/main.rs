use alloy_consensus::Transaction;

use monad_chain_config::{revision::ChainRevision, ChainConfig, MonadChainConfig, MONAD_DEVNET_CHAIN_ID};
use monad_mock_ledger_machine::MonadMockLedgerMachine;
use monad_mock_ledger_machine::BlockType;

use std::collections::VecDeque;
use std::time::Instant;
use std::path::PathBuf;

mod runloop;
use runloop::Runloop;
mod runloop_interface_ocaml;
mod runloop_interface_monad;
mod block_generator;
use block_generator::BlockGenerator;

const CHAIN_ID: u64 = MONAD_DEVNET_CHAIN_ID;

fn take_gas_sum(blocks: &VecDeque<BlockType>, n: u64) -> u64 {
    assert!(blocks.len() >= n.try_into().unwrap());
    let mut total = 0u64;
    for _ in 0..n {
        total +=
            blocks[0].body()
            .execution_body
            .transactions
            .iter()
            .map(|t| t.gas_limit())
            .sum::<u64>();
    }
    total
}

fn main() {
    let chain_config = MonadChainConfig::new(CHAIN_ID, None).unwrap();
    let ledger_path: PathBuf = "/tmp/ledger".into();
    let db_path: PathBuf = "/dev/triedb".into();
    let proposer_private_key = [1u8; 32];
    let mut machine = MonadMockLedgerMachine::new(
        chain_config,
        ledger_path.clone(),
        proposer_private_key);

    let mut generator = BlockGenerator::new(
        0,
        5,
        CHAIN_ID,
        ledger_path,
        db_path,
    );

    let mut delta = 0u64;

    for _ in 0 .. 20 {
        let propose_num = 1 + (generator.next_u64() % 10);
        delta += propose_num;
        let finalize_num = 1 + (generator.next_u64() % delta);
        delta -= finalize_num;
        let chain_params = chain_config
            .get_chain_revision(machine.get_round())
            .chain_params();
        let blocks = generator.gen_blocks(CHAIN_ID, propose_num, &chain_params);
        for b in blocks.into_iter() {
            machine.propose(
                b.txs,
                b.base_fee,
                b.base_fee_trend,
                b.base_fee_moment,
                b.beneficiary,
            );
        }

        let total_gas = take_gas_sum(machine.unfinalized_blocks(), finalize_num);

        for _ in 0..finalize_num {
            machine.finalize();
        }
        let start_instant = Instant::now();

        let nblocks = 1 + (generator.next_u64() % finalize_num);
        generator.get_monad_runloop().run(nblocks);

        let time = start_instant.elapsed();

        // TODO assert instead of print:
        println!(
            "Total {} gas in {} blocks took {} ns",
            total_gas,
            finalize_num,
            time.as_nanos());

        // TODO call ocaml runloop

        // TODO compare state roots
        println!(
            "Monad state root = {:x}",
            generator.get_monad_runloop().get_state_root());
    }
}
