use monad_chain_config::{revision::ChainRevision, ChainConfig, MonadChainConfig, MONAD_DEVNET_CHAIN_ID};
use monad_mock_ledger_machine::MonadMockLedgerMachine;
use monad_mock_ledger_machine::BlockType;
use alloy_consensus::Transaction;
use std::collections::VecDeque;
use std::time::Instant;

mod runloop_interface_monad;
mod block_generator;
use block_generator::BlockGenerator;

fn take_gas_sum(blocks: &VecDeque<BlockType>, n: u64) -> u64 {
    assert!(blocks.len() >= n.try_into().unwrap());
    let mut total = 0u64;
    for i in 0..n {
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
    // TODO init monad runloop
    // TODO init ocaml runloop

    // TODO pass runloop instances to machine

    let chain_config = MonadChainConfig::new(MONAD_DEVNET_CHAIN_ID, None).unwrap();
    let ledger_path = "/tmp/ledger".into();
    let proposer_private_key = [1u8; 32];
    let mut machine = MonadMockLedgerMachine::new(chain_config, ledger_path, proposer_private_key);

    let mut generator = BlockGenerator::new(0, 5);

    let mut delta = 0u64;

    // TODO loop:
    {
        let propose_num = 1 + (generator.next_u64() % 1);
        delta += propose_num;
        let finalize_num = 1 + (generator.next_u64() % delta);
        delta -= finalize_num;
        let gas_limit = chain_config
            .get_chain_revision(machine.get_round())
            .chain_params()
            .proposal_gas_limit;
        let blocks = generator.gen_blocks(propose_num, gas_limit);
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

        // TODO call monad runloop

        let time = start_instant.elapsed();

        // TODO assert instead of print:
        println!(
            "Total {} gas in {} blocks took {} ns",
            total_gas,
            finalize_num,
            time.as_nanos());

        // TODO call ocaml runloop

        // TODO compare state roots
    }
}
