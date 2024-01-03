use monad_consensus_types::{payload::FullTransactionList, txpool::TxPool};
use monad_eth_types::EthTransaction;
use rand::RngCore;
use rand_chacha::{rand_core::SeedableRng, ChaCha20Rng};

const MOCK_DEFAULT_SEED: u64 = 1;
const TXN_SIZE: usize = 32;

pub struct MockTxPool {
    rng: ChaCha20Rng,
}

impl TxPool for MockTxPool {
    fn new() -> Self {
        Self {
            rng: ChaCha20Rng::seed_from_u64(MOCK_DEFAULT_SEED),
        }
    }

    fn insert_tx(&mut self, _tx: EthTransaction) {}

    fn create_proposal(
        &mut self,
        tx_limit: usize,
        _gas_limit: u64,
        _pending_txs: Vec<FullTransactionList>,
    ) -> monad_consensus_types::payload::FullTransactionList {
        if tx_limit == 0 {
            FullTransactionList::empty()
        } else {
            // Random non-empty value with size = num_fetch_txs * hash_size
            let mut buf = Vec::with_capacity(tx_limit * TXN_SIZE);
            buf.resize(tx_limit * TXN_SIZE, 0);
            self.rng.fill_bytes(buf.as_mut_slice());
            FullTransactionList::new(buf.into())
        }
    }
}
