use rand::{rngs::StdRng, Rng, SeedableRng};
use reth_primitives::{
    Address, Signature, Transaction, TransactionSigned, TransactionSignedEcRecovered, TxLegacy,
};

pub fn create_signed_eth_txs(seed: u64, count: u16) -> Vec<TransactionSignedEcRecovered> {
    create_eth_txs(seed, count)
        .into_iter()
        .map(|tx| {
            TransactionSignedEcRecovered::from_signed_transaction(
                TransactionSigned::from_transaction_and_signature(tx, Signature::default()),
                Address::default(),
            )
        })
        .collect()
}

pub fn create_eth_txs(seed: u64, count: u16) -> Vec<Transaction> {
    let mut rng = StdRng::seed_from_u64(seed);

    (0..count)
        .map(|_| {
            Transaction::Legacy(TxLegacy {
                value: rng.gen(),
                ..Default::default()
            })
        })
        .collect()
}
