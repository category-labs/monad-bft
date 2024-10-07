use monad_eth_tx::EthTransaction;
use reth_primitives::{Transaction, TxEip1559, TxEip2930, TxEip4844, TxLegacy};

pub fn effective_tip_per_gas(transaction: &EthTransaction) -> u128 {
    match transaction.transaction {
        Transaction::Legacy(TxLegacy { gas_price, .. })
        | Transaction::Eip2930(TxEip2930 { gas_price, .. }) => gas_price,
        Transaction::Eip1559(TxEip1559 {
            max_priority_fee_per_gas,
            ..
        })
        | Transaction::Eip4844(TxEip4844 {
            max_priority_fee_per_gas,
            ..
        }) => max_priority_fee_per_gas,
    }
}
