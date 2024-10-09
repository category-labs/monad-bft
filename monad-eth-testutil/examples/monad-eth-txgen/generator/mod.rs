use std::time::Duration;

use config::EthTxActivityType;
use reth_primitives::{
    AccessList, Address, Transaction, TransactionKind, TransactionSigned, TxEip1559, U256,
};
use ruint::Uint;
use tokio::time::Instant;

pub use self::config::EthTxGeneratorConfig;
use self::{account::EthAccount, account_pool::AccountPool, config::EthTxAddressConfig};
use crate::{account::PrivateKey, state::ChainStateView};

mod account;
mod account_pool;
mod config;

pub struct EthTxGenerator {
    root_account: (Address, EthAccount),
    account_pool: AccountPool,
    chain_state: ChainStateView,
    activity: EthTxActivityType,
}

impl EthTxGenerator {
    pub async fn new(config: EthTxGeneratorConfig, chain_state: ChainStateView) -> Self {
        let EthTxGeneratorConfig {
            root_private_key,
            addresses: EthTxAddressConfig { from, to },
            activity,
        } = config;

        let (root_account_address, root_account) = PrivateKey::new(root_private_key);
        chain_state.add_new_account(root_account_address).await;

        let account_pool = AccountPool::new_with_config(from, to, &chain_state).await;

        Self {
            root_account: (root_account_address, EthAccount::new(root_account)),
            account_pool,
            chain_state,
            activity,
        }
    }

    pub async fn generate(&mut self) -> (Vec<TransactionSigned>, Instant) {
        let mut txs = Vec::default();

        match &self.activity {
            EthTxActivityType::NativeTokenTransfer { quantity } => {
                let () = self
                    .chain_state
                    .with_ro(|chain_state| {
                        self.account_pool.iter_random(
                            0xFFFF,
                            |from_address, from_account, to_address| {
                                if txs.len() >= 512 {
                                    return false;
                                }

                                let Some(from_account_state) =
                                    chain_state.get_account(&from_address)
                                else {
                                    return true;
                                };

                                if from_account_state
                                    .get_balance()
                                    .le(&Uint::from(1_000_000_000u64))
                                {
                                    let Some(root_account_state) =
                                        chain_state.get_account(&self.root_account.0)
                                    else {
                                        return true;
                                    };

                                    let Some(nonce) = self
                                        .root_account
                                        .1
                                        .get_available_nonce(root_account_state, 512)
                                    else {
                                        return true;
                                    };

                                    let transaction = Transaction::Eip1559(TxEip1559 {
                                        chain_id: 41454,
                                        nonce,
                                        gas_limit: 21_000,
                                        max_fee_per_gas: 1_000,
                                        max_priority_fee_per_gas: 0,
                                        to: TransactionKind::Call(to_address),
                                        value: U256::from(1_000_000_000_000u128).into(),
                                        access_list: AccessList::default(),
                                        input: Vec::default().into(),
                                    });

                                    let signature =
                                        self.root_account.1.sign_transaction(&transaction);

                                    let signed_transaction =
                                        TransactionSigned::from_transaction_and_signature(
                                            transaction,
                                            signature,
                                        );

                                    txs.push(signed_transaction);
                                    return true;
                                }

                                let Some(nonce) =
                                    from_account.get_available_nonce(from_account_state, 6)
                                else {
                                    return true;
                                };

                                let transaction = Transaction::Eip1559(TxEip1559 {
                                    chain_id: 41454,
                                    nonce,
                                    gas_limit: 21_000,
                                    max_fee_per_gas: 1_000,
                                    max_priority_fee_per_gas: 0,
                                    to: TransactionKind::Call(to_address),
                                    value: quantity.to_owned().into(),
                                    access_list: AccessList::default(),
                                    input: Vec::default().into(),
                                });

                                let signature = from_account.sign_transaction(&transaction);

                                let signed_transaction =
                                    TransactionSigned::from_transaction_and_signature(
                                        transaction,
                                        signature,
                                    );

                                txs.push(signed_transaction);

                                true
                            },
                        )
                    })
                    .await;
            }
            EthTxActivityType::Erc20TokenTransfer { contract, quantity } => unimplemented!(),
        }

        (
            txs,
            Instant::now()
                .checked_add(Duration::from_millis(1))
                .unwrap(),
        )
    }
}
