use duplicates::DuplicateTxGenerator;
use high_call_data::HighCallDataTxGenerator;
use non_deterministic_storage::NonDeterministicStorageTxGenerator;
use storage_deletes::StorageDeletesTxGenerator;

use crate::{prelude::*, shared::erc20::ERC20, GeneratorConfig, TxType};

mod duplicates;
mod high_call_data;
mod non_deterministic_storage;
mod storage_deletes;

pub fn make_generator(config: &Config, erc20: ERC20) -> Box<dyn Generator + Send + Sync> {
    let recipient_keys = SeededKeyPool::new(config.recipients, config.recipient_seed);
    let tx_per_sender = config.tx_per_sender();
    match config.generator_config {
        GeneratorConfig::NullGen => Box::new(NullGen),
        GeneratorConfig::FewToMany { tx_type } => Box::new(CreateAccountsGenerator {
            recipient_keys,
            tx_type,
            erc20,
            tx_per_sender,
        }),
        GeneratorConfig::ManyToMany { tx_type } => Box::new(ManyToManyGenerator {
            recipient_keys,
            tx_type,
            tx_per_sender,
            erc20,
        }),
        GeneratorConfig::Duplicates => Box::new(DuplicateTxGenerator {
            recipient_keys,
            tx_per_sender,
            random_priority_fee: false,
        }),
        GeneratorConfig::RandomPriorityFee => Box::new(DuplicateTxGenerator {
            recipient_keys,
            tx_per_sender,
            random_priority_fee: true,
        }),
        GeneratorConfig::HighCallData => Box::new(HighCallDataTxGenerator {
            recipient_keys,
            tx_per_sender,
        }),
        GeneratorConfig::NonDeterministicStorage => Box::new(NonDeterministicStorageTxGenerator {
            recipient_keys,
            tx_per_sender,
            erc20,
        }),
        GeneratorConfig::StorageDeletes => Box::new(StorageDeletesTxGenerator {
            recipient_keys,
            tx_per_sender,
            erc20,
        }),
    }
}

struct NullGen;
impl Generator for NullGen {
    fn handle_acct_group(
        &mut self,
        _accts: &mut [SimpleAccount],
    ) -> Vec<(TransactionSigned, Address)> {
        vec![]
    }
}

struct ManyToManyGenerator {
    recipient_keys: SeededKeyPool,
    tx_per_sender: usize,
    tx_type: TxType,
    pub erc20: ERC20,
}

impl Generator for ManyToManyGenerator {
    fn handle_acct_group(
        &mut self,
        accts: &mut [SimpleAccount],
    ) -> Vec<(TransactionSigned, Address)> {
        let mut idxs: Vec<usize> = (0..accts.len()).collect();
        let mut rng = SmallRng::from_entropy();
        let mut txs = Vec::with_capacity(self.tx_per_sender * accts.len());

        for _ in 0..self.tx_per_sender {
            idxs.shuffle(&mut rng);

            for &idx in &idxs {
                let sender = &mut accts[idx];
                let to = self.recipient_keys.next_addr(); // change sampling strategy?

                let tx = match self.tx_type {
                    TxType::ERC20 => erc20_transfer(sender, to, U256::from(10), &self.erc20),
                    TxType::Native => native_transfer(sender, to, U256::from(10)),
                };

                txs.push((tx, to));
            }
        }

        txs
    }
}

struct CreateAccountsGenerator {
    pub recipient_keys: SeededKeyPool,
    pub tx_per_sender: usize,
    pub tx_type: TxType,
    pub erc20: ERC20,
}

impl Generator for CreateAccountsGenerator {
    fn handle_acct_group(
        &mut self,
        accts: &mut [SimpleAccount],
    ) -> Vec<(TransactionSigned, Address)> {
        let mut txs = Vec::with_capacity(accts.len());

        for sender in accts {
            for _ in 0..self.tx_per_sender {
                let to = self.recipient_keys.next_addr();

                let tx = match self.tx_type {
                    TxType::ERC20 => erc20_transfer(sender, to, U256::from(10), &self.erc20),
                    TxType::Native => native_transfer(sender, to, U256::from(10)),
                };

                txs.push((tx, to));
            }
        }

        txs
    }
}

pub fn native_transfer(from: &mut SimpleAccount, to: Address, amt: U256) -> TransactionSigned {
    let tx = reth_primitives::Transaction::Eip1559(reth_primitives::TxEip1559 {
        chain_id: 41454,
        nonce: from.nonce,
        gas_limit: 21_000,
        max_fee_per_gas: 2_000,
        max_priority_fee_per_gas: 0,
        to: reth_primitives::TransactionKind::Call(to),
        value: amt.into(),
        access_list: reth_primitives::AccessList::default(),
        input: Default::default(),
    });

    // update from
    from.nonce += 1;
    from.native_bal -= amt + U256::from(21_000 * 1_000);

    let sig = from.key.sign_transaction(&tx);
    TransactionSigned::from_transaction_and_signature(tx, sig)
}

pub fn native_transfer_priority_fee(
    from: &mut SimpleAccount,
    to: Address,
    amt: U256,
    priority_fee: u128,
) -> TransactionSigned {
    let tx = reth_primitives::Transaction::Eip1559(reth_primitives::TxEip1559 {
        chain_id: 41454,
        nonce: from.nonce,
        gas_limit: 21_000,
        max_fee_per_gas: 2_000,
        max_priority_fee_per_gas: priority_fee,
        to: reth_primitives::TransactionKind::Call(to),
        value: amt.into(),
        access_list: reth_primitives::AccessList::default(),
        input: Default::default(),
    });

    // update from
    from.nonce += 1;
    from.native_bal -= amt + U256::from(21_000 * 1_000);

    let sig = from.key.sign_transaction(&tx);
    TransactionSigned::from_transaction_and_signature(tx, sig)
}

pub fn erc20_transfer(
    from: &mut SimpleAccount,
    to: Address,
    amt: U256,
    erc20: &ERC20,
) -> TransactionSigned {
    let tx = erc20.construct_transfer(&from.key, to, from.nonce, amt);

    // update from
    from.nonce += 1;
    from.native_bal -= U256::from(400_000 * 1_000); // todo: wire gas correctly, but doesn't really matter given refresher and gen_harness will ensure enough balance
    from.erc20_bal -= amt;

    tx
}

pub fn erc20_mint(from: &mut SimpleAccount, erc20: &ERC20) -> TransactionSigned {
    let tx = erc20.construct_mint(&from.key, from.nonce);

    // update from
    from.nonce += 1;
    from.native_bal -= U256::from(400_000 * 1_000); // todo: wire gas correctly, see above comment
    from.erc20_bal += U256::from(10_u128.pow(30)); // todo: current erc20 impl just mints a constant
    tx
}
