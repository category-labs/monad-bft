use alloy_consensus::{TxEip1559, TxEnvelope};
use alloy_primitives::{Address, FixedBytes, U256};
use alloy_signer_local::PrivateKeySigner;
use alloy_consensus::SignableTransaction;
use alloy_signer::SignerSync;
use alloy_primitives::TxKind;
use monad_chain_config::MONAD_MAINNET_CHAIN_ID;
use monad_eth_block_policy::validation::TFM_MAX_GAS_LIMIT;

use rand::prelude::IteratorRandom;
use rand::{seq::SliceRandom, Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rand::prelude::*;

pub struct Block {
    pub txs: Vec<TxEnvelope>,
    pub base_fee: u64,
    pub base_fee_trend: u64,
    pub base_fee_moment: u64,
    pub beneficiary: [u8; 20],
}

pub struct Account {
    private_key: [u8; 32],
    nonce: u64,
    balance: U256,
}

impl Account {
    fn address(&self) -> [u8; 20] {
        let pk = FixedBytes(self.private_key);
        PrivateKeySigner::from_bytes(&pk).unwrap().address().into()
    }
}

pub struct BlockGenerator {
    rng: ChaCha8Rng,
    next_eoa_index: u64,
    eoas: Vec<Account>,
}

impl BlockGenerator {
    pub fn new(seed: u64, accounts: u64) -> BlockGenerator {
        let mut gen = BlockGenerator {
            rng: ChaCha8Rng::seed_from_u64(seed),
            next_eoa_index: 1,
            eoas: Vec::default(),
        };
        for _ in 0..accounts {
            gen.add_eoa();
        }
        gen
    }

    pub fn next_u64(&mut self) -> u64 {
        return self.rng.next_u64()
    }

    fn add_eoa(&mut self) {
        let a = self.next_eoa_index;
        self.next_eoa_index += 1;

        let mut pk = [0u8; 32];
        for i in 0..8 {
            pk[i] = (a >> (i * 8)) as u8;
        }

        self.eoas.push(Account{
            private_key: pk,
            nonce: 0,
            balance: U256::from(0),
        });
    }

    fn gen_eoa(&mut self) -> &mut Account {
        match self.eoas.choose_mut(&mut self.rng) {
            Some(x) => x,
            None => panic!("gen_eoa"),
        }
    }

    fn gen_block_base_fee(&mut self) -> u64 {
        // TODO bias:
        (0u64 .. 10u64.pow(18)).choose(&mut self.rng).unwrap()
    }

    pub fn gen_blocks(&mut self, num_of_blocks: u64, gas_limit: u64) -> Vec<Block> {
        // TODO: Set balance of all EOAs by query the execution clients.
        // Verify the execution clients agree on balance.
        let mut blocks = Vec::default();
        for _ in 0 .. num_of_blocks {
            let base_fee_trend = self.rng.next_u64();
            let base_fee_moment = self.rng.next_u64();
            let base_fee = self.gen_block_base_fee();
            let txs = self.gen_transactions(gas_limit, base_fee);
            let beneficiary = self.gen_eoa().address();
            blocks.push(Block {
                txs,
                base_fee,
                base_fee_trend,
                base_fee_moment,
                beneficiary,
            });
        }
        blocks
    }

    fn gen_total_gas_limit(&mut self, block_gas_limit: u64) -> u64 {
        (0u64 .. block_gas_limit).choose(&mut self.rng).unwrap()
    }

    fn get_min_tx_gas(&self) -> u64 {
        // TODO min_tx_gas depends on tx. This only works for the currently generated txs:
        let min_tx_gas = 21000;
        min_tx_gas
    }

    fn gen_gas_limit(&mut self) -> u64 {
        // TODO min_tx_gas depends on tx. This only works for the currently generated txs:
        let min_tx_gas = self.get_min_tx_gas();
        let max_tx_gas = TFM_MAX_GAS_LIMIT;
        // TODO bias:
        (min_tx_gas .. max_tx_gas).choose(&mut self.rng).unwrap()
    }

    fn gen_transaction_value(&mut self) -> U256 {
        // TODO bias:
        U256::from((0u64 .. 10u64.pow(18)).choose(&mut self.rng).unwrap())
    }

    fn gen_transaction_sender(
        &mut self,
        value: &U256,
        gas_limit: u64
    ) -> (u64, [u8; 32]) {
        let sender = self.gen_eoa();
        let x = sender.nonce;
        sender.nonce += 1;
        // TODO if sender.balance < gas_limit, then call
        // execution clients to increment balance.
        // Then increment sender.balance accordingly.
        // TODO if sender.balance < gas_limit + value,
        // with 90% probability increment balance.
        // Then increment sender.balance accordingly.
        (x, sender.private_key)
    }

    pub fn gen_transactions(
        &mut self,
        block_gas_limit: u64,
        base_fee: u64
    ) -> Vec<TxEnvelope> {
        let mut txs = Vec::default();
        let mut gas_left = self.gen_total_gas_limit(block_gas_limit);
        let min_tx_gas = self.get_min_tx_gas();
        while gas_left >= min_tx_gas {
            let value = self.gen_transaction_value();
            let mut gas_limit = self.gen_gas_limit();
            gas_left -= gas_limit;
            if gas_limit < min_tx_gas {
                gas_limit += gas_left;
            }
            let (nonce, pk) = self.gen_transaction_sender(&value, gas_limit);
            let max_fee_per_gas: u128 = 
                (base_fee .. base_fee + 2).choose(&mut self.rng).unwrap().into();
            let max_priority_fee_per_gas: u128 = 
                (max_fee_per_gas .. max_fee_per_gas + 2).choose(&mut self.rng).unwrap().into();
            let to = TxKind::Call(self.gen_eoa().address().into());
            let tx = TxEip1559 {
                chain_id: MONAD_MAINNET_CHAIN_ID,
                nonce,
                gas_limit,
                max_priority_fee_per_gas,
                max_fee_per_gas,
                to,
                value,
                access_list: Default::default(),
                input: Default::default(),
            };
            let tx_sender = FixedBytes(pk);
            let tx_signer = PrivateKeySigner::from_bytes(&tx_sender).unwrap();
            let tx_signature = tx_signer.sign_hash_sync(&tx.signature_hash()).unwrap();
            txs.push(tx.into_signed(tx_signature).into());
        }
        txs.shuffle(&mut self.rng);
        txs
    }
}
