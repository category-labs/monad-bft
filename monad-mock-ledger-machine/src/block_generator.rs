use alloy_consensus::{TxEip1559, TxEnvelope};
use alloy_primitives::{FixedBytes, U256};
use alloy_signer_local::PrivateKeySigner;
use alloy_consensus::SignableTransaction;
use alloy_signer::SignerSync;
use alloy_primitives::Address;
use alloy_primitives::TxKind;
use alloy_consensus::Transaction;

use monad_chain_config::revision::ChainParams;
use monad_eth_block_policy::validation::TFM_MAX_GAS_LIMIT;
use monad_eth_block_policy::compute_txn_max_gas_cost;

use rand::prelude::IteratorRandom;
use rand::{seq::SliceRandom, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rand::prelude::*;

use std::cmp::min;
use std::path::PathBuf;

use crate::runloop_interface_monad::MonadRunloop;

pub struct Block {
    pub txs: Vec<TxEnvelope>,
    pub base_fee: u64,
    pub base_fee_trend: u64,
    pub base_fee_moment: u64,
    pub beneficiary: [u8; 20],
}

#[derive(Clone)]
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
    monad_runloop: MonadRunloop,
    rng: ChaCha8Rng,
    next_eoa_index: u64,
    eoas: Vec<Account>,
}

impl BlockGenerator {
    pub fn new(
        seed: u64,
        accounts: u64,
        chain_id: u64,
        ledger_path: PathBuf,
        db_path: PathBuf,
    ) -> BlockGenerator {
        let mut gen = BlockGenerator {
            monad_runloop: MonadRunloop::new(chain_id, ledger_path, db_path),
            rng: ChaCha8Rng::seed_from_u64(seed),
            next_eoa_index: 1,
            eoas: Vec::default(),
        };
        for _ in 0..accounts {
            gen.add_eoa();
        }
        gen
    }

    pub fn get_monad_runloop(&mut self) -> &mut MonadRunloop {
        &mut self.monad_runloop
    }

    pub fn next_u64(&mut self) -> u64 {
        self.rng.next_u64()
    }

    pub fn next_u256(&mut self) -> U256 {
        U256::from_limbs([
            self.next_u64(),
            self.next_u64(),
            self.next_u64(),
            self.next_u64(),
        ])
    }

    pub fn next_ranged_u64(&mut self, lo: u64, hi: u64) -> u64 {
        (lo..=hi).choose(&mut self.rng).unwrap()
    }

    pub fn next_ranged_u128(&mut self, lo: u128, hi: u128) -> u128 {
        (lo..=hi).choose(&mut self.rng).unwrap()
    }

    pub fn next_ranged_u256(&mut self, lo: U256, hi: U256) -> U256 {
        assert!(lo < hi);
        lo + (self.next_u256() % hi)
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

    fn gen_eoa_index(&mut self) -> usize {
        (0..self.eoas.len()).choose(&mut self.rng).unwrap()
    }

    fn gen_eoa(&mut self) -> &Account {
        let i = self.gen_eoa_index();
        &self.eoas[i]
    }

    fn gen_block_base_fee(&mut self) -> u64 {
        // TODO bias:
        self.next_ranged_u64(0u64, 10u64.pow(12))
    }

    fn set_eoa_balances(&mut self) {
        let mut tmp_eoas = self.eoas.clone();
        tmp_eoas.iter_mut().for_each(|a| {
            a.balance = self.monad_runloop.get_balance(a.address().into());
            // TODO assert ocaml runlopp balance is the same
        });
        self.eoas = tmp_eoas;
    }

    pub fn gen_blocks(
        &mut self,
        chain_id: u64,
        num_of_blocks: u64,
        chain_params: &ChainParams,
    ) -> Vec<Block> {
        self.set_eoa_balances();
        let mut blocks = Vec::default();
        for _ in 0 .. num_of_blocks {
            let base_fee_trend = self.rng.next_u64();
            let base_fee_moment = self.rng.next_u64();
            let base_fee = self.gen_block_base_fee();
            let txs = self.gen_transactions(chain_id, chain_params, base_fee);
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
        self.next_ranged_u64(0u64, block_gas_limit)
    }

    fn get_min_tx_gas(&self) -> u64 {
        // TODO min_tx_gas depends on tx. This only works for the currently generated txs:
        let min_tx_gas = 21000;
        min_tx_gas
    }

    fn gen_gas_limit(&mut self) -> u64 {
        // TODO min_tx_gas depends on tx. This only works for the currently generated txs:
        // TODO bias differntly.
        let min_tx_gas = self.get_min_tx_gas();
        //let max_tx_gas = TFM_MAX_GAS_LIMIT;
        let max_tx_gas = TFM_MAX_GAS_LIMIT / 500;
        self.next_ranged_u64(min_tx_gas, max_tx_gas)
    }

    fn gen_transaction_value(&mut self) -> U256 {
        // TODO bias:
        U256::from(self.next_ranged_u64(0u64, 10u64.pow(18)))
    }

    fn update_transaction_sender(
        &mut self,
        sender_ix: usize,
        value: &U256,
        base_fee: u64,
        txn: &TxEnvelope,
    ) {
        self.eoas[sender_ix].nonce += 1;

        let max_gas_cost = compute_txn_max_gas_cost(txn, base_fee);

        let in_balance = self.eoas[sender_ix].balance;
        let update_value: bool = (self.next_u64() % 10) > 0; // 90%
        let min_balance =
            if update_value {
                max_gas_cost + value
            } else {
                max_gas_cost
            };
        let max_balance = min_balance * U256::from(2);
        // TODO bias:
        let assumed_balance = self.next_ranged_u256(min_balance, max_balance);
        let delta_balance = assumed_balance - in_balance;
        let curr_balance = 
            self.monad_runloop.get_balance(self.eoas[sender_ix].address().into());
        self.monad_runloop.set_balance(
            self.eoas[sender_ix].address().into(),
            curr_balance + delta_balance,
        );
        // TODO ocaml runloop set balance

        self.eoas[sender_ix].balance =
            assumed_balance.saturating_sub(
                U256::from(max_gas_cost) + value);
    }

    pub fn gen_transactions(
        &mut self,
        chain_id: u64,
        chain_params: &ChainParams,
        base_fee: u64,
    ) -> Vec<TxEnvelope> {
        let mut txs = Vec::default();
        let mut gas_left = self.gen_total_gas_limit(chain_params.proposal_gas_limit);
        let min_tx_gas = self.get_min_tx_gas();

        let mut gas_limits = Vec::new();
        while gas_left >= min_tx_gas {
            let mut gas_limit = min(self.gen_gas_limit(), gas_left);
            gas_left -= gas_limit;
            if gas_limit < min_tx_gas ||
                    gas_limits.len() == chain_params.tx_limit - 1 {
                gas_limit += gas_left;
            }
            gas_limits.push(gas_limit);
        }
        gas_limits.shuffle(&mut self.rng);

        for gas_limit in gas_limits.into_iter() {
            let value = self.gen_transaction_value();
            let max_priority_fee_per_gas: u128 = 
                self.next_ranged_u64(0, min(base_fee, 2)).into();
            let max_fee_per_gas: u128 = 
                self.next_ranged_u64(
                    base_fee,
                    base_fee + 2).into();
            let to = TxKind::Call(self.gen_eoa().address().into());
            let sender_ix = self.gen_eoa_index();
            let tx = TxEip1559 {
                chain_id,
                nonce: self.eoas[sender_ix].nonce,
                gas_limit,
                max_priority_fee_per_gas,
                max_fee_per_gas,
                to,
                value,
                access_list: Default::default(),
                input: Default::default(),
            };

            let pk_sender = self.eoas[sender_ix].private_key;
            let tx_sender = FixedBytes(pk_sender);
            let tx_signer = PrivateKeySigner::from_bytes(&tx_sender).unwrap();
            let tx_signature = tx_signer.sign_hash_sync(&tx.signature_hash()).unwrap();
            let signed_tx: TxEnvelope = tx.into_signed(tx_signature).into();

            self.update_transaction_sender(
                sender_ix,
                &value,
                base_fee,
                &signed_tx,
            );

            txs.push(signed_tx);
        }
        txs
    }
}
