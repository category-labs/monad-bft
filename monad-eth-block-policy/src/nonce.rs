use std::collections::BTreeMap;

use monad_eth_types::{Balance, EthAccount, EthAddress, Nonce};
use monad_types::{SeqNum, GENESIS_SEQ_NUM};

use crate::StateBackend;

#[derive(Debug, Clone)]
pub struct InMemoryAccount {
    pub nonce: Nonce,
    pub balance: Balance
}

impl Default for InMemoryAccount {
    fn default() -> Self {
        Self {
            nonce: 0,
            balance: Balance::MAX
        }
    }
}

#[derive(Debug, Clone)]
pub struct PreviousTxn {
    pub seq_num: SeqNum,
    pub address: EthAddress,
    pub reserve_balance: Balance 
}

#[derive(Debug, Clone)]
pub struct InMemoryState {
    accounts: BTreeMap<EthAddress, InMemoryAccount>,
    last_state: SeqNum,
    execution_delay: u64,
    previous_txns: Vec<PreviousTxn>
}

impl Default for InMemoryState {
    fn default() -> Self {
        Self {
            accounts: Default::default(),
            last_state: GENESIS_SEQ_NUM,
            execution_delay: 4,
            previous_txns: Vec::new()
        }
    }
}

impl InMemoryState {
    pub fn new(
        existing_accounts: impl IntoIterator<Item = (EthAddress, InMemoryAccount)>,
        last_state: u64,
        execution_delay: u64
    ) -> Self {
        Self {
            accounts: existing_accounts.into_iter().collect(),
            last_state: SeqNum(last_state),
            execution_delay,
            previous_txns: Vec::new()
        }
    }

    // new_account_nonces is the changeset of nonces from a given block
    // if account A's last tx nonce in a block is N, then new_account_nonces should include A=N+1
    // this is because N+1 is the next valid nonce for A
    pub fn update_committed_accounts(
        &mut self,
        seq_num: SeqNum,
        new_accounts_txns: BTreeMap<EthAddress, (InMemoryAccount, PreviousTxn)>,
    ) {
        for (address, acc_txn) in new_accounts_txns {
            self.accounts.insert(address, acc_txn.0);
            self.previous_txns.push(acc_txn.1);
        }
        self.last_state = seq_num;

        while !self.previous_txns.is_empty() && self.previous_txns[0].seq_num.0 <= self.last_state.0 - self.execution_delay {
            let tx = &self.previous_txns[0];
            if let Some(acc) = self.accounts.get_mut(&tx.address) {
                acc.balance += tx.reserve_balance;
            }
            self.previous_txns.pop();
        }
    }

    pub fn get_previous_txns(
        &mut self
    ) -> &mut Vec<PreviousTxn> {
        self.previous_txns.as_mut()
    }
}

impl StateBackend for InMemoryState {
    fn raw_read_account(&self, block: SeqNum, address: &EthAddress) -> Option<EthAccount> {
        assert!(block <= self.last_state, "block = {}, last_state = {}", block.0, self.last_state.0);
        let account = self.accounts.get(address)?;
        Some(EthAccount {
            nonce: account.nonce,
            balance: account.balance,
            code_hash: None,
        })
    }

    fn raw_read_earliest_block(&self) -> SeqNum {
        SeqNum::MIN
    }

    fn raw_read_latest_block(&self) -> SeqNum {
        self.last_state
    }
}
