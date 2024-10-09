use std::{collections::VecDeque, time::Duration};

use reth_primitives::{Signature, Transaction};
use tokio::time::Instant;

use crate::{account::PrivateKey, state::ChainAccountState};

#[derive(Debug)]
pub struct EthAccount {
    key: PrivateKey,
    next_nonce: u64,
    pending_tx_timestamp_retries: VecDeque<(Instant, usize)>,
}

impl EthAccount {
    pub fn new(key: PrivateKey) -> Self {
        Self {
            key,
            next_nonce: 0,
            pending_tx_timestamp_retries: VecDeque::default(),
        }
    }

    pub fn sign_transaction(&self, transaction: &Transaction) -> Signature {
        self.key.sign_transaction(transaction)
    }

    pub fn get_available_nonce(
        &mut self,
        account_state: &ChainAccountState,
        max_inflight: usize,
    ) -> Option<u64> {
        self.update_nonce(account_state);

        for nonce_idx in 0..max_inflight {
            if let Some(nonce) = self.get_available_nonce_at_idx(nonce_idx) {
                return Some(nonce);
            }
        }

        None
    }

    pub fn get_available_nonces<const MAX_NONCES: usize>(
        &mut self,
        account_state: &ChainAccountState,
        max_inflight: usize,
    ) -> Vec<u64> {
        self.update_nonce(account_state);

        let mut tx_nonces = Vec::default();

        for nonce_idx in 0..max_inflight {
            if tx_nonces.len() == MAX_NONCES {
                break;
            }

            if let Some(tx_nonce) = self.get_available_nonce_at_idx(nonce_idx) {
                tx_nonces.push(tx_nonce);
            }
        }

        assert!(
            tx_nonces.len() <= MAX_NONCES,
            "tx nonces length cannot exceed max nonces"
        );

        tx_nonces
    }

    fn update_nonce(&mut self, account_state: &ChainAccountState) {
        let nonce = account_state.get_nonce();

        while self.next_nonce < nonce {
            self.next_nonce = self
                .next_nonce
                .checked_add(1)
                .expect("last nonce does not overflow");

            if self.pending_tx_timestamp_retries.pop_front().is_none() {
                break;
            }
        }

        assert!((nonce == self.next_nonce) || self.pending_tx_timestamp_retries.is_empty());

        self.next_nonce = nonce;
    }

    fn get_available_nonce_at_idx(&mut self, nonce_idx: usize) -> Option<u64> {
        let tx_nonce = self
            .next_nonce
            .checked_add(nonce_idx.try_into().expect("nonce idx is valid u64"))
            .expect("tx nonce does not overflow");

        let Some((existing_tx_timesstamp, existing_tx_retries)) =
            self.pending_tx_timestamp_retries.get_mut(nonce_idx)
        else {
            assert_eq!(self.pending_tx_timestamp_retries.len(), nonce_idx);

            self.pending_tx_timestamp_retries
                .push_back((Instant::now(), 0));

            return Some(tx_nonce);
        };

        let now = Instant::now();

        if now
            < existing_tx_timesstamp
                .checked_add(Duration::from_secs(10))
                .expect("now does not overflow")
        {
            return None;
        }

        *existing_tx_timesstamp = now;
        *existing_tx_retries += 1;

        if *existing_tx_retries >= 5 {
            panic!("retried nonce at least five times :(");
        }

        Some(tx_nonce)
    }
}
