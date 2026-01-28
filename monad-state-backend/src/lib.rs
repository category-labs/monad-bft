// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use alloy_primitives::Address;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::{EthAccount, EthHeader};
use monad_types::{Balance, BlockId, Epoch, Nonce, Round, SeqNum, Stake};
use monad_validator::signature_collection::{SignatureCollection, SignatureCollectionPubKeyType};
use serde::{Deserialize, Serialize};

pub use self::{
    in_memory::{InMemoryBlockState, InMemoryState, InMemoryStateInner},
    mock::NopStateBackend,
    thread::StateBackendThreadClient,
};

mod in_memory;
mod mock;
mod thread;

#[derive(Debug, PartialEq)]
pub enum StateBackendError {
    /// not available yet
    NotAvailableYet,
    /// will never be available
    NeverAvailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReserveBalanceState {
    pub settled: Balance,
    pub pending: Option<Balance>,
    pub pending_block: SeqNum,
}

impl ReserveBalanceState {
    pub fn delayed_reserve_balance(
        &self,
        block_seq_num: SeqNum,
        execution_delay: SeqNum,
    ) -> Balance {
        match self.pending {
            Some(pending)
                if self.pending_block + execution_delay <= block_seq_num =>
            {
                pending
            }
            _ => self.settled,
        }
    }
}

/// Backend provider of account data: balance and nonce
pub trait StateBackend<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn get_account_statuses<'a>(
        &self,
        block_id: &BlockId,
        seq_num: &SeqNum,
        is_finalized: bool,
        addresses: impl Iterator<Item = &'a Address>,
    ) -> Result<Vec<Option<EthAccount>>, StateBackendError>;

    fn get_execution_result(
        &self,
        block_id: &BlockId,
        seq_num: &SeqNum,
        is_finalized: bool,
    ) -> Result<EthHeader, StateBackendError>;

    fn get_reserve_balance_states<'a>(
        &self,
        _block_id: &BlockId,
        _seq_num: &SeqNum,
        _is_finalized: bool,
        addresses: impl Iterator<Item = &'a Address>,
    ) -> Result<Vec<Option<ReserveBalanceState>>, StateBackendError> {
        Ok(addresses.map(|_| None).collect())
    }

    /// Fetches earliest block from storage backend
    fn raw_read_earliest_finalized_block(&self) -> Option<SeqNum>;
    /// Fetches latest block from storage backend
    fn raw_read_latest_finalized_block(&self) -> Option<SeqNum>;

    fn read_valset_at_block(
        &self,
        block_num: SeqNum,
        requested_epoch: Epoch,
    ) -> Vec<(SCT::NodeIdPubKey, SignatureCollectionPubKeyType<SCT>, Stake)>;

    fn total_db_lookups(&self) -> u64;
}

pub trait StateBackendTest<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn ledger_propose(
        &mut self,
        block_id: BlockId,
        seq_num: SeqNum,
        round: Round,
        parent_id: BlockId,
        new_account_nonces: BTreeMap<Address, Nonce>,
    );

    fn ledger_commit(&mut self, block_id: &BlockId, seq_num: &SeqNum);
}

impl<ST, SCT, T> StateBackend<ST, SCT> for Arc<Mutex<T>>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    T: StateBackend<ST, SCT>,
{
    fn get_account_statuses<'a>(
        &self,
        block_id: &BlockId,
        seq_num: &SeqNum,
        is_finalized: bool,
        addresses: impl Iterator<Item = &'a Address>,
    ) -> Result<Vec<Option<EthAccount>>, StateBackendError> {
        let state = self.lock().unwrap();
        state.get_account_statuses(block_id, seq_num, is_finalized, addresses)
    }

    fn get_execution_result(
        &self,
        block_id: &BlockId,
        seq_num: &SeqNum,
        is_finalized: bool,
    ) -> Result<EthHeader, StateBackendError> {
        let state = self.lock().unwrap();
        state.get_execution_result(block_id, seq_num, is_finalized)
    }

    fn get_reserve_balance_states<'a>(
        &self,
        block_id: &BlockId,
        seq_num: &SeqNum,
        is_finalized: bool,
        addresses: impl Iterator<Item = &'a Address>,
    ) -> Result<Vec<Option<ReserveBalanceState>>, StateBackendError> {
        let state = self.lock().unwrap();
        state.get_reserve_balance_states(block_id, seq_num, is_finalized, addresses)
    }

    fn raw_read_earliest_finalized_block(&self) -> Option<SeqNum> {
        let state = self.lock().unwrap();
        state.raw_read_earliest_finalized_block()
    }

    fn raw_read_latest_finalized_block(&self) -> Option<SeqNum> {
        let state = self.lock().unwrap();
        state.raw_read_latest_finalized_block()
    }

    fn read_valset_at_block(
        &self,
        block_num: SeqNum,
        requested_epoch: Epoch,
    ) -> Vec<(
        <SCT as SignatureCollection>::NodeIdPubKey,
        SignatureCollectionPubKeyType<SCT>,
        Stake,
    )> {
        let state = self.lock().unwrap();
        state.read_valset_at_block(block_num, requested_epoch)
    }

    fn total_db_lookups(&self) -> u64 {
        self.lock().unwrap().total_db_lookups()
    }
}

impl<ST, SCT, T> StateBackendTest<ST, SCT> for Arc<Mutex<T>>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    T: StateBackendTest<ST, SCT>,
{
    fn ledger_commit(&mut self, block_id: &BlockId, seq_num: &SeqNum) {
        let mut state = self.lock().unwrap();
        state.ledger_commit(block_id, seq_num);
    }

    fn ledger_propose(
        &mut self,
        block_id: BlockId,
        seq_num: SeqNum,
        round: Round,
        parent_id: BlockId,
        new_account_nonces: BTreeMap<Address, Nonce>,
    ) {
        let mut state = self.lock().unwrap();
        state.ledger_propose(block_id, seq_num, round, parent_id, new_account_nonces);
    }
}
