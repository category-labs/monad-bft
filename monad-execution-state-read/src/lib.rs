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

use std::sync::{Arc, Mutex};

use alloy_consensus::TxEnvelope;
use alloy_primitives::Address;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::{EthAccount, EthHeader};
use monad_types::{BlockId, Epoch, Round, SeqNum, Stake};
use monad_validator::signature_collection::{SignatureCollection, SignatureCollectionPubKeyType};

pub use self::{
    in_memory::{AccountState, InMemoryBlockState, InMemoryState, InMemoryStateInner},
    mock::NopExecutionStateRead,
    thread::ExecutionStateReadThreadClient,
};

mod in_memory;
mod mock;
mod thread;

#[derive(Debug, PartialEq)]
pub enum ExecutionStateReadError {
    /// not available yet
    NotAvailableYet,
    /// will never be available
    NeverAvailable,
}

/// Trie-node cache counters for a state-read backend, cumulative since it was
/// opened. Declared here rather than reusing the triedb type so this crate does
/// not take a dependency on the FFI, matching how `total_db_lookups` returns a
/// plain integer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NodeCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub used_bytes: u64,
    pub entries: u64,
}

/// A read-only view of block state.
pub trait ExecutionStateRead<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn get_account_statuses<'a>(
        &mut self,
        block_id: &BlockId,
        seq_num: &SeqNum,
        is_finalized: bool,
        addresses: impl Iterator<Item = &'a Address>,
    ) -> Result<Vec<Option<EthAccount>>, ExecutionStateReadError>;

    fn get_execution_result(
        &mut self,
        block_id: &BlockId,
        seq_num: &SeqNum,
        is_finalized: bool,
    ) -> Result<EthHeader, ExecutionStateReadError>;

    /// Fetches earliest block from storage backend
    fn raw_read_earliest_finalized_block(&self) -> Option<SeqNum>;
    /// Fetches latest block from storage backend
    fn raw_read_latest_finalized_block(&self) -> Option<SeqNum>;

    fn read_valset_at_block(
        &mut self,
        block_num: SeqNum,
        requested_epoch: Epoch,
    ) -> Vec<(SCT::NodeIdPubKey, SignatureCollectionPubKeyType<SCT>, Stake)>;

    fn total_db_lookups(&self) -> u64;

    /// Trie-node cache counters, or None for a backend with no such cache.
    fn node_cache_stats(&self) -> Option<NodeCacheStats>;
}

pub trait MockExecution<ST, SCT>
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
        txns: Vec<TxEnvelope>,
    );

    fn ledger_commit(&mut self, block_id: &BlockId, seq_num: &SeqNum);
}

impl<ST, SCT, T> ExecutionStateRead<ST, SCT> for Arc<Mutex<T>>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    T: ExecutionStateRead<ST, SCT>,
{
    fn get_account_statuses<'a>(
        &mut self,
        block_id: &BlockId,
        seq_num: &SeqNum,
        is_finalized: bool,
        addresses: impl Iterator<Item = &'a Address>,
    ) -> Result<Vec<Option<EthAccount>>, ExecutionStateReadError> {
        let mut state = self.lock().unwrap();
        state.get_account_statuses(block_id, seq_num, is_finalized, addresses)
    }

    fn get_execution_result(
        &mut self,
        block_id: &BlockId,
        seq_num: &SeqNum,
        is_finalized: bool,
    ) -> Result<EthHeader, ExecutionStateReadError> {
        let mut state = self.lock().unwrap();
        state.get_execution_result(block_id, seq_num, is_finalized)
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
        &mut self,
        block_num: SeqNum,
        requested_epoch: Epoch,
    ) -> Vec<(
        <SCT as SignatureCollection>::NodeIdPubKey,
        SignatureCollectionPubKeyType<SCT>,
        Stake,
    )> {
        let mut state = self.lock().unwrap();
        state.read_valset_at_block(block_num, requested_epoch)
    }

    fn total_db_lookups(&self) -> u64 {
        self.lock().unwrap().total_db_lookups()
    }

    fn node_cache_stats(&self) -> Option<NodeCacheStats> {
        self.lock().unwrap().node_cache_stats()
    }
}

impl<ST, SCT, T> MockExecution<ST, SCT> for Arc<Mutex<T>>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    T: MockExecution<ST, SCT>,
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
        txns: Vec<TxEnvelope>,
    ) {
        let mut state = self.lock().unwrap();
        state.ledger_propose(block_id, seq_num, round, parent_id, txns);
    }
}
