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

use std::{collections::HashMap, future::ready};

use monad_types::SeqNum;

use crate::triedb_env::*;

#[derive(Debug, Default)]
pub struct MockTriedb {
    latest_block: u64,
    accounts: HashMap<EthAddress, Account>,
    tx_locations: HashMap<EthTxHash, TransactionLocation>,
    call_frames: HashMap<TransactionLocation, Vec<u8>>,
    code: String,
    receipts: Vec<ReceiptWithLogIndex>,
    transactions: Vec<TxEnvelopeWithSender>,
}

impl MockTriedb {
    pub fn set_latest_block(&mut self, block_num: u64) {
        self.latest_block = block_num;
    }

    pub fn set_account(&mut self, address: EthAddress, account: Account) {
        self.accounts.insert(address, account);
    }

    pub fn set_transaction_location_by_hash(
        &mut self,
        tx_hash: EthTxHash,
        loc: TransactionLocation,
    ) {
        self.tx_locations.insert(tx_hash, loc);
    }

    pub fn set_call_frame(&mut self, loc: TransactionLocation, frame: Vec<u8>) {
        self.call_frames.insert(loc, frame);
    }

    pub fn set_code(&mut self, code: String) {
        self.code = code;
    }

    pub fn set_receipts(&mut self, receipts: Vec<ReceiptWithLogIndex>) {
        self.receipts = receipts;
    }

    pub fn set_transactions(&mut self, transactions: Vec<TxEnvelopeWithSender>) {
        self.transactions = transactions;
    }
}

impl Triedb for MockTriedb {
    fn get_latest_finalized_block_key(&self) -> FinalizedBlockKey {
        FinalizedBlockKey(SeqNum(self.latest_block))
    }
    fn get_latest_voted_block_key(&self) -> BlockKey {
        BlockKey::Finalized(self.get_latest_finalized_block_key())
    }
    fn get_block_key(&self, block_num: SeqNum) -> Option<BlockKey> {
        Some(BlockKey::Finalized(FinalizedBlockKey(block_num)))
    }

    fn get_state_availability(
        &self,
        _key: BlockKey,
    ) -> impl std::future::Future<Output = Result<bool, String>> + Send {
        ready(Ok(true))
    }

    fn get_account(
        &self,
        _block_key: BlockKey,
        _addr: EthAddress,
    ) -> impl std::future::Future<Output = Result<Account, String>> + Send {
        self.accounts.get(&_addr).map_or_else(
            || ready(Ok(Account::default())),
            |account| ready(Ok(account.clone())),
        )
    }

    fn get_storage_at(
        &self,
        _block_key: BlockKey,
        _addr: EthAddress,
        _at: EthStorageKey,
    ) -> impl std::future::Future<Output = Result<String, String>> + Send {
        ready(Ok("0x0".to_string()))
    }

    fn get_code(
        &self,
        _block_key: BlockKey,
        _code_hash: EthCodeHash,
    ) -> impl std::future::Future<Output = Result<String, String>> + Send {
        ready(Ok(self.code.clone()))
    }

    fn get_receipt(
        &self,
        _block_key: BlockKey,
        _txn_index: u64,
    ) -> impl std::future::Future<Output = Result<Option<ReceiptWithLogIndex>, String>> + Send {
        ready(Ok(self.receipts.get(_txn_index as usize).cloned()))
    }

    fn get_receipts(
        &self,
        _block_key: BlockKey,
    ) -> impl std::future::Future<Output = Result<Vec<ReceiptWithLogIndex>, String>> + Send + Sync
    {
        ready(Ok(self.receipts.clone()))
    }

    fn get_transaction(
        &self,
        _block_key: BlockKey,
        _txn_index: u64,
    ) -> impl std::future::Future<Output = Result<Option<TxEnvelopeWithSender>, String>> + Send
    {
        ready(Ok(self.transactions.get(_txn_index as usize).cloned()))
    }

    fn get_transactions(
        &self,
        _block_key: BlockKey,
    ) -> impl std::future::Future<Output = Result<Vec<TxEnvelopeWithSender>, String>> + Send + Sync
    {
        ready(Ok(self.transactions.clone()))
    }

    fn get_block_header(
        &self,
        _block_key: BlockKey,
    ) -> impl std::future::Future<Output = Result<Option<BlockHeader>, String>> + Send + Sync {
        ready(Ok(None))
    }

    fn get_transaction_location_by_hash(
        &self,
        _block_key: BlockKey,
        tx_hash: EthTxHash,
    ) -> impl std::future::Future<Output = Result<Option<TransactionLocation>, String>> + Send {
        if self.tx_locations.contains_key(&tx_hash) {
            let loc = self.tx_locations[&tx_hash].clone();
            ready(Ok(Some(loc)))
        } else {
            ready(Ok(None))
        }
    }

    fn get_block_number_by_hash(
        &self,
        _block_key: BlockKey,
        _block_hash: EthBlockHash,
    ) -> impl std::future::Future<Output = Result<Option<u64>, String>> + Send {
        ready(Ok(None))
    }

    fn get_call_frame(
        &self,
        block_key: BlockKey,
        txn_index: u64,
    ) -> impl std::future::Future<Output = Result<Option<Vec<u8>>, String>> + Send {
        let key = TransactionLocation {
            tx_index: txn_index,
            block_num: block_key.seq_num().0,
        };
        if self.call_frames.contains_key(&key) {
            let frame = self.call_frames[&key].clone();
            ready(Ok(Some(frame)))
        } else {
            ready(Ok(None))
        }
    }

    fn get_call_frames(
        &self,
        _block_key: BlockKey,
    ) -> impl std::future::Future<Output = Result<Vec<Vec<u8>>, String>> + Send {
        ready(Ok(vec![]))
    }
}
