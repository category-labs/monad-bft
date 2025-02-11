use std::{collections::HashMap, future::ready};

use monad_types::SeqNum;

use crate::triedb_env::*;

#[derive(Debug, Default)]
pub struct MockTriedb {
    latest_block: u64,
    accounts: HashMap<EthAddress, Account>,
}

impl MockTriedb {
    pub fn set_latest_block(&mut self, block_num: u64) {
        self.latest_block = block_num;
    }

    pub fn set_account(&mut self, address: EthAddress, account: Account) {
        self.accounts.insert(address, account);
    }
}

impl Triedb for MockTriedb {
    fn get_latest_finalized_block_key(&self) -> FinalizedBlockKey {
        FinalizedBlockKey(SeqNum(self.latest_block))
    }
    fn get_latest_voted_block_key(&self) -> BlockKey {
        BlockKey::Finalized(self.get_latest_finalized_block_key())
    }
    fn get_block_key(&self, block_num: SeqNum) -> BlockKey {
        BlockKey::Finalized(FinalizedBlockKey(block_num))
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
        ready(Ok("".to_string()))
    }

    fn get_receipt(
        &self,
        _block_key: BlockKey,
        _txn_index: u64,
    ) -> impl std::future::Future<Output = Result<Option<ReceiptWithLogIndex>, String>> + Send {
        ready(Ok(None))
    }

    fn get_receipts(
        &self,
        _block_key: BlockKey,
    ) -> impl std::future::Future<Output = Result<Vec<ReceiptWithLogIndex>, String>> + Send + Sync
    {
        ready(Ok(vec![]))
    }

    fn get_transaction(
        &self,
        _block_key: BlockKey,
        _txn_index: u64,
    ) -> impl std::future::Future<Output = Result<Option<TxEnvelopeWithSender>, String>> + Send
    {
        ready(Ok(None))
    }

    fn get_transactions(
        &self,
        _block_key: BlockKey,
    ) -> impl std::future::Future<Output = Result<Vec<TxEnvelopeWithSender>, String>> + Send + Sync
    {
        ready(Ok(vec![]))
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
        _tx_hash: EthTxHash,
    ) -> impl std::future::Future<Output = Result<Option<TransactionLocation>, String>> + Send {
        ready(Ok(None))
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
        _block_key: BlockKey,
        _txn_index: u64,
    ) -> impl std::future::Future<Output = Result<Option<Vec<u8>>, String>> + Send {
        ready(Ok(None))
    }

    fn get_call_frames(
        &self,
        _block_key: BlockKey,
    ) -> impl std::future::Future<Output = Result<Vec<Vec<u8>>, String>> + Send {
        ready(Ok(vec![]))
    }
}
