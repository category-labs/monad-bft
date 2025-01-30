use std::future::ready;

use crate::triedb_env::*;

#[derive(Debug)]
pub struct MockTriedb;

impl Triedb for MockTriedb {
    fn get_latest_block(&self) -> impl std::future::Future<Output = Result<u64, String>> + Send {
        ready(Ok(0))
    }

    fn get_account(
        &self,
        _addr: EthAddress,
        _block_num: u64,
    ) -> impl std::future::Future<Output = Result<Account, String>> + Send {
        ready(Ok(Account::default()))
    }

    fn get_storage_at(
        &self,
        _addr: EthAddress,
        _at: EthStorageKey,
        _block_num: u64,
    ) -> impl std::future::Future<Output = Result<String, String>> + Send {
        ready(Ok("0x0".to_string()))
    }

    fn get_code(
        &self,
        _code_hash: EthCodeHash,
        _block_num: u64,
    ) -> impl std::future::Future<Output = Result<String, String>> + Send {
        ready(Ok("".to_string()))
    }

    fn get_receipt(
        &self,
        _txn_index: u64,
        _block_num: u64,
    ) -> impl std::future::Future<Output = Result<Option<ReceiptWithLogIndex>, String>> + Send {
        ready(Ok(None))
    }

    fn get_receipts(
        &self,
        _block_num: u64,
    ) -> impl std::future::Future<Output = Result<Vec<ReceiptWithLogIndex>, String>> + Send + Sync
    {
        ready(Ok(vec![]))
    }

    fn get_transaction(
        &self,
        _txn_index: u64,
        _block_num: u64,
    ) -> impl std::future::Future<Output = Result<Option<TxEnvelopeWithSender>, String>> + Send
    {
        ready(Ok(None))
    }

    fn get_transactions(
        &self,
        _block_num: u64,
    ) -> impl std::future::Future<Output = Result<Vec<TxEnvelopeWithSender>, String>> + Send + Sync
    {
        ready(Ok(vec![]))
    }

    fn get_block_header(
        &self,
        _block_num: u64,
    ) -> impl std::future::Future<Output = Result<Option<BlockHeader>, String>> + Send + Sync {
        ready(Ok(None))
    }

    fn get_transaction_location_by_hash(
        &self,
        _tx_hash: EthTxHash,
        _block_num: u64,
    ) -> impl std::future::Future<Output = Result<Option<TransactionLocation>, String>> + Send {
        ready(Ok(None))
    }

    fn get_block_number_by_hash(
        &self,
        _block_hash: EthBlockHash,
        _block_num: u64,
    ) -> impl std::future::Future<Output = Result<Option<u64>, String>> + Send {
        ready(Ok(None))
    }

    fn get_call_frame(
        &self,
        _txn_index: u64,
        _block_num: u64,
    ) -> impl std::future::Future<Output = Result<Option<Vec<u8>>, String>> + Send {
        ready(Ok(None))
    }

    fn get_call_frames(
        &self,
        _block_num: u64,
    ) -> impl std::future::Future<Output = Result<Vec<Vec<u8>>, String>> + Send {
        ready(Ok(vec![]))
    }
}
