use reth_primitives::{Block, ReceiptWithBloom, TransactionSigned};

use crate::{errors::ArchiveError, triedb::BlockHeader};

pub trait ArchiveWriterInterface {
    // Get the latest stored block
    async fn get_latest(&self) -> Result<u64, ArchiveError>;

    // Update latest processed block
    async fn update_latest(&self, block_num: u64) -> Result<(), ArchiveError>;

    /*
        Archive Block
        1) Table1: key = block_number, value = RLP(block)
        2) Table2: key = block_hash, value = block_number
        3) Table3: key = tx_hash, value = RLP(tx)
    */
    async fn archive_block(
        &self,
        block_header: BlockHeader,
        transactions: Vec<TransactionSigned>,
        block_num: u64,
    ) -> Result<(), ArchiveError>;

    /*
        Archive Receipt
        1) Table1: key = block_number, value = RLP(Vec<Receipt>)
        2) Table2: key = tx_hash, value = RLP(Receipt)
    */
    async fn archive_receipts(
        &self,
        receipts: Vec<ReceiptWithBloom>,
        block_num: u64,
    ) -> Result<(), ArchiveError>;

    async fn archive_traces(
        &self,
        traces: Vec<Vec<u8>>,
        block_num: u64,
    ) -> Result<(), ArchiveError>;

    async fn archive_hashes(
        &self,
        transactions: Vec<TransactionSigned>,
        receipts: Vec<ReceiptWithBloom>,
        traces: Vec<Vec<u8>>,
        block_num: u64,
        tx_hashes: Vec<[u8; 32]>,
    ) -> Result<(), ArchiveError>;
}

pub trait ArchiveReaderInterface {
    // Get the latest stored block
    async fn get_latest(&self) -> Result<u64, ArchiveError>;

    /*
        Block Methods
    */

    // eth_getBlockByHash
    async fn get_block_by_hash(&self, block_hash: &[u8; 32]) -> Result<Block, ArchiveError>;

    // eth_getBlockByNumber
    // eth_getRawBlock
    // eth_getRawHeader
    async fn get_block_by_number(&self, block_num: u64) -> Result<Block, ArchiveError>;

    //eth_getBlockTransactionCountByHash
    async fn get_block_transaction_count_by_hash(
        &self,
        block_hash: &[u8; 32],
    ) -> Result<usize, ArchiveError>;

    //eth_getBlockTransactionCountByNumber
    async fn get_block_transaction_count_by_number(
        &self,
        block_num: u64,
    ) -> Result<usize, ArchiveError>;

    /*
        Transaction Methods
    */

    //eth_getTransactionByBlockHashAndIndex
    async fn get_transaction_by_block_hash_and_index(
        &self,
        block_hash: &[u8; 32],
        tx_index: u64,
    ) -> Result<TransactionSigned, ArchiveError>;

    // eth_getTransactionByBlockNumberAndIndex
    async fn get_transaction_by_block_number_and_index(
        &self,
        block_number: u64,
        tx_index: u64,
    ) -> Result<TransactionSigned, ArchiveError>;

    // eth_getTransactionByHash
    // eth_getRawTransaction
    async fn get_transaction_by_hash(
        &self,
        tx_hash: &[u8; 32],
    ) -> Result<TransactionSigned, ArchiveError>;

    /*
        Receipt Methods
    */

    // eth_getBlockReceipts
    // eth_getRawReceipts
    async fn get_block_receipts(
        &self,
        block_number: u64,
    ) -> Result<Vec<ReceiptWithBloom>, ArchiveError>;

    // eth_getTransactionReceipt
    async fn get_transaction_receipt(
        self,
        tx_hash: &[u8; 32],
    ) -> Result<ReceiptWithBloom, ArchiveError>;

    /*
        TODO: Trace Methods
    */
}
