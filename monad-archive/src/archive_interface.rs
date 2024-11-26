use eyre::Result;
use reth_primitives::{Block, ReceiptWithBloom, TransactionSigned};

use monad_triedb_utils::triedb_env::BlockHeader;

pub enum LatestKind {
    Uploaded,
    Indexed,
}

pub trait ArchiveWriter: ArchiveReader {
    // Update latest processed block
    async fn update_latest(&self, block_num: u64, latest_kind: LatestKind) -> Result<()>;

    /*
        Archive Block
        1) Table1: key = block_number, value = RLP(block)
        2) Table2: key = block_hash, value = block_number
    */
    async fn archive_block(
        &self,
        block_header: BlockHeader,
        transactions: Vec<TransactionSigned>,
        block_num: u64,
    ) -> Result<()>;

    /*
        Archive Receipt
        Table: key = block_number, value = RLP(Vec<Receipt>)
    */
    async fn archive_receipts(&self, receipts: Vec<ReceiptWithBloom>, block_num: u64)
        -> Result<()>;

    async fn archive_traces(&self, traces: Vec<Vec<u8>>, block_num: u64) -> Result<()>;
}

pub trait ArchiveReader: Clone + Send {
    // Get the latest stored block
    fn get_latest(
        &self,
        latest_kind: LatestKind,
    ) -> impl std::future::Future<Output = Result<u64>> + Send;

    fn get_block_by_hash(
        &self,
        block_hash: &[u8; 32],
    ) -> impl std::future::Future<Output = Result<Block>> + Send;

    fn get_block_by_number(
        &self,
        block_num: u64,
    ) -> impl std::future::Future<Output = Result<Block>> + Send;

    fn get_block_receipts(
        &self,
        block_number: u64,
    ) -> impl std::future::Future<Output = Result<Vec<ReceiptWithBloom>>> + Send;

    fn get_block_traces(
        &self,
        block_number: u64,
    ) -> impl std::future::Future<Output = Result<Vec<Vec<u8>>>> + Send;
}
