use reth_primitives::{ReceiptWithBloom, TransactionSigned};

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
    */
    async fn archive_block(
        &self,
        block_header: BlockHeader,
        transactions: Vec<TransactionSigned>,
        block_num: u64,
    ) -> Result<(), ArchiveError>;

    /*
        Archive Receipt
        Table: key = block_number, value = RLP(Vec<Receipt>)
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
}
