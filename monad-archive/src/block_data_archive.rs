use core::str;

use alloy_consensus::{Block as AlloyBlock, Header, ReceiptEnvelope, TxEnvelope};
use alloy_primitives::BlockHash;
use alloy_rlp::{Decodable, Encodable};
use futures::try_join;

use crate::prelude::*;

pub type Block = AlloyBlock<TxEnvelope, Header>;

const BLOCK_PADDING_WIDTH: usize = 12;

#[derive(Clone)]
pub struct BlockDataArchive<Store = BlobStoreErased> {
    pub store: Store,

    pub latest_uploaded_table_key: &'static str,
    pub latest_indexed_table_key: &'static str,

    // key =  {block}/{block_number}, value = {RLP(Block)}
    pub block_table_prefix: &'static str,

    // key = {block_hash}/{$block_hash}, value = {str(block_number)}
    pub block_hash_table_prefix: &'static str,

    // key = {receipts}/{block_number}, value = {RLP(Vec<Receipt>)}
    pub receipts_table_prefix: &'static str,

    // key = {traces}/{block_number}, value = {RLP(Vec<Vec<u8>>)}
    pub traces_table_prefix: &'static str,
}

impl<Store: BlobStore> BlockDataReader for BlockDataArchive<Store> {
    fn get_bucket(&self) -> &str {
        self.store.bucket_name()
    }

    async fn get_latest(&self, latest_kind: LatestKind) -> Result<u64> {
        let key = match latest_kind {
            LatestKind::Uploaded => &self.latest_uploaded_table_key,
            LatestKind::Indexed => &self.latest_indexed_table_key,
        };

        let value = self.store.read(key).await?;

        let value_str = String::from_utf8(value.to_vec()).wrap_err("Invalid UTF-8 sequence")?;

        // Parse the string as u64
        value_str.parse::<u64>().wrap_err_with(|| {
            format!("Unable to convert block_number string to number (u64), value: {value_str}")
        })
    }

    async fn get_block_by_number(&self, block_num: u64) -> Result<Block> {
        self.read_block(block_num).await
    }

    async fn get_block_receipts(&self, block_number: u64) -> Result<Vec<ReceiptEnvelope>> {
        let receipts_key = self.receipts_key(block_number);

        let rlp_receipts = self.store.read(&receipts_key).await?;
        let mut rlp_receipts_slice: &[u8] = &rlp_receipts;

        let receipts = Vec::decode(&mut rlp_receipts_slice).wrap_err("Cannot decode block")?;

        Ok(receipts)
    }

    async fn get_block_traces(&self, block_number: u64) -> Result<Vec<Vec<u8>>> {
        let traces_key = self.traces_key(block_number);

        let rlp_traces = self.store.read(&traces_key).await?;
        let mut rlp_traces_slice: &[u8] = &rlp_traces;

        let traces = Vec::decode(&mut rlp_traces_slice).wrap_err("Cannot decode block")?;

        Ok(traces)
    }

    async fn get_block_by_hash(&self, block_hash: &BlockHash) -> Result<Block> {
        let block_hash_key_suffix = hex::encode(block_hash);
        let block_hash_key = format!("{}/{}", self.block_hash_table_prefix, block_hash_key_suffix);

        let block_num_bytes = self.store.read(&block_hash_key).await?;

        let block_num_str =
            String::from_utf8(block_num_bytes.to_vec()).wrap_err("Invalid UTF-8 sequence")?;

        let block_num = block_num_str.parse::<u64>().wrap_err_with(|| {
            format!("Unable to convert block_number string to number (u64), value: {block_num_str}")
        })?;

        self.get_block_by_number(block_num).await
    }
}

impl<Store: BlobStore> BlockDataArchive<Store> {
    pub fn new(archive: Store) -> Self {
        BlockDataArchive {
            store: archive,
            block_table_prefix: "block",
            block_hash_table_prefix: "block_hash",
            receipts_table_prefix: "receipts",
            traces_table_prefix: "traces",
            latest_uploaded_table_key: "latest",
            latest_indexed_table_key: "latest_indexed",
        }
    }

    pub async fn read_block(&self, block_num: u64) -> Result<Block> {
        let bytes = self.store.read(&self.block_key(block_num)).await?;
        let mut bytes: &[u8] = &bytes;
        let block = Block::decode(&mut bytes)?;
        Ok(block)
    }

    pub fn block_key(&self, block_num: u64) -> String {
        format!(
            "{}/{:0width$}",
            self.block_table_prefix,
            block_num,
            width = BLOCK_PADDING_WIDTH
        )
    }

    pub fn receipts_key(&self, block_num: u64) -> String {
        format!(
            "{}/{:0width$}",
            self.receipts_table_prefix,
            block_num,
            width = BLOCK_PADDING_WIDTH
        )
    }

    pub fn traces_key(&self, block_num: u64) -> String {
        format!(
            "{}/{:0width$}",
            self.traces_table_prefix,
            block_num,
            width = BLOCK_PADDING_WIDTH
        )
    }

    pub async fn update_latest(&self, block_num: u64, latest_kind: LatestKind) -> Result<()> {
        let key = match latest_kind {
            LatestKind::Uploaded => &self.latest_uploaded_table_key,
            LatestKind::Indexed => &self.latest_indexed_table_key,
        };
        let latest_value = format!("{:0width$}", block_num, width = BLOCK_PADDING_WIDTH);
        self.store
            .upload(key, latest_value.as_bytes().to_vec())
            .await
    }

    pub async fn archive_block(&self, block: Block) -> Result<()> {
        // 1) Insert into block table
        let block_num = block.header.number;
        let block_key = self.block_key(block_num);

        let mut rlp_block = Vec::with_capacity(8096);
        block.encode(&mut rlp_block);

        // 2) Insert into block_hash table
        let block_hash_key_suffix = hex::encode(block.header.hash_slow());
        let block_hash_key = format!("{}/{}", self.block_hash_table_prefix, block_hash_key_suffix);
        let block_hash_value_string = block_num.to_string();
        let block_hash_value = block_hash_value_string.as_bytes();

        // 3) Join futures
        try_join!(
            self.store.upload(&block_key, rlp_block),
            self.store
                .upload(&block_hash_key, block_hash_value.to_vec())
        )?;
        Ok(())
    }

    pub async fn archive_receipts(
        &self,
        receipts: Vec<ReceiptEnvelope>,
        block_num: u64,
    ) -> Result<()> {
        // 1) Prepare the receipts upload
        let receipts_key = format!(
            "{}/{:0width$}",
            self.receipts_table_prefix,
            block_num,
            width = BLOCK_PADDING_WIDTH
        );

        let mut rlp_receipts = Vec::new();
        receipts.encode(&mut rlp_receipts);
        self.store.upload(&receipts_key, rlp_receipts).await
    }

    pub async fn archive_traces(&self, traces: Vec<Vec<u8>>, block_num: u64) -> Result<()> {
        let traces_key = format!(
            "{}/{:0width$}",
            self.traces_table_prefix,
            block_num,
            width = BLOCK_PADDING_WIDTH
        );

        let mut rlp_traces = vec![];
        traces.encode(&mut rlp_traces);

        self.store.upload(&traces_key, rlp_traces).await
    }
}
