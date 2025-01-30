use core::str;

use alloy_consensus::{Block as AlloyBlock, BlockBody, Header, ReceiptEnvelope, TxEnvelope};
use alloy_primitives::BlockHash;
use alloy_rlp::{Decodable, Encodable};
use eyre::bail;
use futures::try_join;
use monad_triedb_utils::triedb_env::{ReceiptWithLogIndex, TxEnvelopeWithSender};

use crate::prelude::*;

pub type Block = AlloyBlock<TxEnvelopeWithSender, Header>;

const BLOCK_PADDING_WIDTH: usize = 12;

enum BlockStorageRepr {
    V0(AlloyBlock<TxEnvelope, Header>),
    V1(Block),
}

enum ReceiptStorageRepr {
    V0(Vec<ReceiptEnvelope>),
    V1(Vec<ReceiptWithLogIndex>),
}

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
        let bytes = self.store.read(&self.block_key(block_num)).await?;
        BlockStorageRepr::decode_and_convert(&bytes)
            .wrap_err_with(|| format!("Failed to decode block: block_num: {}", block_num))
    }

    async fn get_block_receipts(&self, block_number: u64) -> Result<Vec<ReceiptWithLogIndex>> {
        let receipts_key = self.receipts_key(block_number);

        let rlp_receipts = self.store.read(&receipts_key).await?;

        ReceiptStorageRepr::decode_and_convert(&rlp_receipts).wrap_err_with(|| {
            format!(
                "Failed to decode block receipts: block_num: {}",
                block_number
            )
        })
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

        // 2) Insert into block_hash table
        let block_hash_key_suffix = hex::encode(block.header.hash_slow());
        let block_hash_key = format!("{}/{}", self.block_hash_table_prefix, block_hash_key_suffix);
        let block_hash_value_string = block_num.to_string();
        let block_hash_value = block_hash_value_string.as_bytes();

        // 3) Encode into storage repr
        let encoded_block = BlockStorageRepr::V1(block).encode()?;

        // 4) Join futures
        try_join!(
            self.store.upload(&block_key, encoded_block),
            self.store
                .upload(&block_hash_key, block_hash_value.to_vec())
        )?;
        Ok(())
    }

    pub async fn archive_receipts(
        &self,
        receipts: Vec<ReceiptWithLogIndex>,
        block_num: u64,
    ) -> Result<()> {
        self.store
            .upload(
                &self.receipts_key(block_num),
                ReceiptStorageRepr::V1(receipts).encode()?,
            )
            .await
    }

    pub async fn archive_traces(&self, traces: Vec<Vec<u8>>, block_num: u64) -> Result<()> {
        let mut rlp_traces = vec![];
        traces.encode(&mut rlp_traces);

        self.store
            .upload(&self.traces_key(block_num), rlp_traces)
            .await
    }
}

impl BlockStorageRepr {
    const SENTINEL: u8 = 55;
    const V0_MARKER: u8 = 0;
    const V1_MARKER: u8 = 1;

    fn encode(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(1024);
        {
            let buf = &mut buf as &mut dyn alloy_rlp::bytes::BufMut;
            buf.put_u8(Self::SENTINEL);
            match self {
                BlockStorageRepr::V0(block) => {
                    buf.put_u8(Self::V0_MARKER);
                    block.encode(buf);
                }
                BlockStorageRepr::V1(block) => {
                    buf.put_u8(Self::V1_MARKER);
                    block.encode(buf);
                }
            }
        }
        Ok(buf)
    }

    fn decode_and_convert(buf: &[u8]) -> Result<Block> {
        if buf.len() < 2 {
            bail!(
                "Cannot decode block, len must be > 2: actual: {}",
                buf.len()
            );
        }

        let marker_bytes = [buf[0], buf[1]];
        let decoding_result = match marker_bytes {
            [Self::SENTINEL, Self::V0_MARKER] => {
                AlloyBlock::<TxEnvelope, Header>::decode(&mut &buf[2..]) // fmt
                    .map(BlockStorageRepr::V0)
            }
            [Self::SENTINEL, Self::V1_MARKER] => {
                Block::decode(&mut &buf[2..]).map(BlockStorageRepr::V1)
            }
            _ => {
                AlloyBlock::<TxEnvelope, Header>::decode(&mut &buf[..]) // fmt
                    .map(BlockStorageRepr::V0)
            }
        };

        match decoding_result {
            Ok(decoded) => decoded.convert(),
            Err(e) => {
                info!(?e, "Failed to parse BlockStorageRepr despite sentinel bit being set. Falling back to raw InlineV0 decoding...");
                let v0 =
                    BlockStorageRepr::V0(AlloyBlock::<TxEnvelope, Header>::decode(&mut &buf[..])?);
                v0.convert()
            }
        }
    }

    fn convert(self) -> Result<Block> {
        Ok(match self {
            BlockStorageRepr::V0(block) => Block {
                header: block.header,
                body: BlockBody {
                    ommers: block.body.ommers,
                    withdrawals: block.body.withdrawals,
                    // replace with sender wrapped tx
                    transactions: block
                        .body
                        .transactions
                        .iter()
                        .map(|tx| -> Result<TxEnvelopeWithSender> {
                            Ok(TxEnvelopeWithSender {
                                sender: tx.recover_signer()?,
                                tx: tx.clone(),
                            })
                        })
                        .collect::<Result<Vec<TxEnvelopeWithSender>>>()?,
                },
            },
            BlockStorageRepr::V1(block) => block,
        })
    }
}

impl ReceiptStorageRepr {
    const SENTINEL: u8 = 88;
    const V0_MARKER: u8 = 0;
    const V1_MARKER: u8 = 1;

    fn encode(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(1024);
        {
            let buf = &mut buf as &mut dyn alloy_rlp::bytes::BufMut;
            buf.put_u8(Self::SENTINEL);
            match self {
                ReceiptStorageRepr::V0(receipts) => {
                    buf.put_u8(Self::V0_MARKER);
                    receipts.encode(buf);
                }
                ReceiptStorageRepr::V1(receipts) => {
                    buf.put_u8(Self::V1_MARKER);
                    receipts.encode(buf);
                }
            }
        }
        Ok(buf)
    }

    fn decode_and_convert(buf: &[u8]) -> Result<Vec<ReceiptWithLogIndex>> {
        if buf.len() < 2 {
            bail!(
                "Cannot decode receipt, len must be > 2: actual: {}",
                buf.len()
            );
        }

        let marker_bytes = [buf[0], buf[1]];
        let decoding_result = match marker_bytes {
            [Self::SENTINEL, Self::V0_MARKER] => {
                Vec::<ReceiptEnvelope>::decode(&mut &buf[2..]) // fmt
                    .map(ReceiptStorageRepr::V0)
            }
            [Self::SENTINEL, Self::V1_MARKER] => {
                Vec::<ReceiptWithLogIndex>::decode(&mut &buf[2..]).map(ReceiptStorageRepr::V1)
            }
            _ => {
                Vec::<ReceiptEnvelope>::decode(&mut &buf[..]) // fmt
                    .map(ReceiptStorageRepr::V0)
            }
        };

        match decoding_result {
            Ok(decoded) => decoded.convert(),
            Err(e) => {
                info!(?e, "Failed to parse ReceiptStorageRepr despite sentinel bit being set. Falling back to raw V0 decoding...");
                let v0 = Vec::<ReceiptEnvelope>::decode(&mut &buf[..]) // fmt
                    .map(ReceiptStorageRepr::V0)?;
                v0.convert()
            }
        }
    }

    fn convert(self) -> Result<Vec<ReceiptWithLogIndex>> {
        Ok(match self {
            ReceiptStorageRepr::V0(receipts) => {
                let mut pre_receipts = 0;
                receipts
                    .into_iter()
                    .map(|receipt| {
                        let starting_log_index = pre_receipts as u64;
                        pre_receipts += receipt.logs().len();
                        ReceiptWithLogIndex {
                            starting_log_index,
                            receipt,
                        }
                    })
                    .collect()
            }
            ReceiptStorageRepr::V1(receipts) => receipts,
        })
    }
}
#[cfg(test)]
mod tests {
    use std::iter::repeat;

    use alloy_consensus::{BlockBody, Receipt, ReceiptWithBloom, SignableTransaction, TxEip1559};
    use alloy_primitives::{Bloom, Log, LogData, B256, U256};
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;

    use super::*;
    use crate::storage::memory::MemoryStorage;

    // Helper functions for creating test data
    fn create_test_tx() -> TxEnvelopeWithSender {
        let tx = TxEip1559 {
            nonce: 123,
            gas_limit: 456,
            max_fee_per_gas: 789,
            max_priority_fee_per_gas: 135,
            ..Default::default()
        };
        let signer = PrivateKeySigner::from_bytes(&B256::from(U256::from(123))).unwrap();
        let sig = signer.sign_hash_sync(&tx.signature_hash()).unwrap();
        let tx = tx.into_signed(sig);
        let tx_envelope = TxEnvelope::from(tx);

        TxEnvelopeWithSender {
            sender: tx_envelope.recover_signer().unwrap(),
            tx: tx_envelope,
        }
    }

    fn create_test_block(number: u64) -> Block {
        Block {
            header: Header {
                number,
                ..Default::default()
            },
            body: BlockBody {
                transactions: vec![create_test_tx()],
                ommers: vec![],
                withdrawals: None,
            },
        }
    }

    fn create_test_receipt(receipt_len: usize) -> ReceiptWithLogIndex {
        let receipt = ReceiptEnvelope::Eip1559(ReceiptWithBloom::new(
            Receipt::<Log> {
                logs: vec![Log {
                    address: Default::default(),
                    data: LogData::new(
                        vec![],
                        repeat(42).take(receipt_len).collect::<Vec<u8>>().into(),
                    )
                    .unwrap(),
                }],
                status: alloy_consensus::Eip658Value::Eip658(true),
                cumulative_gas_used: 55,
            },
            Bloom::repeat_byte(b'a'),
        ));
        ReceiptWithLogIndex {
            receipt,
            starting_log_index: 0,
        }
    }

    mod storage_repr {
        use super::*;

        fn create_v0_block() -> AlloyBlock<TxEnvelope, Header> {
            let tx = TxEip1559 {
                nonce: 123,
                gas_limit: 456,
                max_fee_per_gas: 789,
                max_priority_fee_per_gas: 135,
                ..Default::default()
            };
            let signer = PrivateKeySigner::from_bytes(&B256::from(U256::from(123))).unwrap();
            let sig = signer.sign_hash_sync(&tx.signature_hash()).unwrap();
            let tx = tx.into_signed(sig);

            AlloyBlock {
                header: Header {
                    number: 1,
                    ..Default::default()
                },
                body: BlockBody {
                    transactions: vec![tx.into()],
                    ommers: vec![],
                    withdrawals: None,
                },
            }
        }

        fn create_v0_receipts() -> Vec<ReceiptEnvelope> {
            vec![ReceiptEnvelope::Eip1559(ReceiptWithBloom::new(
                Receipt {
                    logs: vec![Log {
                        address: Default::default(),
                        data: LogData::new(vec![], vec![1, 2, 3].into()).unwrap(),
                    }],
                    status: alloy_consensus::Eip658Value::Eip658(true),
                    cumulative_gas_used: 21000,
                },
                Bloom::repeat_byte(b'a'),
            ))]
        }

        #[test]
        fn test_block_storage_v0_encode_decode() {
            let v0_block = create_v0_block();
            let repr = BlockStorageRepr::V0(v0_block.clone());

            let encoded = repr.encode().unwrap();
            assert_eq!(encoded[0], BlockStorageRepr::SENTINEL);
            assert_eq!(encoded[1], BlockStorageRepr::V0_MARKER);

            let decoded = BlockStorageRepr::decode_and_convert(&encoded).unwrap();
            assert_eq!(decoded.header.number, 1);
            assert_eq!(decoded.body.transactions.len(), 1);

            let expected_sender = v0_block.body.transactions[0].recover_signer().unwrap();
            assert_eq!(decoded.body.transactions[0].sender, expected_sender);
        }

        #[test]
        fn test_block_storage_v1_encode_decode() {
            let block = create_test_block(1);
            let repr = BlockStorageRepr::V1(block.clone());

            let encoded = repr.encode().unwrap();
            assert_eq!(encoded[0], BlockStorageRepr::SENTINEL);
            assert_eq!(encoded[1], BlockStorageRepr::V1_MARKER);

            let decoded = BlockStorageRepr::decode_and_convert(&encoded).unwrap();
            assert_eq!(decoded.header.number, block.header.number);
            assert_eq!(
                decoded.body.transactions[0].sender,
                block.body.transactions[0].sender
            );
        }

        #[test]
        fn test_receipt_storage_v0_encode_decode() {
            let v0_receipts = create_v0_receipts();
            let repr = ReceiptStorageRepr::V0(v0_receipts.clone());

            let encoded = repr.encode().unwrap();
            assert_eq!(encoded[0], ReceiptStorageRepr::SENTINEL);
            assert_eq!(encoded[1], ReceiptStorageRepr::V0_MARKER);

            let decoded = ReceiptStorageRepr::decode_and_convert(&encoded).unwrap();
            assert_eq!(decoded.len(), 1);
            assert_eq!(decoded[0].starting_log_index, 0);
            assert_eq!(decoded[0].receipt, v0_receipts[0]);
        }

        #[test]
        fn test_receipt_storage_v1_encode_decode() {
            // Create receipts as V0 first to let conversion handle log indices
            let v0_receipts = vec![
                ReceiptEnvelope::Eip1559(ReceiptWithBloom::new(
                    Receipt {
                        logs: vec![Log {
                            address: Default::default(),
                            data: LogData::new(
                                vec![],
                                repeat(42).take(10).collect::<Vec<u8>>().into(),
                            )
                            .unwrap(),
                        }],
                        status: alloy_consensus::Eip658Value::Eip658(true),
                        cumulative_gas_used: 21000,
                    },
                    Bloom::repeat_byte(b'a'),
                )),
                ReceiptEnvelope::Eip1559(ReceiptWithBloom::new(
                    Receipt {
                        logs: vec![Log {
                            address: Default::default(),
                            data: LogData::new(
                                vec![],
                                repeat(42).take(20).collect::<Vec<u8>>().into(),
                            )
                            .unwrap(),
                        }],
                        status: alloy_consensus::Eip658Value::Eip658(true),
                        cumulative_gas_used: 42000,
                    },
                    Bloom::repeat_byte(b'a'),
                )),
            ];

            // Convert to V1 via proper conversion to get correct indices
            let receipts = ReceiptStorageRepr::V0(v0_receipts).convert().unwrap();
            let repr = ReceiptStorageRepr::V1(receipts);

            let encoded = repr.encode().unwrap();
            assert_eq!(encoded[0], ReceiptStorageRepr::SENTINEL);
            assert_eq!(encoded[1], ReceiptStorageRepr::V1_MARKER);

            let decoded = ReceiptStorageRepr::decode_and_convert(&encoded).unwrap();
            assert_eq!(decoded.len(), 2);
            assert_eq!(decoded[0].starting_log_index, 0);
            assert_eq!(decoded[1].starting_log_index, 1);
        }

        #[test]
        fn test_receipt_storage_log_index_calculation() {
            let v0_receipts = vec![
                // First receipt with 2 logs
                ReceiptEnvelope::Eip1559(ReceiptWithBloom::new(
                    Receipt {
                        logs: vec![
                            Log {
                                address: Default::default(),
                                data: LogData::new(vec![], vec![1].into()).unwrap(),
                            },
                            Log {
                                address: Default::default(),
                                data: LogData::new(vec![], vec![2].into()).unwrap(),
                            },
                        ],
                        status: alloy_consensus::Eip658Value::Eip658(true),
                        cumulative_gas_used: 21000,
                    },
                    Bloom::repeat_byte(b'a'),
                )),
                // Second receipt with 1 log
                ReceiptEnvelope::Eip1559(ReceiptWithBloom::new(
                    Receipt {
                        logs: vec![Log {
                            address: Default::default(),
                            data: LogData::new(vec![], vec![3].into()).unwrap(),
                        }],
                        status: alloy_consensus::Eip658Value::Eip658(true),
                        cumulative_gas_used: 42000,
                    },
                    Bloom::repeat_byte(b'a'),
                )),
            ];

            let repr = ReceiptStorageRepr::V0(v0_receipts);
            let converted = repr.convert().unwrap();

            assert_eq!(converted[0].starting_log_index, 0);
            assert_eq!(converted[1].starting_log_index, 2);
        }

        #[test]
        fn test_invalid_storage_data() {
            assert!(BlockStorageRepr::decode_and_convert(&[]).is_err());
            assert!(ReceiptStorageRepr::decode_and_convert(&[]).is_err());

            assert!(BlockStorageRepr::decode_and_convert(&[
                BlockStorageRepr::SENTINEL,
                99,
                0,
                0,
                0
            ])
            .is_err());
        }
    }

    #[tokio::test]
    async fn test_basic_block_operations() {
        let store = MemoryStorage::new("test");
        let archive = BlockDataArchive::new(store);

        let block = create_test_block(1);
        archive.archive_block(block.clone()).await.unwrap();

        let retrieved_block = archive.get_block_by_number(1).await.unwrap();
        assert_eq!(retrieved_block.header.number, 1);
        assert_eq!(retrieved_block.body.transactions.len(), 1);
        assert_eq!(
            retrieved_block.body.transactions[0].sender,
            block.body.transactions[0].sender
        );

        let block_hash = block.header.hash_slow();
        let retrieved_by_hash = archive.get_block_by_hash(&block_hash).await.unwrap();
        assert_eq!(retrieved_by_hash.header.number, 1);
    }

    #[tokio::test]
    async fn test_receipts_and_traces() {
        let store = MemoryStorage::new("test");
        let archive = BlockDataArchive::new(store);

        let block_num = 1;
        let receipts = vec![create_test_receipt(10)];
        let traces = vec![vec![1, 2, 3]];

        archive
            .archive_receipts(receipts.clone(), block_num)
            .await
            .unwrap();
        archive
            .archive_traces(traces.clone(), block_num)
            .await
            .unwrap();

        let retrieved_receipts = archive.get_block_receipts(block_num).await.unwrap();
        assert_eq!(retrieved_receipts.len(), 1);
        assert_eq!(retrieved_receipts[0].starting_log_index, 0);

        let retrieved_traces = archive.get_block_traces(block_num).await.unwrap();
        assert_eq!(retrieved_traces, traces);
    }

    #[tokio::test]
    async fn test_latest_operations() {
        let store = MemoryStorage::new("test");
        let archive = BlockDataArchive::new(store);

        let initial_latest = archive.get_latest(LatestKind::Uploaded).await.unwrap_or(0);
        assert_eq!(initial_latest, 0);

        archive
            .update_latest(5, LatestKind::Uploaded)
            .await
            .unwrap();

        let latest = archive.get_latest(LatestKind::Uploaded).await.unwrap();
        assert_eq!(latest, 5);
    }
}
