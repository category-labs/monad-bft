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

use core::str;

use alloy_consensus::{Block as AlloyBlock, BlockBody, Header, ReceiptEnvelope, TxEnvelope};
use alloy_primitives::{Address, BlockHash, Bytes, Log, U8};
use alloy_rlp::{Decodable, Encodable, RlpDecodable, RlpEncodable, EMPTY_LIST_CODE};
use bytes::BufMut;
use eyre::bail;
use futures::try_join;
use monad_triedb_utils::triedb_env::{ReceiptWithLogIndex, TxEnvelopeWithSender};
use rayon::iter::{IntoParallelIterator, ParallelIterator};

use crate::{prelude::*, rlp_offset_scanner::get_all_tx_offsets};

pub type Block = AlloyBlock<TxEnvelopeWithSender, Header>;
pub type BlockReceipts = Vec<ReceiptWithLogIndex>;
pub type BlockTraces = Vec<Vec<u8>>;

pub(super) const BLOCK_PADDING_WIDTH: usize = 12;

pub(crate) enum BlockStorageRepr {
    V0(AlloyBlock<TxEnvelope, Header>),
    V1(Block),
    V2(Block),
}

pub(crate) enum ReceiptStorageRepr {
    V0(Vec<ReceiptEnvelope>),
    V1(BlockReceipts),
}

#[derive(Clone)]
pub struct BlockDataArchive {
    pub store: KVStoreErased,

    pub latest_uploaded_table_key: &'static str,
    pub latest_indexed_table_key: &'static str,
    pub block_table_prefix: &'static str,
    pub block_hash_table_prefix: &'static str,
    pub receipts_table_prefix: &'static str,
    pub traces_table_prefix: &'static str,
}

impl BlockDataReader for BlockDataArchive {
    fn get_bucket(&self) -> &str {
        self.store.bucket_name()
    }

    async fn get_latest(&self, latest_kind: LatestKind) -> Result<Option<u64>> {
        let key = match latest_kind {
            LatestKind::Uploaded => &self.latest_uploaded_table_key,
            LatestKind::Indexed => &self.latest_indexed_table_key,
        };

        let value = match self
            .store
            .get(key)
            .await
            .wrap_err("No latest block found")?
        {
            Some(value) => value,
            None => return Ok(None),
        };

        let value_str = String::from_utf8(value.to_vec()).wrap_err("Invalid UTF-8 sequence")?;

        // Parse the string as u64
        value_str
            .parse::<u64>()
            .wrap_err_with(|| {
                format!("Unable to convert block_number string to number (u64), value: {value_str}")
            })
            .map(Some)
    }

    #[doc = "Get a block by its number, or return None if not found"]
    async fn try_get_block_by_hash(&self, block_hash: &BlockHash) -> Result<Option<Block>> {
        let block_hash_key_suffix = hex::encode(block_hash);
        let block_hash_key = format!("{}/{}", self.block_hash_table_prefix, block_hash_key_suffix);

        let Some(block_num_bytes) = self
            .store
            .get(&block_hash_key)
            .await
            .wrap_err("Error getting block hash")?
        else {
            return Ok(None);
        };

        let block_num_str =
            String::from_utf8(block_num_bytes.to_vec()).wrap_err("Invalid UTF-8 sequence")?;

        let block_num = block_num_str.parse::<u64>().wrap_err_with(|| {
            format!("Unable to convert block_number string to number (u64), value: {block_num_str}")
        })?;

        self.try_get_block_by_number(block_num).await
    }

    async fn get_block_data_with_offsets(&self, block_num: u64) -> Result<BlockDataWithOffsets> {
        let block_key = self.block_key(block_num);
        let traces_key = self.traces_key(block_num);
        let receipts_key = self.receipts_key(block_num);
        let (block_rlp, traces_rlp, receipts_rlp) = try_join!(
            self.store.get(&block_key),
            self.store.get(&traces_key),
            self.store.get(&receipts_key),
        )
        .wrap_err("Error getting block data")?;

        let (block_rlp, mut traces_rlp, receipts_rlp): (&[u8], &[u8], &[u8]) = (
            &block_rlp.wrap_err("No block found")?,
            &traces_rlp.wrap_err("No trace found")?,
            &receipts_rlp.wrap_err("No receipt found")?,
        );

        // WARN: extracting offsets is the same for all representations currently, but that may not always be the case
        let offsets = get_all_tx_offsets(
            BlockStorageRepr::rlp_list_slice(block_rlp),
            ReceiptStorageRepr::rlp_list_slice(receipts_rlp),
            traces_rlp,
        )?;

        Ok(BlockDataWithOffsets {
            block: BlockStorageRepr::decode_and_convert(block_rlp)
                .await
                .wrap_err("Failed to decode block")?,
            receipts: ReceiptStorageRepr::decode_and_convert(receipts_rlp)
                .wrap_err("Failed to decode receipts")?,
            traces: Vec::<Vec<u8>>::decode(&mut traces_rlp).wrap_err("Failed to decode traces")?,
            offsets: Some(offsets),
        })
    }

    #[doc = "Get a block by its number, or return None if not found"]
    async fn try_get_block_by_number(&self, block_num: u64) -> Result<Option<Block>> {
        let Some(bytes) = self
            .store
            .get(&self.block_key(block_num))
            .await
            .wrap_err("Error getting block")?
        else {
            return Ok(None);
        };
        BlockStorageRepr::decode_and_convert(&bytes)
            .await
            .wrap_err_with(|| format!("Failed to decode block: block_num: {}", block_num))
            .map(Some)
    }

    #[doc = "Get receipts for a block, or return None if not found"]
    async fn try_get_block_receipts(&self, block_number: u64) -> Result<Option<BlockReceipts>> {
        let receipts_key = self.receipts_key(block_number);

        let Some(rlp_receipts) = self
            .store
            .get(&receipts_key)
            .await
            .wrap_err("Error getting receipts")?
        else {
            return Ok(None);
        };

        ReceiptStorageRepr::decode_and_convert(&rlp_receipts)
            .wrap_err_with(|| {
                format!(
                    "Failed to decode block receipts: block_num: {}",
                    block_number
                )
            })
            .map(Some)
    }

    #[doc = "Get execution traces for a block, or return None if not found"]
    async fn try_get_block_traces(&self, block_number: u64) -> Result<Option<BlockTraces>> {
        let traces_key = self.traces_key(block_number);

        let Some(rlp_traces) = self
            .store
            .get(&traces_key)
            .await
            .wrap_err("Error getting traces")?
        else {
            return Ok(None);
        };
        let mut rlp_traces_slice: &[u8] = &rlp_traces;

        Vec::decode(&mut rlp_traces_slice)
            .wrap_err("Cannot decode block")
            .map(Some)
    }
}

impl BlockDataArchive {
    pub fn new(archive: impl Into<KVStoreErased>) -> Self {
        BlockDataArchive {
            store: archive.into(),
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
        self.store.put(key, latest_value.as_bytes().to_vec()).await
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
            self.store.put(&block_key, encoded_block),
            self.store.put(&block_hash_key, block_hash_value.to_vec())
        )?;
        Ok(())
    }

    pub async fn archive_receipts(&self, receipts: BlockReceipts, block_num: u64) -> Result<()> {
        self.store
            .put(
                &self.receipts_key(block_num),
                ReceiptStorageRepr::V1(receipts).encode()?,
            )
            .await
    }

    pub async fn archive_traces(&self, traces: BlockTraces, block_num: u64) -> Result<()> {
        let mut rlp_traces = vec![];
        traces.encode(&mut rlp_traces);

        self.store
            .put(&self.traces_key(block_num), rlp_traces)
            .await
    }
}

pub fn encode_block(block: Block) -> Result<Vec<u8>> {
    BlockStorageRepr::V2(block).encode()
}

impl BlockStorageRepr {
    const SENTINEL: u8 = 55;
    const V0_MARKER: u8 = 0;
    const V1_MARKER: u8 = 1;
    const V2_MARKER: u8 = 2;

    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
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
                BlockStorageRepr::V2(block) => {
                    buf.put_u8(Self::V2_MARKER);
                    block.encode(buf);
                }
            }
        }
        Ok(buf)
    }

    pub(crate) async fn decode_and_convert(buf: &[u8]) -> Result<Block> {
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
            [Self::SENTINEL, Self::V2_MARKER] => {
                Block::decode(&mut &buf[2..]).map(BlockStorageRepr::V2)
            }
            _ => {
                AlloyBlock::<TxEnvelope, Header>::decode(&mut &buf[..]) // fmt
                    .map(BlockStorageRepr::V0)
            }
        };

        match decoding_result {
            Ok(decoded) => decoded.convert().await,
            Err(e) => {
                info!(?e, "Failed to parse BlockStorageRepr despite sentinel bit being set. Falling back to raw InlineV0 decoding...");
                let v0 =
                    BlockStorageRepr::V0(AlloyBlock::<TxEnvelope, Header>::decode(&mut &buf[..])?);
                v0.convert().await
            }
        }
    }

    async fn convert(self) -> Result<Block> {
        Ok(match self {
            BlockStorageRepr::V0(block) => {
                // Sender recovery is expensive, so run it on a non-worker thread
                let transactions = if block.body.transactions.is_empty() {
                    vec![]
                } else {
                    spawn_rayon_async(move || {
                        block
                            .body
                            .transactions
                            .into_par_iter()
                            .map(|tx| -> Result<TxEnvelopeWithSender> {
                                Ok(TxEnvelopeWithSender {
                                    sender: tx.recover_signer()?,
                                    tx,
                                })
                            })
                            .collect::<Result<Vec<TxEnvelopeWithSender>>>()
                    })
                    .await??
                };

                Block {
                    header: block.header,
                    body: BlockBody {
                        ommers: block.body.ommers,
                        withdrawals: Some(alloy_eips::eip4895::Withdrawals::default()),
                        transactions,
                    },
                }
            }
            BlockStorageRepr::V1(mut block) => {
                if block.body.withdrawals.is_none() {
                    block.body.withdrawals = Some(alloy_eips::eip4895::Withdrawals::default());
                }
                block
            }
            BlockStorageRepr::V2(block) => block,
        })
    }

    fn rlp_list_slice(buf: &[u8]) -> &[u8] {
        if buf.len() < 2 {
            return buf;
        }

        let marker_bytes = [buf[0], buf[1]];
        match marker_bytes {
            [Self::SENTINEL, Self::V0_MARKER] | [Self::SENTINEL, Self::V1_MARKER] => &buf[2..],
            _ => buf,
        }
    }
}

impl ReceiptStorageRepr {
    const SENTINEL: u8 = 88;
    const V0_MARKER: u8 = 0;
    const V1_MARKER: u8 = 1;

    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
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

    pub(crate) fn decode_and_convert(buf: &[u8]) -> Result<BlockReceipts> {
        // empty receipt list
        if buf == [EMPTY_LIST_CODE] {
            return Ok(vec![]);
        }

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

    fn convert(self) -> Result<BlockReceipts> {
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

    fn rlp_list_slice(buf: &[u8]) -> &[u8] {
        if buf.len() < 2 {
            return buf;
        }

        let marker_bytes = [buf[0], buf[1]];
        match marker_bytes {
            [Self::SENTINEL, Self::V0_MARKER] | [Self::SENTINEL, Self::V1_MARKER] => &buf[2..],
            _ => buf,
        }
    }
}

pub fn decode_traces(traces: &BlockTraces) -> Result<Vec<Vec<Vec<CallFrame>>>, alloy_rlp::Error> {
    traces.iter().map(Vec::as_slice).map(decode_trace).collect()
}

pub fn decode_trace(trace: &[u8]) -> Result<Vec<Vec<CallFrame>>, alloy_rlp::Error> {
    Vec::<Vec<CallFrame>>::decode(&mut &trace[..])
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallKind {
    Call,
    DelegateCall,
    CallCode,
    Create,
    Create2,
    SelfDestruct,
    StaticCall,
}

#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct CallFrameLog {
    pub log: Log,
    pub position: U64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CallFrame {
    pub typ: CallKind,
    pub flags: U64,
    pub from: Address,
    pub to: Option<Address>,
    pub value: U256,
    pub gas: U64,
    pub gas_used: U64,
    pub input: Bytes,
    pub output: Bytes,
    pub status: U8,
    pub depth: U64,
    pub logs: Option<Vec<CallFrameLog>>,
}

impl Decodable for CallFrame {
    fn decode(rlp_buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let typ: U8 = U8::decode(rlp_buf)?;
        let flags: U64 = U64::decode(rlp_buf)?;
        let from: Address = Address::decode(rlp_buf)?;

        // Decode the `to` field, handling the case where it's `None`.
        let to: Option<Address> = {
            let first_byte = rlp_buf.first().ok_or(alloy_rlp::Error::InputTooShort)?;
            if *first_byte == 0x80 {
                // If the first byte is 0x80, it represents an empty value (None for the Address).
                *rlp_buf = &rlp_buf[1..]; // Advance the buffer
                None
            } else {
                // Otherwise, decode it as a normal Address.
                Some(Address::decode(rlp_buf)?)
            }
        };

        let value: U256 = U256::decode(rlp_buf)?;
        let gas: U64 = U64::decode(rlp_buf)?;
        let gas_used: U64 = U64::decode(rlp_buf)?;
        let input = Bytes::decode(rlp_buf)?;
        let output = Bytes::decode(rlp_buf)?;
        let status: U8 = U8::decode(rlp_buf)?;
        let depth: U64 = U64::decode(rlp_buf)?;
        let logs = if rlp_buf.is_empty() {
            None
        } else {
            Some(Vec::decode(rlp_buf)?)
        };

        let typ = match typ.to::<u8>() {
            0 if flags == U64::from(1) => CallKind::StaticCall,
            0 => CallKind::Call,
            1 => CallKind::DelegateCall,
            2 => CallKind::CallCode,
            3 => CallKind::Create,
            4 => CallKind::Create2,
            5 => CallKind::SelfDestruct,
            _ => return Err(alloy_rlp::Error::Custom("Invalid call kind")),
        };

        Ok(Self {
            typ,
            flags,
            from,
            to,
            value,
            gas,
            gas_used,
            input,
            output,
            status,
            depth,
            logs,
        })
    }
}

impl Encodable for CallFrame {
    fn encode(&self, out: &mut dyn BufMut) {
        let typ: u8 = match self.typ {
            CallKind::Call => 0,
            CallKind::StaticCall => 0,
            CallKind::DelegateCall => 1,
            CallKind::CallCode => 2,
            CallKind::Create => 3,
            CallKind::Create2 => 4,
            CallKind::SelfDestruct => 5,
        };
        typ.encode(out);
        self.flags.encode(out);
        self.from.encode(out);
        if let Some(to) = self.to {
            to.encode(out);
        } else {
            out.put_u8(0x80);
        }
        self.value.encode(out);
        self.gas.encode(out);
        self.gas_used.encode(out);
        self.input.encode(out);
        self.output.encode(out);
        self.status.encode(out);
        self.depth.encode(out);
        if let Some(logs) = &self.logs {
            logs.encode(out);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{iter::repeat_n, sync::atomic::Ordering};

    use alloy_consensus::{BlockBody, Receipt, ReceiptWithBloom, SignableTransaction, TxEip1559};
    use alloy_primitives::{Bloom, Log, LogData, B256, U256};
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;
    use monad_eth_testutil::make_receipt;

    use super::*;
    use crate::kvstore::memory::MemoryStorage;

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

        #[tokio::test]
        async fn test_block_storage_v0_encode_decode() {
            let v0_block = create_v0_block();
            let repr = BlockStorageRepr::V0(v0_block.clone());

            let encoded = repr.encode().unwrap();
            assert_eq!(encoded[0], BlockStorageRepr::SENTINEL);
            assert_eq!(encoded[1], BlockStorageRepr::V0_MARKER);

            let decoded = BlockStorageRepr::decode_and_convert(&encoded)
                .await
                .unwrap();
            assert_eq!(decoded.header.number, 1);
            assert_eq!(decoded.body.transactions.len(), 1);

            let expected_sender = v0_block.body.transactions[0].recover_signer().unwrap();
            assert_eq!(decoded.body.transactions[0].sender, expected_sender);
        }

        #[tokio::test]
        async fn test_block_storage_v1_encode_decode_convert() {
            let mut block = create_test_block(1);
            block.body.withdrawals = None;
            let repr = BlockStorageRepr::V1(block.clone());

            let encoded = repr.encode().unwrap();
            assert_eq!(encoded[0], BlockStorageRepr::SENTINEL);
            assert_eq!(encoded[1], BlockStorageRepr::V1_MARKER);

            let decoded = BlockStorageRepr::decode_and_convert(&encoded)
                .await
                .unwrap();
            assert_eq!(decoded.header.number, block.header.number);
            assert_eq!(
                decoded.body.transactions[0].sender,
                block.body.transactions[0].sender
            );
            assert_eq!(
                decoded.body.withdrawals,
                Some(alloy_eips::eip4895::Withdrawals::default())
            );
        }

        #[tokio::test]
        async fn test_block_storage_v1_encode_decode() {
            let block = create_test_block(1);
            let repr = BlockStorageRepr::V1(block.clone());

            let encoded = repr.encode().unwrap();
            assert_eq!(encoded[0], BlockStorageRepr::SENTINEL);
            assert_eq!(encoded[1], BlockStorageRepr::V1_MARKER);

            let decoded = BlockStorageRepr::decode_and_convert(&encoded)
                .await
                .unwrap();
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
                                repeat_n(42, 10).collect::<Vec<u8>>().into(),
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
                                repeat_n(42, 20).collect::<Vec<u8>>().into(),
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
        fn test_receipt_storage_empty_receipt() {
            let receipts: Vec<ReceiptEnvelope> = vec![];
            let mut encoded = Vec::new();
            receipts.encode(&mut encoded);

            let decoded = ReceiptStorageRepr::decode_and_convert(&encoded).unwrap();
            assert_eq!(decoded, vec![]);
        }

        #[tokio::test]
        async fn test_invalid_storage_data() {
            assert!(BlockStorageRepr::decode_and_convert(&[]).await.is_err());
            assert!(ReceiptStorageRepr::decode_and_convert(&[]).is_err());

            assert!(BlockStorageRepr::decode_and_convert(&[
                BlockStorageRepr::SENTINEL,
                99,
                0,
                0,
                0
            ])
            .await
            .is_err());
        }
    }

    #[tokio::test]
    async fn test_basic_block_operations() {
        let store = MemoryStorage::new("test");
        let failure_ptr = store.should_fail.clone();
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

        failure_ptr.store(true, Ordering::SeqCst);
        let res = archive.get_block_by_number(1).await;
        assert!(res.is_err());
        assert!(
            !res.unwrap_err()
                .to_string()
                .contains("MemoryStorage simulated failure"),
            "Top level error should not contain store specific error"
        );
    }

    #[tokio::test]
    async fn test_receipts_and_traces() {
        let store = MemoryStorage::new("test");
        let failure_ptr = store.should_fail.clone();
        let archive = BlockDataArchive::new(store);

        let block_num = 1;
        let receipt = ReceiptWithLogIndex {
            receipt: ReceiptEnvelope::Eip1559(make_receipt(10)),
            starting_log_index: 0,
        };
        let receipts = vec![receipt];
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

        failure_ptr.store(true, Ordering::SeqCst);
        let res = archive.get_block_receipts(block_num).await;
        dbg!(&res);
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert!(format!("{:?}", err).contains("MemoryStorage simulated failure"));
        assert!(
            !err.to_string().contains("MemoryStorage simulated failure"),
            "Top level error should not contain store specific error"
        );

        let res = archive.get_block_traces(block_num).await;
        dbg!(&res);
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert!(format!("{:?}", err).contains("MemoryStorage simulated failure"));
        assert!(
            !err.to_string().contains("MemoryStorage simulated failure"),
            "Top level error should not contain store specific error"
        );
    }

    #[tokio::test]
    async fn test_latest_operations() {
        let store = MemoryStorage::new("test");
        let failure_ptr = store.should_fail.clone();
        let archive = BlockDataArchive::new(store);

        let initial_latest = archive.get_latest(LatestKind::Uploaded).await.unwrap();
        assert_eq!(initial_latest, None);

        archive
            .update_latest(5, LatestKind::Uploaded)
            .await
            .unwrap();

        let latest = archive.get_latest(LatestKind::Uploaded).await.unwrap();
        assert_eq!(latest, Some(5));

        failure_ptr.store(true, Ordering::SeqCst);
        let res = archive.get_latest(LatestKind::Uploaded).await;
        dbg!(&res);
        assert!(res.is_err());
        assert!(
            !res.unwrap_err()
                .to_string()
                .contains("MemoryStorage simulated failure"),
            "Top level error should not contain store specific error"
        );
    }

    // A custom block creator so we don’t conflict with existing helpers.
    fn create_custom_block(num: u64) -> Block {
        Block {
            header: Header {
                number: num,
                ..Default::default()
            },
            body: BlockBody {
                transactions: vec![create_test_tx()],
                ommers: vec![],
                withdrawals: None,
            },
        }
    }

    #[tokio::test]
    async fn test_get_nonexistent_block() {
        let store = MemoryStorage::new("nonexistent_block");
        let archive = BlockDataArchive::new(store);
        assert!(archive.get_block_by_number(999).await.is_err());
    }

    #[tokio::test]
    async fn test_get_nonexistent_receipts_and_traces() {
        let store = MemoryStorage::new("nonexistent_receipts_traces");
        let archive = BlockDataArchive::new(store);
        assert!(archive.get_block_receipts(999).await.is_err());
        assert!(archive.get_block_traces(999).await.is_err());
    }

    #[tokio::test]
    async fn test_get_latest_invalid_utf8() {
        let store = MemoryStorage::new("invalid_latest_utf8");
        let archive = BlockDataArchive::new(store);
        // Insert non-UTF8 bytes for latest_uploaded_table_key.
        archive
            .store
            .put(archive.latest_uploaded_table_key, vec![0xff, 0xfe])
            .await
            .unwrap();
        assert!(archive.get_latest(LatestKind::Uploaded).await.is_err());
    }

    #[tokio::test]
    async fn test_get_latest_non_numeric() {
        let store = MemoryStorage::new("non_numeric_latest");
        let archive = BlockDataArchive::new(store);
        archive
            .store
            .put(archive.latest_uploaded_table_key, b"not_a_number".to_vec())
            .await
            .unwrap();
        assert!(archive.get_latest(LatestKind::Uploaded).await.is_err());
    }

    #[test]
    fn test_key_formatting() {
        let store = MemoryStorage::new("test");
        let archive = BlockDataArchive::new(store);
        let expected_block = format!("block/{:0width$}", 42, width = BLOCK_PADDING_WIDTH);
        let expected_receipts = format!("receipts/{:0width$}", 42, width = BLOCK_PADDING_WIDTH);
        let expected_traces = format!("traces/{:0width$}", 42, width = BLOCK_PADDING_WIDTH);
        assert_eq!(archive.block_key(42), expected_block);
        assert_eq!(archive.receipts_key(42), expected_receipts);
        assert_eq!(archive.traces_key(42), expected_traces);
    }

    #[tokio::test]
    async fn test_invalid_block_data() {
        let store = MemoryStorage::new("test");
        let archive = BlockDataArchive::new(store.clone());
        let key = archive.block_key(1);
        archive.store.put(&key, vec![1, 2, 3]).await.unwrap();
        assert!(archive.get_block_by_number(1).await.is_err());
    }

    #[tokio::test]
    async fn test_invalid_receipts_and_traces_data() {
        let store = MemoryStorage::new("test");
        let archive = BlockDataArchive::new(store.clone());
        archive
            .store
            .put(&archive.receipts_key(1), vec![1, 2, 3])
            .await
            .unwrap();
        archive
            .store
            .put(&archive.traces_key(1), vec![1, 2, 3])
            .await
            .unwrap();
        assert!(archive.get_block_receipts(1).await.is_err());
        assert!(archive.get_block_traces(1).await.is_err());
    }

    #[tokio::test]
    async fn test_block_data_with_offsets() {
        let store = MemoryStorage::new("test");
        let archive = BlockDataArchive::new(store.clone());
        let block = create_custom_block(1);
        let receipt = ReceiptWithLogIndex {
            receipt: ReceiptEnvelope::Eip1559(make_receipt(5)),
            starting_log_index: 0,
        };
        let receipts = vec![receipt];
        let traces = vec![vec![4, 5, 6]];
        archive.archive_block(block.clone()).await.unwrap();
        archive.archive_receipts(receipts.clone(), 1).await.unwrap();
        archive.archive_traces(traces.clone(), 1).await.unwrap();

        let data = archive.get_block_data_with_offsets(1).await.unwrap();
        assert_eq!(data.block.header.number, 1);
        assert_eq!(data.receipts.len(), receipts.len());
        assert_eq!(data.traces, traces);
        // Offsets should match the number of transactions in the block.
        assert_eq!(
            data.offsets.unwrap().len(),
            data.block.body.transactions.len()
        );
    }
}
