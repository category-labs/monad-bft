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

use alloy_rlp::{Decodable, Encodable};
use monad_consensus_types::{block::ConsensusBlockHeader, payload::ConsensusBlockBody};
use monad_node_config::{ExecutionProtocolType, SignatureCollectionType, SignatureType};
use monad_types::{BlockId, Hash, SeqNum};

use crate::{kvstore::WritePolicy, prelude::*};

pub type BftBlockBody = ConsensusBlockBody<ExecutionProtocolType>;
pub type BftBlockHeader =
    ConsensusBlockHeader<SignatureType, SignatureCollectionType, ExecutionProtocolType>;

const BFT_BLOCK_NUM_PREFIX: &str = "bft/index/";
const BFT_BLOCK_BODIES_PREFIX: &str = "bft/ledger/bodies/";
const BFT_BLOCK_HEADERS_PREFIX: &str = "bft/ledger/headers/";
const LATEST_UPLOADED_KEY: &str = "bft/ledger/latest_uploaded";

pub fn bft_block_num_key_from_seq_num(seq_num: &SeqNum) -> String {
    format!("{}{}", BFT_BLOCK_NUM_PREFIX, seq_num.0)
}

#[derive(Clone)]
pub struct BftBlockModel {
    pub kv: KVStoreErased,
}

impl BftBlockModel {
    pub fn new(kv: KVStoreErased) -> Self {
        Self { kv }
    }

    pub async fn get_id_by_num(&self, num: u64) -> Result<BlockId> {
        let key = format!("{}{}", BFT_BLOCK_NUM_PREFIX, num);
        let value = self
            .kv
            .get(&key)
            .await?
            .ok_or_eyre("Block number not found")?;
        let id = hex::decode(&value)
            .wrap_err_with(|| format!("Failed to decode hex block id at key {key}"))?;
        let id_len = id.len();
        let id: [u8; 32] = id.try_into().map_err(|_| {
            eyre!("Invalid block id length at key {key}: expected 32, got {id_len}")
        })?;
        Ok(BlockId(Hash(id)))
    }

    pub async fn put_index(&self, seq_num: u64, header_id: &BlockId) -> Result<()> {
        let encoded_hash = hex::encode(header_id.0).into_bytes();
        let key = format!("{}{}", BFT_BLOCK_NUM_PREFIX, seq_num);
        self.kv
            .put(key, encoded_hash, WritePolicy::AllowOverwrite)
            .await?;
        Ok(())
    }

    pub async fn get_header_by_id(&self, id: &BlockId) -> Result<BftBlockHeader> {
        let hash = hex::encode(id.0);
        let key = format!("{BFT_BLOCK_HEADERS_PREFIX}{hash}");
        let value = self.kv.get(&key).await?.ok_or_eyre("Header not found")?;
        BftBlockHeader::decode(&mut &value[..]).wrap_err("Failed to decode header")
    }

    pub async fn put_header_bytes(&self, id: &BlockId, bytes: &[u8]) -> Result<()> {
        let hash = hex::encode(id.0);
        let key = format!("{BFT_BLOCK_HEADERS_PREFIX}{hash}");
        self.kv
            .put(key, bytes.to_vec(), WritePolicy::AllowOverwrite)
            .await?;
        Ok(())
    }

    pub async fn put_header(&self, header: &BftBlockHeader) -> Result<()> {
        let id = header.get_id();
        let mut buf = Vec::new();
        header.encode(&mut buf);
        self.put_header_bytes(&id, &buf).await
    }

    pub async fn get_body_by_id(&self, id: &BlockId) -> Result<BftBlockBody> {
        let hash: &str = &hex::encode(id.0);
        let key = format!("{BFT_BLOCK_BODIES_PREFIX}{hash}");
        let value = self.kv.get(&key).await?.ok_or_eyre("Body not found")?;
        BftBlockBody::decode(&mut &value[..]).wrap_err("Failed to decode body")
    }

    pub async fn put_body(&self, id: &monad_types::Hash, body: &BftBlockBody) -> Result<()> {
        let mut buf = Vec::new();
        body.encode(&mut buf);
        self.put_body_bytes(id, &buf).await
    }

    pub async fn put_body_bytes(&self, id: &monad_types::Hash, bytes: &[u8]) -> Result<()> {
        let hash = hex::encode(id);
        let key = format!("{BFT_BLOCK_BODIES_PREFIX}{hash}");
        self.kv
            .put(key, bytes.to_vec(), WritePolicy::AllowOverwrite)
            .await?;
        Ok(())
    }

    pub async fn get_latest_uploaded(&self) -> Result<Option<u64>> {
        let Some(bytes) = self.kv.get(LATEST_UPLOADED_KEY).await? else {
            return Ok(None);
        };
        let arr: [u8; 8] = bytes.as_ref().try_into().map_err(|_| {
            eyre!(
                "Invalid latest_uploaded value length at key {LATEST_UPLOADED_KEY}: expected 8, got {}",
                bytes.len()
            )
        })?;
        Ok(Some(u64::from_be_bytes(arr)))
    }

    pub async fn set_latest_uploaded(&self, latest_uploaded: u64) -> Result<()> {
        self.kv
            .put(
                LATEST_UPLOADED_KEY,
                latest_uploaded.to_be_bytes().to_vec(),
                WritePolicy::AllowOverwrite,
            )
            .await?;
        Ok(())
    }
}
