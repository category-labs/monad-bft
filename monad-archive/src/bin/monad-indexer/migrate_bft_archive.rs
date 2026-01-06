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

use std::path::{Path, PathBuf};

use alloy_rlp::{Decodable, Encodable, RlpDecodable, RlpEncodable};
use futures::future::join_all;
use monad_archive::{
    model::bft_ledger::{BftBlockHeader, BftBlockModel},
    prelude::*,
};
use monad_block_persist::{block_id_to_hex_prefix, BlockPersist, FileBlockPersist};
use monad_consensus_types::{
    block::ConsensusBlockHeader,
    payload::{ConsensusBlockBody, ConsensusBlockBodyId, RoundSignature},
    quorum_certificate::QuorumCertificate,
    voting::Vote,
};
use monad_node_config::{ExecutionProtocolType, SignatureCollectionType, SignatureType};
use monad_types::{BlockId, Epoch, ExecutionProtocol, Hash, NodeId, Round, SeqNum};
use serde::{Deserialize, Serialize};

const LEGACY_BFT_BLOCKS_PREFIX: &str = "bft_blocks/";
const BFT_INDEX_MARKERS_PREFIX: &str = "bft/index_markers/";

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct IndexedRangeMarker {
    start_num: u64,
    end_num: u64,
    start_id: BlockId,
    end_id: BlockId,
}

#[derive(Clone)]
pub struct BftBlockIndex {
    pub kv: KVStoreErased,
    pub model: BftBlockModel,
}

impl BftBlockIndex {
    pub fn new(kv: KVStoreErased) -> Self {
        Self {
            model: BftBlockModel::new(kv.clone()),
            kv,
        }
    }

    pub async fn index_bft_headers(
        &self,
        concurrency: usize,
        max_num_per_batch: usize,
    ) -> Result<()> {
        let known_committable_heads = Arc::new(self.ensure_markers(concurrency).await?);
        info!("Indexing {} sub-chains", known_committable_heads.len());
        for marker in known_committable_heads.iter() {
            info!("Marker: {}", marker);
        }

        let handles = known_committable_heads.iter().map(|marker| {
            let other_sub_chain_tips = known_committable_heads
                .iter()
                .map(|h| h.start_id)
                .filter(|h| h != &marker.start_id)
                .collect::<HashSet<BlockId>>();

            let mut marker = marker.clone();
            let this = self.clone();
            tokio::spawn(async move {
                this.index_sub_chain(&mut marker, other_sub_chain_tips, max_num_per_batch)
                    .await
                    .wrap_err("Failed to index sub-chain")?;
                info!(
                    "Sub-chain indexed successfully, deleting marker {:?}",
                    &marker
                );
                this.delete_marker(&marker).await
            })
        });

        for result in join_all(handles).await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    error!(?e, "Failed to index sub-chain");
                }
                Err(e) => {
                    error!(?e, "Failed to index sub-chain");
                }
            }
        }

        Ok(())
    }

    async fn ensure_markers(&self, concurrency: usize) -> Result<HashSet<IndexedRangeMarker>> {
        let mut known_committable_heads = self.get_markers().await?;

        if known_committable_heads.len() >= concurrency {
            info!("Found enough committable heads, stopping");
            return Ok(known_committable_heads);
        }

        let candidates = self
            .kv
            // Get extras in case some are not canonical
            .scan_prefix_with_max_keys(LEGACY_BFT_BLOCKS_PREFIX, concurrency * 2)
            .await?
            .into_iter()
            .filter_map(|key| {
                let hex_str = key.strip_prefix(LEGACY_BFT_BLOCKS_PREFIX)?;
                let bytes = hex::decode(hex_str).ok()?;
                let arr: [u8; 32] = bytes.try_into().ok()?;
                Some(BlockId(Hash(arr)))
            });

        for candidate in candidates {
            if known_committable_heads.len() >= concurrency {
                info!("Found enough committable heads, stopping");
                break;
            }

            let header = self.fetch_header_by_id(&candidate).await?;
            match self.find_committable_head(header).await {
                Ok(Some(committable_id)) => {
                    info!("Found committable head {:?}", committable_id);
                    let committable_header = self.fetch_header_by_id(&committable_id).await?;
                    known_committable_heads.insert(IndexedRangeMarker {
                        start_num: committable_header.seq_num.0,
                        end_num: committable_header.seq_num.0,
                        start_id: committable_id,
                        end_id: committable_id,
                    });
                }
                Ok(None) => {}
                Err(e) => {
                    error!(
                        ?e,
                        "Failed to find committable head for header {:?}", candidate
                    );
                }
            }
        }

        futures::stream::iter(known_committable_heads.iter())
            .map(|marker| async move { self.write_marker(&marker).await })
            .buffer_unordered(100)
            .try_collect::<()>()
            .await?;

        Ok(known_committable_heads)
    }

    async fn find_committable_head(
        &self,
        mut bft_header: BftBlockHeader,
    ) -> Result<Option<BlockId>> {
        loop {
            let parent_id = bft_header.get_parent_id();
            let parent_header = self.fetch_header_by_id(&parent_id).await?;
            if let Some(committable) = bft_header.qc.get_committable_id(&parent_header) {
                return Ok(Some(committable));
            }
            bft_header = parent_header;
        }
    }

    async fn index_sub_chain(
        &self,
        marker: &mut IndexedRangeMarker,
        other_sub_chain_tips: HashSet<BlockId>,
        max_num_per_batch: usize,
    ) -> Result<()> {
        while !other_sub_chain_tips.contains(&marker.end_id) {
            info!(
                "Indexing sub-chain from num: {}, to num: {}",
                marker.end_num,
                marker.end_num.saturating_sub(max_num_per_batch as u64)
            );
            for _ in 0..max_num_per_batch {
                if other_sub_chain_tips.contains(&marker.end_id) {
                    return Ok(());
                }
                let current_id = marker.end_id;
                let current_header = self.fetch_header_by_id(&current_id).await?;
                self.model
                    .put_index(current_header.seq_num.0, &current_id)
                    .await?;
                marker.end_id = current_header.get_parent_id();
                marker.end_num = current_header.seq_num.0.saturating_sub(1);
                info!(
                    "Indexed bft block num: {}, id: {}",
                    current_header.seq_num.0,
                    hex::encode(current_id.0)
                );
            }

            self.write_marker(&marker).await?;
            info!("Written marker {:?}", marker);
        }

        Ok(())
    }

    async fn get_markers(&self) -> Result<HashSet<IndexedRangeMarker>> {
        let markers = self.kv.scan_prefix(BFT_INDEX_MARKERS_PREFIX).await?;

        futures::stream::iter(markers)
            .map(|s| async move {
                let value = self
                    .kv
                    .get(&format!("{BFT_INDEX_MARKERS_PREFIX}{s}"))
                    .await?
                    .ok_or_eyre("Marker not found")?;
                serde_json::from_slice(&value[..]).wrap_err("Failed to deserialize index range")
            })
            .buffer_unordered(100)
            .try_collect()
            .await
    }

    async fn write_marker(&self, range: &IndexedRangeMarker) -> Result<()> {
        let key = format!("{BFT_INDEX_MARKERS_PREFIX}{}", range.start_num);
        let bytes = serde_json::to_vec(&range).wrap_err("Failed to serialize index range")?;
        self.kv
            .put(key, bytes)
            .await
            .wrap_err("Failed to write index marker")?;
        Ok(())
    }

    async fn delete_marker(&self, range: &IndexedRangeMarker) -> Result<()> {
        let key = format!("{BFT_INDEX_MARKERS_PREFIX}{}", range.start_num);
        self.kv
            .delete(&key)
            .await
            .wrap_err("Failed to delete index marker")
    }

    pub async fn fetch_header_by_id(&self, hash: &BlockId) -> Result<BftBlockHeader> {
        let key = format!("{}{}", LEGACY_BFT_BLOCKS_PREFIX, hex::encode(hash.0));
        let value = self.kv.get(&key).await?.ok_or_eyre("Header not found")?;
        BftBlockHeader::decode(&mut &value[..]).wrap_err("Failed to decode header")
    }
}

impl std::fmt::Display for IndexedRangeMarker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "IndexedRangeMarker: num: {}..{}, id: {}..{}",
            self.start_num,
            self.end_num,
            hex::encode(self.start_id.0),
            hex::encode(self.end_id.0)
        )
    }
}
