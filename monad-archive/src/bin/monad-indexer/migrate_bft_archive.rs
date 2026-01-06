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

use alloy_rlp::Decodable;
use futures::future::join_all;
use monad_archive::{
    kvstore::WritePolicy, model::bft_ledger::{BftBlockHeader, BftBlockModel}, prelude::*
};
use monad_types::{BlockId, Hash};
use serde::{Deserialize, Serialize};

const LEGACY_BFT_BLOCKS_PREFIX: &str = "bft_block/";
const BFT_INDEX_MARKERS_PREFIX: &str = "bft/index_markers/";
const RETRY_DELAY: Duration = Duration::from_millis(500);
const MAX_STARTUP_RETRIES: usize = 10;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct IndexedRangeMarker {
    start_num: u64,
    end_num: u64,
    start_id: BlockId,
    end_id: BlockId,
}

#[derive(Clone)]
pub struct BftBlockIndex {
    /// Read-only source for legacy bft_blocks/ data
    pub source: KVStoreErased,
    /// Read-write sink for markers and index
    pub sink: KVStoreErased,
    pub model: BftBlockModel,
}

impl BftBlockIndex {
    pub fn new(source: KVStoreErased, sink: KVStoreErased) -> Self {
        Self {
            source,
            model: BftBlockModel::new(sink.clone()),
            sink,
        }
    }

    pub async fn index_bft_headers(
        &self,
        concurrency: usize,
        max_num_per_batch: usize,
        copy_data: bool,
    ) -> Result<()> {
        let known_committable_heads = self.ensure_markers_with_retry(concurrency).await?;
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
                this.index_sub_chain(
                    &mut marker,
                    other_sub_chain_tips,
                    max_num_per_batch,
                    copy_data,
                )
                .await
                .wrap_err("Failed to index sub-chain")?;
                info!(
                    "Sub-chain indexed successfully, deleting marker {:?}",
                    &marker
                );
                this.delete_marker(&marker).await
            })
        });

        let mut errors = Vec::new();
        for result in join_all(handles).await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    error!(?e, "Failed to index sub-chain");
                    errors.push(e);
                }
                Err(e) => {
                    error!(?e, "Task panicked while indexing sub-chain");
                    errors.push(eyre::eyre!("Task panicked: {e}"));
                }
            }
        }

        if let Some(error) = errors.pop() {
            return Err(error);
        }

        Ok(())
    }

    async fn ensure_markers_with_retry(
        &self,
        concurrency: usize,
    ) -> Result<HashSet<IndexedRangeMarker>> {
        let mut retries = 0;
        loop {
            match self.ensure_markers(concurrency).await {
                Ok(markers) => return Ok(markers),
                Err(e) => {
                    retries += 1;
                    if retries >= MAX_STARTUP_RETRIES {
                        return Err(e.wrap_err(format!(
                            "Failed to initialize markers after {retries} retries"
                        )));
                    }
                    error!(?e, retries, "Failed to ensure markers, retrying...");
                    tokio::time::sleep(RETRY_DELAY).await;
                }
            }
        }
    }

    async fn ensure_markers(&self, concurrency: usize) -> Result<HashSet<IndexedRangeMarker>> {
        let mut known_committable_heads = self.get_markers().await?;

        if known_committable_heads.len() >= concurrency {
            info!("Found enough committable heads, stopping");
            return Ok(known_committable_heads);
        }

        let candidates = self
            .source
            // Get extras in case some are not canonical
            .scan_prefix_with_max_keys(LEGACY_BFT_BLOCKS_PREFIX, concurrency * 20)
            .await?;

        info!(?candidates, "Found {} candidates", candidates.len());

        let candidates = candidates.into_iter().into_iter().filter_map(|key| {
            info!("Candidate key: {}", key);
            let hex_str = key
                .strip_prefix(LEGACY_BFT_BLOCKS_PREFIX)?
                .strip_suffix(".header")?;
            info!("Hex string: {}", hex_str);
            let bytes = hex::decode(hex_str).ok()?;
            let arr: [u8; 32] = bytes.try_into().ok()?;
            Some(BlockId(Hash(arr)))
        });

        for candidate in candidates {
            if known_committable_heads.len() >= concurrency {
                info!("Found enough committable heads, stopping");
                break;
            }

            let (header, _) = self.fetch_legacy_header_by_id(&candidate).await?;
            match self.find_committable_head(header).await {
                Ok(Some(committable_id)) => {
                    info!("Found committable head {:?}", committable_id);
                    let (committable_header, _) =
                        self.fetch_legacy_header_by_id(&committable_id).await?;
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
            .for_each_concurrent(Some(100), |marker| {
                retry_forever(
                    move || self.write_marker(&marker),
                    "write marker",
                    RETRY_DELAY,
                )
            })
            .await;

        Ok(known_committable_heads)
    }

    async fn find_committable_head(
        &self,
        mut bft_header: BftBlockHeader,
    ) -> Result<Option<BlockId>> {
        loop {
            let parent_id = bft_header.get_parent_id();
            let (parent_header, _) = self.fetch_legacy_header_by_id(&parent_id).await?;
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
        copy_data: bool,
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
                let (next_id, next_num) = retry_forever(
                    || self.index_single_block(current_id, copy_data),
                    "index block",
                    RETRY_DELAY,
                )
                .await;
                marker.end_id = next_id;
                marker.end_num = next_num;
            }

            retry_forever(|| self.write_marker(&marker), "write marker", RETRY_DELAY).await;
            info!("Written marker {:?}", marker);
        }

        Ok(())
    }

    /// Index a single block and return (next_block_id, next_seq_num) for marker update
    async fn index_single_block(
        &self,
        current_id: BlockId,
        copy_data: bool,
    ) -> Result<(BlockId, u64)> {
        let (current_header, header_bytes) = self.fetch_legacy_header_by_id(&current_id).await?;

        self.model
            .put_index(current_header.seq_num.0, &current_id)
            .await?;

        if copy_data {
            let body_bytes = self.fetch_legacy_body_bytes_by_id(&current_header).await?;
            self.model
                .put_header_bytes(&current_id, &header_bytes)
                .await?;
            self.model
                .put_body_bytes(&current_header.block_body_id.0, &body_bytes)
                .await?;
        }

        info!(
            "Indexed bft block num: {}, id: {}",
            current_header.seq_num.0,
            hex::encode(current_id.0)
        );

        let next_id = current_header.get_parent_id();
        let next_num = current_header.seq_num.0.saturating_sub(1);
        Ok((next_id, next_num))
    }

    async fn get_markers(&self) -> Result<HashSet<IndexedRangeMarker>> {
        let markers = self.sink.scan_prefix(BFT_INDEX_MARKERS_PREFIX).await?;

        futures::stream::iter(markers)
            .map(|s| async move {
                let value = self.sink.get(&s).await?.ok_or_eyre("Marker not found")?;
                serde_json::from_slice(&value[..]).wrap_err("Failed to deserialize index range")
            })
            .buffer_unordered(100)
            .try_collect()
            .await
    }

    fn marker_key(start_num: u64) -> String {
        format!("{BFT_INDEX_MARKERS_PREFIX}{start_num}")
    }

    async fn ensure_marker_slot_matches_start_id(
        &self,
        key: &str,
        start_id: &BlockId,
    ) -> Result<()> {
        let Some(existing) = self.sink.get(key).await? else {
            return Ok(());
        };

        let existing: IndexedRangeMarker = serde_json::from_slice(&existing)
            .wrap_err_with(|| format!("Failed to deserialize marker at key {key}"))?;

        if existing.start_id != *start_id {
            return Err(eyre!(
                "Marker key collision at key={key}: existing start_id={}, attempted start_id={}",
                hex::encode(existing.start_id.0),
                hex::encode(start_id.0),
            ));
        }

        Ok(())
    }

    async fn write_marker(&self, range: &IndexedRangeMarker) -> Result<()> {
        let key = Self::marker_key(range.start_num);
        self.ensure_marker_slot_matches_start_id(&key, &range.start_id)
            .await?;
        let bytes = serde_json::to_vec(&range).wrap_err("Failed to serialize index range")?;
        self.sink
            .put(key, bytes, WritePolicy::AllowOverwrite)
            .await
            .wrap_err("Failed to write index marker")?;
        Ok(())
    }

    async fn delete_marker(&self, range: &IndexedRangeMarker) -> Result<()> {
        let key = Self::marker_key(range.start_num);
        self.ensure_marker_slot_matches_start_id(&key, &range.start_id)
            .await?;
        self.sink
            .delete(&key)
            .await
            .wrap_err("Failed to delete index marker")
    }

    pub async fn fetch_legacy_header_by_id(
        &self,
        hash: &BlockId,
    ) -> Result<(BftBlockHeader, Vec<u8>)> {
        let key = format!("{}{}.header", LEGACY_BFT_BLOCKS_PREFIX, hex::encode(hash.0));
        let bytes = self
            .source
            .get(&key)
            .await?
            .ok_or_eyre("Header not found")?;
        let header = BftBlockHeader::decode(&mut &bytes[..]).wrap_err("Failed to decode header")?;
        Ok((header, bytes.to_vec()))
    }

    async fn fetch_legacy_body_bytes_by_id(&self, header: &BftBlockHeader) -> Result<Vec<u8>> {
        let body_id = header.block_body_id.0;
        let key = format!("{}{}.body", LEGACY_BFT_BLOCKS_PREFIX, hex::encode(body_id));
        Ok(self
            .source
            .get(&key)
            .await?
            .ok_or_eyre("Body not found")?
            .to_vec())
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

#[cfg(test)]
mod tests {
    use monad_archive::kvstore::memory::MemoryStorage;

    use super::*;

    fn block_id(byte: u8) -> BlockId {
        BlockId(Hash([byte; 32]))
    }

    fn marker(
        start_num: u64,
        start_id: BlockId,
        end_num: u64,
        end_id: BlockId,
    ) -> IndexedRangeMarker {
        IndexedRangeMarker {
            start_num,
            end_num,
            start_id,
            end_id,
        }
    }

    fn test_index() -> BftBlockIndex {
        let source: KVStoreErased = MemoryStorage::new("source").into();
        let sink: KVStoreErased = MemoryStorage::new("sink").into();
        BftBlockIndex::new(source, sink)
    }

    #[tokio::test]
    async fn write_marker_rejects_start_id_collision() {
        let index = test_index();
        let first = marker(100, block_id(0xAA), 100, block_id(0xAA));
        let colliding = marker(100, block_id(0xBB), 99, block_id(0xCC));

        index.write_marker(&first).await.unwrap();
        let err = index.write_marker(&colliding).await.unwrap_err();
        assert!(err.to_string().contains("Marker key collision"));
    }

    #[tokio::test]
    async fn write_marker_allows_updates_for_same_start_id() {
        let index = test_index();
        let first = marker(200, block_id(0x11), 200, block_id(0x11));
        let advanced = marker(200, block_id(0x11), 150, block_id(0x22));

        index.write_marker(&first).await.unwrap();
        index.write_marker(&advanced).await.unwrap();

        let key = BftBlockIndex::marker_key(advanced.start_num);
        let stored = index.sink.get(&key).await.unwrap().unwrap();
        let stored: IndexedRangeMarker = serde_json::from_slice(&stored).unwrap();
        assert_eq!(stored, advanced);
    }

    #[tokio::test]
    async fn delete_marker_rejects_start_id_collision() {
        let index = test_index();
        let first = marker(300, block_id(0x01), 300, block_id(0x01));
        let colliding = marker(300, block_id(0x02), 299, block_id(0x03));

        index.write_marker(&first).await.unwrap();
        let err = index.delete_marker(&colliding).await.unwrap_err();
        assert!(err.to_string().contains("Marker key collision"));

        let key = BftBlockIndex::marker_key(first.start_num);
        assert!(index.sink.get(&key).await.unwrap().is_some());
    }
}
