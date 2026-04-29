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

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use alloy_rlp::Decodable;
use futures::future::join_all;
use monad_archive::{
    kvstore::WritePolicy,
    model::{
        bft_ledger::{BftBlockHeader, BftBlockModel},
        bft_paths,
    },
    prelude::*,
};
use monad_types::{BlockId, Hash};
use opentelemetry::KeyValue;
use serde::{Deserialize, Serialize};

const RETRY_DELAY: Duration = Duration::from_millis(500);
const MAX_STARTUP_RETRIES: usize = 10;
// scan_prefix counts .header and .body keys, so we overscan to find enough headers.
const CANDIDATE_SCAN_MULTIPLIER: usize = 40;
const MIGRATION_WORKER_NAME: &str = "bft_migrate_index";

/// Tracks indexing progress for a sub-chain starting at a committable head and
/// walking backwards toward its ancestors. The tail points at the next block
/// to index, which is expected to have a lower seq_num than the head.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct IndexedRangeMarker {
    #[serde(rename = "start_num")]
    head_num: u64,
    #[serde(rename = "end_num")]
    tail_num: u64,
    #[serde(rename = "start_id")]
    head_id: BlockId,
    #[serde(rename = "end_id")]
    tail_id: BlockId,
}

/// Tunables for the migration indexer. Defaults reproduce historic behavior
/// (genesis at seq_num 0, no inventory-derived seed list, no canary cap).
#[derive(Clone, Default)]
pub struct IndexerConfig {
    /// Treat a header at this seq_num as the legitimate stop case for a
    /// sub-chain walk. The entry is still indexed; the parent is not visited.
    pub genesis_seq_num: u64,
    /// Optional inventory-derived list of legacy header BlockIds to seed
    /// committable-head discovery, replacing `scan_prefix` sampling.
    pub seed_tips: Option<Vec<BlockId>>,
    /// Canary mode: stop indexing once this many blocks have been written
    /// across all sub-chains. Markers are deleted as usual on Ok exit, so
    /// canary runs are not resumable — that's intentional, the point is to
    /// validate end-to-end on a fresh sink before kicking off the full run.
    pub max_blocks: Option<u64>,
}

#[derive(Clone)]
pub struct BftBlockIndex {
    /// Read-only source for legacy bft_blocks/ data
    pub source: KVStoreErased,
    /// Read-write sink for markers and index
    pub sink: KVStoreErased,
    pub model: BftBlockModel,
    pub metrics: Metrics,
    pub config: IndexerConfig,
    blocks_indexed: Arc<AtomicU64>,
}

impl BftBlockIndex {
    pub fn new(source: KVStoreErased, sink: KVStoreErased, metrics: Metrics) -> Self {
        Self {
            source,
            model: BftBlockModel::new(sink.clone()),
            sink,
            metrics,
            config: IndexerConfig::default(),
            blocks_indexed: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn with_config(mut self, config: IndexerConfig) -> Self {
        self.config = config;
        self
    }

    fn canary_limit_reached(&self) -> bool {
        match self.config.max_blocks {
            Some(limit) => self.blocks_indexed.load(Ordering::Relaxed) >= limit,
            None => false,
        }
    }

    fn worker_attrs(stage: &'static str, operation: &'static str) -> [KeyValue; 3] {
        [
            KeyValue::new("worker", MIGRATION_WORKER_NAME),
            KeyValue::new("stage", stage),
            KeyValue::new("operation", operation),
        ]
    }

    pub async fn index_bft_headers(
        &self,
        concurrency: usize,
        max_num_per_batch: usize,
        copy_data: bool,
    ) -> Result<()> {
        let known_committable_heads = self.ensure_markers_with_retry(concurrency).await?;
        self.metrics.gauge(
            MetricNames::BFT_MIGRATION_SUBCHAINS_ACTIVE,
            known_committable_heads.len() as u64,
        );
        self.metrics.periodic_gauge_with_attrs(
            MetricNames::WORKER_HEALTH_STATUS,
            1,
            vec![
                KeyValue::new("worker", MIGRATION_WORKER_NAME),
                KeyValue::new("stage", "overall"),
            ],
        );
        info!("Indexing {} sub-chains", known_committable_heads.len());
        for marker in known_committable_heads.iter() {
            info!("Marker: {}", marker);
        }

        let handles = known_committable_heads.iter().map(|marker| {
            let other_sub_chain_tips = known_committable_heads
                .iter()
                .map(|h| h.head_id)
                .filter(|h| h != &marker.head_id)
                .collect::<HashSet<BlockId>>();

            let mut marker = marker.clone();
            let this = self.clone();
            tokio::spawn(async move {
                let result = this
                    .index_sub_chain(
                        &mut marker,
                        other_sub_chain_tips,
                        max_num_per_batch,
                        copy_data,
                    )
                    .await
                    .wrap_err("Failed to index sub-chain");
                if result.is_ok() {
                    info!(
                        "Sub-chain indexed successfully, deleting marker {:?}",
                        &marker
                    );
                    this.delete_marker(&marker).await?;
                    this.metrics
                        .inc_counter(MetricNames::BFT_MIGRATION_SUBCHAINS_COMPLETED);
                }
                result
            })
        });

        let mut errors = Vec::new();
        for result in join_all(handles).await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    error!(?e, "Failed to index sub-chain");
                    self.metrics.periodic_gauge_with_attrs(
                        MetricNames::WORKER_HEALTH_STATUS,
                        0,
                        vec![
                            KeyValue::new("worker", MIGRATION_WORKER_NAME),
                            KeyValue::new("stage", "overall"),
                        ],
                    );
                    errors.push(e);
                }
                Err(e) => {
                    error!(?e, "Task panicked while indexing sub-chain");
                    self.metrics.periodic_gauge_with_attrs(
                        MetricNames::WORKER_HEALTH_STATUS,
                        0,
                        vec![
                            KeyValue::new("worker", MIGRATION_WORKER_NAME),
                            KeyValue::new("stage", "overall"),
                        ],
                    );
                    errors.push(eyre::eyre!("Task panicked: {e}"));
                }
            }
        }

        if let Some(error) = errors.pop() {
            self.metrics.periodic_gauge_with_attrs(
                MetricNames::WORKER_HEALTH_STATUS,
                0,
                vec![
                    KeyValue::new("worker", MIGRATION_WORKER_NAME),
                    KeyValue::new("stage", "overall"),
                ],
            );
            return Err(error);
        }

        self.metrics
            .gauge(MetricNames::BFT_MIGRATION_SUBCHAINS_ACTIVE, 0);
        self.metrics.periodic_gauge_with_attrs(
            MetricNames::WORKER_HEALTH_STATUS,
            1,
            vec![
                KeyValue::new("worker", MIGRATION_WORKER_NAME),
                KeyValue::new("stage", "overall"),
            ],
        );
        Ok(())
    }

    async fn ensure_markers_with_retry(
        &self,
        concurrency: usize,
    ) -> Result<HashSet<IndexedRangeMarker>> {
        let mut retries = 0_usize;
        let retry_attrs = Self::worker_attrs("startup", "ensure_markers");
        loop {
            match self.ensure_markers(concurrency).await {
                Ok(markers) => return Ok(markers),
                Err(e) => {
                    retries = retries.saturating_add(1);
                    self.metrics.counter_with_attrs(
                        MetricNames::WORKER_RETRY_ATTEMPTS,
                        1,
                        &retry_attrs,
                    );
                    if retries >= MAX_STARTUP_RETRIES {
                        self.metrics.periodic_gauge_with_attrs(
                            MetricNames::WORKER_HEALTH_STATUS,
                            0,
                            vec![
                                KeyValue::new("worker", MIGRATION_WORKER_NAME),
                                KeyValue::new("stage", "startup"),
                            ],
                        );
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

        let candidates: Vec<BlockId> = if let Some(seeds) = &self.config.seed_tips {
            info!("Using {} seed tips from inventory", seeds.len());
            seeds.clone()
        } else {
            let keys = self
                .source
                // Get extras in case some are not canonical
                .scan_prefix_with_max_keys(
                    bft_paths::legacy_prefix(),
                    concurrency * CANDIDATE_SCAN_MULTIPLIER,
                )
                .await?;

            info!(?keys, "Found {} candidates", keys.len());

            keys.into_iter()
                .filter_map(|key| {
                    info!("Candidate key: {}", key);
                    let id = bft_paths::parse_legacy_header_path(&key)?;
                    info!("Parsed header id: {}", hex::encode(id.0));
                    Some(id)
                })
                .collect()
        };

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
                        head_num: committable_header.seq_num.0,
                        tail_num: committable_header.seq_num.0,
                        head_id: committable_id,
                        tail_id: committable_id,
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

        let metrics = self.metrics.clone();
        futures::stream::iter(known_committable_heads.iter())
            .for_each_concurrent(Some(100), |marker| {
                let retry_attrs = Self::worker_attrs("marker_init", "write_marker");
                let metrics = metrics.clone();
                retry_forever_with_observer(
                    move || self.write_marker(marker),
                    "write marker",
                    RETRY_DELAY,
                    move |_attempt, _delay| {
                        metrics.counter_with_attrs(
                            MetricNames::WORKER_RETRY_ATTEMPTS,
                            1,
                            &retry_attrs,
                        );
                    },
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

    async fn tail_already_indexed(&self, marker: &IndexedRangeMarker) -> Result<bool> {
        let Some(existing_id) = self.model.get_id_by_num_opt(marker.tail_num).await? else {
            return Ok(false);
        };

        if existing_id == marker.tail_id {
            info!(
                "Sub-chain already indexed at num {}, stopping",
                marker.tail_num
            );
            return Ok(true);
        }

        Err(eyre!(
            "fatal invariant violation: finalized chain mismatch at seq_num={} existing_id={} expected_id={}",
            marker.tail_num,
            hex::encode(existing_id.0),
            hex::encode(marker.tail_id.0),
        ))
    }

    async fn index_sub_chain(
        &self,
        marker: &mut IndexedRangeMarker,
        other_sub_chain_tips: HashSet<BlockId>,
        max_num_per_batch: usize,
        copy_data: bool,
    ) -> Result<()> {
        while !other_sub_chain_tips.contains(&marker.tail_id) {
            info!(
                "Indexing sub-chain from num: {}, to num: {}",
                marker.tail_num,
                marker.tail_num.saturating_sub(max_num_per_batch as u64)
            );
            for _ in 0..max_num_per_batch {
                if other_sub_chain_tips.contains(&marker.tail_id) {
                    return Ok(());
                }
                if self.tail_already_indexed(marker).await? {
                    self.repair_and_advance_indexed_tail(marker, copy_data)
                        .await?;
                    continue;
                }

                let current_id = marker.tail_id;
                let current_header_key = bft_paths::legacy_header_path(&current_id);
                if self.source.get(&current_header_key).await?.is_none() {
                    self.metrics
                        .inc_counter(MetricNames::BFT_MIGRATION_SUBCHAIN_STALLED);
                    return Err(eyre!(
                        "Stopping sub-chain: missing legacy header for id {} at seq_num {}; \
                         marker preserved for operator investigation",
                        hex::encode(current_id.0),
                        marker.tail_num,
                    ));
                }
                let retry_attrs = Self::worker_attrs("index_sub_chain", "index_block");
                let metrics = self.metrics.clone();
                if self.canary_limit_reached() {
                    info!(
                        "Canary mode: --max-blocks={} reached, stopping sub-chain cleanly",
                        self.config.max_blocks.unwrap_or(0)
                    );
                    return Ok(());
                }
                let next = retry_forever_with_observer(
                    || self.index_single_block(current_id, copy_data),
                    "index block",
                    RETRY_DELAY,
                    move |_attempt, _delay| {
                        metrics.counter_with_attrs(
                            MetricNames::WORKER_RETRY_ATTEMPTS,
                            1,
                            &retry_attrs,
                        );
                    },
                )
                .await;
                let Some((next_id, next_num)) = next else {
                    if self.canary_limit_reached() {
                        info!(
                            "Canary mode: --max-blocks={} reached, stopping sub-chain cleanly",
                            self.config.max_blocks.unwrap_or(0)
                        );
                    } else {
                        info!(
                            "Sub-chain reached genesis at seq_num {}, stopping cleanly",
                            self.config.genesis_seq_num
                        );
                    }
                    return Ok(());
                };
                marker.tail_id = next_id;
                marker.tail_num = next_num;
            }

            if other_sub_chain_tips.contains(&marker.tail_id) {
                return Ok(());
            }

            let retry_attrs = Self::worker_attrs("index_sub_chain", "write_marker");
            let metrics = self.metrics.clone();
            retry_forever_with_observer(
                || self.write_marker(marker),
                "write marker",
                RETRY_DELAY,
                move |_attempt, _delay| {
                    metrics.counter_with_attrs(MetricNames::WORKER_RETRY_ATTEMPTS, 1, &retry_attrs);
                },
            )
            .await;
            info!("Written marker {:?}", marker);
        }

        Ok(())
    }

    /// Index a single block. Returns `Some((next_id, next_num))` to advance the
    /// marker, or `None` when the just-indexed block is at the configured
    /// genesis seq_num — the legitimate end of a sub-chain walk.
    async fn index_single_block(
        &self,
        current_id: BlockId,
        copy_data: bool,
    ) -> Result<Option<(BlockId, u64)>> {
        let (current_header, header_bytes) = self.fetch_legacy_header_by_id(&current_id).await?;

        if copy_data {
            let body_bytes = self.fetch_legacy_body_bytes_by_id(&current_header).await?;
            self.put_header_bytes_no_clobber(&current_id, header_bytes)
                .await?;
            self.put_body_bytes_no_clobber(&current_header.block_body_id.0, body_bytes)
                .await?;
        }

        self.model
            .put_index(current_header.seq_num.0, &current_id)
            .await?;
        self.blocks_indexed.fetch_add(1, Ordering::Relaxed);

        info!(
            "Indexed bft block num: {}, id: {}",
            current_header.seq_num.0,
            hex::encode(current_id.0)
        );

        if current_header.seq_num.0 == self.config.genesis_seq_num {
            return Ok(None);
        }

        if self.canary_limit_reached() {
            return Ok(None);
        }

        let next_id = current_header.get_parent_id();
        let next_num = current_header.seq_num.0.saturating_sub(1);
        Ok(Some((next_id, next_num)))
    }

    async fn put_header_bytes_no_clobber(&self, id: &BlockId, bytes: Vec<u8>) -> Result<()> {
        self.sink
            .put(bft_paths::header_path(id), bytes, WritePolicy::NoClobber)
            .await?;
        Ok(())
    }

    async fn put_body_bytes_no_clobber(&self, id: &Hash, bytes: Vec<u8>) -> Result<()> {
        self.sink
            .put(bft_paths::body_path(id), bytes, WritePolicy::NoClobber)
            .await?;
        Ok(())
    }

    async fn repair_and_advance_indexed_tail(
        &self,
        marker: &mut IndexedRangeMarker,
        copy_data: bool,
    ) -> Result<()> {
        let header_key = bft_paths::header_path(&marker.tail_id);
        let (header, repaired_header) = match self.sink.get(&header_key).await? {
            Some(bytes) => {
                let header = BftBlockHeader::decode(&mut &bytes[..]).wrap_err_with(|| {
                    format!(
                        "Failed to decode copied header for indexed block id {}",
                        hex::encode(marker.tail_id.0)
                    )
                })?;
                (header, false)
            }
            None => {
                let (header, bytes) = self.fetch_legacy_header_by_id(&marker.tail_id).await?;
                if copy_data {
                    self.put_header_bytes_no_clobber(&marker.tail_id, bytes)
                        .await?;
                }
                (header, copy_data)
            }
        };

        let repaired_body = if copy_data {
            let body_key = bft_paths::body_path(&header.block_body_id.0);
            if self.sink.get(&body_key).await?.is_some() {
                false
            } else {
                let body_bytes = self.fetch_legacy_body_bytes_by_id(&header).await?;
                self.put_body_bytes_no_clobber(&header.block_body_id.0, body_bytes)
                    .await?;
                true
            }
        } else {
            false
        };

        if repaired_header || repaired_body {
            info!(
                seq_num = marker.tail_num,
                id = hex::encode(marker.tail_id.0),
                repaired_header,
                repaired_body,
                "Repaired copied data for indexed bft block"
            );
        }

        marker.tail_id = header.get_parent_id();
        marker.tail_num = marker.tail_num.saturating_sub(1);
        Ok(())
    }

    async fn get_markers(&self) -> Result<HashSet<IndexedRangeMarker>> {
        let markers = self.sink.scan_prefix(bft_paths::markers_prefix()).await?;

        futures::stream::iter(markers)
            .map(|s| async move {
                let value = self.sink.get(&s).await?.ok_or_eyre("Marker not found")?;
                serde_json::from_slice(&value[..]).wrap_err("Failed to deserialize index range")
            })
            .buffer_unordered(100)
            .try_collect()
            .await
    }

    fn marker_key(head_num: u64) -> String {
        bft_paths::marker_path(head_num)
    }

    async fn ensure_marker_slot_matches_head_id(&self, key: &str, head_id: &BlockId) -> Result<()> {
        let Some(existing) = self.sink.get(key).await? else {
            return Ok(());
        };

        let existing: IndexedRangeMarker = serde_json::from_slice(&existing)
            .wrap_err_with(|| format!("Failed to deserialize marker at key {key}"))?;

        if existing.head_id != *head_id {
            return Err(eyre!(
                "Marker key collision at key={key}: existing head_id={}, attempted head_id={}",
                hex::encode(existing.head_id.0),
                hex::encode(head_id.0),
            ));
        }

        Ok(())
    }

    async fn write_marker(&self, range: &IndexedRangeMarker) -> Result<()> {
        let key = Self::marker_key(range.head_num);
        self.ensure_marker_slot_matches_head_id(&key, &range.head_id)
            .await?;
        let bytes = serde_json::to_vec(&range).wrap_err("Failed to serialize index range")?;
        self.sink
            .put(key, bytes, WritePolicy::AllowOverwrite)
            .await
            .wrap_err("Failed to write index marker")?;
        Ok(())
    }

    async fn delete_marker(&self, range: &IndexedRangeMarker) -> Result<()> {
        let key = Self::marker_key(range.head_num);
        self.ensure_marker_slot_matches_head_id(&key, &range.head_id)
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
        let key = bft_paths::legacy_header_path(hash);
        let bytes = self
            .source
            .get(&key)
            .await?
            .ok_or_eyre("Header not found")?;
        let header = BftBlockHeader::decode(&mut &bytes[..]).wrap_err("Failed to decode header")?;
        Ok((header, bytes.to_vec()))
    }

    async fn fetch_legacy_body_bytes_by_id(&self, header: &BftBlockHeader) -> Result<Vec<u8>> {
        let key = bft_paths::legacy_body_path(&header.block_body_id.0);
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
            self.head_num,
            self.tail_num,
            hex::encode(self.head_id.0),
            hex::encode(self.tail_id.0)
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
        head_num: u64,
        head_id: BlockId,
        tail_num: u64,
        tail_id: BlockId,
    ) -> IndexedRangeMarker {
        IndexedRangeMarker {
            head_num,
            tail_num,
            head_id,
            tail_id,
        }
    }

    fn test_index() -> BftBlockIndex {
        let source: KVStoreErased = MemoryStorage::new("source").into();
        let sink: KVStoreErased = MemoryStorage::new("sink").into();
        BftBlockIndex::new(source, sink, Metrics::none())
    }

    #[tokio::test]
    async fn write_marker_rejects_head_id_collision() {
        let index = test_index();
        let first = marker(100, block_id(0xAA), 100, block_id(0xAA));
        let colliding = marker(100, block_id(0xBB), 99, block_id(0xCC));

        index.write_marker(&first).await.unwrap();
        let err = index.write_marker(&colliding).await.unwrap_err();
        assert!(err.to_string().contains("Marker key collision"));
    }

    #[tokio::test]
    async fn write_marker_allows_updates_for_same_head_id() {
        let index = test_index();
        let first = marker(200, block_id(0x11), 200, block_id(0x11));
        let advanced = marker(200, block_id(0x11), 150, block_id(0x22));

        index.write_marker(&first).await.unwrap();
        index.write_marker(&advanced).await.unwrap();

        let key = BftBlockIndex::marker_key(advanced.head_num);
        let stored = index.sink.get(&key).await.unwrap().unwrap();
        let stored: IndexedRangeMarker = serde_json::from_slice(&stored).unwrap();
        assert_eq!(stored, advanced);
    }

    #[tokio::test]
    async fn delete_marker_rejects_head_id_collision() {
        let index = test_index();
        let first = marker(300, block_id(0x01), 300, block_id(0x01));
        let colliding = marker(300, block_id(0x02), 299, block_id(0x03));

        index.write_marker(&first).await.unwrap();
        let err = index.delete_marker(&colliding).await.unwrap_err();
        assert!(err.to_string().contains("Marker key collision"));

        let key = BftBlockIndex::marker_key(first.head_num);
        assert!(index.sink.get(&key).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn tail_already_indexed_returns_true_for_matching_id() {
        let index = test_index();
        let m = marker(400, block_id(0x04), 399, block_id(0x05));
        index.model.put_index(m.tail_num, &m.tail_id).await.unwrap();

        let already_indexed = index.tail_already_indexed(&m).await.unwrap();
        assert!(already_indexed);
    }

    #[tokio::test]
    async fn tail_already_indexed_fails_for_mismatched_existing_id() {
        let index = test_index();
        let m = marker(500, block_id(0x06), 499, block_id(0x07));
        let wrong = block_id(0x08);
        index.model.put_index(m.tail_num, &wrong).await.unwrap();

        let err = index.tail_already_indexed(&m).await.unwrap_err();
        let err_string = err.to_string();
        assert!(err_string.contains("fatal invariant violation"));
        assert!(err_string.contains("seq_num=499"));
        assert!(err_string.contains(&hex::encode(wrong.0)));
        assert!(err_string.contains(&hex::encode(m.tail_id.0)));
    }

    #[test]
    fn indexer_config_defaults_match_historic_behavior() {
        let cfg = IndexerConfig::default();
        assert_eq!(cfg.genesis_seq_num, 0);
        assert!(cfg.seed_tips.is_none());
    }

    #[test]
    fn with_config_replaces_default() {
        let index = test_index().with_config(IndexerConfig {
            genesis_seq_num: 42,
            seed_tips: Some(vec![block_id(0x99)]),
            max_blocks: Some(10),
        });
        assert_eq!(index.config.genesis_seq_num, 42);
        assert_eq!(index.config.seed_tips.as_deref(), Some(&[block_id(0x99)][..]));
        assert_eq!(index.config.max_blocks, Some(10));
    }

    #[test]
    fn canary_limit_is_disabled_by_default() {
        let index = test_index();
        assert!(!index.canary_limit_reached());
        index.blocks_indexed.store(1_000_000, Ordering::Relaxed);
        assert!(!index.canary_limit_reached());
    }

    #[test]
    fn canary_limit_fires_at_threshold() {
        let index = test_index().with_config(IndexerConfig {
            max_blocks: Some(3),
            ..Default::default()
        });
        assert!(!index.canary_limit_reached());
        index.blocks_indexed.store(2, Ordering::Relaxed);
        assert!(!index.canary_limit_reached());
        index.blocks_indexed.store(3, Ordering::Relaxed);
        assert!(index.canary_limit_reached());
        index.blocks_indexed.store(4, Ordering::Relaxed);
        assert!(index.canary_limit_reached());
    }
}
