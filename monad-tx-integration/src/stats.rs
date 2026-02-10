use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufWriter, Write},
    path::Path,
    time::SystemTime,
};

use monad_peer_score::{ema::ScoreReader, StdClock};
use monad_types::NodeId;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct PeerBlockStats {
    pub peer_id: String,
    pub txs_received_total: u64,
    pub txs_included_this_block: usize,
    pub txs_included_total: u64,
    pub score: f64,
    pub promoted: bool,
}

#[derive(Debug, Serialize)]
pub struct CommitStats {
    pub block_number: u64,
    pub txs_included: usize,
    pub txs_received_total: u64,
    pub commit_latency_us: u64,
    pub timestamp_ms: u128,
    pub peers: Vec<PeerBlockStats>,
}

pub struct StatsCollector {
    writer: BufWriter<File>,
    txs_received_total: u64,
    txs_included_total: u64,
    per_peer_included_total: BTreeMap<NodeId<monad_secp::PubKey>, u64>,
}

impl StatsCollector {
    pub fn new(path: &Path) -> std::io::Result<Self> {
        let file = File::create(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            txs_received_total: 0,
            txs_included_total: 0,
            per_peer_included_total: BTreeMap::new(),
        })
    }

    pub fn record_received(&mut self, count: u64) {
        self.txs_received_total += count;
    }

    pub fn record_commit(
        &mut self,
        block_number: u64,
        txs_included: usize,
        commit_latency_us: u64,
        per_peer_received: &BTreeMap<NodeId<monad_secp::PubKey>, u64>,
        per_peer_included: &BTreeMap<NodeId<monad_secp::PubKey>, usize>,
        score_reader: &ScoreReader<NodeId<monad_secp::PubKey>, StdClock>,
    ) {
        self.txs_included_total += txs_included as u64;

        for (peer, &count) in per_peer_included {
            *self.per_peer_included_total.entry(*peer).or_default() += count as u64;
        }

        let peers: Vec<PeerBlockStats> = per_peer_received
            .iter()
            .map(|(peer_id, &received)| {
                let status = score_reader.score(peer_id);
                let included_this_block = per_peer_included.get(peer_id).copied().unwrap_or(0);
                let included_total = self
                    .per_peer_included_total
                    .get(peer_id)
                    .copied()
                    .unwrap_or(0);
                PeerBlockStats {
                    peer_id: format!("{:?}", peer_id),
                    txs_received_total: received,
                    txs_included_this_block: included_this_block,
                    txs_included_total: included_total,
                    score: 1.0 / status.score().reciprocal(),
                    promoted: status.is_promoted(),
                }
            })
            .collect();

        let stats = CommitStats {
            block_number,
            txs_included,
            txs_received_total: self.txs_received_total,
            commit_latency_us,
            timestamp_ms: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
            peers,
        };

        if let Ok(line) = serde_json::to_string(&stats) {
            let _ = writeln!(self.writer, "{line}");
            let _ = self.writer.flush();
        }

        tracing::info!(
            block = block_number,
            txs = txs_included,
            total_received = self.txs_received_total,
            total_included = self.txs_included_total,
            latency_us = commit_latency_us,
            "block committed"
        );
    }
}
