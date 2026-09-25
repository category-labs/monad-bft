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

use monad_mcp_chorus::ledger::FinalizationPath;
use serde::Serialize;

use crate::index::{BlockSummary, Totals};

pub const WINDOWS: [usize; 2] = [100, 1000];

const NS_PER_MS: f64 = 1e6;

// keeps json small: 3 decimals is µs precision for ms values.
pub fn round3(x: f64) -> f64 {
    (x * 1000.0).round() / 1000.0
}

fn ratio(num: u64, den: u64) -> Option<f64> {
    (den > 0).then(|| round3(num as f64 / den as f64))
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Dist {
    pub avg: f64,
    pub p50: f64,
    pub p95: f64,
    pub min: f64,
    pub max: f64,
}

impl Dist {
    pub fn of(mut samples: Vec<f64>) -> Option<Self> {
        if samples.is_empty() {
            return None;
        }
        samples.sort_by(f64::total_cmp);
        let n = samples.len();
        let rank = |q: f64| samples[((n - 1) as f64 * q).round() as usize];
        Some(Self {
            avg: round3(samples.iter().sum::<f64>() / n as f64),
            p50: round3(rank(0.5)),
            p95: round3(rank(0.95)),
            min: round3(samples[0]),
            max: round3(samples[n - 1]),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct WindowStats {
    pub size: usize,
    pub blocks: usize,
    pub first_slot: u64,
    pub last_slot: u64,
    // slots in [first, last] with no block.
    pub missing_slots: u64,
    // Δ finalized_at between consecutive blocks.
    pub block_time_ms: Option<Dist>,
    // finalized_at − deadline, over blocks with a known deadline.
    pub latency_ms: Option<Dist>,
    pub fast_ratio: Option<f64>,
    // lanes with no committed mini-proposal.
    pub empty_lane_ratio: Option<f64>,
    // positive lanes whose payload holds no txs.
    pub txless_lane_ratio: Option<f64>,
    pub txs: u64,
    pub tx_per_s: Option<f64>,
}

// consecutive block times in ms, oldest first; `blocks` must be slot-ordered.
pub fn block_times_ms<'a>(blocks: impl IntoIterator<Item = &'a BlockSummary>) -> Vec<f64> {
    let mut prev: Option<u64> = None;
    blocks
        .into_iter()
        .filter_map(|b| {
            let dt = prev.map(|p| (b.finalized_at_ns as i128 - p as i128) as f64 / NS_PER_MS);
            prev = Some(b.finalized_at_ns);
            dt
        })
        .collect()
}

// stats over the given slot-ordered blocks.
pub fn window(size: usize, blocks: &[&BlockSummary]) -> Option<WindowStats> {
    let (first, last) = (*blocks.first()?, *blocks.last()?);
    let mut totals = Totals::default();
    let mut fast = 0;
    let mut latencies = Vec::new();
    for b in blocks {
        totals.txs += u64::from(b.tx_count);
        totals.lanes += u64::from(b.num_lanes);
        totals.positive_lanes += u64::from(b.positive_lanes);
        totals.lanes_with_txs += u64::from(b.lanes_with_txs);
        fast += u64::from(b.path == FinalizationPath::Fast);
        latencies.extend(b.latency_ns().map(|l| l as f64 / NS_PER_MS));
    }
    let span_ns = last.finalized_at_ns.saturating_sub(first.finalized_at_ns);
    let slots = last.slot - first.slot + 1;
    Some(WindowStats {
        size,
        blocks: blocks.len(),
        first_slot: first.slot,
        last_slot: last.slot,
        missing_slots: slots - blocks.len() as u64,
        block_time_ms: Dist::of(block_times_ms(blocks.iter().copied())),
        latency_ms: Dist::of(latencies),
        fast_ratio: ratio(fast, blocks.len() as u64),
        empty_lane_ratio: ratio(totals.lanes - totals.positive_lanes, totals.lanes),
        txless_lane_ratio: ratio(
            totals.positive_lanes - totals.lanes_with_txs,
            totals.positive_lanes,
        ),
        txs: totals.txs,
        // txs of the first block landed before the span starts.
        tx_per_s: (span_ns > 0).then(|| {
            round3((totals.txs - u64::from(first.tx_count)) as f64 / (span_ns as f64 / 1e9))
        }),
    })
}

#[cfg(test)]
mod tests {
    use monad_mcp_chorus::ledger::{BlockMeta, LEDGER_VERSION, LaneMeta};

    use super::*;

    // 4 lanes: two positive, the first holding all `txs`.
    fn block(slot: u64, at_ms: u64, latency_ms: Option<u64>, txs: u32) -> BlockSummary {
        let at = at_ms * 1_000_000;
        let lane = |index: u32, positive: bool, tx_count: u32| LaneMeta {
            index,
            proposer: None,
            root: positive.then_some([0; 20]),
            payload_len: u32::from(positive),
            tx_count,
            decode_error: false,
        };
        let meta = BlockMeta {
            version: LEDGER_VERSION,
            slot,
            deadline_ns: latency_ms.map(|l| u128::from(at - l * 1_000_000)),
            finalized_at_ns: at.into(),
            path: if slot.is_multiple_of(4) {
                FinalizationPath::Fallback
            } else {
                FinalizationPath::Fast
            },
            num_lanes: 4,
            lanes: vec![
                lane(0, true, txs),
                lane(1, true, 0),
                lane(2, false, 0),
                lane(3, false, 0),
            ],
        };
        BlockSummary::from_meta(&meta).unwrap()
    }

    #[test]
    fn dist_percentiles() {
        let d = Dist::of((1..=100).map(f64::from).collect()).unwrap();
        assert_eq!(d.avg, 50.5);
        assert_eq!(d.p50, 51.0);
        assert_eq!(d.p95, 95.0);
        assert_eq!((d.min, d.max), (1.0, 100.0));
        assert_eq!(Dist::of(vec![]), None);
        assert_eq!(Dist::of(vec![7.0]).unwrap().p95, 7.0);
    }

    #[test]
    fn window_over_synthetic_sequence() {
        // slots 1..=10 minus slot 5, 100 ms apart except a 300 ms jump over the gap.
        let blocks: Vec<_> = (1..=10u64)
            .filter(|s| *s != 5)
            .map(|s| {
                let at = 1_000 + s * 100 + if s > 5 { 100 } else { 0 };
                block(s, at, (s % 2 == 0).then_some(10 + s), 2)
            })
            .collect();
        let refs: Vec<_> = blocks.iter().collect();
        let w = window(100, &refs).unwrap();
        assert_eq!((w.first_slot, w.last_slot, w.blocks), (1, 10, 9));
        assert_eq!(w.missing_slots, 1);
        let bt = w.block_time_ms.unwrap();
        assert_eq!((bt.min, bt.max, bt.p50), (100.0, 300.0, 100.0));
        // 1000 ms span over 8 deltas.
        assert_eq!(bt.avg, 125.0);
        let lat = w.latency_ms.unwrap();
        assert_eq!((lat.min, lat.max, lat.avg), (12.0, 20.0, 16.0));
        // slots 4 and 8 are fallback.
        assert_eq!(w.fast_ratio, Some(round3(7.0 / 9.0)));
        assert_eq!(w.empty_lane_ratio, Some(0.5));
        assert_eq!(w.txless_lane_ratio, Some(0.5));
        assert_eq!(w.txs, 18);
        // 16 txs after the first block over 1 s.
        assert_eq!(w.tx_per_s, Some(16.0));
    }

    #[test]
    fn window_edge_cases() {
        assert_eq!(window(100, &[]), None);
        let one = block(7, 5, None, 3);
        let w = window(100, &[&one]).unwrap();
        assert_eq!(w.block_time_ms, None);
        assert_eq!(w.latency_ms, None);
        assert_eq!(w.tx_per_s, None);
        assert_eq!(w.missing_slots, 0);
    }

    #[test]
    fn block_times_follow_order() {
        let blocks = [
            block(1, 100, None, 0),
            block(2, 90, None, 0),
            block(3, 250, None, 0),
        ];
        assert_eq!(block_times_ms(&blocks), vec![-10.0, 160.0]);
    }
}
