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

// its own test binary: the counting allocator sees every allocation in the process.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicIsize, Ordering},
};

use bytes::Bytes;
use monad_mcp_chorus::ledger::{
    BlockMeta, FinalizationPath, LEDGER_VERSION, LaneMeta, Tx, encode_batch,
};
use monad_mcp_explorer::index::{Index, IndexConfig, LoadedBlock};

// malloc's per-allocation header and rounding, roughly.
const ALLOC_OVERHEAD: isize = 16;

struct Counting;

static LIVE: AtomicIsize = AtomicIsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        LIVE.fetch_add(layout.size() as isize + ALLOC_OVERHEAD, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size() as isize + ALLOC_OVERHEAD, Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        LIVE.fetch_add(
            new_size as isize - layout.size() as isize,
            Ordering::Relaxed,
        );
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

// one lane of txs, each with its own sender and payload: every tx gets three index entries.
fn block(slot: u64, txs: u64) -> LoadedBlock {
    let txs: Vec<Tx> = (0..txs)
        .map(|n| {
            let id = slot * 1_000 + n;
            let mut sender = [0; 20];
            sender[..8].copy_from_slice(&id.to_be_bytes());
            Tx {
                sender,
                nonce: n,
                payload: Bytes::from(format!("payload {id}")),
                sent_at_ns: 0,             // demo(tx-timeline)
                rpc_received_at_ns: 0,     // demo(tx-timeline)
                mempool_admitted_at_ns: 0, // demo(tx-timeline)
            }
        })
        .collect();
    let meta = BlockMeta {
        version: LEDGER_VERSION,
        slot,
        deadline_ns: Some(u128::from(slot) * 100_000_000),
        finalized_at_ns: u128::from(slot) * 100_000_000 + 7_000_000,
        path: FinalizationPath::Fast,
        num_lanes: 1,
        lanes: vec![LaneMeta {
            index: 0,
            proposer: Some(1),
            root: Some([1; 20]),
            payload_len: encode_batch(u64::MAX, &txs).len() as u32, // demo(tx-timeline)
            tx_count: txs.len() as u32,
            decode_error: false,
        }],
    };
    LoadedBlock::new(&meta, [(0, txs)]).unwrap()
}

#[test]
fn per_tx_memory_budget() {
    const BLOCKS: u64 = 5_000;
    const PER_BLOCK: u64 = 10;
    let n = (BLOCKS * PER_BLOCK) as usize;
    let before = LIVE.load(Ordering::Relaxed);
    let mut ix = Index::new(IndexConfig {
        retain_slots: BLOCKS as usize,
        max_txs: n,
    });
    for slot in 0..BLOCKS {
        ix.insert(block(slot, PER_BLOCK));
    }
    assert_eq!(ix.tx_len(), n);
    let measured = (LIVE.load(Ordering::Relaxed) - before) as usize;
    let per_tx = measured / n;
    // the default max_txs times this stays within the plan's ~100 MB tx share.
    assert!(per_tx <= 520, "{per_tx} bytes per tx");
    assert!(
        per_tx * IndexConfig::default().max_txs <= 100 << 20,
        "default max_txs holds {} MB of txs",
        (per_tx * IndexConfig::default().max_txs) >> 20
    );
    // the estimate tracks real usage.
    let approx = ix.approx_heap_bytes();
    assert!(
        approx * 10 >= measured * 7 && approx * 10 <= measured * 13,
        "approx {approx} vs measured {measured}"
    );
}
