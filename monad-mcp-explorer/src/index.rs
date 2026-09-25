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

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque, hash_map},
    fmt,
    mem::size_of,
    ops::{Bound, Range},
    str::FromStr,
};

use monad_mcp_chorus::ledger::{Address, BlockMeta, FinalizationPath, Hash, LaneMeta, Tx};

pub const PREVIEW_LEN: usize = 32;

// position of a tx in the ledger; orders txs by inclusion.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TxKey {
    pub slot: u64,
    pub lane: u32,
    pub pos: u32,
}

impl TxKey {
    fn first_of(slot: u64, lane: u32) -> Self {
        Self { slot, lane, pos: 0 }
    }

    fn last_of(slot: u64, lane: u32) -> Self {
        Self {
            slot,
            lane,
            pos: u32::MAX,
        }
    }
}

impl fmt::Display for TxKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.slot, self.lane, self.pos)
    }
}

impl FromStr for TxKey {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        const ERR: &str = "tx cursor must be <slot>.<lane>.<pos>";
        let mut parts = s.split('.');
        let mut next = || parts.next().and_then(|p| p.parse().ok()).ok_or(ERR);
        let key = Self {
            slot: next()?,
            lane: u32::try_from(next()?).map_err(|_| ERR)?,
            pos: u32::try_from(next()?).map_err(|_| ERR)?,
        };
        match parts.next() {
            Some(_) => Err(ERR),
            None => Ok(key),
        }
    }
}

// what the index keeps per block; lane roots and proposers are read from meta.rlp on demand.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockSummary {
    pub slot: u64,
    pub finalized_at_ns: u64,
    // split from an Option so it packs into the padding after `path`.
    deadline_ns: u64,
    has_deadline: bool,
    pub payload_bytes: u32,
    pub tx_count: u32,
    pub num_lanes: u16,
    pub positive_lanes: u16,
    pub lanes_with_txs: u16,
    pub decode_errors: u16,
    pub path: FinalizationPath,
}

impl BlockSummary {
    pub fn from_meta(meta: &BlockMeta) -> Result<Self, &'static str> {
        let count = |pred: fn(&&LaneMeta) -> bool| {
            u16::try_from(meta.lanes.iter().filter(pred).count()).map_err(|_| "too many lanes")
        };
        let payload_bytes: u64 = meta.lanes.iter().map(|l| u64::from(l.payload_len)).sum();
        Ok(Self {
            slot: meta.slot,
            finalized_at_ns: u64::try_from(meta.finalized_at_ns)
                .map_err(|_| "finalized_at out of range")?,
            deadline_ns: meta
                .deadline_ns
                .map_or(Ok(0), u64::try_from)
                .map_err(|_| "deadline out of range")?,
            has_deadline: meta.deadline_ns.is_some(),
            payload_bytes: u32::try_from(payload_bytes).map_err(|_| "block payload too large")?,
            tx_count: u32::try_from(meta.tx_count()).map_err(|_| "too many txs")?,
            num_lanes: u16::try_from(meta.lanes.len()).map_err(|_| "too many lanes")?,
            positive_lanes: count(|l| l.is_positive())?,
            lanes_with_txs: count(|l| l.tx_count > 0)?,
            decode_errors: count(|l| l.decode_error)?,
            path: meta.path,
        })
    }

    pub fn deadline_ns(&self) -> Option<u64> {
        self.has_deadline.then_some(self.deadline_ns)
    }

    // finalization minus deadline; negative when finalized before the deadline.
    pub fn latency_ns(&self) -> Option<i64> {
        self.deadline_ns()
            .map(|d| i128::from(self.finalized_at_ns) - i128::from(d))
            .map(|l| l.clamp(i64::MIN as i128, i64::MAX as i128) as i64)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TxEntry {
    pub hash: Hash,
    pub payload_hash: Hash,
    pub sender: Address,
    pub nonce: u64,
    pub payload_len: u16,
    preview_len: u8,
    preview: [u8; PREVIEW_LEN],
}

impl TxEntry {
    pub fn new(tx: &Tx) -> Result<Self, &'static str> {
        let n = tx.payload.len().min(PREVIEW_LEN);
        let mut preview = [0; PREVIEW_LEN];
        preview[..n].copy_from_slice(&tx.payload[..n]);
        Ok(Self {
            hash: tx.hash(),
            payload_hash: tx.payload_hash(),
            sender: tx.sender,
            nonce: tx.nonce,
            payload_len: u16::try_from(tx.payload.len()).map_err(|_| "tx payload too large")?,
            preview_len: n as u8,
            preview,
        })
    }

    // the first PREVIEW_LEN payload bytes.
    pub fn preview(&self) -> &[u8] {
        &self.preview[..usize::from(self.preview_len)]
    }
}

// a block read from disk, ready to insert.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadedBlock {
    pub summary: BlockSummary,
    pub txs: Vec<(TxKey, TxEntry)>,
}

impl LoadedBlock {
    // `lanes` yields the decoded txs of every lane with tx_count > 0, in lane order.
    pub fn new(
        meta: &BlockMeta,
        lanes: impl IntoIterator<Item = (u32, Vec<Tx>)>,
    ) -> Result<Self, &'static str> {
        let summary = BlockSummary::from_meta(meta)?;
        let mut txs = Vec::with_capacity(summary.tx_count as usize);
        let mut expected = meta.lanes.iter().filter(|l| l.tx_count > 0);
        for (lane, lane_txs) in lanes {
            let meta_lane = expected.next().ok_or("unexpected lane txs")?;
            if meta_lane.index != lane || meta_lane.tx_count as usize != lane_txs.len() {
                return Err("lane txs do not match meta");
            }
            for (pos, tx) in (0..).zip(&lane_txs) {
                txs.push((
                    TxKey {
                        slot: meta.slot,
                        lane,
                        pos,
                    },
                    TxEntry::new(tx)?,
                ));
            }
        }
        if expected.next().is_some() {
            return Err("missing lane txs");
        }
        Ok(Self { summary, txs })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Inserted {
    New,
    Duplicate,
    // below the floor, or would be evicted at once.
    TooOld,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct Totals {
    pub blocks: u64,
    pub txs: u64,
    pub fast: u64,
    pub fallback: u64,
    pub lanes: u64,
    pub positive_lanes: u64,
    pub lanes_with_txs: u64,
    pub decode_errors: u64,
    pub payload_bytes: u64,
}

impl Totals {
    fn apply(&mut self, b: &BlockSummary, add: bool) {
        let op = |total: &mut u64, v: u64| {
            *total = if add { *total + v } else { *total - v };
        };
        op(&mut self.blocks, 1);
        op(&mut self.txs, b.tx_count.into());
        match b.path {
            FinalizationPath::Fast => op(&mut self.fast, 1),
            FinalizationPath::Fallback => op(&mut self.fallback, 1),
        }
        op(&mut self.lanes, b.num_lanes.into());
        op(&mut self.positive_lanes, b.positive_lanes.into());
        op(&mut self.lanes_with_txs, b.lanes_with_txs.into());
        op(&mut self.decode_errors, b.decode_errors.into());
        op(&mut self.payload_bytes, b.payload_bytes.into());
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Page<K> {
    Latest,
    // items below the key, newest first.
    Before(K),
    // the oldest items above the key, returned newest first.
    After(K),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PageOf<T> {
    pub items: Vec<T>,
    // Latest/Before: older items remain; After: newer items were cut off.
    pub has_more: bool,
}

// `page` holds partition points into a sorted sequence of `len` items.
fn page_bounds(len: usize, page: Page<usize>, limit: usize) -> (Range<usize>, bool) {
    match page {
        Page::Latest => page_bounds(len, Page::Before(len), limit),
        Page::Before(end) => {
            let start = end.saturating_sub(limit);
            (start..end, start > 0)
        }
        Page::After(start) => {
            let end = start.saturating_add(limit).min(len);
            (start..end, end < len)
        }
    }
}

// the ascending keys sharing a hash, payload or sender; inline for the common single key.
#[derive(Clone, Debug)]
enum Keys {
    One(TxKey),
    // at least two keys; eviction pops the front and boot, loading newest first, pushes it.
    // boxed so every map entry holding the common `One` is 8 bytes smaller.
    #[allow(clippy::box_collection)]
    Many(Box<VecDeque<TxKey>>),
}

impl Keys {
    fn len(&self) -> usize {
        match self {
            Self::One(_) => 1,
            Self::Many(keys) => keys.len(),
        }
    }

    fn get(&self, i: usize) -> TxKey {
        match self {
            Self::One(key) => *key,
            Self::Many(keys) => keys[i],
        }
    }

    fn iter(&self) -> impl ExactSizeIterator<Item = TxKey> + '_ {
        (0..self.len()).map(|i| self.get(i))
    }

    fn partition_point(&self, pred: impl Fn(&TxKey) -> bool) -> usize {
        match self {
            Self::One(key) => usize::from(pred(key)),
            Self::Many(keys) => keys.partition_point(pred),
        }
    }

    fn insert(&mut self, key: TxKey) {
        match self {
            Self::One(one) => {
                let (lo, hi) = if key < *one { (key, *one) } else { (*one, key) };
                *self = Self::Many(Box::new(VecDeque::from([lo, hi])));
            }
            Self::Many(keys) => {
                if keys.back().is_some_and(|b| *b < key) {
                    keys.push_back(key);
                } else if keys.front().is_some_and(|f| key < *f) {
                    keys.push_front(key);
                } else {
                    let at = keys.partition_point(|k| *k < key);
                    keys.insert(at, key);
                }
            }
        }
    }

    // true once no key is left.
    fn remove(&mut self, key: &TxKey) -> bool {
        let Self::Many(keys) = self else {
            return matches!(self, Self::One(one) if one == key);
        };
        if keys.front() == Some(key) {
            keys.pop_front();
        } else if let Ok(at) = keys.binary_search(key) {
            keys.remove(at);
        }
        match keys.len() {
            0 => return true,
            1 => *self = Self::One(keys[0]),
            n if keys.capacity() > 4 * n => keys.shrink_to(2 * n),
            _ => {}
        }
        false
    }

    fn heap_bytes(&self) -> usize {
        match self {
            Self::One(_) => 0,
            Self::Many(keys) => size_of::<VecDeque<TxKey>>() + keys.capacity() * size_of::<TxKey>(),
        }
    }

    fn page(&self, page: Page<TxKey>, limit: usize) -> PageOf<TxKey> {
        let page = match page {
            Page::Latest => Page::Latest,
            Page::Before(k) => Page::Before(self.partition_point(|i| *i < k)),
            Page::After(k) => Page::After(self.partition_point(|i| *i <= k)),
        };
        let (range, has_more) = page_bounds(self.len(), page, limit);
        PageOf {
            items: range.rev().map(|i| self.get(i)).collect(),
            has_more,
        }
    }
}

fn link<M: std::hash::Hash + Eq>(map: &mut HashMap<M, Keys>, id: M, key: TxKey) {
    match map.entry(id) {
        hash_map::Entry::Occupied(mut entry) => entry.get_mut().insert(key),
        hash_map::Entry::Vacant(entry) => {
            entry.insert(Keys::One(key));
        }
    }
}

fn unlink<M: std::hash::Hash + Eq>(map: &mut HashMap<M, Keys>, id: M, key: &TxKey) {
    if let hash_map::Entry::Occupied(mut entry) = map.entry(id)
        && entry.get_mut().remove(key)
    {
        entry.remove();
    }
}

fn keys_page<M: std::hash::Hash + Eq>(
    map: &HashMap<M, Keys>,
    id: &M,
    page: Page<TxKey>,
    limit: usize,
) -> PageOf<TxKey> {
    map.get(id).map_or(
        PageOf {
            items: Vec::new(),
            has_more: false,
        },
        |keys| keys.page(page, limit),
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexConfig {
    pub retain_slots: usize,
    pub max_txs: usize,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            retain_slots: 1_000_000,
            // about 500 B per tx at worst, see tests/memory.rs.
            max_txs: 200_000,
        }
    }
}

impl Default for Index {
    fn default() -> Self {
        Self::new(IndexConfig::default())
    }
}

// the newest blocks of the ledger, bounded by `retain_slots` blocks and `max_txs` txs.
#[derive(Debug)]
pub struct Index {
    config: IndexConfig,
    blocks: VecDeque<BlockSummary>,
    txs: BTreeMap<TxKey, TxEntry>,
    by_hash: HashMap<Hash, Keys>,
    by_payload: HashMap<Hash, Keys>,
    by_sender: HashMap<Address, Keys>,
    // unreadable slots that could still be indexed, so they are not retried.
    skipped: BTreeSet<u64>,
    // one past the newest slot evicted or refused; nothing below it is indexed again.
    floor: u64,
    totals: Totals,
}

impl Index {
    pub fn new(config: IndexConfig) -> Self {
        assert!(config.retain_slots > 0, "retain_slots must be positive");
        Self {
            config,
            blocks: VecDeque::new(),
            txs: BTreeMap::new(),
            by_hash: HashMap::new(),
            by_payload: HashMap::new(),
            by_sender: HashMap::new(),
            skipped: BTreeSet::new(),
            floor: 0,
            totals: Totals::default(),
        }
    }

    pub fn config(&self) -> IndexConfig {
        self.config
    }

    pub fn len(&self) -> usize {
        self.blocks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    pub fn tx_len(&self) -> usize {
        self.txs.len()
    }

    pub fn totals(&self) -> Totals {
        self.totals
    }

    pub fn oldest(&self) -> Option<&BlockSummary> {
        self.blocks.front()
    }

    pub fn newest(&self) -> Option<&BlockSummary> {
        self.blocks.back()
    }

    fn position(&self, slot: u64) -> Result<usize, usize> {
        self.blocks.binary_search_by_key(&slot, |b| b.slot)
    }

    pub fn block(&self, slot: u64) -> Option<&BlockSummary> {
        self.position(slot).ok().map(|i| &self.blocks[i])
    }

    // the blocks just before and after `slot`, which need not be indexed.
    pub fn neighbors(&self, slot: u64) -> (Option<&BlockSummary>, Option<&BlockSummary>) {
        let (before, after) = match self.position(slot) {
            Ok(i) => (i.checked_sub(1), i + 1),
            Err(i) => (i.checked_sub(1), i),
        };
        (before.map(|i| &self.blocks[i]), self.blocks.get(after))
    }

    pub fn contains(&self, slot: u64) -> bool {
        self.position(slot).is_ok()
    }

    // indexed, or known unreadable.
    pub fn is_known(&self, slot: u64) -> bool {
        self.contains(slot) || self.skipped.contains(&slot)
    }

    pub fn floor(&self) -> u64 {
        self.floor
    }

    // false when `insert` refuses every block at `slot`; a block may still overflow `max_txs`.
    pub fn admits(&self, slot: u64) -> bool {
        slot >= self.admitted_from()
    }

    fn admitted_from(&self) -> u64 {
        match self.oldest() {
            Some(o) if self.blocks.len() >= self.config.retain_slots => self.floor.max(o.slot),
            _ => self.floor,
        }
    }

    pub fn mark_skipped(&mut self, slot: u64) {
        if self.admits(slot) {
            self.skipped.insert(slot);
        }
    }

    fn raise_floor(&mut self, slot: u64) {
        self.floor = self.floor.max(slot);
        self.skipped = self.skipped.split_off(&self.admitted_from());
    }

    pub fn skipped_len(&self) -> usize {
        self.skipped.len()
    }

    fn over_bounds(&self) -> bool {
        self.blocks.len() > self.config.retain_slots || self.txs.len() > self.config.max_txs
    }

    pub fn insert(&mut self, block: LoadedBlock) -> Inserted {
        let slot = block.summary.slot;
        let at = match self.position(slot) {
            Ok(_) => return Inserted::Duplicate,
            Err(at) => at,
        };
        if slot < self.floor {
            return Inserted::TooOld;
        }
        let full = self.blocks.len() >= self.config.retain_slots
            || self.txs.len() + block.txs.len() > self.config.max_txs;
        if let Some(oldest) = self.oldest().map(|o| o.slot).filter(|_| at == 0 && full) {
            // anything older is refused too, so an empty block cannot land below a refused one.
            self.raise_floor(oldest);
            return Inserted::TooOld;
        }
        self.skipped.remove(&slot);
        self.totals.apply(&block.summary, true);
        self.blocks.insert(at, block.summary);
        for (key, entry) in block.txs {
            link(&mut self.by_hash, entry.hash, key);
            link(&mut self.by_payload, entry.payload_hash, key);
            link(&mut self.by_sender, entry.sender, key);
            self.txs.insert(key, entry);
        }
        while self.over_bounds() && self.blocks.len() > 1 {
            self.evict_oldest();
        }
        Inserted::New
    }

    pub fn evict_oldest(&mut self) -> Option<BlockSummary> {
        let block = self.blocks.pop_front()?;
        self.totals.apply(&block, false);
        while let Some(entry) = self.txs.first_entry() {
            if entry.key().slot != block.slot {
                break;
            }
            let (key, tx) = entry.remove_entry();
            unlink(&mut self.by_hash, tx.hash, &key);
            unlink(&mut self.by_payload, tx.payload_hash, &key);
            unlink(&mut self.by_sender, tx.sender, &key);
        }
        self.raise_floor(block.slot + 1);
        Some(block)
    }

    // drops blocks below `slot`, which the pruner has deleted.
    pub fn prune_below(&mut self, slot: u64) {
        while self.oldest().is_some_and(|b| b.slot < slot) {
            self.evict_oldest();
        }
        // later writes below `slot` are not pruned, so the floor stays.
        self.skipped = self.skipped.split_off(&slot);
    }

    // the last `n` blocks, oldest first.
    pub fn recent(&self, n: usize) -> impl ExactSizeIterator<Item = &BlockSummary> {
        self.blocks.range(self.blocks.len().saturating_sub(n)..)
    }

    pub fn blocks_page(&self, page: Page<u64>, limit: usize) -> PageOf<&BlockSummary> {
        let page = match page {
            Page::Latest => Page::Latest,
            Page::Before(s) => Page::Before(self.blocks.partition_point(|b| b.slot < s)),
            Page::After(s) => Page::After(self.blocks.partition_point(|b| b.slot <= s)),
        };
        let (range, has_more) = page_bounds(self.blocks.len(), page, limit);
        PageOf {
            items: self.blocks.range(range).rev().collect(),
            has_more,
        }
    }

    pub fn txs_page(&self, page: Page<TxKey>, limit: usize) -> PageOf<(&TxKey, &TxEntry)> {
        let take = limit.saturating_add(1);
        let mut items: Vec<_> = match page {
            Page::Latest => self.txs.iter().rev().take(take).collect(),
            Page::Before(k) => self.txs.range(..k).rev().take(take).collect(),
            Page::After(k) => {
                let mut items: Vec<_> = self
                    .txs
                    .range((Bound::Excluded(k), Bound::Unbounded))
                    .take(take)
                    .collect();
                let has_more = items.len() > limit;
                items.truncate(limit);
                items.reverse();
                return PageOf { items, has_more };
            }
        };
        let has_more = items.len() > limit;
        items.truncate(limit);
        PageOf { items, has_more }
    }

    // txs of one lane from position `from`, in lane order.
    pub fn lane_txs(
        &self,
        slot: u64,
        lane: u32,
        from: u32,
        limit: usize,
    ) -> PageOf<(&TxKey, &TxEntry)> {
        let range = self.txs.range(
            TxKey {
                slot,
                lane,
                pos: from,
            }..=TxKey::last_of(slot, lane),
        );
        let mut items: Vec<_> = range.take(limit.saturating_add(1)).collect();
        let has_more = items.len() > limit;
        items.truncate(limit);
        PageOf { items, has_more }
    }

    pub fn lane_tx_count(&self, slot: u64, lane: u32) -> usize {
        self.txs
            .range(TxKey::first_of(slot, lane)..=TxKey::last_of(slot, lane))
            .count()
    }

    pub fn tx(&self, key: &TxKey) -> Option<&TxEntry> {
        self.txs.get(key)
    }

    // every inclusion of a tx hash, oldest first; a resent tx can land more than once.
    pub fn tx_inclusions(&self, hash: &Hash) -> impl Iterator<Item = TxKey> + '_ {
        self.by_hash.get(hash).into_iter().flat_map(Keys::iter)
    }

    pub fn has_tx(&self, hash: &Hash) -> bool {
        self.by_hash.contains_key(hash)
    }

    pub fn has_payload(&self, hash: &Hash) -> bool {
        self.by_payload.contains_key(hash)
    }

    pub fn has_sender(&self, sender: &Address) -> bool {
        self.by_sender.contains_key(sender)
    }

    pub fn payload_txs(&self, hash: &Hash, page: Page<TxKey>, limit: usize) -> PageOf<TxKey> {
        keys_page(&self.by_payload, hash, page, limit)
    }

    pub fn sender_txs(&self, sender: &Address, page: Page<TxKey>, limit: usize) -> PageOf<TxKey> {
        keys_page(&self.by_sender, sender, page, limit)
    }

    pub fn payload_tx_count(&self, hash: &Hash) -> usize {
        self.by_payload.get(hash).map_or(0, Keys::len)
    }

    pub fn sender_tx_count(&self, sender: &Address) -> usize {
        self.by_sender.get(sender).map_or(0, Keys::len)
    }

    // an estimate of the heap held, from capacities and btree node fill.
    pub fn approx_heap_bytes(&self) -> usize {
        fn map_bytes<K>(map: &HashMap<K, Keys>) -> usize {
            // capacity() is 7/8 of the buckets allocated.
            map.capacity() * 8 / 7 * (size_of::<K>() + size_of::<Keys>() + 1)
                + map.values().map(Keys::heap_bytes).sum::<usize>()
        }
        // btree leaves are at least half full, so twice the payload bounds them.
        let btree_entry = 2 * (size_of::<TxKey>() + size_of::<TxEntry>());
        self.blocks.capacity() * size_of::<BlockSummary>()
            + self.txs.len() * btree_entry
            + self.skipped.len() * 2 * size_of::<u64>()
            + map_bytes(&self.by_hash)
            + map_bytes(&self.by_payload)
            + map_bytes(&self.by_sender)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use monad_mcp_chorus::ledger::LEDGER_VERSION;

    use super::*;

    fn tx(sender: u8, nonce: u64, payload: &str) -> Tx {
        Tx {
            sender: [sender; 20],
            nonce,
            payload: Bytes::copy_from_slice(payload.as_bytes()),
        }
    }

    fn lane_meta(index: u32, txs: Option<&[Tx]>) -> LaneMeta {
        LaneMeta {
            index,
            proposer: Some(u64::from(index) + 10),
            root: txs.map(|_| [index as u8; 20]),
            payload_len: txs.map_or(0, |t| {
                monad_mcp_chorus::ledger::encode_batch(t).len() as u32
            }),
            tx_count: txs.map_or(0, |t| t.len() as u32),
            decode_error: false,
        }
    }

    // lanes[j] = None is a negative lane.
    fn meta(slot: u64, lanes: &[Option<&[Tx]>]) -> BlockMeta {
        BlockMeta {
            version: LEDGER_VERSION,
            slot,
            deadline_ns: Some(u128::from(slot) * 100_000_000),
            finalized_at_ns: u128::from(slot) * 100_000_000 + 7_000_000,
            path: if slot.is_multiple_of(3) {
                FinalizationPath::Fallback
            } else {
                FinalizationPath::Fast
            },
            num_lanes: lanes.len() as u32,
            lanes: (0..).zip(lanes).map(|(i, l)| lane_meta(i, *l)).collect(),
        }
    }

    fn loaded(slot: u64, lanes: &[Option<&[Tx]>]) -> LoadedBlock {
        let m = meta(slot, lanes);
        let txs = (0..)
            .zip(lanes)
            .filter_map(|(i, l)| l.filter(|t| !t.is_empty()).map(|t| (i, t.to_vec())));
        LoadedBlock::new(&m, txs).unwrap()
    }

    fn empty(slot: u64) -> LoadedBlock {
        loaded(slot, &[Some(&[]), None, None])
    }

    fn index(retain_slots: usize, max_txs: usize) -> Index {
        Index::new(IndexConfig {
            retain_slots,
            max_txs,
        })
    }

    fn slots(page: &PageOf<&BlockSummary>) -> Vec<u64> {
        page.items.iter().map(|b| b.slot).collect()
    }

    #[test]
    fn tx_key_round_trip() {
        let key = TxKey {
            slot: 12,
            lane: 3,
            pos: 9,
        };
        assert_eq!(key.to_string(), "12.3.9");
        assert_eq!("12.3.9".parse(), Ok(key));
        for bad in ["", "1.2", "1.2.3.4", "a.b.c", "1.2.-3", "1.99999999999.0"] {
            assert!(bad.parse::<TxKey>().is_err(), "{bad}");
        }
    }

    #[test]
    fn summary_from_meta() {
        let txs = [tx(1, 0, "a"), tx(1, 1, "bb")];
        let m = meta(4, &[Some(&txs), None, Some(&[])]);
        let s = BlockSummary::from_meta(&m).unwrap();
        assert_eq!(
            (s.slot, s.num_lanes, s.positive_lanes, s.lanes_with_txs),
            (4, 3, 2, 1)
        );
        assert_eq!(s.tx_count, 2);
        assert_eq!(s.payload_bytes, m.lanes[0].payload_len + 1);
        assert_eq!(s.latency_ns(), Some(7_000_000));
        assert_eq!(s.finalized_at_ns, 407_000_000);

        let mut far = m.clone();
        far.finalized_at_ns = u128::from(u64::MAX) + 1;
        assert!(BlockSummary::from_meta(&far).is_err());
        let mut zero = m.clone();
        zero.deadline_ns = Some(0);
        assert_eq!(
            BlockSummary::from_meta(&zero).unwrap().deadline_ns(),
            Some(0)
        );
        let mut none = m;
        none.deadline_ns = None;
        let s = BlockSummary::from_meta(&none).unwrap();
        assert_eq!((s.deadline_ns(), s.latency_ns()), (None, None));
    }

    #[test]
    fn loaded_block_checks_lanes_against_meta() {
        let txs = [tx(1, 0, "a")];
        let m = meta(1, &[Some(&txs), Some(&[])]);
        assert!(LoadedBlock::new(&m, []).is_err());
        assert!(LoadedBlock::new(&m, [(1, txs.to_vec())]).is_err());
        assert!(LoadedBlock::new(&m, [(0, vec![])]).is_err());
        assert!(LoadedBlock::new(&m, [(0, txs.to_vec()), (1, txs.to_vec())]).is_err());
        let block = LoadedBlock::new(&m, [(0, txs.to_vec())]).unwrap();
        assert_eq!(block.txs.len(), 1);
        assert_eq!(block.txs[0].1.hash, txs[0].hash());
    }

    #[test]
    fn preview_is_bounded() {
        let long = "x".repeat(100);
        let e = TxEntry::new(&tx(1, 0, &long)).unwrap();
        assert_eq!(e.preview(), &long.as_bytes()[..PREVIEW_LEN]);
        assert_eq!(e.payload_len, 100);
        assert_eq!(TxEntry::new(&tx(1, 0, "")).unwrap().preview(), b"");
        assert!(TxEntry::new(&tx(1, 0, &"x".repeat(1 << 16))).is_err());
    }

    #[test]
    fn insert_keeps_slot_order_and_totals() {
        let mut ix = index(10, 100);
        for s in [5, 2, 9, 7] {
            assert_eq!(ix.insert(empty(s)), Inserted::New);
        }
        assert_eq!(ix.insert(empty(7)), Inserted::Duplicate);
        assert_eq!(slots(&ix.blocks_page(Page::Latest, 10)), vec![9, 7, 5, 2]);
        let t = ix.totals();
        assert_eq!((t.blocks, t.lanes, t.positive_lanes), (4, 12, 4));
        // slot 9 is fallback.
        assert_eq!((t.fast, t.fallback), (3, 1));
        assert_eq!(ix.neighbors(7).0.map(|b| b.slot), Some(5));
        assert_eq!(ix.neighbors(7).1.map(|b| b.slot), Some(9));
        assert_eq!(ix.neighbors(6).0.map(|b| b.slot), Some(5));
        assert_eq!(ix.neighbors(6).1.map(|b| b.slot), Some(7));
        assert_eq!(ix.neighbors(9).1, None);
    }

    #[test]
    fn eviction_drops_tx_payload_and_sender_entries() {
        let mut ix = index(2, 100);
        let a = [tx(1, 0, "shared"), tx(2, 0, "only-a")];
        let b = [tx(3, 0, "shared")];
        ix.insert(loaded(1, &[Some(&a)]));
        ix.insert(loaded(2, &[Some(&b)]));
        let shared = payload_hash_of("shared");
        assert_eq!(ix.payload_tx_count(&shared), 2);
        assert!(ix.has_sender(&[2; 20]));

        ix.insert(empty(3));
        assert!(!ix.contains(1));
        assert!(!ix.has_tx(&a[0].hash()) && !ix.has_tx(&a[1].hash()));
        assert!(!ix.has_payload(&payload_hash_of("only-a")));
        assert!(!ix.has_sender(&[1; 20]) && !ix.has_sender(&[2; 20]));
        // the other block's use of the payload survives.
        assert_eq!(ix.payload_tx_count(&shared), 1);
        assert_eq!(ix.tx_len(), 1);
        assert_eq!(ix.totals().txs, 1);

        ix.insert(empty(4));
        assert_eq!(ix.tx_len(), 0);
        assert!(ix.by_hash.is_empty() && ix.by_payload.is_empty() && ix.by_sender.is_empty());
        assert_eq!(ix.totals().txs, 0);
        assert_eq!(ix.totals().blocks, 2);
    }

    fn payload_hash_of(s: &str) -> Hash {
        monad_mcp_chorus::ledger::payload_hash(s.as_bytes())
    }

    #[test]
    fn full_index_rejects_older_blocks() {
        let mut ix = index(3, 100);
        for s in [10, 11, 12] {
            ix.insert(empty(s));
        }
        assert_eq!(ix.insert(empty(9)), Inserted::TooOld);
        assert_eq!(ix.insert(empty(13)), Inserted::New);
        assert_eq!(ix.oldest().map(|b| b.slot), Some(11));
        // a hole inside the window is still filled, evicting the oldest.
        ix.insert(empty(15));
        assert_eq!(ix.insert(empty(14)), Inserted::New);
        assert_eq!(slots(&ix.blocks_page(Page::Latest, 10)), vec![15, 14, 13]);
    }

    #[test]
    fn max_txs_evicts_oldest_blocks() {
        let mut ix = index(100, 3);
        let t = |n| [tx(1, n, "p"), tx(1, n + 100, "q")];
        let (a, b) = (t(0), t(1));
        ix.insert(loaded(1, &[Some(&a)]));
        ix.insert(loaded(2, &[Some(&b)]));
        assert_eq!(slots(&ix.blocks_page(Page::Latest, 10)), vec![2]);
        assert_eq!(ix.tx_len(), 2);
        // an older block that would overflow is refused instead of evicting newer ones.
        assert_eq!(ix.insert(loaded(1, &[Some(&a)])), Inserted::TooOld);
        assert_eq!(ix.insert(empty(0)), Inserted::TooOld);
    }

    #[test]
    fn tx_bounded_index_never_refills_below_its_floor() {
        let mut ix = index(100, 2);
        let t = |n| [tx(1, n, "p")];
        ix.insert(empty(1));
        ix.insert(loaded(2, &[Some(&t(2))]));
        ix.insert(empty(3));
        ix.insert(loaded(4, &[Some(&t(4))]));
        ix.insert(loaded(5, &[Some(&t(5))]));
        assert_eq!(slots(&ix.blocks_page(Page::Latest, 10)), vec![5, 4, 3]);
        assert_eq!(ix.floor(), 3);
        for s in [1, 2] {
            assert!(!ix.admits(s));
            assert_eq!(ix.insert(empty(s)), Inserted::TooOld);
            ix.mark_skipped(s);
            assert!(!ix.is_known(s));
        }
        assert_eq!(slots(&ix.blocks_page(Page::Latest, 10)), vec![5, 4, 3]);
    }

    #[test]
    fn refused_block_raises_the_floor() {
        let mut ix = index(100, 2);
        let t = |n| [tx(1, n, "p")];
        ix.insert(loaded(10, &[Some(&t(10))]));
        ix.insert(loaded(11, &[Some(&t(11))]));
        // a late tx block below the oldest overflows, so an older empty one must not fill in under it.
        assert_eq!(ix.insert(loaded(9, &[Some(&t(9))])), Inserted::TooOld);
        assert_eq!(ix.insert(empty(8)), Inserted::TooOld);
        assert_eq!(ix.floor(), 10);
        assert_eq!(slots(&ix.blocks_page(Page::Latest, 10)), vec![11, 10]);
    }

    #[test]
    fn skipped_slots_below_the_floor_are_dropped() {
        let mut ix = index(100, 1);
        ix.insert(loaded(5, &[Some(&[tx(1, 0, "p")])]));
        ix.mark_skipped(3);
        ix.mark_skipped(7);
        assert!(ix.is_known(3));
        ix.insert(loaded(8, &[Some(&[tx(1, 1, "p")])]));
        assert_eq!(ix.floor(), 6);
        assert!(!ix.is_known(3) && ix.is_known(7));
        ix.mark_skipped(4);
        assert!(!ix.is_known(4));
        assert_eq!(ix.skipped_len(), 1);
    }

    #[test]
    fn resent_tx_has_every_inclusion() {
        let mut ix = index(10, 100);
        let t = [tx(1, 0, "twice")];
        ix.insert(loaded(1, &[Some(&t)]));
        ix.insert(loaded(3, &[None, Some(&t)]));
        let keys: Vec<_> = ix.tx_inclusions(&t[0].hash()).collect();
        assert_eq!(keys.len(), 2);
        assert_eq!((keys[0].slot, keys[1].slot, keys[1].lane), (1, 3, 1));
        ix.evict_oldest();
        assert_eq!(ix.tx_inclusions(&t[0].hash()).count(), 1);
    }

    #[test]
    fn block_pages() {
        let mut ix = index(100, 100);
        for s in 1..=10 {
            ix.insert(empty(s));
        }
        let p = ix.blocks_page(Page::Latest, 3);
        assert_eq!((slots(&p), p.has_more), (vec![10, 9, 8], true));
        let p = ix.blocks_page(Page::Before(3), 3);
        assert_eq!((slots(&p), p.has_more), (vec![2, 1], false));
        let p = ix.blocks_page(Page::After(4), 3);
        assert_eq!((slots(&p), p.has_more), (vec![7, 6, 5], true));
        let p = ix.blocks_page(Page::After(7), 3);
        assert_eq!((slots(&p), p.has_more), (vec![10, 9, 8], false));
        let p = ix.blocks_page(Page::After(10), 3);
        assert_eq!((slots(&p), p.has_more), (vec![], false));
        // cursors need not be indexed slots.
        let p = ix.blocks_page(Page::Before(100), 2);
        assert_eq!(slots(&p), vec![10, 9]);
    }

    #[test]
    fn tx_pages_and_lanes() {
        let mut ix = index(100, 100);
        let lane0: Vec<Tx> = (0..4).map(|n| tx(7, n, "x")).collect();
        let lane2 = [tx(8, 0, "y")];
        ix.insert(loaded(1, &[Some(&lane0), None, Some(&lane2)]));
        ix.insert(loaded(2, &[Some(&[tx(9, 0, "z")])]));
        let keys = |p: PageOf<(&TxKey, &TxEntry)>| {
            (
                p.items
                    .iter()
                    .map(|(k, _)| k.to_string())
                    .collect::<Vec<_>>(),
                p.has_more,
            )
        };
        assert_eq!(
            keys(ix.txs_page(Page::Latest, 2)),
            (vec!["2.0.0".into(), "1.2.0".into()], true)
        );
        let cursor: TxKey = "1.2.0".parse().unwrap();
        assert_eq!(
            keys(ix.txs_page(Page::Before(cursor), 10)),
            (
                vec![
                    "1.0.3".into(),
                    "1.0.2".into(),
                    "1.0.1".into(),
                    "1.0.0".into()
                ],
                false
            )
        );
        let cursor: TxKey = "1.0.1".parse().unwrap();
        assert_eq!(
            keys(ix.txs_page(Page::After(cursor), 2)),
            (vec!["1.0.3".into(), "1.0.2".into()], true)
        );
        assert_eq!(
            keys(ix.lane_txs(1, 0, 1, 2)),
            (vec!["1.0.1".into(), "1.0.2".into()], true)
        );
        assert_eq!(keys(ix.lane_txs(1, 0, 3, 2)), (vec!["1.0.3".into()], false));
        assert_eq!(ix.lane_tx_count(1, 0), 4);
        assert_eq!(ix.lane_tx_count(1, 1), 0);

        let sender = ix.sender_txs(&[7; 20], Page::Latest, 3);
        assert_eq!(sender.items.len(), 3);
        assert!(sender.has_more);
        assert_eq!(sender.items[0].pos, 3);
        let rest = ix.sender_txs(&[7; 20], Page::Before(sender.items[2]), 3);
        assert_eq!((rest.items.len(), rest.has_more), (1, false));
        let x = ix.payload_txs(&payload_hash_of("x"), Page::Latest, 10);
        assert_eq!(x.items.len(), 4);
    }

    #[test]
    fn skipped_slots_are_pruned_with_the_window() {
        let mut ix = index(2, 100);
        ix.insert(empty(5));
        ix.mark_skipped(6);
        ix.mark_skipped(3);
        // below the oldest, but the index has room for it.
        assert!(ix.is_known(6) && ix.is_known(3));
        ix.insert(empty(7));
        ix.insert(empty(8));
        assert!(!ix.is_known(6) && !ix.is_known(3));
        assert_eq!(ix.skipped_len(), 0);
        ix.mark_skipped(4);
        assert!(!ix.is_known(4));
        ix.mark_skipped(9);
        ix.prune_below(8);
        assert_eq!(slots(&ix.blocks_page(Page::Latest, 10)), vec![8]);
        assert!(ix.is_known(9));
        // a skipped slot that becomes readable is indexed.
        assert_eq!(ix.insert(empty(9)), Inserted::New);
        assert_eq!(ix.skipped_len(), 0);
    }

    #[test]
    fn keys_stay_sorted_under_any_insert_order() {
        let key = |n: u64| TxKey {
            slot: n / 4,
            lane: (n % 4) as u32,
            pos: 0,
        };
        // a fixed permutation of 0..64 mixing front, back and middle inserts.
        let order: Vec<u64> = (0..64).map(|i| (i * 37 + 11) % 64).collect();
        let mut keys = Keys::One(key(order[0]));
        for &n in &order[1..] {
            keys.insert(key(n));
        }
        let all: Vec<TxKey> = keys.iter().collect();
        assert_eq!(all, (0..64).map(key).collect::<Vec<_>>());
        let p = keys.page(Page::Before(key(10)), 3);
        assert_eq!((p.items, p.has_more), (vec![key(9), key(8), key(7)], true));
        let p = keys.page(Page::After(key(60)), 10);
        assert_eq!(
            (p.items, p.has_more),
            (vec![key(63), key(62), key(61)], false)
        );

        for &n in order[1..].iter().rev() {
            assert!(!keys.remove(&key(n)));
        }
        assert!(matches!(keys, Keys::One(k) if k == key(order[0])));
        assert!(!keys.remove(&key(99)));
        assert!(keys.remove(&key(order[0])));
    }

    // one sender and payload for every tx, as a burst from a single client produces.
    fn hot_block(slot: u64, txs: u32) -> LoadedBlock {
        let entry = TxEntry::new(&tx(1, 0, "hello chorus")).unwrap();
        let mut block = empty(slot);
        block.summary.tx_count = txs;
        block.txs = (0..txs)
            .map(|pos| {
                let mut e = entry.clone();
                e.hash[..8].copy_from_slice(&slot.to_be_bytes());
                e.hash[8..12].copy_from_slice(&pos.to_be_bytes());
                (TxKey { slot, lane: 0, pos }, e)
            })
            .collect();
        block
    }

    #[test]
    fn hot_sender_boot_and_eviction_are_linear() {
        const BLOCKS: u64 = 20_000;
        const PER_BLOCK: u32 = 10;
        let max_txs = (BLOCKS * u64::from(PER_BLOCK)) as usize;
        let mut ix = index(1_000_000, max_txs);
        let started = std::time::Instant::now();
        // boot order: newest first.
        for slot in (0..BLOCKS).rev() {
            assert_eq!(ix.insert(hot_block(slot, PER_BLOCK)), Inserted::New);
        }
        assert_eq!(ix.sender_tx_count(&[1; 20]), max_txs);
        // steady state: each new block evicts the oldest.
        for slot in BLOCKS..2 * BLOCKS {
            ix.insert(hot_block(slot, PER_BLOCK));
        }
        let elapsed = started.elapsed();
        assert_eq!(ix.oldest().map(|b| b.slot), Some(BLOCKS));
        assert_eq!(ix.sender_tx_count(&[1; 20]), max_txs);
        let payload = payload_hash_of("hello chorus");
        assert_eq!(ix.payload_tx_count(&payload), max_txs);
        let p = ix.sender_txs(&[1; 20], Page::Latest, 2);
        assert_eq!(p.items[0].to_string(), format!("{}.0.9", 2 * BLOCKS - 1));
        // quadratic memmoves take minutes here.
        assert!(elapsed.as_secs() < 10, "{elapsed:?}");
    }

    #[test]
    fn summary_memory_budget() {
        // 1M retained slots must fit the ~100-150 MB target with room for txs.
        assert!(
            size_of::<BlockSummary>() <= 48,
            "{}",
            size_of::<BlockSummary>()
        );
        let n = 200_000;
        let mut ix = index(n, 100);
        for s in 0..n as u64 {
            ix.insert(empty(s));
        }
        let per_block = ix.approx_heap_bytes() / n;
        assert!(per_block <= 64, "{per_block} bytes per empty block");
    }
}
