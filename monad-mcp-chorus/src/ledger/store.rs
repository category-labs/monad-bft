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
    collections::BTreeSet,
    fs::{self, File},
    io::{self, Write},
    path::{Path, PathBuf},
};

use bytes::Bytes;
use tracing::{debug, warn};

use super::{
    block::{BlockMeta, LEDGER_VERSION, LaneMeta, NewBlock, lane_json},
    tx::{Tx, TxError, decode_batch},
};

pub const BLOCKS_DIR: &str = "blocks";
pub const META_FILE: &str = "meta.rlp";
pub const META_JSON_FILE: &str = "meta.json";
pub const PROOF_FILE: &str = "proof.rlp";

const BLOCK_NAME_DIGITS: usize = 12;

pub fn block_dir_name(slot: u64) -> String {
    format!("{slot:0width$}", width = BLOCK_NAME_DIGITS)
}

// accepts only the canonical name, so foreign digit strings are not blocks.
pub fn parse_block_dir_name(name: &str) -> Option<u64> {
    if name.len() < BLOCK_NAME_DIGITS || !name.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let slot = name.parse().ok()?;
    (block_dir_name(slot) == name).then_some(slot)
}

fn temp_dir_name(slot: u64) -> String {
    format!(".{}.tmp", block_dir_name(slot))
}

fn is_temp_dir_name(name: &str) -> bool {
    name.strip_prefix('.')
        .and_then(|n| n.strip_suffix(".tmp"))
        .and_then(parse_block_dir_name)
        .is_some()
}

pub fn lane_file_name(index: u32) -> String {
    format!("lane-{index}.rlp")
}

pub fn lane_json_file_name(index: u32) -> String {
    format!("lane-{index}.json")
}

#[derive(Debug, thiserror::Error)]
pub enum LedgerError {
    #[error("{op} {}: {source}", path.display())]
    Io {
        op: &'static str,
        path: PathBuf,
        source: io::Error,
    },
    #[error("slot {0} is not in the ledger")]
    NotFound(u64),
    #[error("slot {0} is already in the ledger")]
    Exists(u64),
    // like MissingLane, also seen when a pruner is midway through deleting the block.
    #[error("corrupt ledger entry {}: {reason}", path.display())]
    Corrupt { path: PathBuf, reason: String },
    #[error("slot {slot} has no lane {index}")]
    NoSuchLane { slot: u64, index: u32 },
    #[error("slot {slot} lane {index} is positive but its lane file is missing")]
    MissingLane { slot: u64, index: u32 },
    #[error("invalid block: {0}")]
    Invalid(&'static str),
}

fn io_err<'a>(op: &'static str, path: &'a Path) -> impl FnOnce(io::Error) -> LedgerError + 'a {
    move |source| LedgerError::Io {
        op,
        path: path.to_path_buf(),
        source,
    }
}

fn write_synced(path: &Path, data: &[u8]) -> Result<(), LedgerError> {
    let mut file = File::create(path).map_err(io_err("create", path))?;
    file.write_all(data).map_err(io_err("write", path))?;
    file.sync_all().map_err(io_err("fsync", path))
}

fn sync_dir(path: &Path) -> Result<(), LedgerError> {
    File::open(path)
        .and_then(|dir| dir.sync_all())
        .map_err(io_err("fsync", path))
}

type DecodedLane = Option<Result<Vec<Tx>, TxError>>;

fn assemble(block: &NewBlock) -> Result<(BlockMeta, Vec<DecodedLane>), LedgerError> {
    let num_lanes =
        u32::try_from(block.lanes.len()).map_err(|_| LedgerError::Invalid("too many lanes"))?;
    let mut lanes = Vec::with_capacity(block.lanes.len());
    let mut decoded = Vec::with_capacity(block.lanes.len());
    for (index, lane) in (0..num_lanes).zip(&block.lanes) {
        let (meta, txs) = match &lane.committed {
            None => (
                LaneMeta {
                    index,
                    proposer: lane.proposer,
                    root: None,
                    payload_len: 0,
                    tx_count: 0,
                    decode_error: false,
                },
                None,
            ),
            Some(committed) => {
                let payload_len = u32::try_from(committed.payload.len())
                    .map_err(|_| LedgerError::Invalid("lane payload too large"))?;
                let txs = decode_batch(&committed.payload);
                let tx_count = match &txs {
                    Ok(txs) => u32::try_from(txs.len())
                        .map_err(|_| LedgerError::Invalid("too many txs"))?,
                    Err(_) => 0,
                };
                (
                    LaneMeta {
                        index,
                        proposer: lane.proposer,
                        root: Some(committed.root),
                        payload_len,
                        tx_count,
                        decode_error: txs.is_err(),
                    },
                    Some(txs),
                )
            }
        };
        lanes.push(meta);
        decoded.push(txs);
    }
    let meta = BlockMeta {
        version: LEDGER_VERSION,
        slot: block.slot,
        deadline_ns: block.deadline_ns,
        finalized_at_ns: block.finalized_at_ns,
        path: block.path,
        num_lanes,
        lanes,
    };
    debug_assert_eq!(meta.check(), Ok(()));
    Ok((meta, decoded))
}

// the single writer of `<ledger_dir>/blocks`; slots may be written in any order.
#[derive(Debug)]
pub struct LedgerWriter {
    blocks: PathBuf,
}

impl LedgerWriter {
    // creates the blocks dir and removes temp dirs left by a crash.
    pub fn open(ledger_dir: impl AsRef<Path>) -> Result<Self, LedgerError> {
        let blocks = ledger_dir.as_ref().join(BLOCKS_DIR);
        fs::create_dir_all(&blocks).map_err(io_err("create", &blocks))?;
        let entries = fs::read_dir(&blocks).map_err(io_err("list", &blocks))?;
        for entry in entries {
            let entry = entry.map_err(io_err("list", &blocks))?;
            if entry.file_name().to_str().is_some_and(is_temp_dir_name) {
                let path = entry.path();
                fs::remove_dir_all(&path).map_err(io_err("remove", &path))?;
                debug!(path = %path.display(), "removed leftover ledger temp dir");
            }
        }
        sync_dir(&blocks)?;
        Ok(Self { blocks })
    }

    pub fn blocks_dir(&self) -> &Path {
        &self.blocks
    }

    // builds the block in a temp dir and renames it in, so readers never see a partial block.
    // Ok once the block is published, even if the final fsync of `blocks/` fails.
    pub fn write(&self, block: &NewBlock) -> Result<BlockMeta, LedgerError> {
        let (meta, decoded) = assemble(block)?;
        let dir = self.blocks.join(block_dir_name(block.slot));
        if dir.exists() {
            return Err(LedgerError::Exists(block.slot));
        }
        let tmp = self.blocks.join(temp_dir_name(block.slot));
        if tmp.exists() {
            fs::remove_dir_all(&tmp).map_err(io_err("remove", &tmp))?;
        }
        fs::create_dir(&tmp).map_err(io_err("create", &tmp))?;
        let result = self.fill(&tmp, block, &meta, &decoded).and_then(|()| {
            fs::rename(&tmp, &dir).map_err(|e| match e.kind() {
                io::ErrorKind::AlreadyExists | io::ErrorKind::DirectoryNotEmpty => {
                    LedgerError::Exists(block.slot)
                }
                _ => io_err("rename", &tmp)(e),
            })
        });
        if let Err(e) = result {
            if let Err(cleanup) = fs::remove_dir_all(&tmp) {
                warn!(path = %tmp.display(), %cleanup, "failed to remove ledger temp dir");
            }
            return Err(e);
        }
        if let Err(e) = sync_dir(&self.blocks) {
            warn!(slot = block.slot, error = %e, "ledger block published but not yet durable");
        }
        Ok(meta)
    }

    fn fill(
        &self,
        tmp: &Path,
        block: &NewBlock,
        meta: &BlockMeta,
        decoded: &[DecodedLane],
    ) -> Result<(), LedgerError> {
        for ((lane, new), txs) in meta.lanes.iter().zip(&block.lanes).zip(decoded) {
            let (Some(committed), Some(txs)) = (&new.committed, txs) else {
                continue;
            };
            write_synced(&tmp.join(lane_file_name(lane.index)), &committed.payload)?;
            let json = lane_json(meta.slot, lane, txs) + "\n";
            write_synced(&tmp.join(lane_json_file_name(lane.index)), json.as_bytes())?;
        }
        write_synced(&tmp.join(PROOF_FILE), &block.proof)?;
        write_synced(
            &tmp.join(META_JSON_FILE),
            (meta.to_json() + "\n").as_bytes(),
        )?;
        // meta last: a block dir with a meta has everything else.
        write_synced(&tmp.join(META_FILE), &meta.to_rlp())?;
        sync_dir(tmp)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Since {
    pub blocks: Vec<BlockMeta>,
    // pass back as `after`; advances past skipped entries too.
    pub cursor: Option<u64>,
}

// read-only view of `<ledger_dir>/blocks`; safe alongside a live writer and pruner.
#[derive(Clone, Debug)]
pub struct LedgerReader {
    blocks: PathBuf,
}

impl LedgerReader {
    pub fn new(ledger_dir: impl AsRef<Path>) -> Self {
        Self {
            blocks: ledger_dir.as_ref().join(BLOCKS_DIR),
        }
    }

    pub fn blocks_dir(&self) -> &Path {
        &self.blocks
    }

    pub fn block_dir(&self, slot: u64) -> PathBuf {
        self.blocks.join(block_dir_name(slot))
    }

    // sorted slots of the block dirs present; a missing blocks dir is an empty ledger.
    pub fn scan(&self) -> Result<Vec<u64>, LedgerError> {
        let entries = match fs::read_dir(&self.blocks) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(io_err("list", &self.blocks)(e)),
        };
        let mut slots = Vec::new();
        for entry in entries {
            let entry = entry.map_err(io_err("list", &self.blocks))?;
            let name = entry.file_name();
            let Some(slot) = name.to_str().and_then(parse_block_dir_name) else {
                continue;
            };
            // a pruner may delete the entry under us.
            match entry.file_type() {
                Ok(t) if t.is_dir() => slots.push(slot),
                Ok(_) => debug!(?name, "skipping non-directory ledger entry"),
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(io_err("stat", &entry.path())(e)),
            }
        }
        slots.sort_unstable();
        Ok(slots)
    }

    // distinguishes a pruned block from a file missing inside a present one.
    fn read_block_file(&self, slot: u64, name: &str) -> Result<Option<Vec<u8>>, LedgerError> {
        let dir = self.block_dir(slot);
        let path = dir.join(name);
        match fs::read(&path) {
            Ok(data) => Ok(Some(data)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                if dir.is_dir() {
                    Ok(None)
                } else {
                    Err(LedgerError::NotFound(slot))
                }
            }
            Err(e) => Err(io_err("read", &path)(e)),
        }
    }

    fn corrupt(&self, slot: u64, name: &str, reason: impl Into<String>) -> LedgerError {
        LedgerError::Corrupt {
            path: self.block_dir(slot).join(name),
            reason: reason.into(),
        }
    }

    pub fn read_meta(&self, slot: u64) -> Result<BlockMeta, LedgerError> {
        let data = self
            .read_block_file(slot, META_FILE)?
            .ok_or_else(|| self.corrupt(slot, META_FILE, "missing"))?;
        let meta = BlockMeta::decode_exact(&data)
            .map_err(|e| self.corrupt(slot, META_FILE, e.to_string()))?;
        if meta.slot != slot {
            return Err(self.corrupt(slot, META_FILE, format!("holds slot {}", meta.slot)));
        }
        Ok(meta)
    }

    // None for a negative lane.
    pub fn read_lane(&self, slot: u64, index: u32) -> Result<Option<Bytes>, LedgerError> {
        self.read_lane_for(&self.read_meta(slot)?, index)
    }

    pub fn read_lane_for(
        &self,
        meta: &BlockMeta,
        index: u32,
    ) -> Result<Option<Bytes>, LedgerError> {
        let slot = meta.slot;
        let lane = usize::try_from(index)
            .ok()
            .and_then(|i| meta.lanes.get(i))
            .ok_or(LedgerError::NoSuchLane { slot, index })?;
        if !lane.is_positive() {
            return Ok(None);
        }
        let name = lane_file_name(index);
        let data = self
            .read_block_file(slot, &name)?
            .ok_or(LedgerError::MissingLane { slot, index })?;
        if u32::try_from(data.len()).ok() != Some(lane.payload_len) {
            return Err(self.corrupt(
                slot,
                &name,
                format!("{} bytes, meta says {}", data.len(), lane.payload_len),
            ));
        }
        Ok(Some(data.into()))
    }

    pub fn read_proof(&self, slot: u64) -> Result<Bytes, LedgerError> {
        self.read_block_file(slot, PROOF_FILE)?
            .map(Bytes::from)
            .ok_or_else(|| self.corrupt(slot, PROOF_FILE, "missing"))
    }

    // metas of up to `limit` blocks with slot > `after`, oldest first; unreadable ones are skipped.
    // a slot range, not a change feed: a lower slot written later is missed, see LedgerFollower.
    pub fn since(&self, after: Option<u64>, limit: usize) -> Result<Since, LedgerError> {
        let slots = self.scan()?;
        let start = after.map_or(0, |a| slots.partition_point(|&s| s <= a));
        let mut cursor = after;
        let mut blocks = Vec::new();
        for &slot in slots[start..].iter().take(limit) {
            cursor = Some(slot);
            match self.read_meta(slot) {
                Ok(meta) => blocks.push(meta),
                Err(e) => self.log_skipped(slot, &e),
            }
        }
        Ok(Since { blocks, cursor })
    }

    fn log_skipped(&self, slot: u64, error: &LedgerError) {
        // a pruner deletes file by file, so any error on a block that is now gone is a prune.
        if matches!(error, LedgerError::NotFound(_)) || !self.block_dir(slot).exists() {
            debug!(slot, %error, "ledger block pruned during read");
        } else {
            warn!(slot, %error, "skipping unreadable ledger block");
        }
    }

    pub fn follow(&self) -> LedgerFollower {
        LedgerFollower {
            reader: self.clone(),
            seen: BTreeSet::new(),
        }
    }

    // a follower that skips the blocks present now.
    pub fn follow_from_now(&self) -> Result<LedgerFollower, LedgerError> {
        Ok(LedgerFollower {
            reader: self.clone(),
            seen: self.scan()?.into_iter().collect(),
        })
    }
}

// change feed over the ledger: yields each block once, whatever order the writer wrote it in.
#[derive(Clone, Debug)]
pub struct LedgerFollower {
    reader: LedgerReader,
    // yielded or skipped slots still on disk; pruned ones are dropped, bounding it by the dir.
    seen: BTreeSet<u64>,
}

impl LedgerFollower {
    pub fn reader(&self) -> &LedgerReader {
        &self.reader
    }

    // metas of up to `limit` new blocks, lowest slot first; unreadable ones are skipped once.
    // each call lists the whole blocks dir, O(n log n) in the blocks kept on disk.
    pub fn poll(&mut self, limit: usize) -> Result<Vec<BlockMeta>, LedgerError> {
        let mut seen = BTreeSet::new();
        let mut blocks = Vec::new();
        let mut budget = limit;
        for slot in self.reader.scan()? {
            if self.seen.contains(&slot) {
                seen.insert(slot);
                continue;
            }
            if budget == 0 {
                continue;
            }
            budget -= 1;
            match self.reader.read_meta(slot) {
                Ok(meta) => {
                    blocks.push(meta);
                    seen.insert(slot);
                }
                Err(LedgerError::NotFound(_)) => {
                    debug!(slot, "ledger block pruned during read");
                }
                Err(e) => {
                    self.reader.log_skipped(slot, &e);
                    seen.insert(slot);
                }
            }
        }
        self.seen = seen;
        Ok(blocks)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread,
    };

    use proptest::prelude::*;
    use tempfile::TempDir;

    use super::*;
    use crate::ledger::{
        block::{CommittedLane, FinalizationPath, NewLane},
        tx::encode_batch,
    };

    fn tx(nonce: u64) -> Tx {
        Tx {
            sender: [0x33; 20],
            nonce,
            payload: Bytes::from(format!("payload {nonce}")),
        }
    }

    fn committed(root: u8, payload: Bytes) -> NewLane {
        NewLane {
            proposer: Some(u64::from(root)),
            committed: Some(CommittedLane {
                root: [root; 20],
                payload,
            }),
        }
    }

    fn negative() -> NewLane {
        NewLane {
            proposer: None,
            committed: None,
        }
    }

    // lane 0: two txs, lane 1: negative, lane 2: garbage, lane 3: empty batch.
    fn block(slot: u64) -> NewBlock {
        NewBlock {
            slot,
            deadline_ns: Some(1_000 + u128::from(slot)),
            finalized_at_ns: 2_000 + u128::from(slot),
            path: if slot.is_multiple_of(2) {
                FinalizationPath::Fast
            } else {
                FinalizationPath::Fallback
            },
            lanes: vec![
                committed(1, encode_batch(&[tx(slot), tx(slot + 1)])),
                negative(),
                committed(3, Bytes::from_static(b"\xffgarbage")),
                committed(4, encode_batch(&[])),
            ],
            proof: Bytes::from(format!("proof {slot}")),
        }
    }

    fn setup() -> (TempDir, LedgerWriter, LedgerReader) {
        let dir = TempDir::new().unwrap();
        let writer = LedgerWriter::open(dir.path()).unwrap();
        let reader = LedgerReader::new(dir.path());
        (dir, writer, reader)
    }

    #[test]
    fn block_names() {
        assert_eq!(block_dir_name(12345), "000000012345");
        assert_eq!(block_dir_name(u64::MAX), u64::MAX.to_string());
        for slot in [0, 1, 999_999_999_999, 1_000_000_000_000, u64::MAX] {
            assert_eq!(parse_block_dir_name(&block_dir_name(slot)), Some(slot));
            assert!(is_temp_dir_name(&temp_dir_name(slot)));
        }
        for bad in [
            "",
            "12345",
            "00000001234",
            "00000001234x",
            "+00000001234",
            "0001000000000000",
            "99999999999999999999",
            ".000000012345",
            "000000012345.tmp",
        ] {
            assert_eq!(parse_block_dir_name(bad), None, "{bad}");
        }
        assert!(!is_temp_dir_name(".foo.tmp"));
        assert!(!is_temp_dir_name("000000012345.tmp"));
    }

    #[test]
    fn write_read_round_trip() {
        let (_dir, writer, reader) = setup();
        let new = block(12345);
        let meta = writer.write(&new).unwrap();
        assert_eq!(reader.read_meta(12345).unwrap(), meta);

        assert_eq!(meta.version, LEDGER_VERSION);
        assert_eq!(meta.deadline_ns, new.deadline_ns);
        assert_eq!(meta.finalized_at_ns, new.finalized_at_ns);
        assert_eq!(meta.path, FinalizationPath::Fallback);
        assert_eq!(meta.num_lanes, 4);
        assert_eq!(meta.tx_count(), 2);
        let summary: Vec<_> = meta
            .lanes
            .iter()
            .map(|l| (l.proposer, l.root.map(|r| r[0]), l.tx_count, l.decode_error))
            .collect();
        assert_eq!(
            summary,
            vec![
                (Some(1), Some(1), 2, false),
                (None, None, 0, false),
                (Some(3), Some(3), 0, true),
                (Some(4), Some(4), 0, false),
            ]
        );

        for (index, lane) in (0..).zip(&new.lanes) {
            let expected = lane.committed.as_ref().map(|c| c.payload.clone());
            assert_eq!(reader.read_lane(12345, index).unwrap(), expected);
            assert_eq!(
                meta.lanes[index as usize].payload_len as usize,
                expected.map_or(0, |p| p.len())
            );
        }
        let lane0 = reader.read_lane(12345, 0).unwrap().unwrap();
        assert_eq!(decode_batch(&lane0).unwrap(), vec![tx(12345), tx(12346)]);
        assert_eq!(reader.read_proof(12345).unwrap(), new.proof);
        assert!(matches!(
            reader.read_lane(12345, 4),
            Err(LedgerError::NoSuchLane {
                slot: 12345,
                index: 4
            })
        ));

        let mut files = list_files(&reader.block_dir(12345));
        files.sort();
        assert_eq!(
            files,
            [
                "lane-0.json",
                "lane-0.rlp",
                "lane-2.json",
                "lane-2.rlp",
                "lane-3.json",
                "lane-3.rlp",
                "meta.json",
                "meta.rlp",
                "proof.rlp"
            ]
        );
    }

    #[test]
    fn json_copies() {
        let (_dir, writer, reader) = setup();
        writer.write(&block(7)).unwrap();
        let dir = reader.block_dir(7);
        let read = |name: &str| -> serde_json::Value {
            serde_json::from_str(&fs::read_to_string(dir.join(name)).unwrap()).unwrap()
        };

        let meta = read(META_JSON_FILE);
        assert_eq!(meta["slot"], 7);
        assert_eq!(meta["path"], "fallback");
        assert_eq!(meta["lanes"].as_array().unwrap().len(), 4);
        assert_eq!(meta["lanes"][2]["decode_error"], true);

        let lane0 = read("lane-0.json");
        let txs = lane0["txs"].as_array().unwrap();
        assert_eq!(txs.len(), 2);
        assert_eq!(txs[0]["hash"], format!("0x{}", hex::encode(tx(7).hash())));
        assert_eq!(txs[1]["nonce"], 8);
        assert_eq!(
            txs[1]["payload"],
            format!("0x{}", hex::encode(b"payload 8"))
        );

        let lane2 = read("lane-2.json");
        assert!(lane2["decode_error"].is_string());
        assert!(lane2.get("txs").is_none());
        assert_eq!(read("lane-3.json")["txs"], serde_json::json!([]));
    }

    #[test]
    fn existing_block_is_not_overwritten() {
        let (_dir, writer, reader) = setup();
        let first = writer.write(&block(5)).unwrap();
        let mut again = block(5);
        again.proof = Bytes::from_static(b"other");
        assert!(matches!(writer.write(&again), Err(LedgerError::Exists(5))));
        assert_eq!(reader.read_meta(5).unwrap(), first);
        assert_eq!(reader.read_proof(5).unwrap(), block(5).proof);
        assert!(!writer.blocks_dir().join(temp_dir_name(5)).exists());
    }

    #[test]
    fn scan_order_and_foreign_entries() {
        let (_dir, writer, reader) = setup();
        assert_eq!(reader.scan().unwrap(), Vec::<u64>::new());
        let slots = [1_000_000_000_000, 3, 1, 20, 999_999_999_999, 2];
        for slot in slots {
            writer.write(&block(slot)).unwrap();
        }
        let blocks = writer.blocks_dir();
        fs::create_dir(blocks.join(temp_dir_name(4))).unwrap();
        fs::create_dir(blocks.join("foo")).unwrap();
        fs::create_dir(blocks.join("00000000005x")).unwrap();
        fs::create_dir(blocks.join("0000000000006")).unwrap();
        fs::write(blocks.join(block_dir_name(7)), b"a file").unwrap();
        fs::write(blocks.join("README"), b"hi").unwrap();

        let mut expected = slots.to_vec();
        expected.sort_unstable();
        assert_eq!(reader.scan().unwrap(), expected);

        let since = reader.since(None, usize::MAX).unwrap();
        assert_eq!(
            since.blocks.iter().map(|m| m.slot).collect::<Vec<_>>(),
            expected
        );
        assert_eq!(since.cursor, Some(1_000_000_000_000));
    }

    #[test]
    fn missing_ledger_is_empty() {
        let dir = TempDir::new().unwrap();
        let reader = LedgerReader::new(dir.path().join("nope"));
        assert_eq!(reader.scan().unwrap(), Vec::<u64>::new());
        assert_eq!(
            reader.since(Some(3), 10).unwrap(),
            Since {
                blocks: vec![],
                cursor: Some(3)
            }
        );
        assert!(matches!(reader.read_meta(1), Err(LedgerError::NotFound(1))));
        assert!(matches!(
            reader.read_lane(1, 0),
            Err(LedgerError::NotFound(1))
        ));
        assert!(matches!(
            reader.read_proof(1),
            Err(LedgerError::NotFound(1))
        ));
    }

    #[test]
    fn open_removes_only_temp_dirs() {
        let dir = TempDir::new().unwrap();
        let blocks = dir.path().join(BLOCKS_DIR);
        fs::create_dir_all(blocks.join(temp_dir_name(9))).unwrap();
        fs::write(blocks.join(temp_dir_name(9)).join(META_FILE), b"half").unwrap();
        fs::create_dir(blocks.join(".keep")).unwrap();
        fs::create_dir(blocks.join(".foo.tmp")).unwrap();

        let writer = LedgerWriter::open(dir.path()).unwrap();
        assert!(!blocks.join(temp_dir_name(9)).exists());
        assert!(blocks.join(".keep").exists());
        assert!(blocks.join(".foo.tmp").exists());

        // a stale temp dir for the slot being written is replaced.
        fs::create_dir(blocks.join(temp_dir_name(9))).unwrap();
        fs::write(blocks.join(temp_dir_name(9)).join("junk"), b"x").unwrap();
        writer.write(&block(9)).unwrap();
        assert!(!blocks.join(temp_dir_name(9)).exists());
        assert!(!list_files(&blocks.join(block_dir_name(9))).contains(&"junk".into()));
    }

    fn list_files(dir: &Path) -> Vec<String> {
        fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect()
    }

    #[test]
    fn corrupt_entries_surface_errors() {
        let (_dir, writer, reader) = setup();
        for slot in 1..=6 {
            writer.write(&block(slot)).unwrap();
        }
        let d = |slot| reader.block_dir(slot);

        // positive lane file missing.
        fs::remove_file(d(1).join(lane_file_name(0))).unwrap();
        assert!(matches!(
            reader.read_lane(1, 0),
            Err(LedgerError::MissingLane { slot: 1, index: 0 })
        ));
        assert_eq!(reader.read_lane(1, 1).unwrap(), None);

        // truncated lane file.
        let lane = d(2).join(lane_file_name(0));
        let data = fs::read(&lane).unwrap();
        fs::write(&lane, &data[..data.len() - 1]).unwrap();
        assert!(matches!(
            reader.read_lane(2, 0),
            Err(LedgerError::Corrupt { .. })
        ));

        // garbage meta.
        fs::write(d(3).join(META_FILE), b"garbage").unwrap();
        assert!(matches!(
            reader.read_meta(3),
            Err(LedgerError::Corrupt { .. })
        ));

        // meta for another slot.
        fs::copy(d(5).join(META_FILE), d(4).join(META_FILE)).unwrap();
        let err = reader.read_meta(4).unwrap_err();
        assert!(err.to_string().contains("holds slot 5"), "{err}");

        // missing meta and proof.
        fs::remove_file(d(6).join(META_FILE)).unwrap();
        fs::remove_file(d(6).join(PROOF_FILE)).unwrap();
        assert!(matches!(
            reader.read_meta(6),
            Err(LedgerError::Corrupt { .. })
        ));
        assert!(matches!(
            reader.read_proof(6),
            Err(LedgerError::Corrupt { .. })
        ));

        // since skips the unreadable metas but still advances past them.
        let since = reader.since(None, usize::MAX).unwrap();
        assert_eq!(
            since.blocks.iter().map(|m| m.slot).collect::<Vec<_>>(),
            [1, 2, 5]
        );
        assert_eq!(since.cursor, Some(6));
        assert!(matches!(reader.read_meta(7), Err(LedgerError::NotFound(7))));
    }

    #[test]
    fn since_pages_with_cursor() {
        let (_dir, writer, reader) = setup();
        for slot in [10, 11, 13, 17, 18] {
            writer.write(&block(slot)).unwrap();
        }
        let slots = |s: &Since| s.blocks.iter().map(|m| m.slot).collect::<Vec<_>>();

        let page = reader.since(None, 2).unwrap();
        assert_eq!((slots(&page), page.cursor), (vec![10, 11], Some(11)));
        let page = reader.since(page.cursor, 2).unwrap();
        assert_eq!((slots(&page), page.cursor), (vec![13, 17], Some(17)));
        let page = reader.since(page.cursor, 2).unwrap();
        assert_eq!((slots(&page), page.cursor), (vec![18], Some(18)));
        let page = reader.since(page.cursor, 2).unwrap();
        assert_eq!((slots(&page), page.cursor), (vec![], Some(18)));

        let page = reader.since(Some(12), usize::MAX).unwrap();
        assert_eq!(slots(&page), [13, 17, 18]);
        let page = reader.since(Some(0), 0).unwrap();
        assert_eq!((slots(&page), page.cursor), (vec![], Some(0)));
    }

    fn slots_of(blocks: &[BlockMeta]) -> Vec<u64> {
        blocks.iter().map(|m| m.slot).collect()
    }

    #[test]
    fn follower_yields_late_lower_slot() {
        let (_dir, writer, reader) = setup();
        let mut follower = reader.follow();
        writer.write(&block(10)).unwrap();
        writer.write(&block(12)).unwrap();
        assert_eq!(slots_of(&follower.poll(usize::MAX).unwrap()), [10, 12]);
        let cursor = reader.since(None, usize::MAX).unwrap().cursor;

        writer.write(&block(11)).unwrap();
        assert_eq!(slots_of(&follower.poll(usize::MAX).unwrap()), [11]);
        assert_eq!(follower.poll(usize::MAX).unwrap(), vec![]);
        // the slot cursor of `since` cannot see it.
        assert_eq!(reader.since(cursor, usize::MAX).unwrap().blocks, vec![]);
    }

    #[test]
    fn follower_pages_and_starts_from_now() {
        let (_dir, writer, reader) = setup();
        for slot in [3, 1] {
            writer.write(&block(slot)).unwrap();
        }
        let mut from_now = reader.follow_from_now().unwrap();
        let mut all = reader.follow();
        for slot in [9, 5, 7] {
            writer.write(&block(slot)).unwrap();
        }
        assert_eq!(slots_of(&all.poll(2).unwrap()), [1, 3]);
        writer.write(&block(2)).unwrap();
        assert_eq!(slots_of(&all.poll(2).unwrap()), [2, 5]);
        assert_eq!(slots_of(&all.poll(2).unwrap()), [7, 9]);
        assert_eq!(all.poll(2).unwrap(), vec![]);
        assert_eq!(all.poll(0).unwrap(), vec![]);

        assert_eq!(slots_of(&from_now.poll(usize::MAX).unwrap()), [2, 5, 7, 9]);
        assert_eq!(from_now.poll(usize::MAX).unwrap(), vec![]);
    }

    #[test]
    fn follower_forgets_pruned_and_skips_corrupt_once() {
        let (_dir, writer, reader) = setup();
        for slot in 1..=4 {
            writer.write(&block(slot)).unwrap();
        }
        fs::write(reader.block_dir(3).join(META_FILE), b"garbage").unwrap();
        let mut follower = reader.follow();
        assert_eq!(slots_of(&follower.poll(usize::MAX).unwrap()), [1, 2, 4]);
        assert_eq!(follower.seen, BTreeSet::from([1, 2, 3, 4]));
        assert_eq!(follower.poll(usize::MAX).unwrap(), vec![]);

        fs::remove_dir_all(reader.block_dir(1)).unwrap();
        fs::remove_dir_all(reader.block_dir(3)).unwrap();
        assert_eq!(follower.poll(usize::MAX).unwrap(), vec![]);
        assert_eq!(follower.seen, BTreeSet::from([2, 4]));
    }

    #[test]
    fn follower_sees_each_out_of_order_write_once() {
        let (_dir, writer, reader) = setup();
        let mut follower = reader.follow();
        let done = Arc::new(AtomicBool::new(false));
        let poller = {
            let done = done.clone();
            thread::spawn(move || {
                let mut yielded = Vec::new();
                loop {
                    let finished = done.load(Ordering::Acquire);
                    let got = slots_of(&follower.poll(7).unwrap());
                    if finished && got.is_empty() {
                        return yielded;
                    }
                    yielded.extend(got);
                }
            })
        };
        // each slot lands a few writes after its successors, as out-of-order decodes do.
        let mut order: Vec<u64> = (0..200).collect();
        for chunk in order.chunks_mut(5) {
            chunk.reverse();
        }
        for &slot in &order {
            writer.write(&block(slot)).unwrap();
        }
        done.store(true, Ordering::Release);
        let mut yielded = poller.join().unwrap();
        yielded.sort_unstable();
        assert_eq!(yielded, (0..200).collect::<Vec<_>>());
    }

    #[test]
    fn write_io_error_is_reported() {
        let dir = TempDir::new().unwrap();
        let writer = LedgerWriter::open(dir.path()).unwrap();
        // the blocks dir replaced by a file makes every write fail.
        let blocks = writer.blocks_dir().to_path_buf();
        fs::remove_dir(&blocks).unwrap();
        fs::write(&blocks, b"").unwrap();
        assert!(matches!(
            writer.write(&block(1)),
            Err(LedgerError::Io { .. })
        ));
    }

    #[test]
    fn no_partial_block_visible_to_concurrent_reader() {
        let (_dir, writer, reader) = setup();
        let done = Arc::new(AtomicBool::new(false));
        let checker = {
            let done = done.clone();
            thread::spawn(move || {
                let mut checked = 0usize;
                let mut cursor = None;
                while !done.load(Ordering::Acquire) {
                    for slot in reader.scan().unwrap() {
                        let meta = reader.read_meta(slot).unwrap();
                        for index in 0..meta.num_lanes {
                            let lane = reader.read_lane_for(&meta, index).unwrap();
                            assert_eq!(lane.is_some(), meta.lanes[index as usize].is_positive());
                        }
                        reader.read_proof(slot).unwrap();
                        checked += 1;
                    }
                    let since = reader.since(cursor, usize::MAX).unwrap();
                    for (a, b) in since.blocks.iter().zip(since.blocks.iter().skip(1)) {
                        assert!(a.slot < b.slot);
                    }
                    cursor = since.cursor;
                }
                checked
            })
        };
        for slot in 0..300 {
            writer.write(&block(slot)).unwrap();
        }
        done.store(true, Ordering::Release);
        assert!(checker.join().unwrap() > 0);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn written_lanes_decode(
            batches in proptest::collection::vec(
                proptest::option::of(proptest::collection::vec(any::<u64>(), 0..4)),
                0..6,
            ),
        ) {
            let (_dir, writer, reader) = setup();
            let lanes = batches
                .iter()
                .map(|b| match b {
                    Some(nonces) => committed(
                        9,
                        encode_batch(&nonces.iter().map(|&n| tx(n)).collect::<Vec<_>>()),
                    ),
                    None => negative(),
                })
                .collect();
            let new = NewBlock { lanes, ..block(42) };
            let meta = writer.write(&new).unwrap();
            prop_assert_eq!(&reader.read_meta(42).unwrap(), &meta);
            for (index, b) in (0..).zip(&batches) {
                let lane = reader.read_lane(42, index).unwrap();
                let txs = lane.map(|p| decode_batch(&p).unwrap());
                let expected = b.as_ref().map(|n| n.iter().map(|&n| tx(n)).collect::<Vec<_>>());
                prop_assert_eq!(meta.lanes[index as usize].tx_count as usize,
                    expected.as_ref().map_or(0, Vec::len));
                prop_assert_eq!(txs, expected);
            }
        }
    }
}
