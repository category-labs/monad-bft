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

use monad_mcp_chorus::ledger::{
    BlockMeta, Hash, LedgerError, LedgerFollower, LedgerReader, decode_batch,
};
use tracing::warn;

use crate::pending::Commit;

// blocks read per poll; a backlog drains over successive polls
pub const POLL_BLOCKS: usize = 1024;
// polls a lane that failed with an io error is reread on before it is given up
pub const LANE_RETRIES: u32 = 50;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Sighting {
    pub commits: Vec<(Hash, Commit)>,
    pub blocks: usize,
    pub highest_slot: Option<u64>,
}

struct Retry {
    meta: BlockMeta,
    lane: u32,
    left: u32,
}

// new ledger blocks, each seen once, as the commits they carry
pub struct LedgerWatch {
    follower: LedgerFollower,
    // lanes of already seen blocks, reread until they can be decoded
    retries: Vec<Retry>,
}

impl LedgerWatch {
    // blocks already on disk cannot hold txs submitted from now on
    pub fn from_now(reader: &LedgerReader) -> Result<Self, LedgerError> {
        Ok(Self::new(reader.follow_from_now()?))
    }

    pub fn from_start(reader: &LedgerReader) -> Self {
        Self::new(reader.follow())
    }

    fn new(follower: LedgerFollower) -> Self {
        Self {
            follower,
            retries: Vec::new(),
        }
    }

    pub fn poll(&mut self) -> Result<Sighting, LedgerError> {
        let blocks = self.follower.poll(POLL_BLOCKS)?;
        let reader = self.follower.reader();
        let mut sighting = Sighting {
            blocks: blocks.len(),
            highest_slot: blocks.iter().map(|meta| meta.slot).max(),
            ..Sighting::default()
        };
        let mut retries = Vec::new();
        for retry in std::mem::take(&mut self.retries) {
            if let Err(error) = lane_commits(reader, &retry.meta, retry.lane, &mut sighting.commits)
            {
                if retry.left > 1 {
                    retries.push(Retry {
                        left: retry.left - 1,
                        ..retry
                    });
                } else {
                    let (slot, lane) = (retry.meta.slot, retry.lane);
                    warn!(slot, lane, %error, "giving up on an unreadable ledger lane");
                }
            }
        }
        for meta in &blocks {
            for lane in meta.lanes.iter().filter(|lane| lane.tx_count > 0) {
                if let Err(error) = lane_commits(reader, meta, lane.index, &mut sighting.commits) {
                    warn!(slot = meta.slot, lane = lane.index, %error, "unreadable ledger lane; retrying");
                    retries.push(Retry {
                        meta: meta.clone(),
                        lane: lane.index,
                        left: LANE_RETRIES,
                    });
                }
            }
        }
        self.retries = retries;
        Ok(sighting)
    }
}

// only an io error is returned, as the one a later read can get past
fn lane_commits(
    reader: &LedgerReader,
    meta: &BlockMeta,
    index: u32,
    out: &mut Vec<(Hash, Commit)>,
) -> Result<(), LedgerError> {
    let payload = match reader.read_lane_for(meta, index) {
        Ok(Some(payload)) => payload,
        Ok(None) => return Ok(()),
        Err(error @ LedgerError::Io { .. }) => return Err(error),
        Err(error) => {
            warn!(slot = meta.slot, lane = index, %error, "unreadable ledger lane");
            return Ok(());
        }
    };
    let txs = match decode_batch(&payload) {
        Ok(txs) => txs,
        Err(error) => {
            warn!(slot = meta.slot, lane = index, %error, "undecodable ledger lane");
            return Ok(());
        }
    };
    let commit = Commit {
        slot: meta.slot,
        lane: index,
        path: meta.path,
        finalized_at_ns: meta.finalized_at_ns,
    };
    out.extend(txs.iter().map(|tx| (tx.hash(), commit)));
    Ok(())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use monad_mcp_chorus::ledger::{
        CommittedLane, FinalizationPath, LedgerWriter, NewBlock, NewLane, Tx, encode_batch,
        lane_file_name,
    };

    use super::*;

    fn tx(nonce: u64) -> Tx {
        Tx {
            sender: [2; 20],
            nonce,
            payload: Bytes::from(format!("watch {nonce}")),
            sent_at_ns: 0,             // demo(tx-timeline)
            rpc_received_at_ns: 0,     // demo(tx-timeline)
            mempool_admitted_at_ns: 0, // demo(tx-timeline)
        }
    }

    fn lane(payload: Bytes) -> NewLane {
        NewLane {
            proposer: Some(0),
            committed: Some(CommittedLane {
                root: [7; 20],
                payload,
            }),
        }
    }

    fn block(slot: u64, lanes: Vec<NewLane>) -> NewBlock {
        NewBlock {
            slot,
            deadline_ns: Some(1),
            finalized_at_ns: 2,
            path: FinalizationPath::Fallback,
            fast_block_at_ns: None,     // demo(tx-timeline)
            lane_decoded_at_ns: vec![], // demo(tx-timeline)
            lanes,
            proof: Bytes::from_static(b"proof"),
        }
    }

    fn negative() -> NewLane {
        NewLane {
            proposer: None,
            committed: None,
        }
    }

    #[test]
    fn commits_carry_slot_lane_and_path_and_garbage_lanes_are_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let writer = LedgerWriter::open(dir.path()).unwrap();
        let reader = LedgerReader::new(dir.path());
        writer
            .write(&block(1, vec![lane(encode_batch(1, &[tx(0)]))])) // demo(tx-timeline)
            .unwrap();
        let mut watch = LedgerWatch::from_now(&reader).unwrap();
        assert_eq!(watch.poll().unwrap(), Sighting::default());

        writer
            .write(&block(
                5,
                vec![
                    negative(),
                    lane(Bytes::from_static(b"\xffgarbage")),
                    lane(encode_batch(1, &[])), // demo(tx-timeline)
                    lane(encode_batch(1, &[tx(1), tx(2)])), // demo(tx-timeline)
                ],
            ))
            .unwrap();
        let sighting = watch.poll().unwrap();
        let commit = Commit {
            slot: 5,
            lane: 3,
            path: FinalizationPath::Fallback,
            finalized_at_ns: 2,
        };
        assert_eq!(
            sighting,
            Sighting {
                commits: vec![(tx(1).hash(), commit), (tx(2).hash(), commit)],
                blocks: 1,
                highest_slot: Some(5),
            }
        );
        assert_eq!(watch.poll().unwrap().blocks, 0);
    }

    #[test]
    fn a_block_written_below_the_highest_is_still_seen_once() {
        let dir = tempfile::tempdir().unwrap();
        let writer = LedgerWriter::open(dir.path()).unwrap();
        let mut watch = LedgerWatch::from_start(&LedgerReader::new(dir.path()));
        writer
            .write(&block(9, vec![lane(encode_batch(1, &[tx(9)]))])) // demo(tx-timeline)
            .unwrap();
        assert_eq!(watch.poll().unwrap().commits.len(), 1);
        writer
            .write(&block(4, vec![lane(encode_batch(1, &[tx(4)]))])) // demo(tx-timeline)
            .unwrap();
        let sighting = watch.poll().unwrap();
        assert_eq!(
            sighting.commits,
            vec![(
                tx(4).hash(),
                Commit {
                    slot: 4,
                    lane: 0,
                    path: FinalizationPath::Fallback,
                    finalized_at_ns: 2
                }
            )]
        );
        assert!(watch.poll().unwrap().commits.is_empty());
    }

    #[test]
    fn a_lane_hit_by_an_io_error_is_reread_on_later_polls() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let writer = LedgerWriter::open(dir.path()).unwrap();
        let reader = LedgerReader::new(dir.path());
        let mut watch = LedgerWatch::from_start(&reader);
        writer
            .write(&block(3, vec![lane(encode_batch(1, &[tx(3)]))])) // demo(tx-timeline)
            .unwrap();
        let file = reader.block_dir(3).join(lane_file_name(0));
        let set_mode =
            |mode| std::fs::set_permissions(&file, std::fs::Permissions::from_mode(mode)).unwrap();
        set_mode(0o000);
        if std::fs::read(&file).is_ok() {
            // root reads through the permission bits
            return;
        }
        let sighting = watch.poll().unwrap();
        assert_eq!((sighting.blocks, sighting.commits.len()), (1, 0));
        assert!(watch.poll().unwrap().commits.is_empty());

        set_mode(0o644);
        let sighting = watch.poll().unwrap();
        assert_eq!((sighting.blocks, sighting.commits.len()), (0, 1));
        assert_eq!(sighting.commits[0].0, tx(3).hash());
        assert!(watch.poll().unwrap().commits.is_empty());
    }

    #[test]
    fn an_unreadable_lane_is_given_up_after_its_retries() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let writer = LedgerWriter::open(dir.path()).unwrap();
        let reader = LedgerReader::new(dir.path());
        let mut watch = LedgerWatch::from_start(&reader);
        writer
            .write(&block(3, vec![lane(encode_batch(1, &[tx(3)]))])) // demo(tx-timeline)
            .unwrap();
        let file = reader.block_dir(3).join(lane_file_name(0));
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o000)).unwrap();
        if std::fs::read(&file).is_ok() {
            return;
        }
        for _ in 0..=LANE_RETRIES {
            assert!(watch.poll().unwrap().commits.is_empty());
        }
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(watch.poll().unwrap().commits.is_empty());
    }

    #[test]
    fn a_missing_ledger_dir_is_empty_until_it_appears() {
        let dir = tempfile::tempdir().unwrap();
        let ledger = dir.path().join("later");
        let mut watch = LedgerWatch::from_now(&LedgerReader::new(&ledger)).unwrap();
        assert_eq!(watch.poll().unwrap().blocks, 0);
        let writer = LedgerWriter::open(&ledger).unwrap();
        writer
            .write(&block(1, vec![lane(encode_batch(1, &[tx(1)]))])) // demo(tx-timeline)
            .unwrap();
        assert_eq!(watch.poll().unwrap().commits.len(), 1);
    }
}
