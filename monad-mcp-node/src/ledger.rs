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

//! Finalized slots into the on-disk ledger, on a thread of their own so
//! the fsyncs never stall the node's event loop.

use std::{
    path::Path,
    sync::{Arc, mpsc},
    thread,
};

use monad_mcp_chorus::ledger::{
    CommittedLane, FinalizationPath as LedgerPath, LedgerError, LedgerWriter, NewBlock, NewLane,
};

use crate::{
    chorus::{
        slot::chorus::FinalizationPath,
        types::{ProposerSchedule, ProposerSet},
    },
    finalization::FinalizedSlot,
    logging,
};

// the block the writer stores for a finalized slot; `proposers` is the
// slot's schedule, None if it could not be queried
pub fn new_block(finalized: &FinalizedSlot, proposers: Option<&ProposerSet>) -> NewBlock {
    let roots = finalization_roots(finalized);
    let lanes = roots
        .into_iter()
        .zip(finalized.proposals.clone())
        .enumerate()
        .map(|(index, (root, payload))| NewLane {
            proposer: proposers.and_then(|set| set.proposer(index)).map(u64::from),
            committed: root
                .zip(payload)
                .map(|(root, payload)| CommittedLane { root, payload }),
        })
        .collect();
    NewBlock {
        slot: finalized.slot.0,
        deadline_ns: finalized.deadline.map(|deadline| deadline.as_nanos()),
        finalized_at_ns: finalized.at.as_nanos(),
        path: match finalized.finalization.path() {
            FinalizationPath::Fast => LedgerPath::Fast,
            FinalizationPath::Fallback => LedgerPath::Fallback,
        },
        lanes,
        proof: alloy_rlp::encode(finalized.finalization.certificate_message()).into(),
    }
}

fn finalization_roots(finalized: &FinalizedSlot) -> Vec<Option<[u8; 20]>> {
    finalized
        .finalization
        .roots()
        .into_iter()
        .map(|root| root.map(|root| root.0.0))
        .collect()
}

// the block line of the log: one char per proposal index, + committed or
// - not, green on the fast path and yellow on the fallback path
pub fn log_finalized(finalized: &FinalizedSlot) {
    let mut shape = String::new();
    let roots = finalized.finalization.roots();
    for ((j, root), proposal) in roots.into_indexed_iter().zip(finalized.proposals.as_ref()) {
        shape.push(if root.is_some() { '+' } else { '-' });
        let Some(message) = proposal else {
            continue;
        };
        tracing::debug!(
            slot = finalized.slot.0,
            j,
            ?root,
            len = message.len(),
            "committed"
        );
    }
    let color = match finalized.finalization.path() {
        FinalizationPath::Fast => logging::GREEN,
        FinalizationPath::Fallback => logging::YELLOW,
    };
    let block = logging::paint(color, &shape);
    tracing::info!(
        slot = finalized.slot.0,
        block = %block,
        at = finalized.at.as_nanos(),
        "finalized"
    );
}

// hands blocks to the writer thread, which exits once this is dropped
pub struct LedgerSink {
    blocks: mpsc::SyncSender<NewBlock>,
    dropped: u64,
}

impl LedgerSink {
    // blocks queued for the writer; past this a slow disk drops blocks
    // rather than growing the queue until the node runs out of memory
    pub const QUEUE_BLOCKS: usize = 256;

    // opens the ledger here, so a bad directory fails node startup
    pub fn spawn(ledger_dir: impl AsRef<Path>) -> Result<Self, LedgerError> {
        Self::spawn_with(LedgerWriter::open(ledger_dir)?, Self::QUEUE_BLOCKS, write)
    }

    fn spawn_with(
        writer: LedgerWriter,
        queue_blocks: usize,
        write: impl Fn(&LedgerWriter, &NewBlock) + Send + 'static,
    ) -> Result<Self, LedgerError> {
        let blocks_dir = writer.blocks_dir().to_path_buf();
        tracing::info!(dir = %blocks_dir.display(), "ledger opened");
        let (blocks, received) = mpsc::sync_channel::<NewBlock>(queue_blocks);
        let span = tracing::Span::current();
        thread::Builder::new()
            .name("ledger-writer".into())
            .spawn(move || {
                let _entered = span.enter();
                for block in received {
                    write(&writer, &block);
                }
            })
            .map_err(|source| LedgerError::Io {
                op: "spawn writer for",
                path: blocks_dir,
                source,
            })?;
        Ok(Self { blocks, dropped: 0 })
    }

    // never blocks the caller on disk; a block the writer has no room for
    // leaves a gap in the ledger
    pub fn write(&mut self, block: NewBlock) {
        let slot = block.slot;
        let reason = match self.blocks.try_send(block) {
            Ok(()) => return,
            Err(mpsc::TrySendError::Full(_)) => "ledger writer is behind",
            Err(mpsc::TrySendError::Disconnected(_)) => "ledger writer is gone",
        };
        self.dropped += 1;
        tracing::error!(slot, dropped = self.dropped, "{reason}, block dropped");
    }

    pub fn dropped(&self) -> u64 {
        self.dropped
    }
}

fn write(writer: &LedgerWriter, block: &NewBlock) {
    match writer.write(block) {
        Ok(meta) => tracing::debug!(
            slot = meta.slot,
            txs = meta.tx_count(),
            "ledger block written"
        ),
        // a restart over a kept ledger finalizes the stored slots again
        Err(LedgerError::Exists(slot)) => tracing::info!(slot, "ledger block already stored"),
        Err(error) => tracing::error!(slot = block.slot, %error, "ledger write failed"),
    }
}

// what the node does with each finalized slot: log it, then store it
pub struct Recorder {
    schedule: Arc<dyn ProposerSchedule + Send + Sync>,
    sink: LedgerSink,
}

impl Recorder {
    pub fn new(schedule: Arc<dyn ProposerSchedule + Send + Sync>, sink: LedgerSink) -> Self {
        Self { schedule, sink }
    }

    pub fn record(&mut self, finalized: FinalizedSlot) {
        log_finalized(&finalized);
        let proposers = match self.schedule.proposers_at(finalized.slot) {
            Ok(set) => Some(set),
            Err(error) => {
                tracing::warn!(slot = finalized.slot.0, %error, "no proposers for the ledger");
                None
            }
        };
        self.sink.write(new_block(&finalized, proposers.as_ref()));
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Mutex, time::Duration};

    use monad_mcp_chorus::ledger::LedgerReader;

    use super::*;

    fn block(slot: u64) -> NewBlock {
        NewBlock {
            slot,
            deadline_ns: None,
            finalized_at_ns: 0,
            path: LedgerPath::Fast,
            lanes: vec![NewLane {
                proposer: None,
                committed: None,
            }],
            proof: Default::default(),
        }
    }

    #[test]
    fn a_stalled_writer_drops_blocks_instead_of_queueing_them() {
        let dir = tempfile::tempdir().unwrap();
        let (started, on_start) = mpsc::channel();
        let (release, gate) = mpsc::channel::<()>();
        let gate = Mutex::new(gate);
        let mut sink = LedgerSink::spawn_with(
            LedgerWriter::open(dir.path()).unwrap(),
            2,
            move |writer, block| {
                started.send(block.slot).unwrap();
                gate.lock().unwrap().recv().unwrap();
                write(writer, block);
            },
        )
        .unwrap();

        sink.write(block(1));
        assert_eq!(on_start.recv_timeout(Duration::from_secs(5)), Ok(1));
        // the writer holds slot 1, the queue takes two more
        for slot in 2..=5 {
            sink.write(block(slot));
        }
        assert_eq!(sink.dropped(), 2);

        for _ in 0..3 {
            release.send(()).unwrap();
        }
        drop(sink);
        for slot in [2, 3] {
            assert_eq!(on_start.recv_timeout(Duration::from_secs(5)), Ok(slot));
        }
        // the writer exits once the sink is gone and its queue drained
        assert_eq!(
            on_start.recv_timeout(Duration::from_secs(5)),
            Err(mpsc::RecvTimeoutError::Disconnected)
        );
        assert_eq!(LedgerReader::new(dir.path()).scan().unwrap(), vec![1, 2, 3]);
    }
}
