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

//! Misbehavior evidence (§11) as `<dir>/<slot:012>-<kind>-<culprit>.rlp`,
//! outside `blocks/` so ledger readers never see it. Only signed conflicts
//! prove misbehavior; every other kind is filed as `suspect-<kind>`.

use std::{
    borrow::Cow,
    collections::BTreeMap,
    io,
    path::{Path, PathBuf},
    sync::mpsc,
    thread,
};

use monad_pq_chorus::Evidence;

use crate::chorus::types::{NodeId, Slot};

const SUSPECT: &str = "suspect-";
// at most this many vote dumps per window of slots
const VOTE_DUMPS_PER_WINDOW: usize = 10;
const VOTE_DUMP_WINDOW_SLOTS: u64 = 100;

// two signatures of one validator over conflicting statements prove
// misbehavior by themselves; the rest only do for a timely slot
// (lem:qv:acc-f, lem:qv:acc-half). Header signatures are still DA's
// 65-byte stub (D11), so EquivocatingHeaders proves nothing offline yet.
pub fn is_self_proving(evidence: &Evidence) -> bool {
    matches!(
        evidence,
        Evidence::EquivocatingHeaders { .. }
            | Evidence::ConflictingVotes { .. }
            | Evidence::ConflictingPathVotes { .. }
    )
}

pub fn file_kind(evidence: &Evidence) -> Cow<'static, str> {
    let kind = evidence.kind();
    if is_self_proving(evidence) || kind.starts_with(SUSPECT) {
        Cow::Borrowed(kind)
    } else {
        Cow::Owned(format!("{SUSPECT}{kind}"))
    }
}

// hands evidence to the writer thread, which exits once this is dropped
pub struct EvidenceSink {
    records: mpsc::SyncSender<(Slot, Evidence)>,
    writer: thread::JoinHandle<()>,
    dropped: u64,
}

impl EvidenceSink {
    pub const QUEUE_RECORDS: usize = 256;

    // opens the directory here, so a bad one fails node startup
    pub fn spawn(dir: impl AsRef<Path>, self_id: NodeId, retention_slots: u64) -> io::Result<Self> {
        let mut store = EvidenceStore::open(dir.as_ref().to_path_buf(), self_id, retention_slots)?;
        let (records, received) = mpsc::sync_channel::<(Slot, Evidence)>(Self::QUEUE_RECORDS);
        let span = tracing::Span::current();
        let writer = thread::Builder::new()
            .name("evidence-writer".into())
            .spawn(move || {
                let _entered = span.enter();
                for (slot, evidence) in received {
                    store.store(slot, &evidence);
                }
            })?;
        Ok(Self {
            records,
            writer,
            dropped: 0,
        })
    }

    // never blocks the caller on disk
    pub fn record(&mut self, slot: Slot, evidence: Evidence) {
        let reason = match self.records.try_send((slot, evidence)) {
            Ok(()) => return,
            Err(mpsc::TrySendError::Full(_)) => "evidence writer is behind",
            Err(mpsc::TrySendError::Disconnected(_)) => "evidence writer is gone",
        };
        self.dropped += 1;
        tracing::error!(
            slot = slot.0,
            dropped = self.dropped,
            "{reason}, evidence dropped"
        );
    }

    // waits until everything queued is stored
    pub fn close(self) {
        let Self {
            records, writer, ..
        } = self;
        drop(records);
        if writer.join().is_err() {
            tracing::error!("evidence writer panicked");
        }
    }
}

// the files of the evidence directory; lives on the writer thread
struct EvidenceStore {
    dir: PathBuf,
    // a vote dump names no culprit; it is filed under this node
    self_id: NodeId,
    retention_slots: u64,
    files: BTreeMap<Slot, Vec<PathBuf>>,
    newest: Slot,
    dump_window: u64,
    dumps_in_window: usize,
    dumps_skipped: u64,
}

impl EvidenceStore {
    // indexes the files a previous run left, so they are pruned too
    fn open(dir: PathBuf, self_id: NodeId, retention_slots: u64) -> io::Result<Self> {
        std::fs::create_dir_all(&dir)?;
        let mut files = BTreeMap::<Slot, Vec<PathBuf>>::new();
        for entry in std::fs::read_dir(&dir)? {
            let path = entry?.path();
            if let Some(slot) = slot_of(&path) {
                files.entry(slot).or_default().push(path);
            }
        }
        Ok(Self {
            dir,
            self_id,
            retention_slots,
            files,
            newest: Slot::MIN,
            dump_window: 0,
            dumps_in_window: 0,
            dumps_skipped: 0,
        })
    }

    fn path_of(&self, slot: Slot, evidence: &Evidence) -> PathBuf {
        let culprit = u64::from(evidence.culprit().unwrap_or(self.self_id));
        let name = format!("{:012}-{}-{culprit}.rlp", slot.0, file_kind(evidence));
        self.dir.join(name)
    }

    // a failure is only logged
    fn store(&mut self, slot: Slot, evidence: &Evidence) {
        if matches!(evidence, Evidence::VoteDump { .. }) && !self.admit_vote_dump(slot) {
            return;
        }
        log(slot, evidence);
        let path = self.path_of(slot, evidence);
        match std::fs::write(&path, alloy_rlp::encode(evidence)) {
            Ok(()) => self.files.entry(slot).or_default().push(path),
            Err(error) => tracing::error!(%error, path = %path.display(), "evidence write failed"),
        }
        self.prune(slot);
    }

    fn admit_vote_dump(&mut self, slot: Slot) -> bool {
        let window = slot.0 / VOTE_DUMP_WINDOW_SLOTS;
        if window > self.dump_window {
            self.dump_window = window;
            self.dumps_in_window = 0;
        }
        if self.dumps_in_window >= VOTE_DUMPS_PER_WINDOW {
            self.dumps_skipped += 1;
            tracing::debug!(
                slot = slot.0,
                skipped = self.dumps_skipped,
                "vote dump rate-limited"
            );
            return false;
        }
        self.dumps_in_window += 1;
        true
    }

    // drops every file more than retention_slots behind the newest record
    fn prune(&mut self, slot: Slot) {
        self.newest = self.newest.max(slot);
        let Some(oldest_kept) = self.newest.checked_sub(self.retention_slots) else {
            return;
        };
        let kept = self.files.split_off(&oldest_kept);
        let expired = std::mem::replace(&mut self.files, kept);
        for path in expired.into_values().flatten() {
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => {
                    tracing::warn!(%error, path = %path.display(), "evidence prune failed")
                }
            }
        }
    }
}

fn log(slot: Slot, evidence: &Evidence) {
    let kind = evidence.kind();
    let culprit = evidence.culprit().map(u64::from);
    match evidence {
        Evidence::VoteDump { .. } => tracing::debug!(slot = slot.0, "votes dumped"),
        _ if is_self_proving(evidence) => {
            tracing::warn!(slot = slot.0, kind, ?culprit, "misbehavior proven")
        }
        _ => tracing::info!(slot = slot.0, kind, ?culprit, "misbehavior suspected"),
    }
}

// the slot a file name starts with, if it is an evidence file
fn slot_of(path: &Path) -> Option<Slot> {
    let name = path.file_name()?.to_str()?.strip_suffix(".rlp")?;
    let (slot, _) = name.split_once('-')?;
    slot.parse().ok().map(Slot)
}

#[cfg(test)]
mod tests {
    use monad_pq_chorus::{
        message::VoteBody,
        sig::{Signed, StubPq},
        types::{Entry, ProposalMap},
    };

    use super::*;

    fn conflicting_votes(slot: u64) -> Evidence {
        let signer = NodeId::dummy(3);
        let vote = |entry| {
            let body = VoteBody {
                entries: ProposalMap::new(1, |_| entry),
            };
            Signed::sign(Slot(slot), signer, body, &StubPq::default().signer(signer))
        };
        Evidence::ConflictingVotes {
            signer,
            a: vote(Entry::Bot),
            b: vote(Entry::Bot),
        }
    }

    fn vote_dump() -> Evidence {
        Evidence::VoteDump {
            votes: Vec::new(),
            path_votes: Vec::new(),
        }
    }

    fn store(dir: &Path, retention_slots: u64) -> EvidenceStore {
        EvidenceStore::open(dir.join("evidence"), NodeId::dummy(0), retention_slots).unwrap()
    }

    fn names(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir.join("evidence"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        names
    }

    #[test]
    fn evidence_is_written_as_decodable_rlp() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = store(dir.path(), 1_000);
        let evidence = conflicting_votes(4);
        store.store(Slot(4), &evidence);
        let path = store.path_of(Slot(4), &evidence);
        assert!(path.ends_with("000000000004-conflicting-votes-3.rlp"));
        let read: Evidence = alloy_rlp::decode_exact(std::fs::read(path).unwrap()).unwrap();
        assert_eq!(read, evidence);
    }

    #[test]
    fn only_signed_conflicts_are_filed_as_proof() {
        let fallback_quorum = Evidence::FallbackQuorum {
            j: 2,
            proposer: NodeId::dummy(5),
            path_votes: Vec::new(),
        };
        assert!(is_self_proving(&conflicting_votes(1)));
        assert!(!is_self_proving(&fallback_quorum));
        assert_eq!(file_kind(&conflicting_votes(1)), "conflicting-votes");
        assert_eq!(file_kind(&fallback_quorum), "suspect-fallback-quorum");
        assert_eq!(file_kind(&vote_dump()), "suspect-votes");

        let dir = tempfile::tempdir().unwrap();
        let store = store(dir.path(), 1_000);
        assert!(
            store
                .path_of(Slot(7), &vote_dump())
                .ends_with("000000000007-suspect-votes-0.rlp")
        );
    }

    #[test]
    fn evidence_past_retention_is_pruned_with_a_previous_runs_files() {
        let dir = tempfile::tempdir().unwrap();
        let mut first = store(dir.path(), 10);
        first.store(Slot(3), &conflicting_votes(3));
        drop(first);

        let mut store = store(dir.path(), 10);
        store.store(Slot(12), &conflicting_votes(12));
        assert_eq!(names(dir.path()).len(), 2);
        store.store(Slot(14), &conflicting_votes(14));
        assert_eq!(
            names(dir.path()),
            [
                "000000000012-conflicting-votes-3.rlp",
                "000000000014-conflicting-votes-3.rlp"
            ]
        );
    }

    #[test]
    fn vote_dumps_are_rate_limited_per_window() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = store(dir.path(), 1_000);
        for slot in 0..VOTE_DUMP_WINDOW_SLOTS {
            store.store(Slot(slot), &vote_dump());
        }
        assert_eq!(names(dir.path()).len(), VOTE_DUMPS_PER_WINDOW);
        // proofs are never rate-limited, and the next window dumps again
        store.store(Slot(50), &conflicting_votes(50));
        store.store(Slot(VOTE_DUMP_WINDOW_SLOTS), &vote_dump());
        assert_eq!(names(dir.path()).len(), VOTE_DUMPS_PER_WINDOW + 2);
    }

    #[test]
    fn the_sink_writes_on_its_own_thread() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink =
            EvidenceSink::spawn(dir.path().join("evidence"), NodeId::dummy(0), 1_000).unwrap();
        sink.record(Slot(4), conflicting_votes(4));
        sink.close();
        assert_eq!(names(dir.path()), ["000000000004-conflicting-votes-3.rlp"]);
    }
}
