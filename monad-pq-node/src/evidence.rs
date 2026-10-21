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
//! outside `blocks/` so ledger readers never see it.

use std::{
    io,
    path::{Path, PathBuf},
};

use monad_pq_chorus::Evidence;

use crate::chorus::types::{NodeId, Slot};

pub struct EvidenceSink {
    dir: PathBuf,
    // a vote dump names no culprit; it is filed under this node
    self_id: NodeId,
}

impl EvidenceSink {
    pub fn open(dir: impl AsRef<Path>, self_id: NodeId) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        Ok(Self { dir, self_id })
    }

    pub fn path_of(&self, slot: Slot, evidence: &Evidence) -> PathBuf {
        let culprit = u64::from(evidence.culprit().unwrap_or(self.self_id));
        let name = format!("{:012}-{}-{culprit}.rlp", slot.0, evidence.kind());
        self.dir.join(name)
    }

    // evidence is rare and small: written in place, a failure only logged
    pub fn record(&self, slot: Slot, evidence: &Evidence) {
        let path = self.path_of(slot, evidence);
        if let Err(error) = std::fs::write(&path, alloy_rlp::encode(evidence)) {
            tracing::error!(%error, path = %path.display(), "evidence write failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use monad_pq_chorus::{
        message::VoteBody,
        sig::{Signed, StubPq},
        types::{Entry, ProposalMap},
    };

    use super::*;

    #[test]
    fn evidence_is_written_as_decodable_rlp() {
        let dir = tempfile::tempdir().unwrap();
        let sink = EvidenceSink::open(dir.path().join("evidence"), NodeId::dummy(0)).unwrap();
        let signer = NodeId::dummy(3);
        let vote = |entry| {
            let body = VoteBody {
                entries: ProposalMap::new(1, |_| entry),
            };
            Signed::sign(Slot(4), signer, body, &StubPq::default().signer(signer))
        };
        let evidence = Evidence::ConflictingVotes {
            signer,
            a: vote(Entry::Bot),
            b: vote(Entry::Bot),
        };
        sink.record(Slot(4), &evidence);
        let path = sink.path_of(Slot(4), &evidence);
        assert!(path.ends_with("000000000004-conflicting-votes-3.rlp"));
        let read: Evidence = alloy_rlp::decode_exact(std::fs::read(path).unwrap()).unwrap();
        assert_eq!(read, evidence);
    }
}
