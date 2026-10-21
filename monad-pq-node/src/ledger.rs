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

//! Finalized pq slots into the unchanged on-disk ledger (§5.3): the
//! explorer and rpc read it as they read a Chorus ledger.

use std::sync::Arc;

use monad_mcp_chorus::ledger::{CommittedLane, FinalizationPath as LedgerPath, NewBlock, NewLane};
use monad_mcp_node::ledger::LedgerSink;
use monad_pq_chorus::{PqPath, PqProof};

use crate::{
    chorus::types::{ProposerSchedule, ProposerSet},
    config::LedgerProof,
    finalization::PqFinalizedSlot,
};

const GREEN: u8 = 32;
const YELLOW: u8 = 33;

// D14: the ledger knows two paths; the exact one is in the proof
pub fn ledger_path(path: PqPath) -> LedgerPath {
    match path {
        PqPath::Fast => LedgerPath::Fast,
        PqPath::Coda | PqPath::Gadget => LedgerPath::Fallback,
    }
}

// the block the writer stores for a finalized slot; `proposers` is the
// slot's schedule, None if it could not be queried
pub fn pq_new_block(
    finalized: &PqFinalizedSlot,
    proposers: Option<&ProposerSet>,
    proof: LedgerProof,
) -> NewBlock {
    let lanes = finalized
        .pids()
        .into_iter()
        .zip(finalized.proposals.clone())
        .enumerate()
        .map(|(index, (pid, payload))| NewLane {
            proposer: proposers.and_then(|set| set.proposer(index)).map(u64::from),
            committed: pid.zip(payload).map(|(pid, payload)| CommittedLane {
                pid: *pid.as_bytes(),
                payload,
            }),
        })
        .collect();
    let path_votes = match proof {
        LedgerProof::PathVotes => finalized.path_votes.clone(),
        LedgerProof::Metablock => Vec::new(),
    };
    let proof = PqProof {
        version: PqProof::VERSION,
        path: finalized.path,
        metablock: finalized.metablock.clone(),
        path_votes,
    };
    NewBlock {
        slot: finalized.slot.0,
        deadline_ns: finalized.deadline.map(|deadline| deadline.as_nanos()),
        finalized_at_ns: finalized.at.as_nanos(),
        path: ledger_path(finalized.path),
        // the speculative block is the one finalized only on the fast path
        fast_block_at_ns: finalized
            .speculated_at
            .filter(|_| finalized.path == PqPath::Fast)
            .map(|at| at.as_nanos()),
        lane_decoded_at_ns: finalized
            .lane_decoded_at
            .iter()
            .map(|at| at.map(|at| at.as_nanos()))
            .collect(),
        lanes,
        proof: alloy_rlp::encode(&proof).into(),
    }
}

// the block line of the log, as monad-mcp-node prints it: one char per
// proposal index, + committed or - not, green on the fast path and yellow
// otherwise
pub fn log_finalized(finalized: &PqFinalizedSlot) {
    let mut shape = String::new();
    let pids = finalized.pids();
    for ((j, pid), proposal) in pids.into_indexed_iter().zip(finalized.proposals.as_ref()) {
        shape.push(if pid.is_some() { '+' } else { '-' });
        let Some(message) = proposal else {
            continue;
        };
        tracing::debug!(
            slot = finalized.slot.0,
            j,
            ?pid,
            len = message.len(),
            "committed"
        );
    }
    let color = match finalized.path {
        PqPath::Fast => GREEN,
        PqPath::Coda | PqPath::Gadget => YELLOW,
    };
    let block = format!("\x1b[{color}m{shape}\x1b[0m");
    tracing::info!(
        slot = finalized.slot.0,
        block = %block,
        at = finalized.at.as_nanos(),
        path = ?finalized.path,
        "finalized"
    );
}

// what the node does with each finalized slot: log it, then store it
pub struct PqRecorder {
    schedule: Arc<dyn ProposerSchedule + Send + Sync>,
    sink: LedgerSink,
    proof: LedgerProof,
}

impl PqRecorder {
    pub fn new(
        schedule: Arc<dyn ProposerSchedule + Send + Sync>,
        sink: LedgerSink,
        proof: LedgerProof,
    ) -> Self {
        Self {
            schedule,
            sink,
            proof,
        }
    }

    pub fn record(&mut self, finalized: PqFinalizedSlot) {
        log_finalized(&finalized);
        let proposers = match self.schedule.proposers_at(finalized.slot) {
            Ok(set) => Some(set),
            Err(error) => {
                tracing::warn!(slot = finalized.slot.0, %error, "no proposers for the ledger");
                None
            }
        };
        self.sink
            .write(pq_new_block(&finalized, proposers.as_ref(), self.proof));
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use monad_mcp_chorus::{
        ledger::{LedgerReader, LedgerWriter},
        spec::ProposalHeader as _,
    };
    use monad_pq_chorus::{
        message::PathVoteBody,
        sig::{Signed, StubPq},
        types::{Entry, PathEntry},
    };

    use super::*;
    use crate::chorus::{
        env::{D25, EncodingScheme, MerkleHash, MerkleRoot, ProposalHeader},
        types::{NodeId, ProposalId, ProposalMap, Slot, Timestamp},
    };

    fn pid() -> ProposalId {
        ProposalHeader {
            root: MerkleRoot(MerkleHash([7; 20])),
            scheme: EncodingScheme::D25(D25 {
                slot: Slot(9),
                msg_len: 1,
                unix_ts: 0,
                depth: 3,
            }),
        }
        .pid()
    }

    fn finalized(path: PqPath) -> PqFinalizedSlot {
        let signer = NodeId::dummy(1);
        let body = PathVoteBody {
            entries: ProposalMap::new(2, |j| PathEntry::Fast([Entry::Root(pid()), Entry::Bot][j])),
        };
        let vote = Signed::sign(Slot(9), signer, body, &StubPq::default().signer(signer));
        PqFinalizedSlot {
            slot: Slot(9),
            at: Timestamp::from_millis(1_200),
            deadline: Some(Timestamp::from_millis(1_000)),
            speculated_at: Some(Timestamp::from_millis(1_100)),
            metablock: ProposalMap::new(2, |j| [Entry::Root(pid()), Entry::Bot][j]),
            path,
            at_view: None,
            path_votes: vec![(signer, vote)],
            proposals: ProposalMap::new(2, |j| (j == 0).then(|| Bytes::from_static(b"lane"))),
            lane_decoded_at: vec![Some(Timestamp::from_millis(1_050)), None],
        }
    }

    #[test]
    fn a_block_round_trips_through_the_unchanged_ledger() {
        let dir = tempfile::tempdir().unwrap();
        let writer = LedgerWriter::open(dir.path()).unwrap();
        let slot = finalized(PqPath::Fast);
        let written = writer
            .write(&pq_new_block(&slot, None, LedgerProof::PathVotes))
            .unwrap();
        let reader = LedgerReader::new(dir.path());
        let meta = reader.read_meta(9).unwrap();
        assert_eq!(meta, written);
        assert_eq!(meta.path, LedgerPath::Fast);
        meta.check().unwrap();
        assert_eq!(
            reader.read_lane(9, 0).unwrap().as_deref(),
            Some(&b"lane"[..])
        );
        assert_eq!(reader.read_lane(9, 1).unwrap(), None);

        let proof: PqProof = alloy_rlp::decode_exact(reader.read_proof(9).unwrap()).unwrap();
        assert_eq!(proof.path, PqPath::Fast);
        assert_eq!(proof.metablock, slot.metablock);
        assert_eq!(proof.path_votes, slot.path_votes);
        let timeline = reader.read_timeline(9).unwrap().unwrap();
        assert_eq!(
            timeline.fast_block_at_ns,
            Some(Timestamp::from_millis(1_100).as_nanos())
        );
    }

    #[test]
    fn coda_and_gadget_are_recorded_as_fallback() {
        for path in [PqPath::Coda, PqPath::Gadget] {
            let block = pq_new_block(&finalized(path), None, LedgerProof::Metablock);
            assert_eq!(block.path, LedgerPath::Fallback);
            assert_eq!(block.fast_block_at_ns, None);
            let proof: PqProof = alloy_rlp::decode_exact(&block.proof).unwrap();
            assert_eq!(proof.path, path);
            assert!(proof.path_votes.is_empty());
        }
    }
}
