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

use std::collections::{BTreeMap, HashSet};

use super::{
    chunk::{ChunkData, ProposalEnvelope, WireChunkId},
    types::{NodeId, ProposalHeader},
};

pub struct Dissemination {
    pub to: HashSet<NodeId>,
    pub envelope: ProposalEnvelope,
}

pub(crate) struct ChunkEgress {
    released: bool,
    pending: Vec<Dissemination>,
}

impl ChunkEgress {
    pub(crate) fn new() -> Self {
        Self {
            // todo: flip to true to rebroadcast immediately as in
            // ChunkSync proposal.
            released: false,
            pending: Vec::new(),
        }
    }

    // todo: bound the pending buffer
    pub(crate) fn enqueue(
        &mut self,
        recipients: &HashSet<NodeId>,
        header: &ProposalHeader,
        chunk_id: WireChunkId,
        data: ChunkData,
    ) {
        if let Some(last) = self.pending.last_mut()
            && last.to == *recipients
            && last.envelope.header() == header
        {
            // merge with existing envelop
            last.envelope.insert(chunk_id, data);
            return;
        }

        let chunks = BTreeMap::from([(chunk_id, data)]);
        let envelope = ProposalEnvelope::new(header.clone(), chunks);
        self.pending.push(Dissemination {
            to: recipients.clone(),
            envelope,
        });
    }

    pub(crate) fn release(&mut self) {
        self.released = true;
    }

    pub(crate) fn drain(&mut self) -> Vec<Dissemination> {
        if !self.released {
            return vec![];
        }
        std::mem::take(&mut self.pending)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        super::{
            chunk::Chunk,
            test_util::{epoch_handle, proposal_chunks},
        },
        *,
    };

    fn parts(chunk: &Chunk) -> (ProposalHeader, WireChunkId, ChunkData) {
        chunk.clone().into_parts()
    }

    fn to(ids: impl IntoIterator<Item = u64>) -> HashSet<NodeId> {
        ids.into_iter().map(NodeId::dummy).collect()
    }

    fn released() -> ChunkEgress {
        let mut egress = ChunkEgress::new();
        egress.release();
        egress
    }

    fn enqueue(egress: &mut ChunkEgress, chunk: &Chunk, recipients: &HashSet<NodeId>) {
        let (header, chunk_id, data) = parts(chunk);
        egress.enqueue(recipients, &header, chunk_id, data);
    }

    #[test]
    fn consecutive_chunks_to_the_same_recipients_share_an_envelope() {
        let (_, chunks) = proposal_chunks(&epoch_handle(), 1);
        let mut egress = released();

        enqueue(&mut egress, &chunks[0], &to([2, 3]));
        enqueue(&mut egress, &chunks[1], &to([2, 3]));

        let messages = egress.drain();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].to, to([2, 3]));
        assert_eq!(messages[0].envelope.chunks().len(), 2);
        assert!(egress.drain().is_empty());
    }

    #[test]
    fn a_new_message_starts_on_other_recipients_or_another_header() {
        let epoch_handle = epoch_handle();
        let (_, a) = proposal_chunks(&epoch_handle, 1);
        let (_, b) = proposal_chunks(&epoch_handle, 2);
        let mut egress = released();

        enqueue(&mut egress, &a[0], &to([2, 3]));
        enqueue(&mut egress, &a[1], &to([3]));
        enqueue(&mut egress, &b[0], &to([3]));
        // a repeat of an earlier key is not merged backwards
        enqueue(&mut egress, &a[2], &to([3]));

        let messages = egress.drain();
        assert_eq!(messages.len(), 4);
        for message in &messages {
            assert_eq!(message.envelope.chunks().len(), 1);
        }
    }

    #[test]
    fn nothing_drains_before_release() {
        let (_, chunks) = proposal_chunks(&epoch_handle(), 1);
        let mut egress = ChunkEgress::new();

        enqueue(&mut egress, &chunks[0], &to([2, 3]));
        assert!(egress.drain().is_empty());

        egress.release();
        assert_eq!(egress.drain().len(), 1);
    }
}
