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

use std::collections::BTreeSet;

use super::super::{
    chunk::{ChunkRequest, ChunkRequestType, ChunksSubset},
    instance_rc::MissingChunks,
    types::{NodeId, ProposalId, Timestamp, TimestampDelta},
    util::shuffle_sends,
};

// the pulls for the chunks the pinned proposal still misses: ours from the
// decoded holders, everyone else's from their owners
pub(crate) struct Recovery {
    pid: ProposalId,
    self_id: NodeId,
    // nodes that can derive our chunks: the author, then hinted holders
    decoded_holders: BTreeSet<NodeId>,
    my_chunks_timer: RetryTimer,
    your_chunks_timer: RetryTimer,
}

impl Recovery {
    pub(crate) fn new(
        pid: ProposalId,
        author: NodeId,
        self_id: NodeId,
        first_pull: Timestamp,
        interval: TimestampDelta,
    ) -> Self {
        Self {
            pid,
            self_id,
            decoded_holders: BTreeSet::from([author]),
            my_chunks_timer: RetryTimer::new(first_pull, interval),
            your_chunks_timer: RetryTimer::new(first_pull, interval),
        }
    }

    pub(crate) fn pid(&self) -> ProposalId {
        self.pid
    }

    pub(crate) fn add_decoded_holders(&mut self, node_ids: Vec<NodeId>, now: Timestamp) {
        self.decoded_holders.extend(node_ids);
        self.my_chunks_timer.set_due(now);
    }

    pub(crate) fn pull(
        &mut self,
        mut missing: MissingChunks,
        now: Timestamp,
    ) -> Vec<(NodeId, ChunkRequest)> {
        let mut requests = Vec::new();
        let mine = missing
            .iter()
            .position(|(owner, _)| *owner == self.self_id)
            .map(|i| missing.remove(i).1);
        if let Some(mine) = mine
            && self.my_chunks_timer.try_fire(now)
        {
            requests.extend(self.ask_decoded_holders(mine));
        }
        if !missing.is_empty() && self.your_chunks_timer.try_fire(now) {
            requests.extend(self.ask_owners(missing));
        }
        requests
    }

    fn ask_decoded_holders(&self, mine: ChunksSubset) -> Vec<(NodeId, ChunkRequest)> {
        let pid = self.pid;
        let kind = ChunkRequestType::MyChunks;
        let mut requests = Vec::new();
        for holder in &self.decoded_holders {
            let subset = mine.clone();
            requests.push((*holder, ChunkRequest { pid, kind, subset }));
        }
        shuffle_sends(&mut requests, &self.pid, &self.self_id);
        requests
    }

    fn ask_owners(&self, theirs: MissingChunks) -> Vec<(NodeId, ChunkRequest)> {
        let pid = self.pid;
        let kind = ChunkRequestType::YourChunks;
        let mut requests = Vec::new();
        for (owner, subset) in theirs {
            requests.push((owner, ChunkRequest { pid, kind, subset }));
        }
        shuffle_sends(&mut requests, &self.pid, &self.self_id);
        requests
    }
}

// a request is repeated only once its round trip has run out
struct RetryTimer {
    due: Timestamp,
    interval: TimestampDelta,
}

impl RetryTimer {
    fn new(due: Timestamp, interval: TimestampDelta) -> Self {
        Self { due, interval }
    }

    fn try_fire(&mut self, now: Timestamp) -> bool {
        if now < self.due {
            return false;
        }
        self.due = now + self.interval;
        true
    }

    fn set_due(&mut self, time: Timestamp) {
        self.due = time;
    }
}

// before the header the assignment is unknown, so any validator may own chunks we lack
pub(crate) fn everything_missing<'a>(
    validators: impl Iterator<Item = &'a NodeId>,
) -> MissingChunks {
    validators.map(|node| (*node, ChunksSubset::All)).collect()
}
