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

use bitvec::{bitbox, boxed::BitBox};

use super::super::assignment::ChunkId;

// track the received chunk ids and report once more than `threshold`
// chunks have arrived.
pub(crate) struct DecodingTracker {
    chunks: BitBox,
    count: usize,
    threshold: usize,
}

impl DecodingTracker {
    pub(crate) fn new(num_chunks: usize, threshold: usize) -> Self {
        assert!(0 < threshold && threshold < num_chunks);
        Self {
            chunks: bitbox![0; num_chunks],
            count: 0,
            threshold,
        }
    }

    pub(crate) fn already_received(&self, chunk_id: ChunkId) -> bool {
        self.chunks[usize::from(chunk_id)]
    }

    pub(crate) fn mark(&mut self, chunk_id: ChunkId) {
        self.chunks.set(usize::from(chunk_id), true);
        self.count += 1;
    }

    pub(crate) fn mark_all(&mut self) {
        self.chunks.fill(true);
        self.count = self.chunks.len();
    }

    pub(crate) fn ready(&self) -> bool {
        self.count > self.threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ready_once_more_than_the_threshold_arrived() {
        let mut tracker = DecodingTracker::new(6, 2);

        tracker.mark(ChunkId::unchecked(0));
        tracker.mark(ChunkId::unchecked(1));
        assert!(!tracker.ready());
        assert!(tracker.already_received(ChunkId::unchecked(1)));
        assert!(!tracker.already_received(ChunkId::unchecked(2)));

        tracker.mark(ChunkId::unchecked(2));
        assert!(tracker.ready());

        tracker.mark_all();
        assert!(tracker.already_received(ChunkId::unchecked(5)));
    }
}
