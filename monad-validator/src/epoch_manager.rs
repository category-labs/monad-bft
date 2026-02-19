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

use std::collections::BTreeMap;

use monad_types::{Epoch, Round, SeqNum};
use tracing::info;

/// Stores epoch related information and the associated round numbers
/// of each epoch
#[derive(Debug, Clone)]
pub struct EpochManager {
    /// validator set is updated every 'epoch_length' blocks
    pub epoch_length: SeqNum,
    /// The start of next epoch is 'epoch_start_delay' rounds after
    /// the proposed block
    pub epoch_start_delay: Round,

    /// A key-value (E, R) indicates that Epoch E starts on round R
    pub epoch_starts: BTreeMap<Epoch, Round>,
}

impl EpochManager {
    pub fn new(
        epoch_length: SeqNum,
        epoch_start_delay: Round,
        known_epochs: &[(Epoch, Round)],
    ) -> Self {
        let mut epoch_manager = Self {
            epoch_length,
            epoch_start_delay,
            epoch_starts: BTreeMap::new(),
        };

        for (epoch, round) in known_epochs {
            epoch_manager.insert_epoch_start(*epoch, *round);
        }

        epoch_manager
    }

    /// Insert a new epoch start if the epoch doesn't exist already
    fn insert_epoch_start(&mut self, epoch: Epoch, round: Round) {
        // On consensus restart, the same epoch might be scheduled a second time
        // when we commit the same boundary block again. Assert that value is
        // the same if entry exists
        match self.epoch_starts.entry(epoch) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(round);
            }
            std::collections::btree_map::Entry::Occupied(entry) => {
                assert_eq!(*entry.get(), round, "Conflicting epoch start round");
            }
        }
    }

    /// Schedule next epoch start if the committed block is the last one in the current epoch
    pub fn schedule_epoch_start(&mut self, block_num: SeqNum, block_round: Round) {
        if block_num.is_boundary_block(self.epoch_length) {
            let next_epoch = block_num.to_epoch(self.epoch_length) + Epoch(1);
            let epoch_start_round = block_round + self.epoch_start_delay;
            self.insert_epoch_start(next_epoch, epoch_start_round);
            info!(
                ?next_epoch,
                ?epoch_start_round,
                ?block_round,
                "schedule epoch start epoch",
            );
        }
    }

    /// Get the epoch of the given round. Returns None if round is from an older
    /// epoch whose record we've purged already
    pub fn get_epoch(&self, round: Round) -> Option<Epoch> {
        let epoch_start = self.epoch_starts.iter().rfind(|&k| k.1 <= &round);

        epoch_start.map(|(&epoch, _)| epoch)
    }
}
