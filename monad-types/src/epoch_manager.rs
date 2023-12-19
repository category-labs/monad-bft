use std::collections::BTreeMap;

use crate::{Epoch, Round, SeqNum};

pub struct EpochManager {
    pub current_epoch: Epoch,

    // validator set is updated every 'val_set_update_interval'
    // blocks
    pub val_set_update_interval: SeqNum,
    // The start of next epoch is 'epoch_start_delay' rounds after
    // the proposed block
    pub epoch_start_delay: Round,

    // A key-value (R, E) indicates that Epoch E starts on round R
    pub epoch_starts: BTreeMap<Round, Epoch>,
}

impl EpochManager {
    pub fn new(val_set_update_interval: SeqNum, epoch_start_delay: Round) -> Self {
        let mut epoch_manager = Self {
            current_epoch: Epoch(1),
            val_set_update_interval,
            epoch_start_delay,
            epoch_starts: BTreeMap::new(),
        };

        epoch_manager.insert_epoch_start(Round(1), Epoch(1));

        epoch_manager
    }

    fn insert_epoch_start(&mut self, round: Round, epoch: Epoch) {
        let existing_epoch = self.epoch_starts.insert(round, epoch);

        assert!(
            existing_epoch.is_none(),
            "shouldn't insert epoch start twice"
        );
    }

    pub fn handle_epoch_start(&mut self, block_num: SeqNum, block_round: Round) {
        if block_num.0 % self.val_set_update_interval.0 == 0 {
            self.insert_epoch_start(
                block_round + self.epoch_start_delay,
                self.current_epoch + Epoch(1),
            );
        }
    }

    pub fn handle_advance_epoch(&mut self, current_round: Round) {
        let epoch = self.get_epoch(current_round);
        if epoch > self.current_epoch {
            self.current_epoch = epoch;
        }
    }

    pub fn get_epoch(&self, round: Round) -> Epoch {
        // TODO: Use BTreeMap upper_bound from nightly version
        // or find better way

        // Find the largest round key, r, such that r <= round
        // highest_round is the largest round less than or equal to round
        let mut highest_round = Round(1);
        self.epoch_starts.keys().for_each(|r| {
            if r > &highest_round && r <= &round {
                highest_round = *r;
            }
        });

        *self
            .epoch_starts
            .get(&highest_round)
            .expect("must have epoch value")
    }
}
