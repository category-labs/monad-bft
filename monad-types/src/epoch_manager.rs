use std::collections::BTreeMap;

use crate::{Epoch, Round, SeqNum};

#[derive(Clone)]
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

        epoch_manager.insert_epoch_start(Round(0), Epoch(1));

        epoch_manager
    }

    fn insert_epoch_start(&mut self, round: Round, epoch: Epoch) {
        let existing_epoch = self.epoch_starts.insert(round, epoch);

        assert!(
            existing_epoch.is_none(),
            "shouldn't insert epoch start twice"
        );
    }

    pub fn schedule_epoch_start(&mut self, block_num: SeqNum, block_round: Round) {
        if block_num % self.val_set_update_interval == SeqNum(0) {
            let epoch_start_round = block_round + self.epoch_start_delay;
            self.insert_epoch_start(epoch_start_round, self.current_epoch + Epoch(1));
        }
    }

    pub fn handle_advance_epoch(&mut self, current_round: Round) {
        let round_epoch = self.get_epoch(current_round);
        if round_epoch > self.current_epoch {
            self.current_epoch = round_epoch;
        }
    }

    pub fn get_epoch(&self, round: Round) -> Epoch {
        let epoch_start = self.epoch_starts.keys().rfind(|&k| k <= &round).unwrap();

        *self
            .epoch_starts
            .get(epoch_start)
            .expect("must have epoch value")
    }
}
