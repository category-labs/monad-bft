use std::collections::BTreeMap;

use monad_types::{Epoch, Round, SeqNum};

/// Stores epoch related information and the associated round numbers
/// of each epoch
#[derive(Debug, Clone)]
pub struct EpochManager {
    /// validator set is updated every 'val_set_update_interval'
    /// blocks
    pub val_set_update_interval: SeqNum,
    /// The start of next epoch is 'epoch_start_delay' rounds after
    /// the proposed block
    pub epoch_start_delay: Round,

    /// A key-value (E, R) indicates that Epoch E starts on round R
    pub epoch_starts: BTreeMap<Epoch, Round>,
}

impl EpochManager {
    pub fn new(val_set_update_interval: SeqNum, epoch_start_delay: Round) -> Self {
        let mut epoch_manager = Self {
            val_set_update_interval,
            epoch_start_delay,
            epoch_starts: BTreeMap::new(),
        };

        epoch_manager.insert_epoch_start(Epoch(1), Round(0));

        epoch_manager
    }

    /// Insert a new epoch start if the epoch doesn't exist already
    fn insert_epoch_start(&mut self, epoch: Epoch, round: Round) {
        assert!(
            !self.epoch_starts.contains_key(&epoch),
            "should't insert epoch start twice"
        );

        self.epoch_starts.insert(epoch, round);
    }

    /// Schedule next epoch start if the committed block is the last one in the current epoch
    pub fn schedule_epoch_start(&mut self, block_num: SeqNum, block_round: Round) {
        if block_num % self.val_set_update_interval == SeqNum(0) {
            let epoch = Epoch((block_num / self.val_set_update_interval).0 + 1);
            let epoch_start_round = block_round + self.epoch_start_delay;
            self.insert_epoch_start(epoch, epoch_start_round);
        }
    }

    /// Get the epoch of the given round
    pub fn get_epoch(&self, round: Round) -> Epoch {
        let epoch_start = self.epoch_starts.iter().rfind(|&k| k.1 <= &round).unwrap();

        *epoch_start.0
    }
}
