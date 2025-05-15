use std::{
    collections::{BTreeMap, BTreeSet},
    hash::{DefaultHasher, Hash, Hasher},
    time::{Duration, Instant},
};

use monad_consensus_types::{
    clock::{Clock, TimestampAdjusterConfig},
    quorum_certificate::{TimestampAdjustment, TimestampAdjustmentDirection},
    signature_collection::SignatureCollection,
    validator_data::ValidatorSetDataWithEpoch,
};
use monad_crypto::certificate_signature::PubKey;
use monad_types::{Epoch, NodeId, PingSequence, Round};
use tracing::debug;

use crate::timestamp_adjuster::TimestampAdjuster;

const MAX_LATENCY_SAMPLES: usize = 100;

pub const PING_PERIOD_SEC: usize = 30;

pub const PING_TICK_DURATION: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Invalid,     // timestamp did not increment compared to previous block
    OutOfBounds, // timestamp is out of bounds compared to local time
}

#[derive(Debug, Clone)]
struct RunningAverage {
    sum: Duration,
    max_samples: usize,
    samples: Vec<Duration>,
    avg: Option<Duration>,
    next_index: usize,
}

impl RunningAverage {
    pub fn new(max_samples: usize) -> Self {
        assert!(max_samples > 0);
        Self {
            sum: Default::default(),
            max_samples,
            samples: Vec::new(),
            avg: None,
            next_index: 0,
        }
    }

    pub fn add_sample(&mut self, sample: Duration) {
        if self.samples.len() == self.max_samples {
            self.sum -= self.samples[self.next_index];
            self.samples[self.next_index] = sample;
            self.next_index = (self.next_index + 1) % self.max_samples;
        } else {
            self.samples.push(sample);
        }
        self.sum += sample;
        self.avg = Some(self.sum / self.samples.len() as u32);
    }

    pub fn get_avg(&self) -> Option<Duration> {
        self.avg
    }
}

/// Ping state per validator
#[derive(Debug, Clone)]
pub struct ValidatorPingState {
    sequence: PingSequence,       // sequence number of the last ping sent
    ping_latency: RunningAverage, // average latency of the last pings
    last_ping_time: Instant,      // time of the last ping sent
}

impl Default for ValidatorPingState {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            sequence: PingSequence(0),
            ping_latency: RunningAverage::new(MAX_LATENCY_SAMPLES),
            last_ping_time: now,
        }
    }
}

impl ValidatorPingState {
    pub fn new() -> Self {
        Self::default()
    }
    // initiate a new ping
    pub fn start_next(&mut self) -> PingSequence {
        self.sequence.0 = self.sequence.0.wrapping_add(1);
        self.last_ping_time = Instant::now();
        self.sequence
    }

    pub fn pong_received(&mut self, sequence: PingSequence) -> Option<Duration> {
        if sequence != self.sequence {
            return None;
        }
        // estimate latency as half the round trip time
        let elapsed = self.last_ping_time.elapsed() / 2;
        self.update_latency(elapsed);
        Some(elapsed)
    }

    pub fn avg_latency(&self) -> Option<Duration> {
        self.ping_latency.get_avg()
    }

    fn update_latency(&mut self, latency: Duration) {
        self.ping_latency.add_sample(latency);
    }
}

#[derive(Debug, Clone)]
struct PingState<P: PubKey> {
    current_epoch: Epoch,
    epoch_validators: BTreeMap<Epoch, Vec<NodeId<P>>>,
    validators: BTreeMap<NodeId<P>, ValidatorPingState>, // a union of the current and the next epoch validator sets
    schedule: Box<[Vec<NodeId<P>>; PING_PERIOD_SEC]>,    // schedule of validators to ping
    tick: usize,                                         // current tick index into schedule
}

impl<P: PubKey> PingState<P> {
    pub fn new() -> Self {
        Self {
            current_epoch: Epoch(0),
            epoch_validators: BTreeMap::new(),
            validators: BTreeMap::new(),
            schedule: Box::new([const { Vec::new() }; PING_PERIOD_SEC]),
            tick: 0,
        }
    }

    pub fn pong_received(&mut self, node_id: NodeId<P>, sequence: PingSequence) {
        if let Some(validator_state) = self.validators.get_mut(&node_id) {
            if let Some(elapsed) = validator_state.pong_received(sequence) {
                debug!(?node_id, elapsed_ms = ?elapsed.as_millis(), avg_latency_ms = ?validator_state.avg_latency().unwrap_or_default().as_millis(), "ping latency");
            }
        }
    }

    fn update_validators<SCT>(
        &mut self,
        val_set: &ValidatorSetDataWithEpoch<SCT>,
        my_node: &NodeId<P>,
    ) where
        SCT: SignatureCollection<NodeIdPubKey = P>,
    {
        match self.epoch_validators.entry(val_set.epoch) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(
                    val_set
                        .validators
                        .0
                        .iter()
                        .filter(|v| v.node_id != *my_node)
                        .map(|v| v.node_id)
                        .collect(),
                );
            }
            std::collections::btree_map::Entry::Occupied(entry) => {
                let val_set: Vec<_> = val_set
                    .validators
                    .0
                    .iter()
                    .filter(|v| v.node_id != *my_node)
                    .map(|v| v.node_id)
                    .collect();
                assert_eq!(
                    (*entry.get()).iter().copied().collect::<BTreeSet<_>>(),
                    val_set.iter().copied().collect::<BTreeSet<_>>(),
                    "Validators update is not matching existing for the same epoch"
                )
            }
        }
    }

    fn compute_schedule(&mut self) {
        let active_validators = self
            .epoch_validators
            .iter()
            .filter(|entry| {
                *entry.0 == self.current_epoch || *entry.0 == self.current_epoch + Epoch(1)
            })
            .flat_map(|entry| entry.1)
            .cloned()
            .collect::<BTreeSet<NodeId<P>>>();

        self.validators
            .retain(|node_id, _| active_validators.contains(node_id));

        for s in self.schedule.iter_mut() {
            s.clear();
        }

        // TODO: reimplement shuffling with rand.
        for validator in active_validators.iter() {
            self.validators.entry(*validator).or_default();
            let mut hasher = DefaultHasher::new();
            validator.hash(&mut hasher);
            let idx = (hasher.finish() as usize) % self.schedule.len();
            self.schedule[idx].push(*validator);
        }
    }

    // returns list of nodes to send pings to on this tick
    fn tick(&mut self) -> Vec<(NodeId<P>, PingSequence)> {
        let mut pings = Vec::new();
        for node_id in self.schedule[self.tick].iter() {
            let validator_state = self.validators.get_mut(node_id).unwrap();
            let sequence = validator_state.start_next();
            pings.push((*node_id, sequence));
        }
        self.tick = (self.tick + 1) % self.schedule.len();
        pings
    }

    fn get_latency(&self, node_id: &NodeId<P>) -> Option<Duration> {
        self.validators.get(node_id).and_then(|v| v.avg_latency())
    }
}

#[derive(Debug)]
struct SentVote {
    round: Round,
    timestamp: Instant,
}
#[derive(Debug)]
pub struct BlockTimestamp<P: PubKey, T: Clock> {
    clock: T,
    max_delta_ns: u128,
    ping_state: PingState<P>,

    last_sent_vote: Option<SentVote>, // last voted round and timestamp

    default_latency_estimate_ns: u128,
    adjuster: Option<TimestampAdjuster>,
}

impl<P: PubKey, T: Clock> BlockTimestamp<P, T> {
    pub fn new(
        max_delta_ns: u128,
        default_latency_estimate_ns: u128,
        adjuster_config: TimestampAdjusterConfig,
    ) -> Self {
        assert!(default_latency_estimate_ns > 0);
        println!("adjuster_config: {:?}", adjuster_config);
        Self {
            clock: T::new(),
            max_delta_ns,
            default_latency_estimate_ns,
            ping_state: PingState::new(),
            last_sent_vote: None,
            adjuster: match adjuster_config {
                TimestampAdjusterConfig::Disabled => None,
                TimestampAdjusterConfig::Enabled {
                    max_delta,
                    adjustment_period,
                } => Some(TimestampAdjuster::new(max_delta, adjustment_period)),
            },
        }
    }

    fn is_valid_bounds(&self, timestamp: u128, vote_delay_ns: u128) -> bool {
        let adjusted_now = self.get_adjusted_time();
        let max_delta_ns = self.max_delta_ns.saturating_add(vote_delay_ns);

        let lower_bound = adjusted_now.saturating_sub(max_delta_ns);
        let upper_bound = adjusted_now.saturating_add(max_delta_ns);

        lower_bound <= timestamp && timestamp <= upper_bound
    }

    fn handle_adjustment(&mut self, delta: TimestampAdjustment) {
        if let Some(adjuster) = &mut self.adjuster {
            adjuster.handle_adjustment(delta);
        }
    }

    fn get_current_time(&self) -> u128 {
        self.clock.get()
    }

    pub fn update_time(&mut self, time: u128) {
        self.clock.update(time);
    }

    pub fn get_adjusted_time(&self) -> u128 {
        let now = self.get_current_time();
        if let Some(adjuster) = &self.adjuster {
            let adjustment = adjuster.get_adjustment();
            if adjustment >= 0 {
                now.checked_add(adjustment as u128).unwrap_or(now)
            } else {
                now.saturating_sub(adjustment.unsigned_abs() as u128)
            }
        } else {
            now
        }
    }

    pub fn validate_block_timestamp(
        &self,
        prev_block_ts: u128,
        curr_block_ts: u128,
        vote_delay_ns: u128,
    ) -> Result<(), Error> {
        if curr_block_ts <= prev_block_ts {
            // block timestamp must be strictly monotonically increasing
            return Err(Error::Invalid);
        }
        if !self.is_valid_bounds(curr_block_ts, vote_delay_ns) {
            return Err(Error::OutOfBounds);
        }
        Ok(())
    }

    fn compute_block_timestamp_adjustment(
        &self,
        curr_block_ts: u128,
        author: &NodeId<P>,
    ) -> Result<Option<TimestampAdjustment>, Error> {
        // adjust for estimated latency
        let latency = self
            .ping_state
            .get_latency(author)
            .unwrap_or(Duration::from_nanos(
                self.default_latency_estimate_ns.try_into().unwrap(),
            ));

        let now = self.get_current_time();
        // TODO: add proposal estimated time
        let expected_block_ts = now.saturating_sub(latency.as_nanos());

        debug!(
            ?curr_block_ts,
            ?expected_block_ts,
            ?now,
            ?latency,
            "Compute block ts adjustment"
        );
        if curr_block_ts > expected_block_ts {
            Ok(Some(TimestampAdjustment {
                delta: curr_block_ts - expected_block_ts,
                direction: TimestampAdjustmentDirection::Forward,
            }))
        } else {
            Ok(Some(TimestampAdjustment {
                delta: expected_block_ts - curr_block_ts,
                direction: TimestampAdjustmentDirection::Backward,
            }))
        }
    }

    pub fn update_validators<SCT>(
        &mut self,
        val_set: &ValidatorSetDataWithEpoch<SCT>,
        my_node: &NodeId<P>,
    ) where
        SCT: SignatureCollection<NodeIdPubKey = P>,
    {
        self.ping_state.update_validators(val_set, my_node);
    }

    pub fn tick(&mut self) -> Vec<(NodeId<P>, PingSequence)> {
        self.ping_state.tick()
    }

    pub fn pong_received(&mut self, node_id: NodeId<P>, sequence: PingSequence) {
        self.ping_state.pong_received(node_id, sequence);
    }

    pub fn vote_sent(&mut self, round: Round) {
        self.last_sent_vote = Some(SentVote {
            round,
            timestamp: Instant::now(),
        });
    }

    pub fn enter_epoch(&mut self, epoch: Epoch) {
        assert!(epoch >= self.ping_state.current_epoch);
        debug!(?epoch, "Enter epoch");
        if epoch > self.ping_state.current_epoch {
            self.ping_state.current_epoch = epoch;
            self.ping_state
                .epoch_validators
                .retain(|key, _| key >= &epoch);
            self.ping_state.compute_schedule();
        }
    }

    pub fn proposal_received(&mut self, round: Round, received_block_ts: u128, author: &NodeId<P>) {
        debug!(?round, ?author, ?received_block_ts, "Proposal received",);
        // TODO: validate and add proposal data
        if let Ok(Some(delta)) = self.compute_block_timestamp_adjustment(received_block_ts, &author)
        {
            self.handle_adjustment(delta);
        }
    }
}

#[cfg(test)]
mod test {
    use std::{collections::BTreeSet, time::Duration};

    use monad_consensus_types::{
        clock::TestClock,
        quorum_certificate::{TimestampAdjustment, TimestampAdjustmentDirection},
        validator_data::{ValidatorData, ValidatorSetData, ValidatorSetDataWithEpoch},
    };
    use monad_crypto::{
        certificate_signature::CertificateKeyPair, NopKeyPair, NopPubKey, NopSignature,
    };
    use monad_testutil::signing::{create_keys, get_key, MockSignatures};
    use monad_types::{Epoch, NodeId, Round, Stake};

    use super::{Error, PING_PERIOD_SEC};
    use crate::{
        timestamp::{PingState, TimestampAdjusterConfig, ValidatorPingState},
        BlockTimestamp,
    };

    type SignatureType = NopSignature;
    type SignatureCollection = MockSignatures<SignatureType>;

    #[test]
    fn test_block_timestamp_validate() {
        let mut b =
            BlockTimestamp::<NopPubKey, TestClock>::new(10, 1, TimestampAdjusterConfig::Disabled);
        let author = NodeId::new(NopKeyPair::from_bytes(&mut [0; 32]).unwrap().pubkey());
        b.update_time(0);

        assert!(matches!(
            b.validate_block_timestamp(1, 1, 0).err().unwrap(),
            Error::Invalid
        ));
        assert!(matches!(
            b.validate_block_timestamp(2, 1, 0).err().unwrap(),
            Error::Invalid
        ));
        assert!(matches!(
            b.validate_block_timestamp(0, 11, 0).err().unwrap(),
            Error::OutOfBounds
        ));

        b.ping_state
            .validators
            .insert(author, ValidatorPingState::new());

        b.ping_state
            .validators
            .get_mut(&author)
            .unwrap()
            .update_latency(Duration::from_nanos(1));

        b.update_time(10);
        b.vote_sent(Round(0));

        assert!(matches!(
            b.validate_block_timestamp(11, 11, 0).err().unwrap(),
            Error::Invalid
        ));
        assert!(matches!(
            b.validate_block_timestamp(12, 11, 0).err().unwrap(),
            Error::Invalid
        ));
        assert!(matches!(
            b.validate_block_timestamp(9, 21, 0).err().unwrap(),
            Error::OutOfBounds
        ));
    }

    #[test]
    fn test_compute_block_timestamp_adjustment() {
        let mut b =
            BlockTimestamp::<NopPubKey, TestClock>::new(10, 1, TimestampAdjusterConfig::Disabled);
        let author = NodeId::new(NopKeyPair::from_bytes(&mut [0; 32]).unwrap().pubkey());
        b.update_time(12);

        // the tests below compute adjustment based on the default_latency set in the BlockTimestamp::new()
        // and assuming that the time set in the update_time is the time when the proposal has arrived.
        assert!(matches!(
            b.compute_block_timestamp_adjustment(20, &author),
            Ok(Some(TimestampAdjustment {
                delta: 9,
                direction: TimestampAdjustmentDirection::Forward
            }))
        ));

        assert!(matches!(
            b.compute_block_timestamp_adjustment(12, &author),
            Ok(Some(TimestampAdjustment {
                delta: 1,
                direction: TimestampAdjustmentDirection::Forward
            }))
        ));

        assert!(matches!(
            b.compute_block_timestamp_adjustment(10, &author),
            Ok(Some(TimestampAdjustment {
                delta: 1,
                direction: TimestampAdjustmentDirection::Backward
            }))
        ));
    }

    #[test]
    fn test_ping_state() {
        let mut state = ValidatorPingState::new();
        let seq = state.start_next();
        state.update_latency(Duration::from_millis(1));

        assert_eq!(state.avg_latency().unwrap().as_millis(), 1);
        state.update_latency(Duration::from_millis(3));
        assert_eq!(state.avg_latency().unwrap().as_millis(), 2);
        state.update_latency(Duration::from_millis(6));
        assert_eq!(state.avg_latency().unwrap().as_millis(), 3);

        for _ in 0..100 {
            state.update_latency(Duration::from_millis(10));
        }

        assert_eq!(
            state.ping_latency.sum,
            state.ping_latency.samples.iter().sum::<Duration>()
        );

        assert_eq!(
            state.avg_latency().unwrap(),
            state.ping_latency.sum / state.ping_latency.samples.len() as u32
        );

        for _ in 0..50 {
            state.update_latency(Duration::from_millis(50));
        }

        assert_eq!(
            state.ping_latency.sum,
            state.ping_latency.samples.iter().sum::<Duration>()
        );

        assert_eq!(
            state.avg_latency().unwrap(),
            state.ping_latency.sum / state.ping_latency.samples.len() as u32
        );
    }

    #[test]
    fn test_update_validators() {
        let mut b =
            BlockTimestamp::<NopPubKey, TestClock>::new(10, 1, TimestampAdjusterConfig::Disabled);
        let keys = create_keys::<NopSignature>(4);
        let nodes: Vec<_> = keys.iter().map(|k| NodeId::new(k.pubkey())).collect();

        let my_node = nodes[0];

        let validators = vec![
            ValidatorData::<SignatureCollection> {
                node_id: nodes[1],
                stake: Stake(1),
                cert_pubkey: nodes[1].pubkey(),
            },
            ValidatorData {
                node_id: nodes[2],
                stake: Stake(1),
                cert_pubkey: nodes[2].pubkey(),
            },
        ];

        b.enter_epoch(Epoch(1));
        let val_set = ValidatorSetDataWithEpoch {
            validators: ValidatorSetData(validators.clone()),
            epoch: Epoch(1),
        };
        b.update_validators(&val_set, &my_node);
        let vals = b
            .ping_state
            .epoch_validators
            .get(&Epoch(1))
            .expect("get validators");
        assert_eq!(
            vals.iter().copied().collect::<BTreeSet<_>>(),
            validators
                .iter()
                .map(|x| x.node_id)
                .collect::<BTreeSet<_>>()
        );

        // test sending identical to existing set of validators for the same epoch
        b.update_validators(&val_set, &my_node);
        let vals = b
            .ping_state
            .epoch_validators
            .get(&Epoch(1))
            .expect("get validators");
        assert_eq!(
            vals.iter().copied().collect::<BTreeSet<_>>(),
            validators
                .iter()
                .map(|x| x.node_id)
                .collect::<BTreeSet<_>>()
        );

        let validators_1 = vec![validators[0].clone()]; // nodes[1]
        let val_set_1 = ValidatorSetDataWithEpoch {
            validators: ValidatorSetData(validators_1.clone()),
            epoch: Epoch(2),
        };

        b.update_validators(&val_set_1, &my_node);
        assert_eq!(
            b.ping_state
                .epoch_validators
                .get(&Epoch(2))
                .unwrap()
                .clone(),
            validators_1.iter().map(|x| x.node_id).collect::<Vec<_>>()
        );

        // compute_schedule will add new validators.
        assert!(!b.ping_state.validators.contains_key(&nodes[1]));
        b.ping_state.compute_schedule();
        assert!(b.ping_state.validators.contains_key(&nodes[1]));

        let validators_2 = vec![ValidatorData::<SignatureCollection> {
            node_id: nodes[3],
            stake: Stake(1),
            cert_pubkey: nodes[3].pubkey(),
        }];
        let val_set_2 = ValidatorSetDataWithEpoch {
            validators: ValidatorSetData(validators_2.clone()),
            epoch: Epoch(3),
        };

        b.update_validators(&val_set_2, &my_node);
        assert_eq!(
            b.ping_state
                .epoch_validators
                .get(&Epoch(3))
                .unwrap()
                .clone(),
            validators_2.iter().map(|x| x.node_id).collect::<Vec<_>>()
        );

        assert!(!b.ping_state.validators.contains_key(&nodes[3]));
        b.ping_state.compute_schedule();
        assert!(!b.ping_state.validators.contains_key(&nodes[3])); // current_epoch is Epoch(1), so Epoch(3) validators will not show up

        b.enter_epoch(Epoch(2));
        assert!(b.ping_state.validators.contains_key(&nodes[3])); // current_epoch is Epoch(2), so Epoch(3) validators will show up
    }

    #[test]
    fn test_enter_epoch() {
        let mut b =
            BlockTimestamp::<NopPubKey, TestClock>::new(10, 1, TimestampAdjusterConfig::Disabled);

        let val_cnt = 5;
        let keys = create_keys::<NopSignature>(val_cnt);
        let nodes = keys
            .iter()
            .map(|k| NodeId::new(k.pubkey()))
            .collect::<Vec<_>>();

        let my_node = nodes[0];

        let e1 = Epoch(1);
        let e1_vals = vec![
            ValidatorData {
                node_id: nodes[0],
                stake: Stake(1),
                cert_pubkey: nodes[0].pubkey(),
            },
            ValidatorData::<SignatureCollection> {
                node_id: nodes[1],
                stake: Stake(1),
                cert_pubkey: nodes[1].pubkey(),
            },
            ValidatorData {
                node_id: nodes[2],
                stake: Stake(1),
                cert_pubkey: nodes[2].pubkey(),
            },
        ];

        let e2 = Epoch(2);
        let e2_vals = vec![
            ValidatorData::<SignatureCollection> {
                node_id: nodes[2],
                stake: Stake(1),
                cert_pubkey: nodes[2].pubkey(),
            },
            ValidatorData {
                node_id: nodes[3],
                stake: Stake(1),
                cert_pubkey: nodes[3].pubkey(),
            },
        ];

        let val_set_1 = ValidatorSetDataWithEpoch {
            validators: ValidatorSetData(e1_vals.clone()),
            epoch: e1,
        };
        let expected_e1 = [e1_vals[1].node_id, e1_vals[2].node_id];
        b.update_validators(&val_set_1, &my_node);
        let vals = b
            .ping_state
            .epoch_validators
            .get(&e1)
            .expect("get validators");
        assert_eq!(
            vals.iter().copied().collect::<BTreeSet<_>>(),
            expected_e1.iter().copied().collect::<BTreeSet<_>>()
        );

        b.enter_epoch(e1);
        assert_eq!(b.ping_state.current_epoch, e1);
        assert_eq!(
            b.ping_state
                .validators
                .keys()
                .copied()
                .collect::<BTreeSet<_>>(),
            expected_e1.iter().copied().collect::<BTreeSet<_>>()
        );

        let mut pings: Vec<NodeId<NopPubKey>> = Vec::new();

        for _ in 0..PING_PERIOD_SEC {
            pings.extend(
                b.tick()
                    .iter()
                    .map(|x| x.0)
                    .collect::<Vec<NodeId<NopPubKey>>>(),
            );
        }
        assert_eq!(
            pings.iter().copied().collect::<BTreeSet<_>>(),
            expected_e1.iter().copied().collect::<BTreeSet<_>>()
        );

        let val_set_2 = ValidatorSetDataWithEpoch {
            validators: ValidatorSetData(e2_vals.clone()),
            epoch: e2,
        };
        let expected_e2 = [e2_vals[0].node_id, e2_vals[1].node_id];
        b.update_validators(&val_set_2, &my_node);
        b.enter_epoch(e2);
        assert_eq!(b.ping_state.current_epoch, e2);
        assert_eq!(
            b.ping_state
                .validators
                .keys()
                .copied()
                .collect::<BTreeSet<_>>(),
            expected_e2.iter().copied().collect::<BTreeSet<_>>()
        );

        let mut pings: Vec<NodeId<NopPubKey>> = Vec::new();

        for _ in 0..PING_PERIOD_SEC {
            pings.extend(
                b.tick()
                    .iter()
                    .map(|x| x.0)
                    .collect::<Vec<NodeId<NopPubKey>>>(),
            );
        }
        assert_eq!(
            pings.iter().copied().collect::<BTreeSet<_>>(),
            expected_e2.iter().copied().collect::<BTreeSet<_>>()
        );

        // Test that BlockTimestamp works as expected after entering the same epoch again.
        b.enter_epoch(e2);
        assert_eq!(b.ping_state.current_epoch, e2);
        assert_eq!(
            b.ping_state
                .validators
                .keys()
                .copied()
                .collect::<BTreeSet<_>>(),
            expected_e2.iter().copied().collect::<BTreeSet<_>>()
        );

        let mut pings: Vec<NodeId<NopPubKey>> = Vec::new();

        for _ in 0..PING_PERIOD_SEC {
            pings.extend(
                b.tick()
                    .iter()
                    .map(|x| x.0)
                    .collect::<Vec<NodeId<NopPubKey>>>(),
            );
        }
        assert_eq!(
            pings.iter().copied().collect::<BTreeSet<_>>(),
            expected_e2.iter().copied().collect::<BTreeSet<_>>()
        );
    }

    #[test]
    fn test_new_epoch() {
        let mut b =
            BlockTimestamp::<NopPubKey, TestClock>::new(10, 1, TimestampAdjusterConfig::Disabled);

        let val_cnt = 5;
        let keys = create_keys::<NopSignature>(val_cnt);
        let nodes = keys
            .iter()
            .map(|k| NodeId::new(k.pubkey()))
            .collect::<Vec<_>>();

        let my_node = nodes[0];

        let e1 = Epoch(1);
        let e1_vals = vec![
            ValidatorData {
                node_id: nodes[0],
                stake: Stake(1),
                cert_pubkey: nodes[0].pubkey(),
            },
            ValidatorData::<SignatureCollection> {
                node_id: nodes[1],
                stake: Stake(1),
                cert_pubkey: nodes[1].pubkey(),
            },
            ValidatorData {
                node_id: nodes[2],
                stake: Stake(1),
                cert_pubkey: nodes[2].pubkey(),
            },
        ];

        let e2 = Epoch(2);
        let e2_vals = vec![
            ValidatorData::<SignatureCollection> {
                node_id: nodes[2],
                stake: Stake(1),
                cert_pubkey: nodes[2].pubkey(),
            },
            ValidatorData {
                node_id: nodes[3],
                stake: Stake(1),
                cert_pubkey: nodes[3].pubkey(),
            },
        ];

        let e3 = Epoch(3);
        let e3_vals = vec![
            ValidatorData::<SignatureCollection> {
                node_id: nodes[3],
                stake: Stake(1),
                cert_pubkey: nodes[3].pubkey(),
            },
            ValidatorData {
                node_id: nodes[4],
                stake: Stake(1),
                cert_pubkey: nodes[4].pubkey(),
            },
        ];

        let expected_e1 = [e1_vals[1].node_id, e1_vals[2].node_id];
        let expected_e2 = [e2_vals[0].node_id, e2_vals[1].node_id];

        let val_set_1 = ValidatorSetDataWithEpoch {
            validators: ValidatorSetData(e1_vals.clone()),
            epoch: e1,
        };
        let val_set_2 = ValidatorSetDataWithEpoch {
            validators: ValidatorSetData(e2_vals.clone()),
            epoch: e2,
        };
        let val_set_3 = ValidatorSetDataWithEpoch {
            validators: ValidatorSetData(e3_vals.clone()),
            epoch: e3,
        };

        b.update_validators(&val_set_1, &my_node);
        b.update_validators(&val_set_2, &my_node);
        b.update_validators(&val_set_3, &my_node);
        assert_eq!(b.ping_state.epoch_validators.len(), 3);
        let vals = b
            .ping_state
            .epoch_validators
            .get(&e1)
            .expect("get validators");
        assert_eq!(
            vals.iter().copied().collect::<BTreeSet<_>>(),
            expected_e1.iter().copied().collect::<BTreeSet<_>>()
        );

        let vals = b
            .ping_state
            .epoch_validators
            .get(&e2)
            .expect("get validators");
        assert_eq!(
            vals.iter().copied().collect::<BTreeSet<_>>(),
            expected_e2.iter().copied().collect::<BTreeSet<_>>()
        );

        let expected_e1_e2 = [e1_vals[1].node_id, e2_vals[0].node_id, e2_vals[1].node_id]; // union of validators for Epoch(1) and Epoch(2) except my_node
        b.enter_epoch(e1);

        assert_eq!(
            b.ping_state
                .validators
                .keys()
                .copied()
                .collect::<BTreeSet<_>>(),
            expected_e1_e2.iter().copied().collect::<BTreeSet<_>>()
        );

        let mut pings: Vec<NodeId<NopPubKey>> = Vec::new();

        for _ in 0..PING_PERIOD_SEC {
            pings.extend(
                b.tick()
                    .iter()
                    .map(|x| x.0)
                    .collect::<Vec<NodeId<NopPubKey>>>(),
            );
        }
        assert_eq!(
            pings.iter().copied().collect::<BTreeSet<_>>(),
            expected_e1_e2.iter().copied().collect::<BTreeSet<_>>()
        );

        let expected_e2_e3 = [e2_vals[0].node_id, e2_vals[1].node_id, e3_vals[1].node_id]; // union of validators for Epoch(2) and Epoch(3) except my_node
        b.enter_epoch(e2);
        assert_eq!(
            b.ping_state
                .validators
                .keys()
                .copied()
                .collect::<BTreeSet<_>>(),
            expected_e2_e3.iter().copied().collect::<BTreeSet<_>>()
        );

        let mut pings: Vec<NodeId<NopPubKey>> = Vec::new();

        for _ in 0..PING_PERIOD_SEC {
            pings.extend(
                b.tick()
                    .iter()
                    .map(|x| x.0)
                    .collect::<Vec<NodeId<NopPubKey>>>(),
            );
        }

        assert_eq!(
            pings.iter().copied().collect::<BTreeSet<_>>(),
            expected_e2_e3.iter().copied().collect::<BTreeSet<_>>()
        );
    }

    #[test]
    fn test_ticks() {
        let mut s = PingState::<NopPubKey>::new();

        let my_key = get_key::<SignatureType>(1_u64).pubkey();
        let my_node = NodeId::new(my_key);

        let k_1 = get_key::<SignatureType>(2_u64).pubkey();
        let node_1 = NodeId::new(k_1);

        let k_2 = get_key::<SignatureType>(3_u64).pubkey();
        let node_2 = NodeId::new(k_2);

        let validators = vec![
            ValidatorData::<SignatureCollection> {
                node_id: node_1,
                stake: Stake(1),
                cert_pubkey: node_1.pubkey(),
            },
            ValidatorData {
                node_id: node_2,
                stake: Stake(1),
                cert_pubkey: node_2.pubkey(),
            },
        ];

        let val_set = ValidatorSetDataWithEpoch {
            validators: ValidatorSetData(validators.clone()),
            epoch: Epoch(1),
        };
        s.current_epoch = Epoch(1);
        s.update_validators(&val_set, &my_node);
        s.compute_schedule();

        let mut pings = Vec::new();
        for _ in 0..PING_PERIOD_SEC {
            pings.extend(
                s.tick()
                    .iter()
                    .map(|x| x.0)
                    .collect::<Vec<NodeId<NopPubKey>>>(),
            );
        }

        assert_eq!(
            pings.iter().copied().collect::<BTreeSet<_>>(),
            validators
                .iter()
                .map(|x| x.node_id)
                .collect::<BTreeSet<_>>()
        );
    }
}
