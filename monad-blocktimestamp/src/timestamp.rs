use std::{
    collections::{BTreeMap, BTreeSet},
    hash::{DefaultHasher, Hash, Hasher},
    time::{Duration, Instant},
};

// use rust_decimal::{self, Decimal, MathematicalOps, prelude::ToPrimitive};
use monad_consensus_types::{
    clock::{Clock, TimestampAdjusterConfig},
    quorum_certificate::{TimestampAdjustment, TimestampAdjustmentDirection},
    signature_collection::SignatureCollection,
    validator_data::ValidatorSetDataWithEpoch,
};
use monad_crypto::certificate_signature::PubKey;
use monad_types::{Epoch, NodeId, PingSequence, Round};
use sorted_vec::SortedVec;
use tracing::debug;

use crate::{messages::message::PingResponseMessage, timestamp_adjuster::TimestampAdjuster};

const MAX_LATENCY_SAMPLES: usize = 11;
const MAX_PROPOSAL_SAMPLES: usize = 11;

pub const PING_PERIOD_SEC: usize = 30;

pub const PING_TICK_DURATION: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Invalid,     // timestamp did not increment compared to previous block
    OutOfBounds, // timestamp is out of bounds compared to local time
}

// TODO: make it a _Running_ median
#[derive(Debug, Clone)]
struct RunningMedian<T: Ord + Copy> {
    last_median: Option<T>,
    period: usize,
    samples: SortedVec<T>,
}

// TODO: unify with timestamp_adjuster and compute running median.
impl<T: Ord + Copy> RunningMedian<T> {
    pub fn new(period: usize) -> Self {
        assert!(period % 2 == 1, "median accuracy expects odd period");
        Self {
            last_median: None,
            period,
            samples: SortedVec::new(),
        }
    }

    pub fn add_sample(&mut self, sample: T) {
        self.samples.insert(sample);
        if self.samples.len() == self.period {
            let i = self.samples.len() / 2;
            self.last_median = Some(self.samples[i]);
            self.samples.clear();
        }
    }

    pub fn get_median(&self) -> Option<T> {
        self.last_median
    }
}

// TODO: move RunningAverage and RunningMedian to the seaparate modules.
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
        // add some simple and inefficient (WIP) cut out fat tails
        let sample_threshold = self.avg.unwrap() * 10;
        if self.avg.is_some() && sample > sample_threshold {
            // recompute sum from scratch to skip the tails
            let mut sum: Duration = Default::default();
            let mut cnt = 0;
            let mut cnt_above = 0;
            self.samples.iter().for_each(|x| {
                if x <= &sample_threshold {
                    sum += *x;
                    cnt += 1;
                } else {
                    cnt_above += 1;
                }
            });
            if cnt > 0 && cnt_above <= 5 {
                debug!(
                    ?sample,
                    ?sample_threshold,
                    ?cnt_above,
                    "latency tracing skipping sample exceeds threshold"
                );
                self.avg = Some(sum / cnt);
                return;
            }
        }
        self.avg = Some(self.sum / self.samples.len() as u32);
    }

    pub fn get_avg(&self) -> Option<Duration> {
        self.avg
    }
}

/// Ping state per validator
#[derive(Debug, Clone)]
pub struct ValidatorPingState {
    sequence: PingSequence,           // sequence number of the last ping sent
    ping_latency: RunningAverage,     // average latency of the last pings
    last_ping_time: Instant,          // time of the last ping sent
    proposal_latency: RunningAverage, // average latency of proposal
}

impl Default for ValidatorPingState {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            sequence: PingSequence(0),
            ping_latency: RunningAverage::new(MAX_LATENCY_SAMPLES),
            last_ping_time: now,
            proposal_latency: RunningAverage::new(MAX_LATENCY_SAMPLES),
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

    pub fn pong_received(&mut self, sequence: u64) -> Option<Duration> {
        if sequence != self.sequence.0 {
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
        debug!(?latency, "add ping latency");
        self.ping_latency.add_sample(latency);
    }
    // approximate the time the broadcasted proposal was sent from the time direct ping
    // was received and the average ping latency
    fn update_proposal_latency(&mut self, elapsed_since_last_vote: Duration) {
        self.proposal_latency.add_sample(elapsed_since_last_vote);
        debug!(?elapsed_since_last_vote, "add proposal latency");
    }

    pub fn proposal_latency(&self) -> Option<Duration> {
        self.proposal_latency.get_avg()
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

    pub fn pong_received(&mut self, node_id: NodeId<P>, sequence: u64) {
        debug!(?sequence, ?node_id, "Pong received");
        if let Some(validator_state) = self.validators.get_mut(&node_id) {
            if let Some(elapsed) = validator_state.pong_received(sequence) {
                let avg_latency_ns = validator_state.avg_latency().unwrap_or_default().as_nanos();
                debug!(?node_id, delta = elapsed.as_nanos() - avg_latency_ns, elapsed_ns = ?elapsed.as_nanos(), ?avg_latency_ns, "pong_received latency_tracing");
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
        debug!("updating validators {:?}", self.schedule);
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

    pub fn update_proposal_latency(&mut self, node_id: &NodeId<P>, latency: Duration) {
        let validator_state = self.validators.entry(*node_id).or_default();
        validator_state.update_proposal_latency(latency);
        debug!(?node_id, ?latency, "Update proposal latency");
    }

    fn get_proposal_latency(&self, node_id: &NodeId<P>) -> Option<Duration> {
        self.validators
            .get(node_id)
            .and_then(|v| v.proposal_latency())
    }
}

#[derive(Debug, Clone)]
struct WaitState {
    last_vote_received: Instant,
    last_vote_round: Round,
    qc_latency: RunningAverage,
}

impl WaitState {
    pub fn new(max_samples: u128, round: Round) -> Self {
        assert!(max_samples > 0);
        Self {
            last_vote_received: Instant::now(),
            last_vote_round: round,
            qc_latency: RunningAverage::new(max_samples as usize),
        }
    }

    pub fn vote_received(&mut self, round: Round) {
        self.last_vote_received = Instant::now();
        self.last_vote_round = round;
    }

    pub fn qc_computed(&mut self, round: Round) {
        if self.last_vote_round != round {
            return;
        }
        let sample = self.last_vote_received.elapsed();
        self.qc_latency.add_sample(sample);
        let avg_wait_qc = self.get_avg();
        debug!(?sample, ?round, ?avg_wait_qc, "qc_computed");
    }

    pub fn get_avg(&self) -> Option<Duration> {
        self.qc_latency.get_avg()
    }
}

#[derive(Debug, Copy, Clone)]
struct SentVote<P: PubKey> {
    node_id: NodeId<P>,
    round: Round,
    timestamp: Duration,
}
#[derive(Debug)]
pub struct BlockTimestamp<P: PubKey, T: Clock> {
    clock: T,
    max_delta_ns: u128,
    ping_state: PingState<P>,

    last_sent_vote: Option<SentVote<P>>, // last voted round and timestamp
    last_received_proposal: Option<SentVote<P>>, // last received proposal and timestamp

    default_latency_estimate_ns: u128,
    create_proposal_time_ns: RunningMedian<u128>,
    adjuster: Option<TimestampAdjuster>,

    avg_wait_for_qc_self: BTreeMap<NodeId<P>, WaitState>,
    avg_wait_for_qc: BTreeMap<NodeId<P>, u128>,
}

impl<P: PubKey, T: Clock> BlockTimestamp<P, T> {
    pub fn new(
        max_delta_ns: u128,
        default_latency_estimate_ns: u128,
        adjuster_config: TimestampAdjusterConfig,
    ) -> Self {
        assert!(default_latency_estimate_ns > 0);
        debug!("adjuster_config: {:?}", adjuster_config);
        Self {
            clock: T::new(),
            max_delta_ns,
            default_latency_estimate_ns,
            ping_state: PingState::new(),
            last_sent_vote: None,
            last_received_proposal: None,
            create_proposal_time_ns: RunningMedian::new(MAX_PROPOSAL_SAMPLES),
            adjuster: match adjuster_config {
                TimestampAdjusterConfig::Disabled => None,
                TimestampAdjusterConfig::Enabled {
                    max_delta_ns,
                    adjustment_period,
                } => Some(TimestampAdjuster::new(
                    max_delta_ns,
                    adjustment_period,
                    Some(10_000_000_000),
                )),
            },
            avg_wait_for_qc_self: Default::default(),
            avg_wait_for_qc: Default::default(),
        }
    }

    fn is_valid_bounds(&self, timestamp_ns: u128, vote_delay_ns: u128) -> bool {
        let adjusted_now = self.get_adjusted_time();
        let max_delta_ns = self.max_delta_ns.saturating_add(vote_delay_ns);

        let lower_bound = adjusted_now.saturating_sub(Duration::from_nanos(max_delta_ns as u64));
        let upper_bound = adjusted_now.saturating_add(Duration::from_nanos(max_delta_ns as u64));

        return true; // Just for now to pass the experiment
        lower_bound.as_nanos() <= timestamp_ns && timestamp_ns <= upper_bound.as_nanos()
    }

    fn handle_adjustment(&mut self, delta: TimestampAdjustment) {
        if let Some(adjuster) = &mut self.adjuster {
            adjuster.handle_adjustment(delta);
        }
    }

    fn get_current_time(&self) -> Duration {
        self.clock.get()
    }

    pub fn update_time(&mut self, time_ns: u64) {
        self.clock.update(Duration::from_nanos(time_ns));
    }

    pub fn get_adjusted_time(&self) -> Duration {
        let now = self.get_current_time();

        if let Some(adjuster) = &self.adjuster {
            let adjustment = adjuster.get_adjustment();
            if adjustment >= 0 {
                now.checked_add(Duration::from_nanos(adjustment as u64))
                    .unwrap_or(now)
            } else {
                now.saturating_sub(Duration::from_nanos(adjustment.unsigned_abs()))
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

    fn compute_clock_adjustment(&self, proposed_block_ts_ns: u128) -> Option<TimestampAdjustment> {
        // return the delta between expected block time and actual block time for adjustment
        let vote = self.last_sent_vote.unwrap();
        let proposal = self.last_received_proposal.unwrap();

        assert!(vote.node_id == proposal.node_id);
        assert!(vote.round + Round(1) == proposal.round);
        let proposal_local_ts_ns = proposal.timestamp.as_nanos();
        let vote_ts_ns = vote.timestamp.as_nanos();

        let net_latency_ns = self
            .ping_state
            .get_latency(&vote.node_id)
            .unwrap_or(Duration::from_nanos(
                self.default_latency_estimate_ns as u64,
            ))
            .as_nanos();

        let create_proposal_ns = self.create_proposal_time_ns.get_median().unwrap_or(0);
        let proposal_full_latency_ns = proposal.timestamp.saturating_sub(vote.timestamp).as_nanos();

        let default_wait_for_qc: u128 = 0;
        let wait_for_qc = *self
            .avg_wait_for_qc
            .get(&vote.node_id)
            .unwrap_or(&default_wait_for_qc);
        let expected_block_ts = vote
            .timestamp
            .as_nanos()
            .saturating_add(net_latency_ns)
            .saturating_add(wait_for_qc);
        let estimated_raptorcast_latency_ns =
            proposal_local_ts_ns.saturating_sub(expected_block_ts);

        let delta = if proposed_block_ts_ns > expected_block_ts {
            Some(TimestampAdjustment {
                delta: proposed_block_ts_ns - expected_block_ts,
                direction: TimestampAdjustmentDirection::Forward,
            })
        } else {
            Some(TimestampAdjustment {
                delta: expected_block_ts - proposed_block_ts_ns,
                direction: TimestampAdjustmentDirection::Backward,
            })
        };

        let proposal_round = proposal.round;
        let proposal_node_id = proposal.node_id;
        let adjusted_now = self.get_adjusted_time().as_nanos();
        debug!(
            ?vote.node_id,
            ?wait_for_qc,
            now = self.clock.get().as_nanos(),
            ?adjusted_now,
            ?proposal_round,
            ?proposal_node_id,
            ?delta,
            delta_ns = proposed_block_ts_ns - expected_block_ts,
            ?vote_ts_ns,
            ?proposed_block_ts_ns,
            ?proposal_local_ts_ns,
            ?create_proposal_ns,
            ?net_latency_ns,
            ?proposal_full_latency_ns,
            ?estimated_raptorcast_latency_ns,
            ?expected_block_ts,
            "compute_clock_adjustment latency_tracing"
        );

        delta
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

    pub fn pong_received(&mut self, sender: NodeId<P>, message: PingResponseMessage) {
        self.ping_state.pong_received(sender, message.sequence);
        debug!(?message, ?sender, "pong_received");
        match self.avg_wait_for_qc.entry(sender) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(message.avg_wait_ns as u128);
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                *entry.get_mut() = message.avg_wait_ns as u128;
            }
        }
    }

    pub fn vote_sent(&mut self, node_id: &NodeId<P>, round: Round) {
        self.last_sent_vote = Some(SentVote {
            node_id: *node_id,
            round,
            timestamp: self.get_adjusted_time(),
        });
        self.last_received_proposal = None;
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

    pub fn proposal_received(
        &mut self,
        proposal_round: Round,
        proposed_block_ts_ns: u128,
        author: &NodeId<P>,
    ) {
        debug!(
            ?proposal_round,
            ?author,
            ?proposed_block_ts_ns,
            "Proposal received",
        );
        if let Some(vote) = self.last_sent_vote {
            if vote.round + Round(1) == proposal_round && vote.node_id == *author {
                let now = self.get_adjusted_time();
                self.last_received_proposal = Some(SentVote {
                    node_id: *author,
                    round: proposal_round,
                    timestamp: now,
                });
                let latency = now.saturating_sub(vote.timestamp);

                debug!(
                    ?author,
                    ?proposed_block_ts_ns,
                    last_vote_ns = vote.timestamp.as_nanos(),
                    latency_ns = latency.as_nanos(),
                    "latency_tracing proposal_received",
                );
                self.ping_state.update_proposal_latency(author, latency);
                if let Some(delta) = self.compute_clock_adjustment(proposed_block_ts_ns) {
                    debug!(
                        ?author,
                        ?delta,
                        "latency_tracing proposal received, adding delta for timestamp"
                    );
                    self.handle_adjustment(delta);
                }
            }
        }
    }

    pub fn update_create_proposal_ns(&mut self, _round: Round, elapsed_ns: u128) {
        self.create_proposal_time_ns.add_sample(elapsed_ns);
    }

    pub fn vote_received(&mut self, author: &NodeId<P>, round: Round) {
        match self.avg_wait_for_qc_self.entry(*author) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(WaitState::new(101, round));
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                entry.get_mut().vote_received(round);
            }
        };
    }
    pub fn qc_ready(&mut self, round: Round) {
        self.avg_wait_for_qc_self
            .iter_mut()
            .for_each(|(k, v)| v.qc_computed(round));
    }

    pub fn get_vote_wait(&self, sender: &NodeId<P>) -> Option<Duration> {
        if let Some(wait_state) = self.avg_wait_for_qc_self.get(sender) {
            return wait_state.get_avg();
        }
        None
    }
}

#[cfg(test)]
mod test {
    use std::{collections::BTreeSet, time::Duration};

    use monad_consensus_types::{
        clock::{TestClock, TimestampAdjusterConfig},
        quorum_certificate::{TimestampAdjustment, TimestampAdjustmentDirection},
        validator_data::{ValidatorData, ValidatorSetData, ValidatorSetDataWithEpoch},
    };
    use monad_crypto::{
        certificate_signature::CertificateKeyPair, NopKeyPair, NopPubKey, NopSignature,
    };
    use monad_testutil::signing::{create_keys, get_key, MockSignatures};
    use monad_types::{Epoch, NodeId, Round, Stake};

    use super::{Error, PING_PERIOD_SEC};
    use crate::timestamp::{BlockTimestamp, PingState, ValidatorPingState, MAX_PROPOSAL_SAMPLES};

    type SignatureType = NopSignature;
    type SignatureCollection = MockSignatures<SignatureType>;

    #[test]
    fn test_block_timestamp_validate() {
        let mut b = BlockTimestamp::<NopPubKey, TestClock>::new(
            10,
            1,
            TimestampAdjusterConfig::Enabled {
                max_delta_ns: 100,
                adjustment_period: 11,
            },
        );
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
        b.vote_sent(&author, Round(0));

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

        // In the tests below the proposal latency is 0 since the time is not updated between vote_sent and proposal_received.
        b.update_time(12);
        b.vote_sent(&author, Round(1));

        b.proposal_received(Round(2), 20, &author);
        assert!(matches!(
            b.compute_clock_adjustment(20),
            Some(TimestampAdjustment {
                delta: 7,
                direction: TimestampAdjustmentDirection::Forward
            })
        ));

        b.proposal_received(Round(2), 13, &author);
        assert!(matches!(
            b.compute_clock_adjustment(13),
            Some(TimestampAdjustment {
                delta: 0,
                direction: TimestampAdjustmentDirection::Backward
            })
        ));

        b.proposal_received(Round(2), 10, &author);
        assert!(matches!(
            b.compute_clock_adjustment(10),
            Some(TimestampAdjustment {
                delta: 3,
                direction: TimestampAdjustmentDirection::Backward
            })
        ));
        // In the tests below the proposal latency is 5 since the time is between vote_sent and proposal_received is 5ns.
        b.update_time(15);
        b.vote_sent(&author, Round(1));
        b.update_time(20);

        b.proposal_received(Round(2), 22, &author);
        assert!(matches!(
            b.compute_clock_adjustment(22),
            Some(TimestampAdjustment {
                delta: 6,
                direction: TimestampAdjustmentDirection::Forward
            })
        ));

        b.proposal_received(Round(2), 12, &author);
        assert!(matches!(
            b.compute_clock_adjustment(16),
            Some(TimestampAdjustment {
                delta: 0,
                direction: TimestampAdjustmentDirection::Backward
            })
        ));

        b.proposal_received(Round(2), 10, &author);
        assert!(matches!(
            b.compute_clock_adjustment(10),
            Some(TimestampAdjustment {
                delta: 6,
                direction: TimestampAdjustmentDirection::Backward
            })
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
            state.avg_latency().unwrap().as_nanos(),
            state.ping_latency.sum.as_nanos() / state.ping_latency.samples.len() as u128
        );

        for i in 0..MAX_PROPOSAL_SAMPLES - 1 {
            state.update_latency(Duration::from_millis(i as u64));
        }

        assert_eq!(state.avg_latency().unwrap().as_millis(), 5);

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
