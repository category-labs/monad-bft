use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
    time,
};

use monad_consensus_types::{
    quorum_certificate::{TimestampAdjustment, TimestampAdjustmentDirection},
    signature_collection::SignatureCollection,
    validator_data::ValidatorData,
};
use monad_crypto::certificate_signature::PubKey;
use monad_types::{NodeId, PingSequence};
use tracing::info;

const MAX_LATENCY_SAMPLES: usize = 100;

/// Ping state per validator
#[derive(Debug)]
pub struct ValidatorPingState {
    sequence: PingSequence,              // sequence number of the last ping sent
    latencies: Vec<time::Duration>,      // latencies of the last pings
    sum: time::Duration,                 // sum of the last latencies
    next_index: usize,                   // index of the next latency to be replaced
    last_ping_time: time::Instant,       // time of the last ping sent
    avg_latency: Option<time::Duration>, // average latency
}

impl Default for ValidatorPingState {
    fn default() -> Self {
        Self {
            sequence: PingSequence(0),
            next_index: 0,
            sum: Default::default(),
            latencies: Vec::new(),
            last_ping_time: time::Instant::now(),
            avg_latency: None,
        }
    }
}

impl ValidatorPingState {
    pub fn new() -> Self {
        Self::default()
    }

    // initiate a new ping
    pub fn start_next(&mut self) -> PingSequence {
        self.sequence.0 += 1;
        self.last_ping_time = time::Instant::now();
        self.sequence
    }

    fn update_latency(&mut self, latency: time::Duration) {
        if self.latencies.len() == MAX_LATENCY_SAMPLES {
            self.sum -= self.latencies[self.next_index];
            self.latencies[self.next_index] = latency;
            self.next_index = (self.next_index + 1) % MAX_LATENCY_SAMPLES;
        } else {
            self.latencies.push(latency);
        }
        self.sum += latency;
        self.avg_latency = Some(self.sum / self.latencies.len() as u32);
    }

    pub fn pong_received(&mut self, sequence: PingSequence) {
        if sequence != self.sequence {
            return;
        }
        self.update_latency(self.last_ping_time.elapsed());
    }

    pub fn avg_latency(&self) -> Option<time::Duration> {
        self.avg_latency
    }
}

const PING_PERIOD: time::Duration = time::Duration::from_secs(30);

#[derive(Debug)]
struct PingState<P: PubKey> {
    validators: HashMap<NodeId<P>, ValidatorPingState>, // ping state per validator
    schedule: Vec<Vec<NodeId<P>>>, // schedule of validators to ping, length is period
    tick: usize,                   // current tick index into schedule
    period: usize,                 // number of ticks in full schedule
}

impl<P: PubKey> PingState<P> {
    pub fn new() -> Self {
        let period = PING_PERIOD.as_secs() as usize;
        Self {
            validators: HashMap::new(),
            schedule: vec![Vec::new(); period],
            tick: 0,
            period,
        }
    }

    pub fn pong_received(&mut self, node_id: NodeId<P>, sequence: PingSequence) {
        if let Some(validator_state) = self.validators.get_mut(&node_id) {
            validator_state.pong_received(sequence);
            info!(
                "node {:?} latency {:?}",
                node_id,
                validator_state.avg_latency()
            );
        }
    }

    fn update_validators<SCT>(&mut self, validators: &Vec<ValidatorData<SCT>>, my_node: &NodeId<P>)
    where
        SCT: SignatureCollection<NodeIdPubKey = P>,
    {
        let removed_nodes = self
            .validators
            .keys()
            .filter(|node_id| !validators.iter().any(|v| v.node_id == **node_id))
            .cloned()
            .collect::<Vec<_>>();
        for node_id in removed_nodes {
            self.validators.remove(&node_id);
        }
        for validator in validators {
            if validator.node_id == *my_node {
                continue;
            }
            self.validators
                .entry(NodeId::new(validator.node_id.pubkey()))
                .or_default();
        }

        // map validators to schedule slots by hashing node id
        for s in self.schedule.iter_mut() {
            s.clear();
        }
        for node in self.validators.keys() {
            let mut hasher = DefaultHasher::new();
            node.hash(&mut hasher);
            let idx = (hasher.finish() as usize) % self.period;
            self.schedule[idx].push(*node);
        }
        info!("updating validators {:?}", self.schedule);
    }

    // returns list of nodes to send pings to on this tick
    fn tick(&mut self) -> Vec<(NodeId<P>, PingSequence)> {
        let mut pings = Vec::new();
        for node_id in self.schedule[self.tick].iter() {
            let validator_state = self.validators.get_mut(node_id).unwrap();
            let sequence = validator_state.start_next();
            pings.push((*node_id, sequence));
        }
        self.tick = (self.tick + 1) % self.period;
        pings
    }
}

#[derive(Debug)]
pub struct BlockTimestamp<P: PubKey> {
    local_time: u64,

    max_delta: u64,

    ping_state: PingState<P>,

    /// TODO: this needs an upper-bound
    latency_estimate_ms: u64,
}

impl<P: PubKey> BlockTimestamp<P> {
    pub fn new(max_delta: u64, latency_estimate_ms: u64) -> Self {
        assert!(latency_estimate_ms > 0);
        Self {
            local_time: 0,
            max_delta,
            latency_estimate_ms,
            ping_state: PingState::new(),
        }
    }

    pub fn update_time(&mut self, time: u64) {
        self.local_time = time;
    }

    pub fn get_current_time(&self) -> u64 {
        self.local_time
    }

    pub fn get_valid_block_timestamp(&self, prev_block_ts: u64) -> u64 {
        if self.local_time <= prev_block_ts {
            prev_block_ts + 1
        } else {
            self.local_time
        }
    }

    fn valid_bounds(&self, timestamp: u64) -> bool {
        let lower_bound = self.local_time.saturating_sub(self.max_delta);
        let upper_bound = self.local_time.saturating_add(self.max_delta);

        lower_bound <= timestamp && timestamp <= upper_bound
    }

    pub fn valid_block_timestamp(
        &self,
        prev_block_ts: u64,
        curr_block_ts: u64,
    ) -> Option<TimestampAdjustment> {
        let delta = curr_block_ts.checked_sub(prev_block_ts);
        match delta {
            // block timestamp must be strictly monotonically increasing
            None => None,
            Some(0) => None,
            // check that its not higher than the valid upper bound
            Some(_) => {
                if !self.valid_bounds(curr_block_ts) {
                    None
                } else {
                    // return the delta between local time and block time for adjustment
                    let mut adjustment = self.local_time.abs_diff(curr_block_ts);
                    // adjust for estimated latency
                    adjustment = adjustment.saturating_sub(self.latency_estimate_ms);
                    if curr_block_ts > self.local_time {
                        Some(TimestampAdjustment {
                            delta: adjustment,
                            direction: TimestampAdjustmentDirection::Forward,
                        })
                    } else {
                        Some(TimestampAdjustment {
                            delta: adjustment,
                            direction: TimestampAdjustmentDirection::Backward,
                        })
                    }
                }
            }
        }
    }

    pub fn update_validators<SCT>(
        &mut self,
        validators: &Vec<ValidatorData<SCT>>,
        my_node: &NodeId<P>,
    ) where
        SCT: SignatureCollection<NodeIdPubKey = P>,
    {
        self.ping_state.update_validators(validators, my_node);
    }

    pub fn tick(&mut self) -> Vec<(NodeId<P>, PingSequence)> {
        self.ping_state.tick()
    }

    pub fn pong_received(&mut self, node_id: NodeId<P>, sequence: PingSequence) {
        self.ping_state.pong_received(node_id, sequence);
    }
}

#[cfg(test)]
mod test {
    use std::time;

    use monad_consensus_types::quorum_certificate::{
        TimestampAdjustment, TimestampAdjustmentDirection,
    };
    use monad_crypto::{
        certificate_signature::CertificateKeyPair, NopKeyPair, NopPubKey, NopSignature,
    };
    use monad_types::{NodeId, Stake};

    use crate::{
        timestamp::{PingState, ValidatorPingState},
        BlockTimestamp, ValidatorData,
    };

    #[test]
    fn test_block_timestamp_validate() {
        let mut b = BlockTimestamp::<NopPubKey>::new(10, 1);
        b.update_time(0);

        assert!(b.valid_block_timestamp(1, 1).is_none());
        assert!(b.valid_block_timestamp(2, 1).is_none());
        assert!(b.valid_block_timestamp(0, 11).is_none());

        assert!(matches!(
            b.valid_block_timestamp(1, 2),
            Some(TimestampAdjustment {
                delta: 1,
                direction: TimestampAdjustmentDirection::Forward
            })
        ));

        b.update_time(10);

        assert!(matches!(
            b.valid_block_timestamp(5, 8),
            Some(TimestampAdjustment {
                delta: 1,
                direction: TimestampAdjustmentDirection::Backward
            })
        ));
    }

    #[test]
    fn test_ping_state() {
        let mut state = ValidatorPingState::new();
        let seq = state.start_next();
        state.update_latency(time::Duration::from_millis(1));

        assert_eq!(state.avg_latency().unwrap().as_millis(), 1);
        state.update_latency(time::Duration::from_millis(3));
        assert_eq!(state.avg_latency().unwrap().as_millis(), 2);
        state.update_latency(time::Duration::from_millis(6));
        assert_eq!(state.avg_latency().unwrap().as_millis(), 3);

        for _ in 0..100 {
            state.update_latency(time::Duration::from_millis(10));
        }

        assert_eq!(state.sum, state.latencies.iter().sum::<time::Duration>());
        assert_eq!(
            state.avg_latency().unwrap(),
            state.sum / state.latencies.len() as u32
        );

        for _ in 0..50 {
            state.update_latency(time::Duration::from_millis(50));
        }

        assert_eq!(state.sum, state.latencies.iter().sum::<time::Duration>());
        assert_eq!(
            state.avg_latency().unwrap(),
            state.sum / state.latencies.len() as u32
        );
    }

    use monad_testutil::signing::MockSignatures;

    use super::PING_PERIOD;

    type SignatureType = NopSignature;
    type SignatureCollection = MockSignatures<SignatureType>;

    #[test]
    fn test_update_validators() {
        let mut s = PingState::<NopPubKey>::new();

        let my_key = NopKeyPair::from_bytes(&mut [0; 32]).unwrap().pubkey();
        let my_node = NodeId::new(my_key);

        let k_1 = NopKeyPair::from_bytes(&mut [1; 32]).unwrap().pubkey();
        let node_1 = NodeId::new(k_1);

        let k_2 = NopKeyPair::from_bytes(&mut [2; 32]).unwrap().pubkey();
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

        s.update_validators(&validators, &my_node);

        assert_eq!(s.validators.len(), 2);

        let validators_1 = vec![validators[0].clone()];

        s.update_validators(&validators_1, &my_node);

        assert_eq!(s.validators.len(), 1);
        assert!(s.validators.contains_key(&node_1));

        let validators_2 = vec![validators[1].clone()];
        s.update_validators(&validators_2, &my_node);

        assert_eq!(s.validators.len(), 1);
        assert!(s.validators.contains_key(&node_2));
    }

    #[test]
    fn test_ticks() {
        let mut s = PingState::<NopPubKey>::new();

        let my_key = NopKeyPair::from_bytes(&mut [0; 32]).unwrap().pubkey();
        let my_node = NodeId::new(my_key);

        let k_1 = NopKeyPair::from_bytes(&mut [1; 32]).unwrap().pubkey();
        let node_1 = NodeId::new(k_1);

        let k_2 = NopKeyPair::from_bytes(&mut [2; 32]).unwrap().pubkey();
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

        s.update_validators(&validators, &my_node);

        let mut pings = Vec::new();
        for _ in 0..PING_PERIOD.as_secs() {
            pings.extend(s.tick());
        }

        assert_eq!(
            pings.len(),
            validators.len(),
            "number of pings in total period should match number of validators"
        );
    }
}
