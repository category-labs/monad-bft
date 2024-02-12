use std::time::Duration;

use hex::ToHex;
use monad_consensus_types::metrics::Metrics;
use monad_crypto::certificate_signature::{CertificateSignaturePubKey, PubKey};
use monad_transformer::ID;
use monad_types::Round;

use crate::{mock_swarm::Nodes, swarm_relation::SwarmRelation};

type FetchMetricFunction = fn(&Metrics) -> (u64, &str);

#[macro_export]
macro_rules! fetch_metrics {
    ( $( $k:ident ).+ ) => {{
         |s: &Metrics| { (s.$($k).+, stringify!($($k).+)) }
    }};
}

#[derive(Debug, PartialEq, Eq)]
enum ExpectedTick {
    /// Exact tick timestamp
    Exact(Duration),
    /// Range(tick, delta)
    /// Expects swarm tick in the range [tick - delta, tick + delta]
    Range(Duration, Duration),
    /// No expected
    None,
}

#[derive(Debug)]
enum ExpectedMetric {
    // Exact metric value
    Exact(u64),
    // Expect metric is in the range [lower, upper]
    Range(u64, u64),
    // Minimum metric value
    Threshold(u64),
}

#[derive(Debug)]
pub struct MockSwarmVerifier<S: SwarmRelation> {
    tick: ExpectedTick,

    metrics: Vec<(
        ID<CertificateSignaturePubKey<S::SignatureType>>,
        FetchMetricFunction,
        ExpectedMetric,
    )>,
}

impl<S: SwarmRelation> Default for MockSwarmVerifier<S> {
    fn default() -> Self {
        Self {
            tick: ExpectedTick::None,
            metrics: Vec::new(),
        }
    }
}

impl<S: SwarmRelation> MockSwarmVerifier<S> {
    pub fn tick_exact(mut self, tick: Duration) -> Self {
        if self.tick != ExpectedTick::None {
            panic!("Tick verify already set");
        }
        self.tick = ExpectedTick::Exact(tick);
        self
    }

    pub fn tick_range(mut self, mean_tick: Duration, delta: Duration) -> Self {
        if self.tick != ExpectedTick::None {
            panic!("Tick verify already set");
        }
        self.tick = ExpectedTick::Range(mean_tick, delta);
        self
    }

    pub fn metric_exact(
        mut self,
        node_ids: Vec<ID<CertificateSignaturePubKey<S::SignatureType>>>,
        fetch_metric: FetchMetricFunction,
        value: u64,
    ) -> Self {
        for node_id in node_ids {
            self.metrics
                .push((node_id, fetch_metric, ExpectedMetric::Exact(value)));
        }
        self
    }

    pub fn metric_range(
        mut self,
        node_ids: Vec<ID<CertificateSignaturePubKey<S::SignatureType>>>,
        fetch_metric: FetchMetricFunction,
        lower: u64,
        upper: u64,
    ) -> Self {
        for node_id in node_ids {
            self.metrics
                .push((node_id, fetch_metric, ExpectedMetric::Range(lower, upper)));
        }
        self
    }

    pub fn metric_threshold(
        mut self,
        node_ids: Vec<ID<CertificateSignaturePubKey<S::SignatureType>>>,
        fetch_metric: FetchMetricFunction,
        minimum: u64,
    ) -> Self {
        for node_id in node_ids {
            self.metrics
                .push((node_id, fetch_metric, ExpectedMetric::Threshold(minimum)));
        }
        self
    }
}

impl<S: SwarmRelation> MockSwarmVerifier<S> {
    pub fn verify(&self, swarm: &Nodes<S>) -> bool {
        let mut verification_passed = true;

        let actual_tick = swarm.tick;
        match self.tick {
            ExpectedTick::Exact(tick) => {
                if actual_tick != tick {
                    eprintln!(
                        "Tick verify error expected={:?} actual={:?}",
                        tick, swarm.tick
                    );
                    verification_passed = false;
                }
            }
            ExpectedTick::Range(mean, delta) => {
                let lower = mean.saturating_sub(delta);
                let upper = mean.saturating_add(delta);
                if actual_tick < lower || actual_tick > upper {
                    eprintln!(
                        "Tick verify error expected=[{:?},{:?}] actual={:?}",
                        lower, upper, actual_tick
                    );
                    verification_passed = false;
                }
            }
            ExpectedTick::None => {}
        }

        for (node_id, fetch_metric, expected_metric) in self.metrics.iter() {
            let node = swarm.states.get(node_id).unwrap();
            let node_id_as_hex: String = node_id.get_peer_id().pubkey().bytes().encode_hex();
            let (actual_metric, metric_name) = fetch_metric(node.state.metrics());
            match expected_metric {
                ExpectedMetric::Exact(metric) => {
                    if actual_metric != *metric {
                        eprintln!(
                            "Metric verify error: node_id: {}, metric: {}, expected={:?} actual={:?}",
                            node_id_as_hex, metric_name, metric, actual_metric
                        );
                        verification_passed = false;
                    }
                }
                ExpectedMetric::Range(lower, upper) => {
                    if actual_metric < *lower || actual_metric > *upper {
                        eprintln!(
                            "Metric verify error: node_id: {}, metric: {}, expected=[{:?},{:?}] actual={:?}",
                            node_id_as_hex, metric_name, lower, upper, actual_metric
                        );
                        verification_passed = false;
                    }
                }
                ExpectedMetric::Threshold(minimum) => {
                    if actual_metric < *minimum {
                        eprintln!(
                            "Metric verify error: node_id: {}, metric: {}, expected>={:?} actual={:?}",
                            node_id_as_hex, metric_name, minimum, actual_metric
                        );
                        verification_passed = false;
                    }
                }
            }
        }

        verification_passed
    }
}

/// Computes the tick number for a happy path NoSer-like swarm to finish on a
/// certain round. For the NoSer-like swarm, message is delivered exactly after
/// exactly delta, regardless of message size
pub fn happy_path_tick_by_round(round: Round, delta: Duration) -> Duration {
    // (1 <timeout> + (epoch_start_round - 1 <the leader enters before delivered
    // to other nodes>) * 2 <round trip>) * delta
    delta * (1 + (round.0 - 1) * 2) as u32
}

/// Computes the tick number for a happy path NoSer-like swarm to finish when
/// any nodes has committed given number of blocks
pub fn happy_path_tick_by_block(block: usize, delta: Duration) -> Duration {
    // (1 <timeout> + (epoch_start_round + 1 <leader forms a QC-of-QC to commit>) *
    // 2 <round trip>) * delta
    delta * (1 + (block + 2) * 2) as u32
}
