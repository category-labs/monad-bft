use std::{collections::BTreeMap, time::Duration};

use monad_crypto::certificate_signature::{CertificateSignaturePubKey, PubKey};
use monad_transformer::ID;
use monad_types::Round;
use monad_updaters::ledger::MockableLedger;

use crate::{mock_swarm::Nodes, swarm_relation::SwarmRelation};

pub trait NodesTerminator<S>
where
    S: SwarmRelation,
{
    fn should_terminate(&mut self, nodes: &Nodes<S>, next_tick: Duration) -> bool;
}

#[derive(Clone, Copy)]
pub struct UntilTerminator {
    until_tick: Duration,
    until_block: usize,
    until_round: Round,
    until_step: usize,
}

impl Default for UntilTerminator {
    fn default() -> Self {
        Self::new()
    }
}

impl UntilTerminator {
    pub fn new() -> Self {
        UntilTerminator {
            until_tick: Duration::MAX,
            until_block: usize::MAX,
            until_round: Round(u64::MAX),
            until_step: usize::MAX,
        }
    }

    pub fn until_tick(mut self, tick: Duration) -> Self {
        self.until_tick = tick;
        self
    }

    pub fn until_block(mut self, b_cnt: usize) -> Self {
        self.until_block = b_cnt;
        self
    }

    pub fn until_round(mut self, round: Round) -> Self {
        self.until_round = round;
        self
    }

    /// run for N number of steps
    /// note that this behavior might differ for step_batch
    /// this is because multiple events may be emitted per logical "step"
    pub fn until_step(mut self, step: usize) -> Self {
        assert!(step >= 1);
        self.until_step = step;
        self
    }
}

impl<S> NodesTerminator<S> for UntilTerminator
where
    S: SwarmRelation,
{
    fn should_terminate(&mut self, nodes: &Nodes<S>, next_tick: Duration) -> bool {
        let should_terminate = self.until_step == 0
            || next_tick > self.until_tick
            || nodes
                .states
                .values()
                .any(|node| node.executor.ledger().get_blocks().len() > self.until_block)
            || nodes
                .states
                .values()
                .any(|node| node.state.consensus().get_current_round() > self.until_round);
        self.until_step -= 1;
        should_terminate
    }
}

// observe and monitor progress of certain nodes until commit progress is achieved for all
#[derive(Clone)]
pub struct ProgressTerminator<PT: PubKey> {
    // NodeId -> Ledger len
    nodes_monitor: BTreeMap<ID<PT>, usize>,
    timeout: Duration,
}

impl<PT: PubKey> ProgressTerminator<PT> {
    pub fn new(nodes_monitor: BTreeMap<ID<PT>, usize>, timeout: Duration) -> Self {
        ProgressTerminator {
            nodes_monitor,
            timeout,
        }
    }

    pub fn extend_all(&mut self, progress: usize) {
        // extend the required termination progress of all monitor
        for original_progress in self.nodes_monitor.values_mut() {
            *original_progress += progress;
        }
    }
}

impl<S> NodesTerminator<S> for ProgressTerminator<CertificateSignaturePubKey<S::SignatureType>>
where
    S: SwarmRelation,
{
    fn should_terminate(&mut self, nodes: &Nodes<S>, _next_tick: Duration) -> bool {
        if nodes.tick > self.timeout {
            panic!(
                "ProgressTerminator timed-out, expecting nodes 
                to reach following progress before timeout: {:?},
                but the actual progress is: {:?}",
                self.nodes_monitor,
                nodes
                    .states
                    .iter()
                    .map(|(id, nodes)| (id, nodes.executor.ledger().get_blocks().len()))
                    .collect::<BTreeMap<_, _>>()
            );
        }

        let mut block_ref = None;
        for (peer_id, expected_len) in &self.nodes_monitor {
            let blocks = nodes
                .states
                .get(peer_id)
                .expect("node must exists")
                .executor
                .ledger()
                .get_blocks();
            if blocks.len() < *expected_len {
                return false;
            }
            match block_ref {
                None => block_ref = Some(blocks),
                Some(reference) => {
                    if reference.len() < blocks.len() {
                        block_ref = Some(blocks);
                    }
                }
            }
        }

        // reference to the longest ledger
        let block_ref = block_ref.expect("must have at least 1 entry");
        // once termination condition is met, all the ledger should also have identical blocks
        for (peer_id, expected_len) in &self.nodes_monitor {
            let blocks = nodes
                .states
                .get(peer_id)
                .expect("node must exists")
                .executor
                .ledger()
                .get_blocks();
            for i in 0..(*expected_len) {
                assert!(block_ref[i] == blocks[i]);
            }
        }

        true
    }
}
