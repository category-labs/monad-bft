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

use bytes::Bytes;
use monad_mcp_common::{FinalizationPath, Lanes, Proposal, Slot, SlotLifecycle, Timestamp};
use monad_mcp_ledger::Tx;
use rand_chacha::ChaCha8Rng;

use super::Component;
use crate::{
    common::NodeId,
    config::{ProposalConfig, SourceKind},
    proposing::{
        self, FixedLeadPlanner, Mempool, MempoolInput, MempoolSource, ProposingRuntime,
        RandomSource, VoteReceived,
    },
};

pub enum ProposingInput {
    Lifecycle(SlotLifecycle),
    // demo(tx-timeline): `at` stamps mempool admission
    Tx { at: Timestamp, from: NodeId, tx: Tx },
    // the lanes the slot's finalization commits, and the path it took
    Finalized(Slot, Lanes<bool>, FinalizationPath),
    // every lane and every voter, this node included
    VoteReceived(VoteReceived),
}

// a sealed proposal for DA, with the first-hop send order the planner
// chose; None: DA's own order (chunk-id order)
pub struct ProposingOutput {
    pub proposal: Proposal<Bytes>,
    pub send_order: Option<Vec<NodeId>>,
}

impl From<Proposal<Bytes>> for ProposingOutput {
    fn from(proposal: Proposal<Bytes>) -> Self {
        Self {
            proposal,
            send_order: None,
        }
    }
}

// any proposing component; the node holds one and never knows which
pub type Proposing = Box<dyn Component<Input = ProposingInput, Output = ProposingOutput> + Send>;

impl Component for Proposing {
    type Input = ProposingInput;
    type Output = ProposingOutput;

    fn handle(&mut self, now: Timestamp, input: ProposingInput) {
        (**self).handle(now, input);
    }

    fn poll(&mut self) -> Option<ProposingOutput> {
        (**self).poll()
    }

    fn next_due(&self) -> Option<Timestamp> {
        (**self).next_due()
    }

    fn handle_due(&mut self, now: Timestamp) {
        (**self).handle_due(now);
    }
}

// decides when we propose in a slot and proposes the mempool's head
pub struct MempoolProposing {
    runtime: ProposingRuntime<MempoolSource>,
}

impl MempoolProposing {
    pub fn new(planner: FixedLeadPlanner, config: &ProposalConfig, mempool: Mempool) -> Self {
        let source = MempoolSource::new(mempool, config.proposal_size_limit());
        let runtime = ProposingRuntime::new(planner, source);
        Self { runtime }
    }
}

impl Component for MempoolProposing {
    type Input = ProposingInput;
    type Output = ProposingOutput;

    fn handle(&mut self, now: Timestamp, input: ProposingInput) {
        use proposing::ProposingInput as RuntimeInput;

        let input = match input {
            ProposingInput::Lifecycle(event) => RuntimeInput::Lifecycle(event),
            ProposingInput::Tx { at, from, tx } => {
                RuntimeInput::Source(MempoolInput::Tx { tx, from, at })
            }
            ProposingInput::Finalized(slot, committed, _) => {
                RuntimeInput::Source(MempoolInput::Settled { slot, committed })
            }
            // no planner reads votes yet
            ProposingInput::VoteReceived(_) => return self.runtime.handle_due(now),
        };
        self.runtime.handle_input(input, now);
    }

    fn next_due(&self) -> Option<Timestamp> {
        self.runtime.next_due()
    }

    fn handle_due(&mut self, now: Timestamp) {
        self.runtime.handle_due(now);
    }

    fn poll(&mut self) -> Option<ProposingOutput> {
        self.runtime.poll().map(ProposingOutput::from)
    }
}

// proposes random bytes when the schedule says so, for load tests without
// clients and for scenarios that do not care what a proposal carries; with
// the fixed source, every proposal is exactly the size limit
pub struct RandomProposing {
    runtime: ProposingRuntime<RandomSource>,
}

impl RandomProposing {
    pub fn new(planner: FixedLeadPlanner, config: &ProposalConfig, rng: ChaCha8Rng) -> Self {
        let limit = config.proposal_size_limit();
        let source = match config.source {
            SourceKind::Fixed => RandomSource::fixed_size(limit, rng),
            SourceKind::Random | SourceKind::Mempool => RandomSource::log_uniform(limit, rng),
        };
        let runtime = ProposingRuntime::new(planner, source);
        Self { runtime }
    }
}

impl Component for RandomProposing {
    type Input = ProposingInput;
    type Output = ProposingOutput;

    fn handle(&mut self, now: Timestamp, input: ProposingInput) {
        use proposing::ProposingInput as RuntimeInput;

        match input {
            ProposingInput::Lifecycle(event) => {
                self.runtime
                    .handle_input(RuntimeInput::Lifecycle(event), now);
            }
            ProposingInput::Tx { from, .. } => {
                tracing::debug!(?from, "no mempool, tx dropped");
                self.runtime.handle_due(now);
            }
            ProposingInput::Finalized(..) | ProposingInput::VoteReceived(_) => {
                self.runtime.handle_due(now);
            }
        }
    }

    fn next_due(&self) -> Option<Timestamp> {
        self.runtime.next_due()
    }

    fn handle_due(&mut self, now: Timestamp) {
        self.runtime.handle_due(now);
    }

    fn poll(&mut self) -> Option<ProposingOutput> {
        self.runtime.poll().map(ProposingOutput::from)
    }
}

#[cfg(test)]
mod tests {
    use monad_mcp_ledger::decode_sealed_batch; // demo(tx-timeline)

    use super::*;
    use crate::config::NodeConfig;

    fn config() -> NodeConfig {
        NodeConfig::single_node(0, Timestamp::from_millis(10_000), "unused-ledger")
    }

    fn proposing_of(config: &NodeConfig) -> MempoolProposing {
        let epoch_handle = config.epoch_manager().unwrap().genesis();
        let planner = config.planner(epoch_handle);
        let mempool = Mempool::new(config.mempool.into());
        MempoolProposing::new(planner, &config.proposal, mempool)
    }

    // demo(tx-timeline)
    #[test]
    fn a_proposal_is_sealed_at_the_time_it_is_released() {
        let mut proposing = proposing_of(&config());
        let deadline = Timestamp::from_millis(10_000);
        let slot = Slot(0);
        let opened = SlotLifecycle::Opened { slot, deadline };
        proposing.handle(
            Timestamp::from_millis(1_000),
            ProposingInput::Lifecycle(opened),
        );
        let due = proposing.next_due().expect("a seal is due");
        proposing.handle_due(due);

        let Some(ProposingOutput { proposal, .. }) = proposing.poll() else {
            panic!("a proposal at the seal time");
        };
        assert_eq!(proposal.slot, Slot(0));
        let sealed_at_ns = due.as_nanos() as u64;
        assert_eq!(
            decode_sealed_batch(&proposal.payload).unwrap(),
            (sealed_at_ns, vec![])
        );
    }
}
