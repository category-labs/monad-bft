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

use std::{collections::HashMap, net::SocketAddr, num::NonZeroU64, sync::Arc};

use serde::Deserialize;

use crate::{
    chorus::{
        conductor::{ConductorConfig, ConductorError},
        proposing::PlannerConfig,
        slot::chorus::ChorusConfig,
        types::{
            KeyPair, NodeId, ProposerConfig, PubKey, RotatingProposerSchedule,
            RoundRobinLeaderSchedule, ScheduleError, Stake, Timestamp, TimestampDelta,
            ValidatorData,
        },
    },
    da::{self, ProposalKeyPair, header_auth},
    epoch::{EpochHandle, NodeProposerSchedule},
    repeater::RepeaterConfig,
};

#[derive(Deserialize)]
pub struct NodeConfig {
    #[serde(deserialize_with = "de::node_id")]
    pub node_id: NodeId,
    #[serde(deserialize_with = "de::proposal_key_pair")]
    pub proposal_key_pair: ProposalKeyPair,
    #[serde(deserialize_with = "de::key_pair")]
    pub cadence_key_pair: KeyPair,
    pub validators: Vec<ValidatorConfig>,

    // todo: move this into cadence config.
    #[serde(deserialize_with = "de::unix_millis")]
    pub genesis_deadline: Timestamp,
    pub network: NetworkConfig,
    #[serde(default)]
    pub cadence: CadenceConfig,
    #[serde(default)]
    pub da: DAConfig,
    #[serde(default)]
    pub proposal: ProposalConfig,
    // Absent table: outbound slot messages are never re-sent.
    pub repeater: Option<RepeaterSection>,
}

impl NodeConfig {
    pub fn repeater(&self) -> Option<RepeaterConfig> {
        self.repeater.map(|section| RepeaterConfig {
            interval: section.interval,
            certificate_retention: section.certificate_retention,
        })
    }

    pub fn epoch_handle(&self) -> Result<EpochHandle, ScheduleError> {
        let validator_data = Arc::new(self.validator_data());
        let proposers = Arc::new(self.proposal.schedule(validator_data.clone())?);
        let header_auth = Arc::new(header_auth(proposers.clone(), validator_data.clone()));

        Ok(EpochHandle {
            self_id: self.node_id,
            key: Arc::new(self.cadence_key_pair.clone()),
            proposal_key: Arc::new(self.proposal_key_pair.clone()),
            validator_data,
            proposers,
            header_auth,
        })
    }

    pub fn peers(&self) -> HashMap<NodeId, SocketAddr> {
        let mut peers = HashMap::new();
        for validator in &self.validators {
            peers.insert(validator.node_id, validator.address);
        }
        peers
    }

    fn validator_data(&self) -> ValidatorData {
        let mut valset = HashMap::new();
        let mut mapping = HashMap::new();
        for validator in &self.validators {
            valset.insert(validator.node_id, Stake::from(validator.stake));
            mapping.insert(validator.node_id, validator.chorus_pubkey);
        }
        ValidatorData::new(valset, mapping)
    }
}

// todo: the proposal pubkey. The stub proposal signature recovers the
// author's NodeId, so it has no key to configure yet.
#[derive(Deserialize)]
pub struct ValidatorConfig {
    #[serde(deserialize_with = "de::node_id")]
    pub node_id: NodeId,
    pub stake: u64,
    #[serde(deserialize_with = "de::pubkey")]
    pub chorus_pubkey: PubKey,
    pub address: SocketAddr,
}

#[derive(Clone, Copy, Deserialize)]
pub struct NetworkConfig {
    pub port: u16,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(default)]
pub struct CadenceConfig {
    #[serde(deserialize_with = "de::millis")]
    pub delta: TimestampDelta,
    #[serde(deserialize_with = "de::millis")]
    pub slot_interval: TimestampDelta,
    pub slots_per_window: NonZeroU64,
    pub sync_boundary_slots: NonZeroU64,
    // How far a peer's announced cap must lead the local one before it is
    // trusted as a jump; one window by default.
    pub lag_threshold: NonZeroU64,
}

impl Default for CadenceConfig {
    fn default() -> Self {
        Self {
            // todo: set proper default parameters
            delta: TimestampDelta::from_millis(150),
            slot_interval: TimestampDelta::from_millis(100),
            slots_per_window: NonZeroU64::new(100).expect("nonzero"),
            sync_boundary_slots: NonZeroU64::new(80).expect("nonzero"),
            lag_threshold: NonZeroU64::new(20).expect("nonzero"),
        }
    }
}

impl CadenceConfig {
    pub fn conductor(
        &self,
        genesis_deadline: Timestamp,
    ) -> Result<ConductorConfig, ConductorError> {
        ConductorConfig::new(
            self.slots_per_window,
            self.sync_boundary_slots,
            self.slot_interval,
            genesis_deadline,
            self.lag_threshold,
        )
    }

    pub fn chorus(&self) -> ChorusConfig {
        ChorusConfig { delta: self.delta }
    }
}

#[derive(Clone, Copy, Deserialize)]
#[serde(default)]
pub struct RepeaterSection {
    #[serde(deserialize_with = "de::millis")]
    pub interval: TimestampDelta,
    pub certificate_retention: u64,
}

impl Default for RepeaterSection {
    fn default() -> Self {
        Self {
            interval: TimestampDelta::from_millis(5_000),
            // matches the DA layer's completed_slot_retention
            certificate_retention: 50,
        }
    }
}

#[derive(Clone, Copy, Deserialize)]
#[serde(default)]
pub struct DAConfig {
    pub completed_slot_retention: u64,
}

impl Default for DAConfig {
    fn default() -> Self {
        Self {
            completed_slot_retention: 50,
        }
    }
}

impl DAConfig {
    pub fn runtime(&self) -> da::DAConfig {
        da::DAConfig {
            completed_slot_retention: self.completed_slot_retention,
        }
    }
}

#[derive(Clone, Copy, Deserialize)]
#[serde(default)]
pub struct ProposalConfig {
    pub num_proposals: usize,
    // how long before the slot's deadline we propose
    #[serde(deserialize_with = "de::millis")]
    pub propose_before_deadline: TimestampDelta,
    // a seal with less lead than this is withheld instead of disseminated;
    // 0 disables the gate. Set it to the cadence delta to withhold every
    // proposal that cannot arrive before the deadline.
    #[serde(deserialize_with = "de::millis")]
    pub withhold_before_deadline: TimestampDelta,
}

impl ProposalConfig {
    // TODO: observation_cutoff is a Cadence deployment constant and must be
    // the same value every consumer sees; derive it from the conductor
    // configuration once that carries the parameter.
    const OBSERVATION_CUTOFF: u64 = 5;
    const ROTATION_SLACK: u64 = 3;
    const SLOTS_PER_EPOCH: u64 = 400;

    fn proposer_config(&self) -> ProposerConfig {
        ProposerConfig {
            concurrent_proposers: self.num_proposals,
            observation_cutoff: Self::OBSERVATION_CUTOFF,
            rotation_slack: Self::ROTATION_SLACK,
            slots_per_epoch: Self::SLOTS_PER_EPOCH,
        }
    }

    fn schedule(
        &self,
        validator_data: Arc<ValidatorData>,
    ) -> Result<NodeProposerSchedule, ScheduleError> {
        let cfg = self.proposer_config();
        let algorithm = RoundRobinLeaderSchedule::new(&cfg);
        RotatingProposerSchedule::new(cfg, algorithm, validator_data)
    }

    // the gate's cutoff is the schedule's own: the two cannot disagree
    pub fn planner(&self, proposers: &NodeProposerSchedule) -> PlannerConfig {
        PlannerConfig {
            lead: self.propose_before_deadline,
            min_lead: self.withhold_before_deadline,
            observation_cutoff: proposers.config().observation_cutoff,
        }
    }
}

impl Default for ProposalConfig {
    fn default() -> Self {
        Self {
            num_proposals: 5,
            propose_before_deadline: TimestampDelta::from_millis(500),
            withhold_before_deadline: TimestampDelta::ZERO,
        }
    }
}

// the stub env derives every key from a u64
mod de {
    use monad_mcp_chorus::spec::vote::KeyPair as _;
    use serde::{Deserialize, Deserializer};

    use crate::{
        chorus::types::{KeyPair, NodeId, PubKey, Timestamp, TimestampDelta},
        da::ProposalKeyPair,
    };

    pub fn node_id<'de, D: Deserializer<'de>>(d: D) -> Result<NodeId, D::Error> {
        u64::deserialize(d).map(NodeId::dummy)
    }

    pub fn key_pair<'de, D: Deserializer<'de>>(d: D) -> Result<KeyPair, D::Error> {
        u64::deserialize(d).map(KeyPair::dummy)
    }

    pub fn pubkey<'de, D: Deserializer<'de>>(d: D) -> Result<PubKey, D::Error> {
        let key_pair = key_pair(d)?;
        Ok(key_pair.pubkey())
    }

    pub fn proposal_key_pair<'de, D: Deserializer<'de>>(d: D) -> Result<ProposalKeyPair, D::Error> {
        node_id(d).map(ProposalKeyPair::dummy)
    }

    pub fn millis<'de, D: Deserializer<'de>>(d: D) -> Result<TimestampDelta, D::Error> {
        u64::deserialize(d).map(TimestampDelta::from_millis)
    }

    pub fn unix_millis<'de, D: Deserializer<'de>>(d: D) -> Result<Timestamp, D::Error> {
        u64::deserialize(d).map(Timestamp::from_millis)
    }
}

#[cfg(test)]
mod tests {
    use monad_mcp_chorus::spec::vote::KeyPair as _;

    use super::*;
    use crate::chorus::types::{ProposerSchedule as _, Slot};

    fn validator_data(n: u64) -> Arc<ValidatorData> {
        let valset = (0..n)
            .map(|id| (NodeId::dummy(id), Stake::from(1)))
            .collect();
        let mapping = (0..n)
            .map(|id| (NodeId::dummy(id), NodeId::dummy(id).keypair().pubkey()))
            .collect();
        Arc::new(ValidatorData::new(valset, mapping))
    }

    // the default K exceeds a small devnet's validator count; the effective
    // window clamps to the stake set rather than failing at startup
    #[test]
    fn the_default_schedule_builds_for_any_validator_count() {
        for n in 1..=7 {
            let schedule = ProposalConfig::default()
                .schedule(validator_data(n))
                .unwrap_or_else(|err| panic!("{n} validators: {err}"));
            let set = schedule.proposers_at(Slot(0)).expect("genesis epoch");
            assert!(set.iter().any(|(_, proposer)| proposer.is_some()));
        }
    }

    // an absent [repeater] table leaves the node without a repeater; a
    // present one overrides the defaults field by field
    #[test]
    fn the_repeater_table_is_optional() {
        #[derive(Deserialize)]
        struct Top {
            repeater: Option<RepeaterSection>,
        }

        let without: Top = toml::from_str("").unwrap();
        assert!(without.repeater.is_none());

        let with: Top = toml::from_str("[repeater]\ninterval = 1000").unwrap();
        let repeater = with.repeater.expect("the table is present");
        assert_eq!(repeater.interval, TimestampDelta::from_millis(1_000));
        assert_eq!(
            repeater.certificate_retention,
            RepeaterSection::default().certificate_retention
        );
    }

    // the gate and the rotation vacancy mirror the same deployment constant
    #[test]
    fn the_planner_takes_its_cutoff_from_the_schedule() {
        let config = ProposalConfig::default();
        let schedule = config.schedule(validator_data(4)).unwrap();
        let planner = config.planner(&schedule);
        assert_eq!(
            planner.observation_cutoff,
            schedule.config().observation_cutoff
        );
        assert_eq!(planner.lead, config.propose_before_deadline);
        assert_eq!(planner.min_lead, config.withhold_before_deadline);
    }

    // the gate is off unless the deployment asks for it
    #[test]
    fn the_withhold_floor_defaults_off_and_reaches_the_planner() {
        assert_eq!(
            ProposalConfig::default().withhold_before_deadline,
            TimestampDelta::ZERO
        );

        let config: ProposalConfig = toml::from_str("withhold_before_deadline = 150").unwrap();
        let schedule = config.schedule(validator_data(4)).unwrap();
        assert_eq!(
            config.planner(&schedule).min_lead,
            TimestampDelta::from_millis(150)
        );
    }
}
