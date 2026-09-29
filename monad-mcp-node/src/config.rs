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

use std::{collections::HashMap, fmt, net::SocketAddr, num::NonZeroU64, path::PathBuf, sync::Arc};

use alloy_rlp::Encodable as _;
use monad_mcp_chorus::{ledger::BatchBuilder, spec::vote::KeyPair as _};
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
    component::{RepeaterConfig, mempool},
    da::{self, ProposalKeyPair, header_auth},
    epoch::{EpochHandle, NodeProposerSchedule},
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
    #[serde(default)]
    pub mempool: MempoolConfig,
    pub ledger: LedgerConfig,
}

impl NodeConfig {
    // a lone validator on localhost with the 100 ms local-demo parameters of
    // config.example.toml and the mempool source
    pub fn single_node(
        port: u16,
        genesis_deadline: Timestamp,
        ledger_dir: impl Into<PathBuf>,
    ) -> Self {
        let id = 0;
        Self {
            node_id: NodeId::dummy(id),
            proposal_key_pair: ProposalKeyPair::dummy(NodeId::dummy(id)),
            cadence_key_pair: KeyPair::dummy(id),
            validators: vec![ValidatorConfig {
                node_id: NodeId::dummy(id),
                stake: 1,
                chorus_pubkey: KeyPair::dummy(id).pubkey(),
                address: SocketAddr::from(([127, 0, 0, 1], port)),
            }],
            genesis_deadline,
            network: NetworkConfig { port },
            cadence: CadenceConfig::local_demo(),
            da: DAConfig::default(),
            proposal: ProposalConfig {
                source: SourceKind::Mempool,
                ..ProposalConfig::local_demo()
            },
            repeater: None,
            mempool: MempoolConfig::default(),
            ledger: LedgerConfig {
                dir: ledger_dir.into(),
            },
        }
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        let proposal_size_limit = self.proposal.proposal_size_limit();
        if proposal_size_limit == 0 || proposal_size_limit > ProposalConfig::MAX_PROPOSAL_SIZE_LIMIT
        {
            return Err(ConfigError(format!(
                "proposal.max_payload_bytes must be in 1..={}",
                ProposalConfig::MAX_PROPOSAL_SIZE_LIMIT
            )));
        }
        if self.proposal.source == SourceKind::Mempool {
            let largest = mempool::largest_tx();
            if !BatchBuilder::new(proposal_size_limit).fits(&largest) {
                return Err(ConfigError(format!(
                    "proposal.max_payload_bytes {proposal_size_limit} cannot hold the largest tx"
                )));
            }
            let largest = largest.length();
            if self.mempool.max_txs == 0 || self.mempool.max_bytes < largest {
                return Err(ConfigError(format!(
                    "mempool must hold at least one {largest}-byte tx"
                )));
            }
        }
        Ok(())
    }

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
    // single node on localhost, 100 ms slots; see config.example.toml
    pub fn local_demo() -> Self {
        Self {
            delta: TimestampDelta::from_millis(DEMO_DELTA_MS),
            slot_interval: TimestampDelta::from_millis(100),
            ..Self::default()
        }
    }

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

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SourceKind {
    // random bytes, for load tests without clients
    #[default]
    Random,
    // txs sent to this node over udp
    Mempool,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(default)]
pub struct ProposalConfig {
    pub num_proposals: usize,
    pub source: SourceKind,
    // proposal size limit in bytes; absent: 1 MiB for random, 64 KiB for mempool
    pub max_payload_bytes: Option<usize>,
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

    // the largest message swiper-11 encodes, which monad-mcp-da keeps private
    pub const MAX_PROPOSAL_SIZE_LIMIT: usize = 1 << 20;
    pub const RANDOM_PROPOSAL_SIZE_LIMIT: usize = Self::MAX_PROPOSAL_SIZE_LIMIT;
    pub const MEMPOOL_PROPOSAL_SIZE_LIMIT: usize = 64 << 10;

    // single node on localhost, 100 ms slots; see config.example.toml
    pub fn local_demo() -> Self {
        Self {
            propose_before_deadline: TimestampDelta::from_millis(DEMO_PROPOSE_BEFORE_MS),
            ..Self::default()
        }
    }

    pub fn proposal_size_limit(&self) -> usize {
        self.max_payload_bytes.unwrap_or(match self.source {
            SourceKind::Random => Self::RANDOM_PROPOSAL_SIZE_LIMIT,
            SourceKind::Mempool => Self::MEMPOOL_PROPOSAL_SIZE_LIMIT,
        })
    }

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
            source: SourceKind::Random,
            max_payload_bytes: None,
            propose_before_deadline: TimestampDelta::from_millis(500),
            withhold_before_deadline: TimestampDelta::ZERO,
        }
    }
}

const DEMO_DELTA_MS: u64 = 50;
const DEMO_PROPOSE_BEFORE_MS: u64 = 200;

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(default)]
pub struct MempoolConfig {
    pub max_txs: usize,
    // total rlp bytes of the queued txs
    pub max_bytes: usize,
    // committed tx hashes remembered to reject re-sends as duplicates
    pub recent_txs: usize,
}

impl Default for MempoolConfig {
    fn default() -> Self {
        Self {
            max_txs: 10_000,
            max_bytes: 16 << 20,
            recent_txs: 100_000,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct LedgerConfig {
    pub dir: PathBuf,
}

#[derive(Debug)]
pub struct ConfigError(String);

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid config: {}", self.0)
    }
}

impl std::error::Error for ConfigError {}

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

    const MINIMAL: &str = r#"
node_id = 0
proposal_key_pair = 0
cadence_key_pair = 0
genesis_deadline = 1789412000000

[[validators]]
node_id = 0
stake = 1
chorus_pubkey = 0
address = "127.0.0.1:9000"

[network]
port = 9000
"#;

    #[test]
    fn the_example_config_parses_and_validates() {
        let config: NodeConfig = toml::from_str(include_str!("../config.example.toml")).unwrap();
        config.validate().unwrap();
        assert_eq!(config.proposal.source, SourceKind::Random);
        assert_eq!(config.mempool.max_txs, MempoolConfig::default().max_txs);
    }

    const LEDGER: &str = "
[ledger]
dir = \"/var/mcp/ledger\"
";

    // a config with only the required sections proposes random payloads
    #[test]
    fn the_optional_sections_default_to_random_payloads() {
        let config: NodeConfig = toml::from_str(&format!("{MINIMAL}{LEDGER}")).unwrap();
        assert_eq!(config.proposal.source, SourceKind::Random);
        assert_eq!(
            config.proposal.proposal_size_limit(),
            ProposalConfig::RANDOM_PROPOSAL_SIZE_LIMIT
        );
        assert_eq!(config.ledger.dir, PathBuf::from("/var/mcp/ledger"));
        config.validate().unwrap();
    }

    #[test]
    fn a_config_without_a_ledger_is_rejected() {
        let Err(error) = toml::from_str::<NodeConfig>(MINIMAL) else {
            panic!("a config without [ledger] parsed");
        };
        assert!(
            error.to_string().contains("missing field `ledger`"),
            "{error}"
        );
    }

    #[test]
    fn the_demo_sections_parse() {
        let text = format!(
            "{MINIMAL}{LEDGER}
[proposal]
source = \"mempool\"
max_payload_bytes = 4096

[mempool]
max_txs = 7
"
        );
        let config: NodeConfig = toml::from_str(&text).unwrap();
        assert_eq!(config.proposal.source, SourceKind::Mempool);
        assert_eq!(config.proposal.proposal_size_limit(), 4096);
        assert_eq!(config.mempool.max_txs, 7);
        assert_eq!(config.mempool.max_bytes, MempoolConfig::default().max_bytes);
        config.validate().unwrap();
    }

    #[test]
    fn an_unknown_source_is_rejected() {
        let text = format!("{MINIMAL}{LEDGER}\n[proposal]\nsource = \"file\"\n");
        assert!(toml::from_str::<NodeConfig>(&text).is_err());
    }

    #[test]
    fn a_mempool_proposal_size_limit_must_hold_the_largest_tx() {
        let mut config =
            NodeConfig::single_node(9000, Timestamp::from_millis(0), "/var/mcp/ledger");
        config.validate().unwrap();
        let largest = crate::component::mempool::largest_tx().length();
        config.proposal.max_payload_bytes = Some(largest);
        assert!(config.validate().is_err());
        config.proposal.max_payload_bytes = Some(largest + 3);
        config.validate().unwrap();
        // a proposal size limit beyond the s11 bound drains batches that never encode
        config.proposal.max_payload_bytes = Some(ProposalConfig::MAX_PROPOSAL_SIZE_LIMIT);
        config.validate().unwrap();
        config.proposal.max_payload_bytes = Some(ProposalConfig::MAX_PROPOSAL_SIZE_LIMIT + 1);
        assert!(config.validate().is_err());
        config.proposal.max_payload_bytes = Some(largest + 3);

        config.mempool.max_bytes = largest - 1;
        assert!(config.validate().is_err());
        config.mempool.max_bytes = largest;
        config.mempool.max_txs = 0;
        assert!(config.validate().is_err());

        // a random source only needs a positive proposal size limit
        config.proposal.source = SourceKind::Random;
        config.proposal.max_payload_bytes = Some(1);
        config.validate().unwrap();
        config.proposal.max_payload_bytes = Some(0);
        assert!(config.validate().is_err());
        config.proposal.max_payload_bytes = Some(ProposalConfig::MAX_PROPOSAL_SIZE_LIMIT + 1);
        assert!(config.validate().is_err());
    }

    #[test]
    fn the_single_node_config_uses_the_demo_parameters() {
        let config = NodeConfig::single_node(9000, Timestamp::from_millis(0), "/var/mcp/ledger");
        assert_eq!(
            config.cadence.slot_interval,
            TimestampDelta::from_millis(100)
        );
        assert_eq!(
            config.cadence.delta,
            TimestampDelta::from_millis(DEMO_DELTA_MS)
        );
        assert_eq!(
            config.proposal.propose_before_deadline,
            TimestampDelta::from_millis(DEMO_PROPOSE_BEFORE_MS)
        );
        assert_eq!(config.proposal.source, SourceKind::Mempool);
        assert_eq!(
            config.proposal.proposal_size_limit(),
            ProposalConfig::MEMPOOL_PROPOSAL_SIZE_LIMIT
        );
        assert_eq!(config.peers().len(), 1);
    }
}
