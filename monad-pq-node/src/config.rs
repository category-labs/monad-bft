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

//! `node.toml` for the pq node (§12): a strict superset of monad-mcp-node's
//! `NodeConfig`, so the unchanged rpc reads the same file. Every pq
//! parameter lives in the `[quantum]` table.

use std::{collections::HashSet, fmt, path::PathBuf, sync::Arc};

use monad_mcp_node::{
    EpochHandle,
    config::{
        CadenceConfig, LeaderElectionConfig, NodeConfig, ProposalConfig, RepeaterSection,
        SourceKind,
    },
};
use monad_pq_chorus::{
    QuantumConfig, QuantumContext,
    config::ReleaseChunks,
    sig::{PqVerifier, StubPq},
};
use serde::Deserialize;

use crate::{
    chorus::types::{NodeId, Slot, Timestamp, TimestampDelta},
    da::{self, AssembledProposal, ProposalKeyPair},
};

pub struct PqNodeConfig {
    pub base: NodeConfig,
    pub quantum: QuantumSection,
    // the proposal key's id, read again: monad-mcp-da's key pair hides it
    pub proposal_key: u64,
    // the file carries an [rpc] table, so the rpc routes txs here
    pub has_rpc: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SignatureKind {
    #[default]
    Stub,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReleaseChunksKind {
    #[default]
    Vote,
    Receipt,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LedgerProof {
    Metablock,
    #[default]
    PathVotes,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QuantumSection {
    pub signature: SignatureKind,
    pub stub_signature_len: usize,
    // ms; absent: cadence.delta
    pub retransmit_interval: Option<u64>,
    // ms; absent: cadence.delta / 2; at most cadence.delta / 2
    pub decode_grace: Option<u64>,
    pub coda_timeout_deltas: u32,
    pub release_chunks: ReleaseChunksKind,
    pub early_vote: bool,
    pub ledger_proof: LedgerProof,
    // write evidence files under <ledger.dir>/evidence
    pub evidence: bool,
}

impl Default for QuantumSection {
    fn default() -> Self {
        Self {
            signature: SignatureKind::Stub,
            stub_signature_len: monad_pq_chorus::sig::STUB_SIGNATURE_LEN,
            retransmit_interval: None,
            decode_grace: None,
            coda_timeout_deltas: 5,
            release_chunks: ReleaseChunksKind::Vote,
            early_vote: true,
            ledger_proof: LedgerProof::PathVotes,
            evidence: true,
        }
    }
}

// the second pass over the file: what NodeConfig ignores
#[derive(Deserialize)]
struct PqFile {
    proposal_key_pair: u64,
    #[serde(default)]
    quantum: QuantumSection,
    rpc: Option<toml::Table>,
}

#[derive(Debug)]
pub struct PqConfigError(pub String);

impl fmt::Display for PqConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid pq config: {}", self.0)
    }
}

impl std::error::Error for PqConfigError {}

fn invalid(what: impl fmt::Display) -> PqConfigError {
    PqConfigError(what.to_string())
}

// the identities and schedule of the running epoch, in the three shapes
// proposing, DA and the slots take
pub struct PqEpoch {
    pub handle: EpochHandle,
    pub da: da::EpochHandle,
    pub context: QuantumContext,
}

// local demo values (§8.3): Δ 50, 100 ms slots, seal 100 ms ahead and
// withhold a seal with less than 90 ms lead
pub const DEMO_DELTA_MS: u64 = 50;
pub const DEMO_PROPOSE_BEFORE_MS: u64 = 100;
pub const DEMO_WITHHOLD_BEFORE_MS: u64 = 90;
pub const DEMO_REPEATER_INTERVAL_MS: u64 = 200;
// a lane rotates every 100 slots and is vacant for 5 of them
pub const DEMO_ROTATION_SLACK: u64 = 95;

impl PqNodeConfig {
    pub fn parse(text: &str) -> Result<Self, PqConfigError> {
        let base: NodeConfig = toml::from_str(text).map_err(invalid)?;
        let file: PqFile = toml::from_str(text).map_err(invalid)?;
        Ok(Self {
            base,
            quantum: file.quantum,
            proposal_key: file.proposal_key_pair,
            has_rpc: file.rpc.is_some(),
        })
    }

    pub fn load(path: impl Into<PathBuf>) -> Result<Self, PqConfigError> {
        let path = path.into();
        let text = std::fs::read_to_string(&path)
            .map_err(|error| invalid(format!("read {}: {error}", path.display())))?;
        Self::parse(&text)
    }

    // a lone validator on localhost with the local demo values and the
    // mempool source
    pub fn single_node(
        port: u16,
        genesis_deadline: Timestamp,
        ledger_dir: impl Into<PathBuf>,
    ) -> Self {
        let mut base = NodeConfig::single_node(port, genesis_deadline, ledger_dir);
        base.cadence = CadenceConfig {
            delta: TimestampDelta::from_millis(DEMO_DELTA_MS),
            slot_interval: TimestampDelta::from_millis(100),
            ..CadenceConfig::default()
        };
        base.proposal = ProposalConfig {
            source: SourceKind::Mempool,
            propose_before_deadline: TimestampDelta::from_millis(DEMO_PROPOSE_BEFORE_MS),
            withhold_before_deadline: TimestampDelta::from_millis(DEMO_WITHHOLD_BEFORE_MS),
            ..ProposalConfig::default()
        };
        base.leader_election = LeaderElectionConfig {
            rotation_slack: DEMO_ROTATION_SLACK,
        };
        base.repeater = Some(RepeaterSection {
            interval: TimestampDelta::from_millis(DEMO_REPEATER_INTERVAL_MS),
            ..RepeaterSection::default()
        });
        Self {
            base,
            quantum: QuantumSection::default(),
            proposal_key: 0,
            has_rpc: false,
        }
    }

    pub fn quantum_config(&self) -> QuantumConfig {
        let delta = self.base.cadence.delta;
        let mut config = QuantumConfig::with_delta(delta);
        if let Some(ms) = self.quantum.retransmit_interval {
            config.retransmit_interval = TimestampDelta::from_millis(ms);
        }
        if let Some(ms) = self.quantum.decode_grace {
            config.decode_grace = TimestampDelta::from_millis(ms);
        }
        config.coda_timeout_deltas = self.quantum.coda_timeout_deltas;
        config.release_chunks = match self.quantum.release_chunks {
            ReleaseChunksKind::Vote => ReleaseChunks::Vote,
            ReleaseChunksKind::Receipt => ReleaseChunks::Receipt,
        };
        config.early_vote = self.quantum.early_vote;
        config
    }

    pub fn validate(&self) -> Result<(), PqConfigError> {
        self.base.validate().map_err(invalid)?;
        self.quantum_config().validate().map_err(invalid)?;
        let Some(repeater) = self.base.repeater else {
            return Err(invalid(
                "[repeater] is required: it keeps Finalized alive after halt",
            ));
        };
        if repeater.interval == TimestampDelta::ZERO {
            return Err(invalid("repeater.interval must be positive"));
        }
        let lag_threshold = self.base.cadence.lag_threshold.get();
        if repeater.certificate_retention < lag_threshold {
            return Err(invalid(format!(
                "repeater.certificate_retention {} must be at least cadence.lag_threshold {lag_threshold}",
                repeater.certificate_retention
            )));
        }
        let stakes: HashSet<u64> = self.base.validators.iter().map(|v| v.stake).collect();
        if stakes.len() != 1 {
            return Err(invalid("every validator must have the same stake"));
        }
        if self.has_rpc && self.base.proposal.source != SourceKind::Mempool {
            return Err(invalid("[rpc] needs [proposal] source = \"mempool\""));
        }
        self.check_every_validator_owed()?;

        let proposal = &self.base.proposal;
        let lead = proposal.propose_before_deadline.as_nanos();
        let floor = lead.saturating_sub(TimestampDelta::from_millis(10).as_nanos());
        if proposal.withhold_before_deadline.as_nanos() < floor {
            tracing::warn!(
                withhold_ms = proposal.withhold_before_deadline.as_nanos() / 1_000_000,
                propose_ms = lead / 1_000_000,
                "withhold_before_deadline below propose_before_deadline - 10 may split late seals"
            );
        }
        if self.base.validators.len().is_multiple_of(3) {
            tracing::warn!(
                n = self.base.validators.len(),
                "n divisible by 3: f+1 holders do not guarantee decoding"
            );
        }
        Ok(())
    }

    // §4.2: with equal stake every validator holds a chunk of every
    // proposal, or the early vote waits on chunks that never come
    fn check_every_validator_owed(&self) -> Result<(), PqConfigError> {
        let handle = self
            .base
            .epoch_handle()
            .map_err(|e| invalid(format!("{e:?}")))?;
        let da = self.da_handle(&handle);
        let proposal = AssembledProposal::build_s11(&da, Slot(0), 0, &[0], 0)
            .ok_or_else(|| invalid("the s11 scheme cannot encode a proposal"))?;
        let mut first_hop = proposal.disseminate();
        for validator in &self.base.validators {
            let owned = first_hop
                .split_off(&validator.node_id)
                .map_or(0, |envelope| envelope.num_chunks());
            if owned == 0 {
                return Err(invalid(format!(
                    "validator {} owns no s11 chunk",
                    u64::from(validator.node_id)
                )));
            }
        }
        Ok(())
    }

    fn da_handle(&self, handle: &EpochHandle) -> da::EpochHandle {
        let key = ProposalKeyPair::dummy(NodeId::dummy(self.proposal_key));
        da::EpochHandle {
            self_id: handle.self_id,
            key_pair: Arc::new(key),
            header_auth: handle.header_auth.clone(),
            validator_data: handle.validator_data.clone(),
        }
    }

    pub fn epoch(&self) -> Result<PqEpoch, PqConfigError> {
        let handle = self
            .base
            .epoch_handle()
            .map_err(|e| invalid(format!("{e:?}")))?;
        let da = self.da_handle(&handle);
        let SignatureKind::Stub = self.quantum.signature;
        let scheme = StubPq::new(self.quantum.stub_signature_len);
        let verifier: Arc<dyn PqVerifier> = Arc::new(scheme);
        let context = QuantumContext::new(
            handle.self_id,
            Arc::new(scheme.signer(handle.self_id)),
            verifier,
            handle.validator_data.clone(),
            handle.header_auth.clone(),
            handle.proposers.clone(),
        )
        .map_err(invalid)?;
        Ok(PqEpoch {
            handle,
            da,
            context,
        })
    }

    pub fn evidence_dir(&self) -> Option<PathBuf> {
        self.quantum
            .evidence
            .then(|| self.base.ledger.dir.join("evidence"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXAMPLE: &str = include_str!("../config.example.toml");

    #[test]
    fn the_example_config_parses_and_validates() {
        let config = PqNodeConfig::parse(EXAMPLE).unwrap();
        config.validate().unwrap();
        assert_eq!(config.base.proposal.source, SourceKind::Mempool);
        assert_eq!(config.base.cadence.delta, TimestampDelta::from_millis(50));
        assert_eq!(
            config.base.proposal.withhold_before_deadline,
            TimestampDelta::from_millis(90)
        );
        let quantum = config.quantum_config();
        assert_eq!(quantum.decode_grace, TimestampDelta::from_millis(25));
        assert_eq!(quantum.retransmit_interval, TimestampDelta::from_millis(50));
        assert_eq!(config.quantum.ledger_proof, LedgerProof::PathVotes);
        config.epoch().unwrap();
    }

    // §5.1: the unchanged rpc parses the node file as NodeConfig and its
    // own [rpc] table separately; both ignore [quantum]
    #[test]
    fn the_example_config_is_a_node_config() {
        let text = format!("{EXAMPLE}\n[rpc]\nlead_margin_ms = 50\n");
        let node: NodeConfig = toml::from_str(&text).unwrap();
        node.validate().unwrap();

        #[derive(Deserialize)]
        struct RpcFile {
            rpc: RpcTable,
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct RpcTable {
            lead_margin_ms: Option<u64>,
        }
        let file: RpcFile = toml::from_str(&text).unwrap();
        assert_eq!(file.rpc.lead_margin_ms, Some(50));

        let config = PqNodeConfig::parse(&text).unwrap();
        assert!(config.has_rpc);
        config.validate().unwrap();
    }

    #[test]
    fn an_unknown_quantum_key_is_rejected() {
        let text = format!("{EXAMPLE}\n[quantum]\nearly = true\n");
        assert!(PqNodeConfig::parse(&text).is_err());
    }

    #[test]
    fn the_quantum_table_overrides_the_defaults() {
        let mut config = PqNodeConfig::parse(EXAMPLE).unwrap();
        config.quantum.retransmit_interval = Some(30);
        config.quantum.decode_grace = Some(10);
        config.quantum.release_chunks = ReleaseChunksKind::Receipt;
        config.quantum.early_vote = false;
        let quantum = config.quantum_config();
        assert_eq!(quantum.retransmit_interval, TimestampDelta::from_millis(30));
        assert_eq!(quantum.decode_grace, TimestampDelta::from_millis(10));
        assert_eq!(quantum.release_chunks, ReleaseChunks::Receipt);
        assert!(!quantum.early_vote);
    }

    #[test]
    fn the_pq_checks_reject_bad_configs() {
        let base = || PqNodeConfig::parse(EXAMPLE).unwrap();

        let mut config = base();
        config.quantum.decode_grace = Some(26);
        assert!(config.validate().is_err());

        let mut config = base();
        config.base.repeater = None;
        assert!(config.validate().is_err());

        let mut config = base();
        config.base.repeater.as_mut().unwrap().certificate_retention = 19;
        assert!(config.validate().is_err());

        let mut config = base();
        config.has_rpc = true;
        config.base.proposal.source = SourceKind::Random;
        assert!(config.validate().is_err());
    }

    #[test]
    fn unequal_stakes_are_rejected() {
        let mut config = PqNodeConfig::parse(EXAMPLE).unwrap();
        let mut second = toml::from_str::<NodeConfig>(EXAMPLE).unwrap().validators;
        let mut extra = second.remove(0);
        extra.node_id = NodeId::dummy(1);
        extra.stake = 2;
        config.base.validators.push(extra);
        assert!(config.validate().is_err());
    }

    #[test]
    fn the_single_node_config_validates() {
        let config = PqNodeConfig::single_node(9300, Timestamp::from_millis(0), "/tmp/unused");
        config.validate().unwrap();
        assert_eq!(config.base.proposal.source, SourceKind::Mempool);
    }
}
