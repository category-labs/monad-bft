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

use std::{
    collections::HashSet,
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
};

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
    sig::{
        ML_DSA_44_SIGNATURE_LEN, ML_DSA_SEED_LEN, MlDsa44PublicKey, MlDsa44Signer, MlDsa44Verifier,
        PqSigner, PqVerifier, StubPq,
    },
};
use serde::Deserialize;

use crate::{
    chorus::types::{NodeId, Slot, Timestamp, TimestampDelta},
    da::{self, AssembledProposal, ProposalKeyPair},
    wire::{self, Fragmenting, MAX_MESSAGE_LEN, MAX_PAYLOAD_LEN, SlotWire, Unfragmented},
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
    // FIPS 204; messages over one frame are fragmented
    #[serde(rename = "ml-dsa-44")]
    MlDsa44,
}

// a validator's ML-DSA-44 public key, hex encoded
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuantumValidator {
    pub node_id: u64,
    pub public_key: String,
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
    // ml-dsa-44: the hex FIPS 204 seed, with every validator's key in
    // `validators`; both absent: keys derived from node ids (test networks)
    pub secret_key_file: Option<PathBuf>,
    pub validators: Vec<QuantumValidator>,
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
    // evidence files more than this many slots behind the newest are pruned
    pub evidence_retention_slots: u64,
}

impl Default for QuantumSection {
    fn default() -> Self {
        Self {
            signature: SignatureKind::Stub,
            stub_signature_len: monad_pq_chorus::sig::STUB_SIGNATURE_LEN,
            secret_key_file: None,
            validators: Vec::new(),
            retransmit_interval: None,
            decode_grace: None,
            coda_timeout_deltas: 5,
            release_chunks: ReleaseChunksKind::Vote,
            early_vote: false,
            ledger_proof: LedgerProof::PathVotes,
            evidence: true,
            evidence_retention_slots: 1_000,
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
// a repeater interval past this many deltas draws a warning
pub const MAX_REPEATER_DELTAS: u32 = 4;
const NANOS_PER_MS: u128 = TimestampDelta::NANOS_PER_MILLISECOND as u128;

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
        self.check_repeater_interval(repeater.interval)?;
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
        self.check_message_sizes()?;
        self.check_signature_keys()?;
        if self.quantum.evidence_retention_slots == 0 {
            return Err(invalid("quantum.evidence_retention_slots must be positive"));
        }

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

    // stub signatures send one frame per message, ml-dsa-44 fragments
    fn check_message_sizes(&self) -> Result<(), PqConfigError> {
        let k = self.base.proposal.num_proposals;
        match self.quantum.signature {
            SignatureKind::Stub => {
                let sig_len = self.quantum.stub_signature_len;
                if let Some((kind, len)) = wire::first_misfit(k, sig_len) {
                    return Err(invalid(format!(
                        "a {kind} of {len} bytes exceeds the {MAX_PAYLOAD_LEN}-byte frame at num_proposals {k} and stub_signature_len {sig_len}; stub signatures do not fragment"
                    )));
                }
            }
            SignatureKind::MlDsa44 => {
                if let Some((kind, len)) = wire::first_unfragmentable(k, ML_DSA_44_SIGNATURE_LEN) {
                    return Err(invalid(format!(
                        "a {kind} of {len} bytes exceeds the {MAX_MESSAGE_LEN}-byte fragmented message at num_proposals {k}"
                    )));
                }
            }
        }
        Ok(())
    }

    // the listed keys, without reading the secret key file
    fn check_signature_keys(&self) -> Result<(), PqConfigError> {
        let quantum = &self.quantum;
        let listed = !quantum.validators.is_empty();
        match quantum.signature {
            SignatureKind::Stub if quantum.secret_key_file.is_some() || listed => Err(invalid(
                "quantum.secret_key_file and [[quantum.validators]] need signature = \"ml-dsa-44\"",
            )),
            SignatureKind::Stub => Ok(()),
            SignatureKind::MlDsa44 if quantum.secret_key_file.is_some() != listed => Err(invalid(
                "ml-dsa-44 needs both quantum.secret_key_file and [[quantum.validators]], or neither for keys derived from node ids",
            )),
            SignatureKind::MlDsa44 if listed => self.ml_dsa_public_keys().map(|_| ()),
            SignatureKind::MlDsa44 => Ok(()),
        }
    }

    // exactly one valid key per validator
    fn ml_dsa_public_keys(&self) -> Result<Vec<(NodeId, MlDsa44PublicKey)>, PqConfigError> {
        let validators: HashSet<NodeId> = self.base.validators.iter().map(|v| v.node_id).collect();
        let mut seen = HashSet::new();
        let mut keys = Vec::with_capacity(self.quantum.validators.len());
        for listed in &self.quantum.validators {
            let id = NodeId::dummy(listed.node_id);
            if !validators.contains(&id) {
                return Err(invalid(format!(
                    "quantum.validators lists {} outside [[validators]]",
                    listed.node_id
                )));
            }
            if !seen.insert(id) {
                return Err(invalid(format!(
                    "quantum.validators lists {} twice",
                    listed.node_id
                )));
            }
            let key = hex::decode(listed.public_key.trim())
                .ok()
                .and_then(|bytes| MlDsa44PublicKey::from_bytes(&bytes))
                .ok_or_else(|| {
                    invalid(format!(
                        "quantum.validators {}: public_key is not a hex ML-DSA-44 key",
                        listed.node_id
                    ))
                })?;
            keys.push((id, key));
        }
        if let Some(missing) = validators.iter().find(|id| !seen.contains(id)) {
            return Err(invalid(format!(
                "quantum.validators has no key for {}",
                u64::from(*missing)
            )));
        }
        Ok(keys)
    }

    fn signature_scheme(
        &self,
        self_id: NodeId,
    ) -> Result<(Arc<dyn PqSigner>, Arc<dyn PqVerifier>), PqConfigError> {
        if self.quantum.signature == SignatureKind::Stub {
            let scheme = StubPq::new(self.quantum.stub_signature_len);
            return Ok((Arc::new(scheme.signer(self_id)), Arc::new(scheme)));
        }
        let Some(path) = &self.quantum.secret_key_file else {
            tracing::warn!("ml-dsa-44 keys derived from node ids: for test networks only");
            let validators = self.base.validators.iter().map(|v| v.node_id);
            let verifier = MlDsa44Verifier::dev(validators);
            return Ok((Arc::new(MlDsa44Signer::dev(self_id)), Arc::new(verifier)));
        };
        let signer = MlDsa44Signer::from_seed(&load_ml_dsa_seed(path)?);
        let verifier = MlDsa44Verifier::new(self.ml_dsa_public_keys()?);
        if verifier.public_key(&self_id) != Some(&signer.public_key()) {
            return Err(invalid(format!(
                "{} is not the secret key of node {}'s listed public key",
                path.display(),
                u64::from(self_id)
            )));
        }
        Ok((Arc::new(signer), Arc::new(verifier)))
    }

    pub fn slot_wire(&self) -> Box<dyn SlotWire> {
        match self.quantum.signature {
            SignatureKind::Stub => Box::<Unfragmented>::default(),
            SignatureKind::MlDsa44 => {
                let ttl = self.base.cadence.delta.as_nanos().saturating_mul(2);
                Box::new(Fragmenting::new(TimestampDelta::from_nanos(ttl)))
            }
        }
    }

    // a node behind the sync boundary needs at least two repeats of each
    // Finalized before the window closes
    fn check_repeater_interval(&self, interval: TimestampDelta) -> Result<(), PqConfigError> {
        if interval == TimestampDelta::ZERO {
            return Err(invalid("repeater.interval must be positive"));
        }
        let cadence = &self.base.cadence;
        let tail_slots = cadence
            .slots_per_window
            .get()
            .saturating_sub(cadence.sync_boundary_slots.get());
        let bound = u128::from(cadence.slot_interval.as_nanos()) * u128::from(tail_slots) / 2;
        let interval_ns = u128::from(interval.as_nanos());
        if interval_ns > bound {
            return Err(invalid(format!(
                "repeater.interval {} ms exceeds half of the {tail_slots} slots after the sync boundary ({} ms)",
                interval_ns / NANOS_PER_MS,
                bound / NANOS_PER_MS
            )));
        }
        let delta_ns = u128::from(cadence.delta.as_nanos());
        if interval_ns > delta_ns * u128::from(MAX_REPEATER_DELTAS) {
            tracing::warn!(
                interval_ms = interval_ns / NANOS_PER_MS,
                delta_ms = delta_ns / NANOS_PER_MS,
                "repeater.interval above {MAX_REPEATER_DELTAS} deltas slows catch-up after halt"
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
        let (signer, verifier) = self.signature_scheme(handle.self_id)?;
        let context = QuantumContext::new(
            handle.self_id,
            signer,
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

/// An ML-DSA-44 secret key file: the 32-byte FIPS 204 seed in hex.
pub fn load_ml_dsa_seed(path: &Path) -> Result<[u8; ML_DSA_SEED_LEN], PqConfigError> {
    let text = std::fs::read_to_string(path)
        .map_err(|error| invalid(format!("read {}: {error}", path.display())))?;
    hex::decode(text.trim())
        .ok()
        .and_then(|bytes| <[u8; ML_DSA_SEED_LEN]>::try_from(bytes).ok())
        .ok_or_else(|| {
            invalid(format!(
                "{} must hold a {ML_DSA_SEED_LEN}-byte hex seed",
                path.display()
            ))
        })
}

#[cfg(test)]
mod tests {
    use monad_mcp_rpc::config::RpcSection;

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

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("node.toml");
        std::fs::write(&path, &text).unwrap();
        let mut rpc = RpcSection::parse(&text).unwrap();
        assert_eq!(rpc.lead_margin_ms, Some(50));
        rpc.node_config = Some(path);
        let rpc = rpc.build().unwrap();
        assert_eq!(rpc.node_id, node.node_id);
        assert_eq!(rpc.ledger_dir, node.ledger.dir);

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

        let mut config = base();
        config.quantum.evidence_retention_slots = 0;
        assert!(config.validate().is_err());
    }

    // 20 slots of 100 ms past the sync boundary: at most 1000 ms
    #[test]
    fn a_repeater_too_slow_for_the_sync_window_is_rejected() {
        let mut config = PqNodeConfig::parse(EXAMPLE).unwrap();
        let repeater = config.base.repeater.as_mut().unwrap();
        repeater.interval = TimestampDelta::from_millis(1_000);
        config.validate().unwrap();
        config.base.repeater.as_mut().unwrap().interval = TimestampDelta::from_millis(1_001);
        assert!(config.validate().is_err());
        config.base.repeater.as_mut().unwrap().interval = TimestampDelta::ZERO;
        assert!(config.validate().is_err());
    }

    #[test]
    fn slot_messages_that_cannot_fit_a_frame_are_rejected() {
        let mut config = PqNodeConfig::parse(EXAMPLE).unwrap();
        config.quantum.stub_signature_len = 2420;
        assert!(config.validate().is_err());

        let mut config = PqNodeConfig::parse(EXAMPLE).unwrap();
        config.base.proposal.num_proposals = 64;
        assert!(config.validate().is_err());
    }

    // [quantum] is the example's last table: `extra` may add key tables
    fn ml_dsa(extra: &str) -> Result<PqNodeConfig, PqConfigError> {
        let text = EXAMPLE.replace("signature = \"stub\"", "signature = \"ml-dsa-44\"");
        let config = PqNodeConfig::parse(&format!("{text}{extra}"))?;
        config.validate()?;
        Ok(config)
    }

    fn key_table(id: u64, key: &MlDsa44PublicKey) -> String {
        format!(
            "\n[[quantum.validators]]\nnode_id = {id}\npublic_key = \"{}\"\n",
            hex::encode(key.to_bytes())
        )
    }

    // stubs default; ml-dsa-44 messages take more than one frame and pass
    #[test]
    fn ml_dsa_44_dev_keys_validate_and_fragment() {
        assert_eq!(
            PqNodeConfig::parse(EXAMPLE).unwrap().quantum.signature,
            SignatureKind::Stub
        );
        let mut config = ml_dsa("").unwrap();
        assert_eq!(config.quantum.signature, SignatureKind::MlDsa44);
        assert!(wire::first_misfit(5, ML_DSA_44_SIGNATURE_LEN).is_some());
        let epoch = config.epoch().unwrap();
        assert_eq!(
            epoch.context.verifier.max_sig_len(),
            ML_DSA_44_SIGNATURE_LEN
        );

        config.base.proposal.num_proposals = 64;
        config.validate().unwrap();
        config.base.proposal.num_proposals = 400;
        let error = config.validate().unwrap_err().0;
        assert!(error.contains("fragmented message"), "{error}");
    }

    #[test]
    fn ml_dsa_44_keys_load_from_a_seed_file() {
        let dir = tempfile::tempdir().unwrap();
        let seed = [3u8; ML_DSA_SEED_LEN];
        let path = dir.path().join("node.key");
        std::fs::write(&path, format!("{}\n", hex::encode(seed))).unwrap();
        let own = MlDsa44Signer::from_seed(&seed).public_key();
        let file = format!("secret_key_file = \"{}\"", path.display());

        let config = ml_dsa(&format!("{file}{}", key_table(0, &own))).unwrap();
        let epoch = config.epoch().unwrap();
        let sig = epoch.context.signer.sign(b"m");
        assert!(epoch.context.verifier.verify(NodeId::dummy(0), b"m", &sig));

        // the listed key is not the file's
        let other = MlDsa44Signer::dev(NodeId::dummy(0)).public_key();
        let config = ml_dsa(&format!("{file}{}", key_table(0, &other))).unwrap();
        assert!(config.epoch().is_err());

        // a file that is not a seed
        std::fs::write(&path, "abcd").unwrap();
        let config = ml_dsa(&format!("{file}{}", key_table(0, &own))).unwrap();
        assert!(config.epoch().is_err());
        assert!(load_ml_dsa_seed(&dir.path().join("absent")).is_err());
    }

    #[test]
    fn ml_dsa_44_key_lists_are_checked() {
        let key = MlDsa44Signer::dev(NodeId::dummy(0)).public_key();
        let file = "secret_key_file = \"/unused\"";
        // only one of the two
        assert!(ml_dsa(file).is_err());
        assert!(ml_dsa(&key_table(0, &key)).is_err());
        // a key outside the set, twice, or malformed
        assert!(ml_dsa(&format!("{file}{}", key_table(1, &key))).is_err());
        let twice = format!("{file}{}{}", key_table(0, &key), key_table(0, &key));
        assert!(ml_dsa(&twice).is_err());
        let malformed =
            format!("{file}\n[[quantum.validators]]\nnode_id = 0\npublic_key = \"00\"\n");
        assert!(ml_dsa(&malformed).is_err());
        assert!(ml_dsa(&format!("{file}{}", key_table(0, &key))).is_ok());

        // the keys need ml-dsa-44
        let text = format!("{EXAMPLE}{}", key_table(0, &key));
        let config = PqNodeConfig::parse(&text).unwrap();
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
