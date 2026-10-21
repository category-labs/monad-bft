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

use std::sync::Arc;

use monad_mcp_chorus::{
    spec::validator::ValidatorData as _,
    stub::types::{HeaderAuth, ProposerSchedule, ValidatorData},
};

use crate::{
    quorum::{Quorum, QuorumError},
    sig::{PqSigner, PqVerifier},
    types::{NodeId, TimestampDelta},
};

/// When this validator forwards its owned chunks (§4.4).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReleaseChunks {
    /// at its vote, the DA image of attaching chunks to the vote
    Vote,
    /// on the first DA event of the slot (paper §7 evaluation variant)
    Receipt,
}

/// Per-slot parameters, handed to every slot instance.
#[derive(Clone, Debug)]
pub struct QuantumConfig {
    pub delta: TimestampDelta,
    /// How long the path vote waits past `D + Δ` for an unresolved root
    /// with `f+1` positive votes before voting `⟨fallback, ⊥⟩` (D1).
    pub decode_grace: TimestampDelta,
    pub retransmit_interval: TimestampDelta,
    /// The Coda view timeout, in multiples of `delta` (paper: 5).
    pub coda_timeout_deltas: u32,
    pub release_chunks: ReleaseChunks,
    pub early_vote: bool,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ConfigError {
    DecodeGraceTooLong,
    ZeroRetransmitInterval,
    ZeroCodaTimeout,
    Quorum(QuorumError),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DecodeGraceTooLong => write!(f, "decode_grace must be at most delta / 2"),
            Self::ZeroRetransmitInterval => write!(f, "retransmit_interval must be positive"),
            Self::ZeroCodaTimeout => write!(f, "coda_timeout_deltas must be positive"),
            Self::Quorum(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for ConfigError {}

impl QuantumConfig {
    /// The defaults of §8.3 for a given `delta`.
    pub fn with_delta(delta: TimestampDelta) -> Self {
        Self {
            delta,
            decode_grace: TimestampDelta::from_nanos(delta.as_nanos() / 2),
            retransmit_interval: delta,
            coda_timeout_deltas: 5,
            release_chunks: ReleaseChunks::Vote,
            early_vote: true,
        }
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.decode_grace.as_nanos() > self.delta.as_nanos() / 2 {
            return Err(ConfigError::DecodeGraceTooLong);
        }
        if self.retransmit_interval == TimestampDelta::ZERO {
            return Err(ConfigError::ZeroRetransmitInterval);
        }
        if self.coda_timeout_deltas == 0 {
            return Err(ConfigError::ZeroCodaTimeout);
        }
        Ok(())
    }

    pub fn coda_timeout(&self) -> TimestampDelta {
        self.delta * self.coda_timeout_deltas as usize
    }
}

/// State shared by every slot instance of a node.
pub struct QuantumContext {
    pub node_id: NodeId,
    pub signer: Arc<dyn PqSigner>,
    pub verifier: Arc<dyn PqVerifier>,
    pub validator_data: Arc<ValidatorData>,
    pub header_auth: Arc<HeaderAuth>,
    /// The number of proposal indices per slot (`K`) and who proposes where.
    pub proposers: Arc<dyn ProposerSchedule + Send + Sync>,
    quorum: Quorum,
    validators: Arc<[NodeId]>,
}

impl QuantumContext {
    /// Fails unless every validator has the same stake (D8).
    pub fn new(
        node_id: NodeId,
        signer: Arc<dyn PqSigner>,
        verifier: Arc<dyn PqVerifier>,
        validator_data: Arc<ValidatorData>,
        header_auth: Arc<HeaderAuth>,
        proposers: Arc<dyn ProposerSchedule + Send + Sync>,
    ) -> Result<Self, ConfigError> {
        let quorum = Quorum::from_validators(&validator_data).map_err(ConfigError::Quorum)?;
        let validators = validator_data.nodes().copied().collect();
        Ok(Self {
            node_id,
            signer,
            verifier,
            validator_data,
            header_auth,
            proposers,
            quorum,
            validators,
        })
    }

    pub fn quorum(&self) -> Quorum {
        self.quorum
    }

    /// The validators in their canonical (sorted) order.
    pub fn validators(&self) -> &Arc<[NodeId]> {
        &self.validators
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_validate() {
        let config = QuantumConfig::with_delta(TimestampDelta::from_millis(150));
        assert_eq!(config.decode_grace, TimestampDelta::from_millis(75));
        assert_eq!(config.coda_timeout(), TimestampDelta::from_millis(750));
        assert_eq!(config.validate(), Ok(()));
    }

    #[test]
    fn invalid_configs_rejected() {
        let base = QuantumConfig::with_delta(TimestampDelta::from_millis(150));

        let mut config = base.clone();
        config.decode_grace = TimestampDelta::from_millis(76);
        assert_eq!(config.validate(), Err(ConfigError::DecodeGraceTooLong));

        let mut config = base.clone();
        config.retransmit_interval = TimestampDelta::ZERO;
        assert_eq!(config.validate(), Err(ConfigError::ZeroRetransmitInterval));

        let mut config = base;
        config.coda_timeout_deltas = 0;
        assert_eq!(config.validate(), Err(ConfigError::ZeroCodaTimeout));
    }
}
