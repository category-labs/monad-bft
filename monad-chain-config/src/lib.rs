use execution_revision::MonadExecutionRevision;
use monad_types::Round;
use revision::{ChainParams, ChainRevision, MockChainRevision, MonadChainRevision};
use serde::Deserialize;
use thiserror::Error;
use tracing::{info, warn};

pub mod execution_revision;
pub mod revision;

/// CHAIN_ID
pub const ETHEREUM_MAINNET_CHAIN_ID: u64 = 1;
pub const MONAD_MAINNET_CHAIN_ID: u64 = 143;
pub const MONAD_TESTNET_CHAIN_ID: u64 = 10143;
pub const MONAD_DEVNET_CHAIN_ID: u64 = 20143;

pub trait ChainConfig<CR: ChainRevision>: Copy + Clone {
    fn chain_id(&self) -> u64;
    fn get_chain_revision(&self, round: Round) -> CR;
    fn get_execution_chain_revision(&self, execution_timestamp_s: u64) -> MonadExecutionRevision;
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MonadChainConfig {
    pub chain_id: u64,
    pub v_0_7_0_activation: Round,
    pub v_0_8_0_activation: Round,

    pub execution_v_one_activation: u64,
    pub execution_v_two_activation: u64,
}

#[derive(Debug, Error)]
pub enum ChainConfigError {
    WrongOverrideChainId(u64),
    UnsupportedChainId(u64),
}

impl std::fmt::Display for ChainConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl MonadChainConfig {
    pub fn new(
        chain_id: u64,
        devnet_override: Option<MonadChainConfig>,
    ) -> Result<Self, ChainConfigError> {
        if chain_id == MONAD_MAINNET_CHAIN_ID {
            if devnet_override.is_some() {
                warn!("Ignoring chain config from file in mainnet");
            }
            Ok(MONAD_MAINNET_CHAIN_CONFIG)
        } else if chain_id == MONAD_TESTNET_CHAIN_ID {
            if devnet_override.is_some() {
                warn!("Ignoring chain config from file in testnet");
            }
            Ok(MONAD_TESTNET_CHAIN_CONFIG)
        } else if chain_id == MONAD_DEVNET_CHAIN_ID {
            let Some(override_config) = devnet_override else {
                info!("Using default devnet chain config");
                return Ok(MONAD_DEVNET_CHAIN_CONFIG);
            };

            if override_config.chain_id != MONAD_DEVNET_CHAIN_ID {
                return Err(ChainConfigError::WrongOverrideChainId(
                    override_config.chain_id,
                ));
            }

            info!("Using override devnet chain config");
            Ok(override_config)
        } else {
            Err(ChainConfigError::UnsupportedChainId(chain_id))
        }
    }
}

impl ChainConfig<MonadChainRevision> for MonadChainConfig {
    fn chain_id(&self) -> u64 {
        self.chain_id
    }

    #[allow(clippy::if_same_then_else)]
    fn get_chain_revision(&self, round: Round) -> MonadChainRevision {
        if round >= self.v_0_8_0_activation {
            MonadChainRevision::V_0_8_0
        } else if round >= self.v_0_7_0_activation {
            MonadChainRevision::V_0_7_0
        } else {
            MonadChainRevision::V_0_7_0
        }
    }

    fn get_execution_chain_revision(&self, execution_timestamp_s: u64) -> MonadExecutionRevision {
        if execution_timestamp_s >= self.execution_v_two_activation {
            MonadExecutionRevision::V_TWO
        } else if execution_timestamp_s >= self.execution_v_one_activation {
            MonadExecutionRevision::V_ONE
        } else {
            MonadExecutionRevision::V_ZERO
        }
    }
}

const MONAD_DEVNET_CHAIN_CONFIG: MonadChainConfig = MonadChainConfig {
    chain_id: MONAD_DEVNET_CHAIN_ID,
    v_0_7_0_activation: Round::MIN,
    v_0_8_0_activation: Round::MIN,

    execution_v_one_activation: 0,
    execution_v_two_activation: 0,
};

const MONAD_TESTNET_CHAIN_CONFIG: MonadChainConfig = MonadChainConfig {
    chain_id: MONAD_TESTNET_CHAIN_ID,
    v_0_7_0_activation: Round::MIN,
    v_0_8_0_activation: Round(3263000),

    execution_v_one_activation: 1739559600, // 2025-02-14T19:00:00.000Z
    execution_v_two_activation: 1741978800, // 2025-03-14T19:00:00.000Z
};

// Mainnet uses latest version of testnet from genesis
const MONAD_MAINNET_CHAIN_CONFIG: MonadChainConfig = MonadChainConfig {
    chain_id: MONAD_MAINNET_CHAIN_ID,
    v_0_7_0_activation: Round::MIN,
    v_0_8_0_activation: Round::MIN,

    execution_v_one_activation: 0,
    execution_v_two_activation: 0,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MockChainConfig {
    chain_params: &'static ChainParams,
}

impl MockChainConfig {
    pub fn new(chain_params: &'static ChainParams) -> Self {
        Self { chain_params }
    }
}

impl ChainConfig<MockChainRevision> for MockChainConfig {
    fn chain_id(&self) -> u64 {
        20143
    }

    fn get_chain_revision(&self, _round: Round) -> MockChainRevision {
        MockChainRevision {
            chain_params: self.chain_params,
        }
    }

    fn get_execution_chain_revision(&self, _execution_timestamp_s: u64) -> MonadExecutionRevision {
        MonadExecutionRevision::LATEST
    }
}
