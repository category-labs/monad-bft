use monad_types::Round;
use revision::{ChainParams, ChainRevision, MockChainRevision, MonadChainRevision};
use serde::Deserialize;
use thiserror::Error;
use tracing::{info, warn};

pub mod revision;

/// CHAIN_ID
// Intentionally disabled
// pub const MAINNET_CHAIN_ID: u64 = 143;
pub const TESTNET_CHAIN_ID: u64 = 10143;
pub const DEVNET_CHAIN_ID: u64 = 20143;

pub trait ChainConfig<CR: ChainRevision>: Copy + Clone {
    fn chain_id(&self) -> u64;
    fn get_chain_revision(&self, round: Round) -> CR;
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MonadChainConfig {
    pub chain_id: u64,
    pub v_0_7_0_activation: Round,
    pub v_0_8_0_activation: Round,
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
        if chain_id == TESTNET_CHAIN_ID {
            if devnet_override.is_some() {
                warn!("Ignoring chain config from file in testnet");
            }
            Ok(MONAD_TESTNET_CHAIN_CONFIG)
        } else if chain_id == DEVNET_CHAIN_ID {
            let Some(override_config) = devnet_override else {
                info!("Using default devnet chain config");
                return Ok(MONAD_DEVNET_CHAIN_CONFIG);
            };

            if override_config.chain_id != DEVNET_CHAIN_ID {
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
            MonadChainRevision::V0_8_0
        } else if round >= self.v_0_7_0_activation {
            MonadChainRevision::V0_7_0
        } else {
            MonadChainRevision::V0_7_0
        }
    }
}

const MONAD_DEVNET_CHAIN_CONFIG: MonadChainConfig = MonadChainConfig {
    chain_id: DEVNET_CHAIN_ID,
    v_0_7_0_activation: Round::MIN,
    v_0_8_0_activation: Round::MIN,
};

const MONAD_TESTNET_CHAIN_CONFIG: MonadChainConfig = MonadChainConfig {
    chain_id: TESTNET_CHAIN_ID,
    v_0_7_0_activation: Round::MIN,
    v_0_8_0_activation: Round(3263000),
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
}
