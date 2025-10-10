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

use execution_revision::MonadExecutionRevision;
use monad_types::{Epoch, Round, SeqNum};
use revision::{
    ChainParams, ChainRevision, MockChainRevision, MonadChainRevision, CHAIN_PARAMS_LATEST,
};
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
pub const MONAD_TESTNET2_CHAIN_ID: u64 = 30143;

pub trait ChainConfig<CR: ChainRevision>: Copy + Clone {
    fn chain_id(&self) -> u64;
    fn get_epoch_length(&self) -> SeqNum;
    fn get_epoch_start_delay(&self) -> Round;
    fn get_staking_activation(&self) -> Epoch;
    fn get_staking_rewards_activation(&self) -> Epoch;
    fn get_chain_revision(&self, round: Round) -> CR;
    fn get_execution_chain_revision(&self, execution_timestamp_s: u64) -> MonadExecutionRevision;
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MonadChainConfig {
    pub chain_id: u64,
    pub epoch_length: SeqNum,
    pub epoch_start_delay: Round,

    pub v_0_7_0_activation: Round,
    pub v_0_8_0_activation: Round,
    pub v_0_10_0_activation: Round,
    pub v_0_11_0_activation: Round,

    pub staking_activation: Epoch,
    // TODO replace this with staking-specific chain config
    // this is necessary to support different rewards across different nets
    pub staking_rewards_activation: Epoch,

    pub execution_v_one_activation: u64,
    pub execution_v_two_activation: u64,
    pub execution_v_four_activation: u64,
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
        } else if chain_id == MONAD_TESTNET2_CHAIN_ID {
            if devnet_override.is_some() {
                warn!("Ignoring chain config from file in testnet");
            }
            Ok(MONAD_TESTNET2_CHAIN_CONFIG)
        } else {
            Err(ChainConfigError::UnsupportedChainId(chain_id))
        }
    }
}

impl ChainConfig<MonadChainRevision> for MonadChainConfig {
    fn chain_id(&self) -> u64 {
        self.chain_id
    }

    fn get_epoch_length(&self) -> SeqNum {
        self.epoch_length
    }

    fn get_epoch_start_delay(&self) -> Round {
        self.epoch_start_delay
    }

    fn get_staking_activation(&self) -> Epoch {
        self.staking_activation
    }

    fn get_staking_rewards_activation(&self) -> Epoch {
        self.staking_rewards_activation
    }

    #[allow(clippy::if_same_then_else)]
    fn get_chain_revision(&self, round: Round) -> MonadChainRevision {
        if round >= self.v_0_11_0_activation {
            MonadChainRevision::V_0_11_0
        } else if round >= self.v_0_10_0_activation {
            MonadChainRevision::V_0_10_0
        } else if round >= self.v_0_8_0_activation {
            MonadChainRevision::V_0_8_0
        } else if round >= self.v_0_7_0_activation {
            MonadChainRevision::V_0_7_0
        } else {
            MonadChainRevision::V_0_7_0
        }
    }

    fn get_execution_chain_revision(&self, execution_timestamp_s: u64) -> MonadExecutionRevision {
        if execution_timestamp_s >= self.execution_v_four_activation {
            MonadExecutionRevision::V_FOUR
        } else if execution_timestamp_s >= self.execution_v_two_activation {
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
    epoch_length: SeqNum(5_000),
    epoch_start_delay: Round(500),

    v_0_7_0_activation: Round::MIN,
    v_0_8_0_activation: Round::MIN,
    v_0_10_0_activation: Round::MIN,
    v_0_11_0_activation: Round::MIN,

    staking_activation: Epoch(4),
    staking_rewards_activation: Epoch(5),

    execution_v_one_activation: 0,
    execution_v_two_activation: 0,
    execution_v_four_activation: 0,
};

const MONAD_TESTNET_CHAIN_CONFIG: MonadChainConfig = MonadChainConfig {
    chain_id: MONAD_TESTNET_CHAIN_ID,
    epoch_length: SeqNum(50_000),
    epoch_start_delay: Round(5_000),

    v_0_7_0_activation: Round::MIN,
    v_0_8_0_activation: Round(3_263_000),
    v_0_10_0_activation: Round(32_026_929), // 2025-08-12T13:30:00.000Z
    v_0_11_0_activation: Round(42_036_176), // Approx 2025-09-29T13:30:00.000Z

    staking_activation: Epoch(809),
    staking_rewards_activation: Epoch(810),

    execution_v_one_activation: 1739559600, // 2025-02-14T19:00:00.000Z
    execution_v_two_activation: 1741978800, // 2025-03-14T19:00:00.000Z
    execution_v_four_activation: 1759152600, // 2025-09-29T13:30:00.000Z
};

const MONAD_TESTNET2_CHAIN_CONFIG: MonadChainConfig = MonadChainConfig {
    chain_id: MONAD_TESTNET2_CHAIN_ID,
    epoch_length: SeqNum(5_000),
    epoch_start_delay: Round(500),

    v_0_7_0_activation: Round::MIN,
    v_0_8_0_activation: Round::MIN,
    v_0_10_0_activation: Round::MIN,
    v_0_11_0_activation: Round::MIN,

    staking_activation: Epoch(4),
    staking_rewards_activation: Epoch(5),

    execution_v_one_activation: 0,
    execution_v_two_activation: 0,
    execution_v_four_activation: 1758029400, // 2025-09-16T13:30:00.000Z
};

// Mainnet uses latest version of testnet from genesis
const MONAD_MAINNET_CHAIN_CONFIG: MonadChainConfig = MonadChainConfig {
    chain_id: MONAD_MAINNET_CHAIN_ID,
    epoch_length: SeqNum(50_000),
    epoch_start_delay: Round(5_000),

    v_0_7_0_activation: Round::MIN,
    v_0_8_0_activation: Round::MIN,
    v_0_10_0_activation: Round(15643179), // 2025-08-13T13:30:00.000Z
    v_0_11_0_activation: Round::MAX,

    staking_activation: Epoch::MAX,
    staking_rewards_activation: Epoch::MAX,

    execution_v_one_activation: 0,
    execution_v_two_activation: 0,
    execution_v_four_activation: u64::MAX,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MockChainConfig {
    chain_params: &'static ChainParams,
    epoch_length: SeqNum,
    epoch_start_delay: Round,
}

impl Default for MockChainConfig {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl MockChainConfig {
    pub const DEFAULT: Self = Self {
        chain_params: &CHAIN_PARAMS_LATEST,
        epoch_length: SeqNum::MAX,
        epoch_start_delay: Round::MAX,
    };

    pub const fn new(chain_params: &'static ChainParams) -> Self {
        Self {
            chain_params,
            epoch_length: SeqNum::MAX,
            epoch_start_delay: Round::MAX,
        }
    }

    pub const fn new_with_epoch_params(
        chain_params: &'static ChainParams,
        epoch_length: SeqNum,
        epoch_start_delay: Round,
    ) -> Self {
        Self {
            chain_params,
            epoch_length,
            epoch_start_delay,
        }
    }
}

impl ChainConfig<MockChainRevision> for MockChainConfig {
    fn chain_id(&self) -> u64 {
        1337
    }

    fn get_epoch_length(&self) -> SeqNum {
        self.epoch_length
    }

    fn get_epoch_start_delay(&self) -> Round {
        self.epoch_start_delay
    }

    fn get_staking_activation(&self) -> Epoch {
        Epoch::MAX
    }

    fn get_staking_rewards_activation(&self) -> Epoch {
        Epoch::MAX
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
