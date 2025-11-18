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

use monad_types::Epoch;
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
struct BlockRewardConfig {
    block_reward_activation: Epoch,
    block_reward_mon: u64,
}

impl BlockRewardConfig {
    pub const fn unused() -> Self {
        Self {
            block_reward_activation: Epoch::MAX,
            block_reward_mon: 0,
        }
    }
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct MonadStakingConfig {
    staking_activation: Epoch,

    block_reward_v_one: BlockRewardConfig,
    block_reward_v_two: BlockRewardConfig,
}

impl MonadStakingConfig {
    pub fn get_staking_activation(&self) -> Epoch {
        self.staking_activation
    }

    pub fn get_block_reward_mon(&self, epoch: Epoch) -> u64 {
        if epoch >= self.block_reward_v_two.block_reward_activation {
            self.block_reward_v_two.block_reward_mon
        } else if epoch >= self.block_reward_v_one.block_reward_activation {
            self.block_reward_v_one.block_reward_mon
        } else {
            0
        }
    }
}

pub const MONAD_DEVNET_STAKING_CONFIG: MonadStakingConfig = MonadStakingConfig {
    staking_activation: Epoch(2),

    block_reward_v_one: BlockRewardConfig {
        block_reward_activation: Epoch(5),
        block_reward_mon: 1,
    },
    block_reward_v_two: BlockRewardConfig {
        block_reward_activation: Epoch(8),
        block_reward_mon: 25,
    },
    block_reward_v_two: BlockRewardConfig::unused(),
};

pub const MONAD_TESTNET_STAKING_CONFIG: MonadStakingConfig = MonadStakingConfig {
    staking_activation: Epoch(868),

    block_reward_v_one: BlockRewardConfig {
        block_reward_activation: Epoch(869),
        block_reward_mon: 1,
    },
    block_reward_v_two: BlockRewardConfig {
        block_reward_activation: Epoch(953),
        block_reward_mon: 25,
    },
};

pub const MONAD_MAINNET_STAKING_CONFIG: MonadStakingConfig = MonadStakingConfig {
    staking_activation: Epoch(675),

    block_reward_v_one: BlockRewardConfig {
        block_reward_activation: Epoch(747),
        block_reward_mon: 25,
    },
    block_reward_v_two: BlockRewardConfig::unused(),
};
