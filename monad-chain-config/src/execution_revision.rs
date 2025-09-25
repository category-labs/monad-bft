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

#[allow(non_camel_case_types)]
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub enum MonadExecutionRevision {
    V_ZERO,
    V_ONE,
    V_TWO,
    V_FOUR,
    V_FIVE,
}

impl MonadExecutionRevision {
    pub const LATEST: Self = Self::V_FIVE;
}

impl MonadExecutionRevision {
    pub fn execution_chain_params(&self) -> &'static ExecutionChainParams {
        match &self {
            Self::V_ZERO => &EXECUTION_CHAIN_PARAMS_V_ZERO,
            Self::V_ONE => &EXECUTION_CHAIN_PARAMS_V_ONE,
            Self::V_TWO => &EXECUTION_CHAIN_PARAMS_V_TWO,
            Self::V_FOUR => &EXECUTION_CHAIN_PARAMS_V_FOUR,
            Self::V_FIVE => &EXECUTION_CHAIN_PARAMS_V_FIVE,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExecutionChainParams {
    pub max_code_size: usize,
    pub tfm_enabled: bool,
    pub prague_enabled: bool,
    pub validate_system_txs: bool,
}

const EXECUTION_CHAIN_PARAMS_V_ZERO: ExecutionChainParams = ExecutionChainParams {
    max_code_size: 24 * 1024,
    tfm_enabled: false,
    prague_enabled: false,
    validate_system_txs: false,
};

const EXECUTION_CHAIN_PARAMS_V_ONE: ExecutionChainParams = ExecutionChainParams {
    max_code_size: 24 * 1024,
    tfm_enabled: false,
    prague_enabled: false,
    validate_system_txs: false,
};

const EXECUTION_CHAIN_PARAMS_V_TWO: ExecutionChainParams = ExecutionChainParams {
    max_code_size: 128 * 1024,
    tfm_enabled: false,
    prague_enabled: false,
    validate_system_txs: false,
};

const EXECUTION_CHAIN_PARAMS_V_FOUR: ExecutionChainParams = ExecutionChainParams {
    max_code_size: 128 * 1024,
    tfm_enabled: true,
    prague_enabled: true,
    validate_system_txs: true,
};

const EXECUTION_CHAIN_PARAMS_V_FIVE: ExecutionChainParams = ExecutionChainParams {
    max_code_size: 128 * 1024,
    tfm_enabled: true,
    prague_enabled: true,
    validate_system_txs: true,
};
