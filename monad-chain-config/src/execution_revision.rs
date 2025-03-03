#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub enum MonadExecutionRevision {
    V_ZERO,
    V_ONE,
    V_TWO,
}

impl MonadExecutionRevision {
    pub const LATEST: Self = Self::V_TWO;
}

impl MonadExecutionRevision {
    pub fn execution_chain_params(&self) -> &'static ExecutionChainParams {
        match &self {
            Self::V_ZERO => &EXECUTION_CHAIN_PARAMS_V_ZERO,
            Self::V_ONE => &EXECUTION_CHAIN_PARAMS_V_ONE,
            Self::V_TWO => &EXECUTION_CHAIN_PARAMS_V_TWO,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExecutionChainParams {
    pub max_code_size: usize,
}

const EXECUTION_CHAIN_PARAMS_V_ZERO: ExecutionChainParams = ExecutionChainParams {
    max_code_size: 24 * 1024,
};

const EXECUTION_CHAIN_PARAMS_V_ONE: ExecutionChainParams = ExecutionChainParams {
    max_code_size: 24 * 1024,
};

const EXECUTION_CHAIN_PARAMS_V_TWO: ExecutionChainParams = ExecutionChainParams {
    max_code_size: 128 * 1024,
};
