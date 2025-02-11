use std::{fmt::Debug, time::Duration};

pub const CHAIN_PARAMS_LATEST: ChainParams = CHAIN_PARAMS_V_0_8_0;

pub trait ChainRevision: Copy + Clone {
    fn chain_params(&self) -> &'static ChainParams;
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub enum MonadChainRevision {
    V_0_7_0,
    V_0_8_0,
}

impl ChainRevision for MonadChainRevision {
    fn chain_params(&self) -> &'static ChainParams {
        match &self {
            MonadChainRevision::V_0_7_0 => &CHAIN_PARAMS_V_0_7_0,
            MonadChainRevision::V_0_8_0 => &CHAIN_PARAMS_V_0_8_0,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MockChainRevision {
    pub chain_params: &'static ChainParams,
}

impl ChainRevision for MockChainRevision {
    fn chain_params(&self) -> &'static ChainParams {
        self.chain_params
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChainParams {
    pub tx_limit: usize,
    pub proposal_gas_limit: u64,
    // Max proposal size in bytes (average transactions ~400 bytes)
    pub proposal_byte_limit: u64,
    pub vote_pace: Duration,
}

const CHAIN_PARAMS_V_0_7_0: ChainParams = ChainParams {
    tx_limit: 10_000,
    proposal_gas_limit: 300_000_000,
    proposal_byte_limit: 4_000_000,
    vote_pace: Duration::from_millis(1000),
};

const CHAIN_PARAMS_V_0_8_0: ChainParams = ChainParams {
    tx_limit: 5_000,
    proposal_gas_limit: 150_000_000,
    proposal_byte_limit: 2_000_000,
    vote_pace: Duration::from_millis(500),
};

#[cfg(test)]
mod test {
    use crate::revision::MonadChainRevision;

    #[test]
    fn chain_revision_ord() {
        assert!(MonadChainRevision::V_0_7_0 < MonadChainRevision::V_0_8_0);
    }
}
