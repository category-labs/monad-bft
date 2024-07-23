use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConsensusConfig {
    pub block_txn_limit: usize,
    pub block_gas_limit: u64,
    pub execution_delay: u64,
    pub max_reserve_balance: u64,
    pub reserve_balance_check_mode: u8,
}
