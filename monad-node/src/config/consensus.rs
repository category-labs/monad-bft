use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConsensusConfig {
    pub block_txn_limit: usize,
    pub execution_delay: u64,
}
