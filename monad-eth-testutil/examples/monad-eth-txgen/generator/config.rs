use std::error::Error;

use ruint::aliases::U256;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct EthTxGeneratorConfig {
    pub(super) root_private_key: String,
    pub(super) addresses: EthTxAddressConfig,
    pub(super) activity: EthTxActivityType,
    pub(super) interval_ms: u64,
}

impl EthTxGeneratorConfig {
    pub fn new_from_file(config_path: std::path::PathBuf) -> Result<Self, Box<dyn Error>> {
        let config = std::fs::read_to_string(config_path)?;

        Ok(serde_json::from_str(&config)?)
    }
}

#[derive(Deserialize)]
pub struct EthTxAddressConfig {
    pub(super) from: EthTxAddressPoolConfig,
    pub(super) to: EthTxAddressPoolConfig,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EthTxAddressPoolConfig {
    Single { private_key: String },
    RandomSeeded { seed: u64, count: usize },
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EthTxActivityType {
    NativeTokenTransfer { quantity: U256 },
    Erc20TokenTransfer { contract: String, quantity: U256 },
}
