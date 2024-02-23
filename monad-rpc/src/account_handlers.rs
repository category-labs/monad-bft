use log::{debug, trace};
use serde::Deserialize;
use serde_json::Value;

use crate::{
    eth_json_types::{deserialize_block_tags, deserialize_fixed_data, BlockTags, EthAddress},
    jsonrpc::JsonRpcError,
};

#[derive(Deserialize, Debug)]
struct MonadEthGetBalanceParams {
    #[serde(deserialize_with = "deserialize_fixed_data")]
    account: EthAddress,
    #[serde(deserialize_with = "deserialize_block_tags")]
    block: BlockTags,
}

#[allow(non_snake_case)]
pub async fn monad_eth_getBalance(params: Value) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_getBalance: {params:?}");

    let p: MonadEthGetBalanceParams = match serde_json::from_value(params) {
        Ok(s) => s,
        Err(e) => {
            debug!("invalid params {e}");
            return Err(JsonRpcError::invalid_params());
        }
    };

    Ok(().into())
}
