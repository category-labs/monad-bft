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

use std::collections::BTreeMap;

use futures::{StreamExt, TryStreamExt};
use monad_rpc_docs::rpc;
use monad_triedb_utils::triedb_env::Triedb;
use serde::Deserialize;
use tracing::trace;

use crate::{
    data::{get_block_key_from_tag_or_hash, DataProvider},
    types::{
        eth_json::{BlockTagOrHash, EthAddress, EthHash, FixedData, StorageKey},
        jsonrpc::{ErrorCode, JsonRpcError, JsonRpcResult},
    },
};

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetBalanceParams {
    account: EthAddress,
    block_number: BlockTagOrHash,
}

#[rpc(method = "eth_getBalance")]
#[allow(non_snake_case)]
/// Returns the balance of the account of given address.
pub async fn monad_eth_getBalance<T: Triedb>(
    data_provider: &DataProvider<T>,
    params: MonadEthGetBalanceParams,
) -> JsonRpcResult<String> {
    trace!("monad_eth_getBalance: {params:?}");

    let block_key = get_block_key_from_tag_or_hash(&data_provider.triedb_env, params.block_number)
        .await
        .ok_or_else(JsonRpcError::block_not_found)?;
    let account = data_provider
        .triedb_env
        .get_account(block_key, params.account.0)
        .await
        .map_err(JsonRpcError::internal_error)?;

    match data_provider
        .triedb_env
        .get_state_availability(block_key)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        true => Ok(format!("0x{:x}", account.balance)),
        false => Err(JsonRpcError::block_not_found()),
    }
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetCodeParams {
    account: EthAddress,
    block: BlockTagOrHash,
}

#[rpc(method = "eth_getCode")]
#[allow(non_snake_case)]
/// Returns code at a given address.
pub async fn monad_eth_getCode<T: Triedb>(
    data_provider: &DataProvider<T>,
    params: MonadEthGetCodeParams,
) -> JsonRpcResult<String> {
    trace!("monad_eth_getCode: {params:?}");

    let block_key = get_block_key_from_tag_or_hash(&data_provider.triedb_env, params.block)
        .await
        .ok_or_else(JsonRpcError::block_not_found)?;
    let account = data_provider
        .triedb_env
        .get_account(block_key, params.account.0)
        .await
        .map_err(JsonRpcError::internal_error)?;

    let code = if let Some(code_hash) = account.code_hash {
        data_provider
            .triedb_env
            .get_code(block_key, code_hash)
            .await
            .map_err(JsonRpcError::internal_error)?
    } else {
        Vec::default()
    };

    match data_provider
        .triedb_env
        .get_state_availability(block_key)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        true => Ok(format!("0x{}", hex::encode(code))),
        false => Err(JsonRpcError::block_not_found()),
    }
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetStorageAtParams {
    account: EthAddress,
    position: StorageKey,
    block: BlockTagOrHash,
}

#[rpc(method = "eth_getStorageAt")]
#[allow(non_snake_case)]
/// Returns the value from a storage position at a given address.
pub async fn monad_eth_getStorageAt<T: Triedb>(
    data_provider: &DataProvider<T>,
    params: MonadEthGetStorageAtParams,
) -> JsonRpcResult<String> {
    trace!("monad_eth_getStorageAt: {params:?}");

    let block_key = get_block_key_from_tag_or_hash(&data_provider.triedb_env, params.block)
        .await
        .ok_or_else(JsonRpcError::block_not_found)?;
    let storage_value = data_provider
        .triedb_env
        .get_storage_at(block_key, params.account.0, params.position.0)
        .await
        .map_err(JsonRpcError::internal_error)?;

    match data_provider
        .triedb_env
        .get_state_availability(block_key)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        true => Ok(format!("0x{}", hex::encode(storage_value))),
        false => Err(JsonRpcError::block_not_found()),
    }
}

const MAX_GET_STORAGE_VALUES_SLOTS: usize = 1024;
const MAX_CONCURRENT_STORAGE_READS: usize = 64;

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetStorageValuesParams {
    requests: BTreeMap<EthAddress, Vec<StorageKey>>,
    #[serde(default)]
    block: BlockTagOrHash,
}

#[rpc(method = "eth_getStorageValues")]
#[allow(non_snake_case)]
/// Returns the values of multiple storage slots for multiple accounts, up to 1024 slots per request.
pub async fn monad_eth_getStorageValues<T: Triedb>(
    data_provider: &DataProvider<T>,
    params: MonadEthGetStorageValuesParams,
) -> JsonRpcResult<BTreeMap<EthAddress, Vec<EthHash>>> {
    trace!("monad_eth_getStorageValues: {params:?}");

    let total_slots = params.requests.values().map(Vec::len).sum::<usize>();
    if total_slots == 0 {
        return Err(JsonRpcError::with_message(
            ErrorCode::InvalidParams,
            "empty request",
        ));
    }
    if total_slots > MAX_GET_STORAGE_VALUES_SLOTS {
        return Err(JsonRpcError::with_message(
            ErrorCode::InvalidParams,
            format!(
                "Too many storage slots requested: {}, maximum allowed is {}",
                total_slots, MAX_GET_STORAGE_VALUES_SLOTS
            ),
        ));
    }

    let block_key = get_block_key_from_tag_or_hash(&data_provider.triedb_env, params.block)
        .await
        .ok_or_else(JsonRpcError::block_not_found)?;

    let reads = params
        .requests
        .iter()
        .flat_map(|(address, slots)| slots.iter().map(move |slot| (address, slot)));
    let values: Vec<EthHash> = futures::stream::iter(reads)
        .map(|(address, slot)| {
            data_provider
                .triedb_env
                .get_storage_at(block_key, address.0, slot.0)
        })
        .buffered(MAX_CONCURRENT_STORAGE_READS)
        .map_ok(FixedData)
        .try_collect()
        .await
        .map_err(JsonRpcError::internal_error)?;

    let mut values = values.into_iter();
    let result: BTreeMap<_, Vec<_>> = params
        .requests
        .into_iter()
        .map(|(address, slots)| (address, values.by_ref().take(slots.len()).collect()))
        .collect();

    match data_provider
        .triedb_env
        .get_state_availability(block_key)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        true => Ok(result),
        false => Err(JsonRpcError::block_not_found()),
    }
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetTransactionCountParams {
    account: EthAddress,
    block: BlockTagOrHash,
}

#[rpc(method = "eth_getTransactionCount")]
#[allow(non_snake_case)]
/// Returns the number of transactions sent from an address.
pub async fn monad_eth_getTransactionCount<T: Triedb>(
    data_provider: &DataProvider<T>,
    params: MonadEthGetTransactionCountParams,
) -> JsonRpcResult<String> {
    trace!("monad_eth_getTransactionCount: {params:?}");

    let block_key = get_block_key_from_tag_or_hash(&data_provider.triedb_env, params.block)
        .await
        .ok_or_else(JsonRpcError::block_not_found)?;
    let account = data_provider
        .triedb_env
        .get_account(block_key, params.account.0)
        .await
        .map_err(JsonRpcError::internal_error)?;

    match data_provider
        .triedb_env
        .get_state_availability(block_key)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        true => Ok(format!("0x{:x}", account.nonce)),
        false => Err(JsonRpcError::block_not_found()),
    }
}

#[allow(non_snake_case)]
/// Returns an object with data about the sync status or false.
#[rpc(method = "eth_syncing")]
pub async fn monad_eth_syncing() -> JsonRpcResult<bool> {
    trace!("monad_eth_syncing");

    Ok(false)
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, sync::Arc};

    use monad_triedb_utils::mock_triedb::MockTriedb;

    use super::{
        monad_eth_getStorageValues, MonadEthGetStorageAtParams, MonadEthGetStorageValuesParams,
        MAX_GET_STORAGE_VALUES_SLOTS,
    };
    use crate::{
        data::DataProvider,
        types::{
            eth_json::{BlockTagOrHash, BlockTags, EthAddress, FixedData, Quantity},
            jsonrpc::{ErrorCode, JsonRpcError},
        },
    };

    #[test]
    fn params_without_eip_1898() {
        let res: MonadEthGetStorageAtParams = serde_json::from_str(
            r#"["0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF", "0x0", "latest"]"#,
        )
        .unwrap();
        assert!(matches!(
            res.block,
            super::BlockTagOrHash::BlockTags(BlockTags::Latest)
        ));
        let res: MonadEthGetStorageAtParams =
            serde_json::from_str(r#"["0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF", "0x0", "0x1"]"#)
                .unwrap();
        assert!(matches!(
            res.block,
            super::BlockTagOrHash::BlockTags(BlockTags::Number(Quantity(1)))
        ));
    }

    #[test]
    fn eip_1898_blockhash() {
        let res: MonadEthGetStorageAtParams = serde_json::from_str(r#"["0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF", "0x0", {"blockHash": "0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3"}]"#).unwrap();
        assert!(matches!(res.block, super::BlockTagOrHash::Hash(_)));
        let res: MonadEthGetStorageAtParams = serde_json::from_str(r#"["0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF", "0x0", {"blockHash": "0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3", "requireCanonical": false}]"#).unwrap();
        assert!(matches!(res.block, super::BlockTagOrHash::Hash(_)));
        let res: MonadEthGetStorageAtParams = serde_json::from_str(r#"["0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF", "0x0", {"blockHash": "0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3", "requireCanonical": true}]"#).unwrap();
        assert!(matches!(res.block, super::BlockTagOrHash::Hash(_)));
    }

    #[test]
    fn eip_1898_blocknumber() {
        let res: MonadEthGetStorageAtParams = serde_json::from_str(
            r#"["0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF", "0x0", {"blockNumber": "0x0"}]"#,
        )
        .unwrap();
        assert!(matches!(
            res.block,
            super::BlockTagOrHash::BlockTags(BlockTags::Number(Quantity(0)))
        ));
        let res: MonadEthGetStorageAtParams = serde_json::from_str(
            r#"["0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF", "0x0", {"blockNumber": "latest"}]"#,
        )
        .unwrap();
        assert!(matches!(
            res.block,
            super::BlockTagOrHash::BlockTags(BlockTags::Latest)
        ));
    }

    #[test]
    fn storage_key_valid_and_right_aligned() {
        let mut expected = [0u8; 32];
        expected[31] = 1;

        let res: MonadEthGetStorageAtParams = serde_json::from_str(
            r#"["0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF", "0x0000000000000000000000000000000000000000000000000000000000000001", "latest"]"#,
        )
        .unwrap();
        assert_eq!(res.position.0, expected);

        let res: MonadEthGetStorageAtParams = serde_json::from_str(
            r#"["0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF", "0x1", "latest"]"#,
        )
        .unwrap();
        assert_eq!(res.position.0, expected);
    }

    #[test]
    fn storage_key_too_many_hex_digits_rejected() {
        assert!(serde_json::from_str::<MonadEthGetStorageAtParams>(
            r#"["0xaa00000000000000000000000000000000000000", "0x00000000000000000000000000000000000000000000000000000000000000000", "latest"]"#,
        )
        .is_err());
    }

    #[test]
    fn storage_key_invalid_hex_rejected() {
        assert!(serde_json::from_str::<MonadEthGetStorageAtParams>(
            r#"["0xaa00000000000000000000000000000000000000", "0xasdf", "latest"]"#,
        )
        .is_err());
    }

    #[tokio::test]
    async fn get_storage_values() {
        let account_a = EthAddress::from_str("0xaa00000000000000000000000000000000000000").unwrap();
        let account_b = EthAddress::from_str("0xbb00000000000000000000000000000000000000").unwrap();
        let slot_0 = [0u8; 32];
        let mut slot_1 = [0u8; 32];
        slot_1[31] = 1;

        let mut triedb = MockTriedb::default();
        triedb.set_storage(account_a.0, slot_0, [0xa0; 32]);
        triedb.set_storage(account_a.0, slot_1, [0xa1; 32]);
        triedb.set_storage(account_b.0, slot_0, [0xb0; 32]);
        let provider = DataProvider::new(None, Arc::new(triedb), None);

        let params: MonadEthGetStorageValuesParams = serde_json::from_str(
            r#"[{"0xaa00000000000000000000000000000000000000": ["0x1", "0x0"], "0xbb00000000000000000000000000000000000000": ["0x0", "0x1"]}, "latest"]"#,
        )
        .unwrap();
        let res = monad_eth_getStorageValues(&provider, params).await.unwrap();

        assert_eq!(res.len(), 2);
        assert_eq!(
            res[&account_a],
            vec![FixedData([0xa1; 32]), FixedData([0xa0; 32])]
        );
        assert_eq!(
            res[&account_b],
            vec![FixedData([0xb0; 32]), FixedData([0x00; 32])]
        );
        assert_eq!(
            serde_json::to_value(&res[&account_b]).unwrap(),
            serde_json::json!([
                format!("0x{}", "b0".repeat(32)),
                format!("0x{}", "00".repeat(32)),
            ])
        );
    }

    async fn get_storage_values_err(params: serde_json::Value) -> JsonRpcError {
        let provider = DataProvider::new(None, Arc::new(MockTriedb::default()), None);
        let params: MonadEthGetStorageValuesParams = serde_json::from_value(params).unwrap();
        monad_eth_getStorageValues(&provider, params)
            .await
            .unwrap_err()
    }

    #[tokio::test]
    async fn get_storage_values_limits() {
        let slots = vec!["0x0"; MAX_GET_STORAGE_VALUES_SLOTS + 1];
        let err = get_storage_values_err(serde_json::json!([
            {"0xaa00000000000000000000000000000000000000": slots},
            "latest"
        ]))
        .await;

        assert_eq!(err.code, ErrorCode::InvalidParams.code());
        assert!(err
            .message
            .contains(&MAX_GET_STORAGE_VALUES_SLOTS.to_string()));
    }

    #[tokio::test]
    async fn get_storage_values_max_slots() {
        let provider = DataProvider::new(None, Arc::new(MockTriedb::default()), None);
        let slots = vec!["0x0"; MAX_GET_STORAGE_VALUES_SLOTS];
        let params: MonadEthGetStorageValuesParams = serde_json::from_value(serde_json::json!([
            {"0xaa00000000000000000000000000000000000000": slots},
            "latest"
        ]))
        .unwrap();
        let res = monad_eth_getStorageValues(&provider, params).await.unwrap();

        assert_eq!(
            res.values().map(Vec::len).sum::<usize>(),
            MAX_GET_STORAGE_VALUES_SLOTS
        );
    }

    #[tokio::test]
    async fn get_storage_values_empty() {
        let empty = JsonRpcError::with_message(ErrorCode::InvalidParams, "empty request");

        assert_eq!(
            get_storage_values_err(serde_json::json!([{}, "latest"])).await,
            empty
        );
        assert_eq!(
            get_storage_values_err(serde_json::json!([
                {
                    "0xaa00000000000000000000000000000000000000": [],
                    "0xbb00000000000000000000000000000000000000": []
                },
                "latest"
            ]))
            .await,
            empty
        );
    }

    #[test]
    fn get_storage_values_default_block() {
        let params: MonadEthGetStorageValuesParams =
            serde_json::from_str(r#"[{"0xaa00000000000000000000000000000000000000": ["0x0"]}]"#)
                .unwrap();

        assert_eq!(params.block, BlockTagOrHash::BlockTags(BlockTags::Latest));
    }
}
