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

use alloy_primitives::Address;
use monad_rpc_docs::rpc;
use monad_triedb_utils::triedb_env::Triedb;
use serde::{Deserialize, Serialize};
use tracing::trace;

use crate::{
    data::DataProvider,
    types::{
        eth_json::{
            BlockTagOrHash, BlockTags, EthHash, FixedData, MonadBlock, MonadTransactionReceipt,
            Quantity,
        },
        jsonrpc::{ChainStateResultMap, JsonRpcResult},
    },
};

#[rpc(method = "eth_blockNumber")]
#[allow(non_snake_case)]
#[tracing::instrument(level = "debug", skip_all)]
/// Returns the number of most recent block.
pub async fn monad_eth_blockNumber<T: Triedb>(
    data_provider: &DataProvider<T>,
) -> JsonRpcResult<Quantity> {
    trace!("monad_eth_blockNumber");

    let block_num = data_provider.get_latest_block_number();
    Ok(Quantity(block_num))
}

#[rpc(method = "eth_chainId", ignore = "chain_id")]
#[allow(non_snake_case)]
#[tracing::instrument(level = "debug")]
/// Returns the chain ID of the current network.
pub async fn monad_eth_chainId(chain_id: u64) -> JsonRpcResult<Quantity> {
    trace!("monad_eth_chainId");

    Ok(Quantity(chain_id))
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetBlockByHashParams {
    block_hash: EthHash,
    return_full_txns: bool,
}

#[derive(Serialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetBlock {
    #[serde(flatten)]
    block: MonadBlock,
}

#[rpc(method = "eth_getBlockByHash")]
#[allow(non_snake_case)]
#[tracing::instrument(level = "debug", skip_all)]
/// Returns information about a block by hash.
pub async fn monad_eth_getBlockByHash<T: Triedb>(
    data_provider: &DataProvider<T>,
    params: MonadEthGetBlockByHashParams,
) -> JsonRpcResult<Option<MonadEthGetBlock>> {
    trace!("monad_eth_getBlockByHash: {params:?}");
    data_provider
        .get_block(
            BlockTagOrHash::Hash(params.block_hash),
            params.return_full_txns,
        )
        .await
        .map_present_and_no_err(|block| MonadEthGetBlock {
            block: MonadBlock(block),
        })
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetBlockByNumberParams {
    block_number: BlockTags,
    return_full_txns: bool,
}

#[rpc(method = "eth_getBlockByNumber")]
#[allow(non_snake_case)]
#[tracing::instrument(level = "debug", skip_all)]
/// Returns information about a block by number.
pub async fn monad_eth_getBlockByNumber<T: Triedb>(
    data_provider: &DataProvider<T>,
    params: MonadEthGetBlockByNumberParams,
) -> JsonRpcResult<Option<MonadEthGetBlock>> {
    trace!("monad_eth_getBlockByNumber: {params:?}");
    data_provider
        .get_block(
            BlockTagOrHash::BlockTags(params.block_number),
            params.return_full_txns,
        )
        .await
        .map_present_and_no_err(|block| MonadEthGetBlock {
            block: MonadBlock(block),
        })
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadGetDomainHeaderParams {
    block_number: BlockTagOrHash,
}

#[derive(Serialize, Debug, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MonadDomainHeader {
    pub number: Quantity,
    pub state_root: EthHash,
    pub parent_hash: EthHash,
    pub timestamp: Quantity,
}

#[rpc(method = "monad_getDomainHeader", ignore = "domain")]
#[allow(non_snake_case)]
#[tracing::instrument(level = "debug", skip_all)]
/// Returns the domain's latest committed block header as of the given L1
/// block: its number (the L1 height it committed at) and its state root.
/// Only available on the /domain/<chain id> RPC route.
pub async fn monad_eth_getDomainHeader<T: Triedb>(
    data_provider: &DataProvider<T>,
    domain: Option<Address>,
    params: MonadGetDomainHeaderParams,
) -> JsonRpcResult<Option<MonadDomainHeader>> {
    trace!("monad_getDomainHeader: {params:?}");

    let Some(domain) = domain else {
        return Err(crate::types::jsonrpc::JsonRpcError::method_not_found());
    };

    let block_key =
        crate::data::get_block_key_from_tag_or_hash(&data_provider.triedb_env, params.block_number)
            .await
            .ok_or_else(crate::types::jsonrpc::JsonRpcError::block_not_found)?;

    let header = data_provider
        .triedb_env
        .get_domain_block_header(block_key, domain.0 .0)
        .await
        .map_err(crate::types::jsonrpc::JsonRpcError::internal_error)?;

    Ok(header.map(|header| MonadDomainHeader {
        number: Quantity(header.header.number),
        state_root: FixedData(header.header.state_root.0),
        parent_hash: FixedData(header.header.parent_hash.0),
        timestamp: Quantity(header.header.timestamp),
    }))
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetBlockTransactionCountByHashParams {
    block_hash: EthHash,
}

#[rpc(method = "eth_getBlockTransactionCountByHash")]
#[allow(non_snake_case)]
#[tracing::instrument(level = "debug", skip_all)]
/// Returns the number of transactions in a block from a block matching the given block hash.
pub async fn monad_eth_getBlockTransactionCountByHash<T: Triedb>(
    data_provider: &DataProvider<T>,
    params: MonadEthGetBlockTransactionCountByHashParams,
) -> JsonRpcResult<Option<String>> {
    trace!("monad_eth_getBlockTransactionCountByHash: {params:?}");
    data_provider
        .get_block(BlockTagOrHash::Hash(params.block_hash), true)
        .await
        .map_present_and_no_err(|block| format!("0x{:x}", block.transactions.len()))
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetBlockTransactionCountByNumberParams {
    block_tag: BlockTags,
}

#[rpc(method = "eth_getBlockTransactionCountByNumber")]
#[allow(non_snake_case)]
#[tracing::instrument(level = "debug", skip_all)]
/// Returns the number of transactions in a block matching the given block number.
pub async fn monad_eth_getBlockTransactionCountByNumber<T: Triedb>(
    data_provider: &DataProvider<T>,
    params: MonadEthGetBlockTransactionCountByNumberParams,
) -> JsonRpcResult<Option<String>> {
    trace!("monad_eth_getBlockTransactionCountByNumber: {params:?}");
    data_provider
        .get_block(BlockTagOrHash::BlockTags(params.block_tag), true)
        .await
        .map_present_and_no_err(|block| format!("0x{:x}", block.transactions.len()))
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetBlockReceiptsParams {
    block: BlockTagOrHash,
}

#[derive(Serialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetBlockReceiptsResult(Vec<MonadTransactionReceipt>);

#[rpc(method = "eth_getBlockReceipts", ignore = "base_chain_id,domain")]
#[allow(non_snake_case)]
#[tracing::instrument(level = "debug", skip_all)]
/// Returns the receipts of a block by number or hash.
pub async fn monad_eth_getBlockReceipts<T: Triedb>(
    data_provider: &DataProvider<T>,
    base_chain_id: u64,
    domain: Option<Address>,
    params: MonadEthGetBlockReceiptsParams,
) -> JsonRpcResult<Option<MonadEthGetBlockReceiptsResult>> {
    trace!("monad_eth_getBlockReceipts: {params:?}");

    data_provider
        .get_block_receipts_for_domain(params.block, base_chain_id, domain)
        .await
        .map_present_and_no_err(MonadEthGetBlockReceiptsResult)
}
