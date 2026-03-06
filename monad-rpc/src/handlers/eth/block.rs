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

use monad_rpc_docs::rpc;
use monad_triedb_utils::triedb_env::Triedb;
use serde::{Deserialize, Serialize};
use tracing::trace;

use crate::{
    chainstate::ChainState,
    types::{
        eth_json::{
            BlockTagOrHash, BlockTags, EthHash, MonadBlock, MonadTransactionReceipt, Quantity,
        },
        jsonrpc::{ChainStateResultMap, JsonRpcResult},
    },
};

#[rpc(method = "eth_blockNumber")]
#[allow(non_snake_case)]
#[tracing::instrument(level = "debug", skip_all)]
/// Returns the number of most recent block.
pub async fn monad_eth_blockNumber<T: Triedb>(
    chain_state: &ChainState<T>,
) -> JsonRpcResult<Quantity> {
    trace!("monad_eth_blockNumber");

    let block_num = chain_state.get_latest_block_number();
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
    chain_state: &ChainState<T>,
    params: MonadEthGetBlockByHashParams,
) -> JsonRpcResult<Option<MonadEthGetBlock>> {
    trace!("monad_eth_getBlockByHash: {params:?}");
    chain_state
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
    chain_state: &ChainState<T>,
    params: MonadEthGetBlockByNumberParams,
) -> JsonRpcResult<Option<MonadEthGetBlock>> {
    trace!("monad_eth_getBlockByNumber: {params:?}");
    chain_state
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
pub struct MonadEthGetBlockTransactionCountByHashParams {
    block_hash: EthHash,
}

#[rpc(method = "eth_getBlockTransactionCountByHash")]
#[allow(non_snake_case)]
#[tracing::instrument(level = "debug", skip_all)]
/// Returns the number of transactions in a block from a block matching the given block hash.
pub async fn monad_eth_getBlockTransactionCountByHash<T: Triedb>(
    chain_state: &ChainState<T>,
    params: MonadEthGetBlockTransactionCountByHashParams,
) -> JsonRpcResult<Option<String>> {
    trace!("monad_eth_getBlockTransactionCountByHash: {params:?}");
    chain_state
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
    chain_state: &ChainState<T>,
    params: MonadEthGetBlockTransactionCountByNumberParams,
) -> JsonRpcResult<Option<String>> {
    trace!("monad_eth_getBlockTransactionCountByNumber: {params:?}");
    chain_state
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

#[rpc(method = "eth_getBlockReceipts")]
#[allow(non_snake_case)]
#[tracing::instrument(level = "debug", skip_all)]
/// Returns the receipts of a block by number or hash.
pub async fn monad_eth_getBlockReceipts<T: Triedb>(
    chain_state: &ChainState<T>,
    params: MonadEthGetBlockReceiptsParams,
) -> JsonRpcResult<Option<MonadEthGetBlockReceiptsResult>> {
    trace!("monad_eth_getBlockReceipts: {params:?}");

    chain_state
        .get_block_receipts(params.block)
        .await
        .map_present_and_no_err(MonadEthGetBlockReceiptsResult)
}
