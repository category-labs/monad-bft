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

use std::{
    ops::{Div, Sub},
    sync::Arc,
};

use alloy_consensus::{Header, Transaction, TxEnvelope};
use alloy_primitives::{Address, TxKind, U256, U64};
use alloy_rpc_types::{FeeHistory, TransactionReceipt};
use futures::stream::StreamExt;
use itertools::Itertools;
use monad_ethcall::{CallResult, EthCallExecutor, MonadTracer, StateOverrideSet};
use monad_rpc_docs::rpc;
use monad_triedb_utils::triedb_env::{BlockKey, FinalizedBlockKey, ProposedBlockKey, Triedb};
use monad_types::{BlockId, Hash, SeqNum};
use serde::Deserialize;
use tracing::trace;

use crate::{
    chainstate::{get_block_key_from_tag, ChainState},
    eth_json_types::{BlockTagOrHash, BlockTags, MonadFeeHistory, Quantity},
    handlers::eth::call::{fill_gas_params, CallRequest},
    jsonrpc::{JsonRpcError, JsonRpcResult},
};

/// Additional gas added during a CALL.
const CALL_STIPEND: u64 = 2_300;

trait EthCallProvider {
    async fn eth_call(
        &self,
        txn: TxEnvelope,
        eth_call_executor: Option<Arc<EthCallExecutor>>,
    ) -> CallResult;
}

struct GasEstimator {
    chain_id: u64,
    block_header: Header,
    sender: Address,
    block_key: BlockKey,
    state_override: StateOverrideSet,
    gas_specified: bool,
}

impl GasEstimator {
    fn new(
        chain_id: u64,
        block_header: Header,
        sender: Address,
        block_key: BlockKey,
        state_override: StateOverrideSet,
        gas_specified: bool,
    ) -> Self {
        Self {
            chain_id,
            block_header,
            sender,
            block_key,
            state_override,
            gas_specified,
        }
    }
}

impl EthCallProvider for GasEstimator {
    async fn eth_call(
        &self,
        txn: TxEnvelope,
        eth_call_executor: Option<Arc<EthCallExecutor>>,
    ) -> CallResult {
        let (block_number, block_id) = match self.block_key {
            BlockKey::Finalized(FinalizedBlockKey(SeqNum(n))) => (n, None),
            BlockKey::Proposed(ProposedBlockKey(SeqNum(n), BlockId(Hash(id)))) => (n, Some(id)),
        };

        let chain_id = self.chain_id;
        let header = self.block_header.clone();
        let sender = self.sender;
        let state_override = self.state_override.clone();
        let gas_specified = self.gas_specified;

        monad_ethcall::eth_call(
            chain_id,
            txn,
            header,
            sender,
            block_number,
            block_id,
            eth_call_executor.unwrap(),
            &state_override,
            MonadTracer::NoopTracer,
            gas_specified,
        )
        .await
    }
}

async fn estimate_gas<T: EthCallProvider>(
    provider: &T,
    eth_call_executor: Option<Arc<EthCallExecutor>>,
    call_request: &mut CallRequest,
    original_tx_gas: U256,
    provider_gas_limit: u64,
    protocol_gas_limit: u64,
) -> Result<Quantity, JsonRpcError> {
    let mut txn: TxEnvelope = call_request.clone().try_into()?;

    let (gas_used, gas_refund) = match provider
        .eth_call(txn.clone(), eth_call_executor.clone())
        .await
    {
        monad_ethcall::CallResult::Success(monad_ethcall::SuccessCallResult {
            gas_used,
            gas_refund,
            ..
        }) => (gas_used, gas_refund),
        monad_ethcall::CallResult::Failure(error) => match error.error_code {
            monad_ethcall::EthCallResult::OutOfGas => {
                if provider_gas_limit < protocol_gas_limit
                    && U256::from(provider_gas_limit) < original_tx_gas
                {
                    return Err(JsonRpcError::eth_call_error(
                        "provider-specified eth_estimateGas gas limit exceeded".to_string(),
                        error.data,
                    ));
                }
                return Err(JsonRpcError::eth_call_error(
                    "out of gas".to_string(),
                    error.data,
                ));
            }
            _ => return Err(JsonRpcError::eth_call_error(error.message, error.data)),
        },
        _ => {
            return Err(JsonRpcError::internal_error(
                "Unexpected CallResult type".into(),
            ))
        }
    };

    let upper_bound_gas_limit = txn.gas_limit();
    // Set gas to used + refund + call stipend and apply the 63/64 rule
    call_request.gas = Some(U256::from((gas_used + gas_refund + CALL_STIPEND) * 64 / 63));
    txn = call_request.clone().try_into()?;

    let (mut lower_bound_gas_limit, mut upper_bound_gas_limit) =
        if txn.gas_limit() < upper_bound_gas_limit {
            match provider
                .eth_call(txn.clone(), eth_call_executor.clone())
                .await
            {
                monad_ethcall::CallResult::Success(monad_ethcall::SuccessCallResult {
                    gas_used,
                    ..
                }) => (gas_used.sub(1), txn.gas_limit()),
                monad_ethcall::CallResult::Failure(_error_message) => {
                    (txn.gas_limit(), upper_bound_gas_limit)
                }
                _ => {
                    return Err(JsonRpcError::internal_error(
                        "Unexpected CallResult type".into(),
                    ))
                }
            }
        } else {
            (gas_used.sub(1), upper_bound_gas_limit)
        };

    // Binary search for the lowest gas limit.
    while (upper_bound_gas_limit - lower_bound_gas_limit) > 1 {
        // Error ratio from geth https://github.com/ethereum/go-ethereum/blob/c736b04d9b3bec8d9281146490b05075a91e7eea/internal/ethapi/api.go#L57
        if (upper_bound_gas_limit - lower_bound_gas_limit) as f64 / (upper_bound_gas_limit as f64)
            < 0.015
        {
            break;
        }

        let mid = (upper_bound_gas_limit + lower_bound_gas_limit) / 2;

        call_request.gas = Some(U256::from(mid));
        txn = call_request.clone().try_into()?;

        match provider.eth_call(txn, eth_call_executor.clone()).await {
            monad_ethcall::CallResult::Success(monad_ethcall::SuccessCallResult { .. }) => {
                upper_bound_gas_limit = mid;
            }
            monad_ethcall::CallResult::Failure(_error_message) => {
                lower_bound_gas_limit = mid;
            }
            _ => {
                return Err(JsonRpcError::internal_error(
                    "Unexpected CallResult type".into(),
                ))
            }
        };
    }

    Ok(Quantity(upper_bound_gas_limit))
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthEstimateGasParams {
    tx: CallRequest,
    #[serde(default)]
    block: BlockTags,
    #[schemars(skip)] // TODO: move StateOverrideSet from monad-cxx
    #[serde(default)]
    state_override_set: StateOverrideSet,
}

#[rpc(
    method = "eth_estimateGas",
    ignore = "chain_id,provider_gas_limit,eth_call_executor"
)]
#[allow(non_snake_case)]
/// Generates and returns an estimate of how much gas is necessary to allow the transaction to complete.
pub async fn monad_eth_estimateGas<T: Triedb>(
    triedb_env: &T,
    eth_call_executor: Arc<EthCallExecutor>,
    chain_id: u64,
    provider_gas_limit: u64,
    params: MonadEthEstimateGasParams,
) -> JsonRpcResult<Quantity> {
    trace!("monad_eth_estimateGas: {params:?}");

    let mut params = params;

    params.tx.input.input = match (params.tx.input.input.take(), params.tx.input.data.take()) {
        (Some(input), Some(data)) => {
            if input != data {
                return Err(JsonRpcError::invalid_params());
            }
            Some(input)
        }
        (None, data) | (data, None) => data,
    };

    if params.tx.gas > Some(U256::from(provider_gas_limit)) {
        return Err(JsonRpcError::eth_call_error(
            "user-specified gas exceeds provider limit".to_string(),
            None,
        ));
    }

    let block_key =
        get_block_key_from_tag(triedb_env, params.block).ok_or(JsonRpcError::block_not_found())?;

    let mut header = match triedb_env
        .get_block_header(block_key)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        Some(header) => header,
        None => return Err(JsonRpcError::block_not_found()),
    };

    let gas_specified = params.tx.gas.is_some();
    let provider_gas_limit = provider_gas_limit.min(header.header.gas_limit);
    let original_tx_gas = params.tx.gas.unwrap_or(U256::from(header.header.gas_limit));
    fill_gas_params(
        triedb_env,
        block_key,
        &mut params.tx,
        &mut header.header,
        &params.state_override_set,
        U256::from(provider_gas_limit),
    )
    .await?;

    if let Some(tx_chain_id) = params.tx.chain_id {
        if tx_chain_id != U64::from(chain_id) {
            return Err(JsonRpcError::invalid_chain_id(
                chain_id,
                tx_chain_id.to::<u64>(),
            ));
        }
    } else {
        params.tx.chain_id = Some(U64::from(chain_id));
    }

    let sender = params.tx.from.unwrap_or_default();
    let tx_chain_id = params
        .tx
        .chain_id
        .expect("chain id must be populated")
        .to::<u64>();

    let protocol_gas_limit = header.header.gas_limit;
    let eth_call_provider = GasEstimator::new(
        tx_chain_id,
        header.header,
        sender,
        block_key,
        params.state_override_set,
        gas_specified,
    );

    // If the transaction is a regular value transfer, execute the transaction with a 21000 gas limit and return that gas limit if executes successfully.
    // Returning 21000 without execution is risky since some transaction field combinations can increase the price even for regular transfers.
    let txn: TxEnvelope = params.tx.clone().try_into()?;
    if matches!(txn.kind(), TxKind::Call(_)) && txn.input().is_empty() && txn.to().is_some() {
        let mut request = params.tx.clone();
        request.gas = Some(U256::from(21_000));
        let txn: TxEnvelope = request.try_into()?;

        let to = txn.to().unwrap();
        if let Ok(acct) = triedb_env.get_account(block_key, to.into()).await {
            // If the account has no code, then execute the call with gas limit 21000
            if acct.code_hash == [0; 32]
                && matches!(
                    eth_call_provider
                        .eth_call(txn.clone(), Some(eth_call_executor.clone()))
                        .await,
                    monad_ethcall::CallResult::Success(_)
                )
            {
                return Ok(Quantity(21_000));
            }
        }
    };

    estimate_gas(
        &eth_call_provider,
        Some(eth_call_executor),
        &mut params.tx,
        original_tx_gas,
        provider_gas_limit,
        protocol_gas_limit,
    )
    .await
}

pub async fn suggested_priority_fee() -> Result<u64, JsonRpcError> {
    // TODO: hardcoded as 2 gwei for now, need to implement gas oracle
    // Refer to <https://github.com/ethereum/pm/issues/328#issuecomment-853234014>
    Ok(2000000000)
}

#[rpc(method = "eth_gasPrice")]
#[allow(non_snake_case)]
/// Returns the current price per gas in wei.
pub async fn monad_eth_gasPrice<T: Triedb>(chain_state: &ChainState<T>) -> JsonRpcResult<Quantity> {
    trace!("monad_eth_gasPrice");

    let header = chain_state
        .get_block_header(BlockTagOrHash::BlockTags(BlockTags::Latest))
        .await
        .map_err(|_| JsonRpcError::internal_error("could not get block data".into()))?;

    // Obtain base fee from latest block header
    let base_fee_per_gas = header.base_fee_per_gas.unwrap_or_default();

    // Obtain suggested priority fee
    let priority_fee = suggested_priority_fee().await.unwrap_or_default();

    Ok(Quantity(base_fee_per_gas + priority_fee))
}

#[rpc(method = "eth_maxPriorityFeePerGas")]
#[allow(non_snake_case)]
/// Returns the current maxPriorityFeePerGas per gas in wei.
pub async fn monad_eth_maxPriorityFeePerGas() -> JsonRpcResult<Quantity> {
    trace!("monad_eth_maxPriorityFeePerGas");

    let priority_fee = suggested_priority_fee().await.unwrap_or_default();
    Ok(Quantity(priority_fee))
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthHistoryParams {
    block_count: Quantity,
    newest_block: BlockTags,
    #[serde(default)]
    reward_percentiles: Option<Vec<f64>>,
}

#[rpc(method = "eth_feeHistory")]
#[allow(non_snake_case)]
/// Transaction fee history
/// Returns transaction base fee per gas and effective priority fee per gas for the requested/supported block range.
pub async fn monad_eth_feeHistory<T: Triedb>(
    chain_state: &ChainState<T>,
    params: MonadEthHistoryParams,
) -> JsonRpcResult<MonadFeeHistory> {
    trace!("monad_eth_feeHistory");

    let block_count = params.block_count.0;
    match block_count {
        0 => return Ok(MonadFeeHistory(FeeHistory::default())),
        1..=1024 => (),
        _ => {
            return Err(JsonRpcError::custom(
                "block count must be between 1 and 1024".to_string(),
            ))
        }
    }

    let header = chain_state
        .get_block_header(BlockTagOrHash::BlockTags(params.newest_block))
        .await
        .map_err(|_| JsonRpcError::internal_error("could not get block data".into()))?;

    let percentiles = match params.reward_percentiles {
        Some(percentiles) => {
            if percentiles.len() > 100 {
                return Err(JsonRpcError::internal_error(
                    "number of reward percentiles must be less than or equal to 100".into(),
                ));
            }

            // Check percentiles are between 0-100
            if percentiles.iter().any(|p| *p < 0.0 || *p > 100.0) {
                return Err(JsonRpcError::internal_error(
                    "reward percentiles must be between 0-100".into(),
                ));
            }

            // Check percentiles are sorted
            if !percentiles.windows(2).all(|w| w[0] <= w[1]) {
                return Err(JsonRpcError::internal_error(
                    "reward percentiles must be sorted".into(),
                ));
            }

            if percentiles.is_empty() {
                None
            } else {
                Some(percentiles)
            }
        }
        None => None,
    };

    let oldest_block = header.number.saturating_sub(block_count - 1);
    let mut base_fee_per_gas_history: Vec<u128> = Vec::with_capacity(block_count as usize + 1);
    let mut gas_used_ratio_history = Vec::with_capacity(block_count as usize);
    let mut rewards = Vec::with_capacity(block_count as usize + 1);

    let block_range = oldest_block..=header.number;

    let block_data_futures = block_range.map(|blk_num| async move {
        let block = chain_state
            .get_block(
                BlockTagOrHash::BlockTags(BlockTags::Number(Quantity(blk_num))),
                true,
            )
            .await
            .map_err(|_| JsonRpcError::internal_error("could not get block data".into()))?;

        let receipts = chain_state
            .get_block_receipts(BlockTagOrHash::BlockTags(BlockTags::Number(Quantity(
                blk_num,
            ))))
            .await
            .map_err(|_| JsonRpcError::internal_error("could not get block receipts".into()))?;

        Ok::<_, JsonRpcError>((blk_num, block, receipts))
    });

    let block_data: Vec<_> = futures::stream::iter(block_data_futures)
        .buffered(20)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, JsonRpcError>>()?;

    for (_blk_num, block, receipts) in block_data.into_iter() {
        let header = block.header;
        let base_fee = header.base_fee_per_gas.unwrap_or_default();
        base_fee_per_gas_history.push(header.base_fee_per_gas.unwrap_or_default().into());

        gas_used_ratio_history.push((header.gas_used as f64).div(header.gas_limit as f64));

        let txns: Vec<alloy_rpc_types::Transaction> =
            block.transactions.into_transactions().collect::<Vec<_>>();

        let receipts = receipts.into_iter().map(|r| r.0).collect::<Vec<_>>();

        // Rewards are the requested percentiles of the effective priority fees per gas. Sorted in ascending order and weighted by gas used.
        let percentile_rewards = calculate_fee_history_rewards(
            txns,
            receipts,
            base_fee,
            header.gas_used,
            percentiles.as_ref(),
        );

        rewards.push(percentile_rewards);
    }

    let last_base_fee = base_fee_per_gas_history
        .last()
        .map(|&fee| fee as u64)
        .unwrap_or_default();

    // Get the newest block after the last block in the range
    let next_block_base_fee =
        get_next_block_base_fee(chain_state, params.newest_block, last_base_fee).await?;
    base_fee_per_gas_history.push(next_block_base_fee.into());

    let rewards = if percentiles.is_some() {
        Some(rewards)
    } else {
        Some(vec![])
    };

    Ok(MonadFeeHistory(FeeHistory {
        base_fee_per_gas: base_fee_per_gas_history,
        gas_used_ratio: gas_used_ratio_history,
        base_fee_per_blob_gas: vec![0; (block_count + 1) as usize],
        blob_gas_used_ratio: vec![0.0; (block_count) as usize],
        oldest_block,
        reward: rewards,
    }))
}

fn calculate_fee_history_rewards(
    transactions: Vec<alloy_rpc_types::Transaction>,
    receipts: Vec<TransactionReceipt>,
    base_fee: u64,
    block_gas_used: u64,
    percentiles: Option<&Vec<f64>>,
) -> Vec<u128> {
    if percentiles.is_none() {
        return vec![];
    }

    if transactions.is_empty() {
        return vec![0; percentiles.unwrap().len()];
    }

    // Get the reward and gas used for each transaction using receipt.
    let gas_and_rewards = transactions
        .iter()
        .zip(receipts)
        .map(|(tx, receipt)| {
            let gas_used = receipt.gas_used;
            let reward = tx.effective_tip_per_gas(base_fee).unwrap_or_default();
            (gas_used, reward)
        })
        .sorted_by_key(|(gas_used, _)| *gas_used)
        .collect::<Vec<_>>();

    let mut idx = 0;
    let mut cumulative_gas_used: u128 = 0;
    let mut rewards = Vec::new();

    for pct in percentiles.unwrap() {
        let gas_threshold = (block_gas_used as f64 * pct / 100.0).round() as u128;
        while cumulative_gas_used < gas_threshold && idx < transactions.len() {
            cumulative_gas_used += gas_and_rewards[idx].0;
            idx += 1;
        }
        // Clamp idx to valid range
        let reward_idx = idx.min(transactions.len() - 1);
        rewards.push(gas_and_rewards[reward_idx].1);
    }

    rewards
}

pub async fn get_next_block_base_fee<T>(
    chain_state: &ChainState<T>,
    latest: BlockTags,
    previous_base_fee: u64,
) -> JsonRpcResult<u64>
where
    T: Triedb,
{
    let latest_plus_one = match latest {
        BlockTags::Latest | BlockTags::Safe => {
            // Latest/Safe block is the voted block
            // TODO: rpc does not have access to consensus headers to calculate the next block base fee.
            // Return base fee of the previous block.
            return Ok(previous_base_fee);
        }
        BlockTags::Number(num) => {
            let latest_block_num = chain_state.get_latest_block_number();
            if num.0 + 1 > latest_block_num {
                return Ok(previous_base_fee);
            }
            BlockTags::Number(Quantity(num.0 + 1))
        }
        BlockTags::Finalized => BlockTags::Latest,
    };

    let header = chain_state
        .get_block_header(BlockTagOrHash::BlockTags(latest_plus_one))
        .await
        .map_err(|_| JsonRpcError::internal_error("could not get block data".into()))?;

    Ok(header.base_fee_per_gas.unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use alloy_consensus::{
        Block, Eip658Value, Receipt, ReceiptEnvelope, ReceiptWithBloom, SignableTransaction,
        TxEip1559,
    };
    use alloy_primitives::{Bloom, Bytes, FixedBytes, Log, LogData};
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;
    use monad_ethcall::{FailureCallResult, SuccessCallResult};
    use monad_triedb_utils::{mock_triedb::MockTriedb, triedb_env::ReceiptWithLogIndex};

    use super::*;
    use crate::handlers::eth::call::CallRequest;

    struct MockGasEstimator {
        gas_used: u64,
        gas_refund: u64,
    }

    impl EthCallProvider for MockGasEstimator {
        async fn eth_call(&self, txn: TxEnvelope, _: Option<Arc<EthCallExecutor>>) -> CallResult {
            if txn.gas_limit() >= self.gas_used + self.gas_refund {
                CallResult::Success(SuccessCallResult {
                    gas_used: self.gas_used,
                    gas_refund: self.gas_refund,
                    ..Default::default()
                })
            } else {
                CallResult::Failure(FailureCallResult {
                    ..Default::default()
                })
            }
        }
    }

    #[tokio::test]
    async fn test_gas_limit_too_low() {
        // user specified gas limit lower than actual gas used
        let mut call_request = CallRequest {
            gas: Some(U256::from(30_000)),
            ..Default::default()
        };
        let provider = MockGasEstimator {
            gas_used: 50_000,
            gas_refund: 10_000,
        };

        // should return gas estimation failure
        let result = estimate_gas(
            &provider,
            None,
            &mut call_request,
            U256::from(30_000),
            u64::MAX,
            u64::MAX,
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_gas_limit_unspecified() {
        // user did not specify gas limit
        let mut call_request = CallRequest::default();
        let provider = MockGasEstimator {
            gas_used: 50_000,
            gas_refund: 10_000,
        };

        // should return correct gas estimation
        let result = estimate_gas(
            &provider,
            None,
            &mut call_request,
            U256::MAX,
            u64::MAX,
            u64::MAX,
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Quantity(60795));
    }

    #[tokio::test]
    async fn test_gas_limit_sufficient() {
        // user specify gas limit that is sufficient
        let mut call_request = CallRequest {
            gas: Some(U256::from(70_000)),
            ..Default::default()
        };
        let provider = MockGasEstimator {
            gas_used: 50_000,
            gas_refund: 10_000,
        };

        // should return correct gas estimation
        let result = estimate_gas(
            &provider,
            None,
            &mut call_request,
            U256::from(70_000),
            u64::MAX,
            u64::MAX,
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Quantity(60795));
    }

    #[tokio::test]
    async fn test_gas_limit_just_sufficient() {
        // user specify gas limit that is just sufficient
        let mut call_request = CallRequest {
            gas: Some(U256::from(60_000)),
            ..Default::default()
        };
        let provider = MockGasEstimator {
            gas_used: 50_000,
            gas_refund: 10_000,
        };

        // should return correct gas estimation
        let result = estimate_gas(
            &provider,
            None,
            &mut call_request,
            U256::from(60_000),
            u64::MAX,
            u64::MAX,
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Quantity(60_000));
    }

    fn make_block(num: u64, base_fee: u64, txns: Vec<TxEnvelope>) -> Block<TxEnvelope> {
        let mut blk = Block::<TxEnvelope>::default();
        blk.header.gas_limit = 30_000_000;
        blk.header.gas_used = txns.iter().map(|t| t.gas_limit()).sum();
        blk.header.base_fee_per_gas = Some(base_fee);
        blk.header.number = num;
        blk.body.transactions = txns;
        blk
    }

    fn make_tx(
        sender: FixedBytes<32>,
        max_fee_per_gas: u128,
        max_priority_fee_per_gas: u128,
        gas_limit: u64,
        nonce: u64,
        chain_id: u64,
    ) -> TxEnvelope {
        let transaction = TxEip1559 {
            chain_id,
            nonce,
            gas_limit,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            to: TxKind::Call(Address::repeat_byte(0u8)),
            value: Default::default(),
            access_list: Default::default(),
            input: vec![].into(),
        };

        let signer = PrivateKeySigner::from_bytes(&sender).unwrap();
        let signature = signer
            .sign_hash_sync(&transaction.signature_hash())
            .unwrap();
        transaction.into_signed(signature).into()
    }

    #[tokio::test]
    async fn test_eth_fee_history() {
        let mut mock_triedb = MockTriedb::default();
        mock_triedb.set_latest_block(1000);
        let sender = FixedBytes::<32>::from([1u8; 32]);

        // Fetch fee history for an empty block.
        mock_triedb.set_finalized_block(SeqNum(1000), make_block(1000, 1_000, vec![]));

        let chain_state = ChainState::new(None, mock_triedb, None);
        let res = monad_eth_feeHistory(
            &chain_state,
            MonadEthHistoryParams {
                block_count: Quantity(1),
                newest_block: BlockTags::Latest,
                reward_percentiles: Some(vec![0.0, 25.0, 50.0, 75.0, 100.0]),
            },
        )
        .await
        .expect("should get fee history");
        assert_eq!(res.0.oldest_block, 1000);
        assert_eq!(res.0.base_fee_per_blob_gas, vec![0, 0]);
        assert_eq!(res.0.blob_gas_used_ratio, vec![0.0]);
        assert_eq!(res.0.gas_used_ratio, vec![0.0]);
        assert_eq!(res.0.base_fee_per_gas, vec![1_000, 1_000]);
        assert_eq!(res.0.reward, Some(vec![vec![0, 0, 0, 0, 0]]));

        // Fetch fee history for blocks that have 4 transactions.
        let mut txs = Vec::new();
        let mut receipts = Vec::new();
        for i in 1..=4 {
            let tx = make_tx(sender, 1000 * i, 1000 * i, 21_000, 1, 1);
            txs.push(tx);
            let receipt = ReceiptWithBloom::new(
                Receipt::<Log> {
                    logs: vec![Log {
                        address: Default::default(),
                        data: LogData::new(vec![], Bytes::default()).unwrap(),
                    }],
                    status: Eip658Value::Eip658(true),
                    cumulative_gas_used: 21000 * i,
                },
                Bloom::repeat_byte(b'a'),
            );
            receipts.push(ReceiptWithLogIndex {
                receipt: ReceiptEnvelope::Eip1559(receipt),
                starting_log_index: 0,
            });
        }
        let mut mock_triedb = MockTriedb::default();
        mock_triedb.set_latest_block(1000);
        mock_triedb.set_finalized_block(SeqNum(1000), make_block(1000, 2_000, txs.clone()));
        mock_triedb.set_finalized_block(SeqNum(999), make_block(999, 1_000, txs));
        mock_triedb.set_finalized_block(SeqNum(998), make_block(999, 5_000, vec![]));
        mock_triedb.set_receipts(SeqNum(1000), receipts.clone());
        mock_triedb.set_receipts(SeqNum(999), receipts);

        let chain_state = ChainState::new(None, mock_triedb, None);
        let res = monad_eth_feeHistory(
            &chain_state,
            MonadEthHistoryParams {
                block_count: Quantity(1),
                newest_block: BlockTags::Latest,
                reward_percentiles: Some(vec![50.0, 75.0]),
            },
        )
        .await
        .expect("should get fee history");

        let gas_used = 21_000.0 * 4.0 / 30_000_000.0;

        assert_eq!(res.0.oldest_block, 1000);
        assert_eq!(res.0.base_fee_per_gas, vec![2_000, 2_000]);
        assert_eq!(res.0.gas_used_ratio, vec![gas_used]);
        assert_eq!(res.0.reward, Some(vec![vec![1000, 2000]]));

        // Fetch block history with explicit block heights
        let res = monad_eth_feeHistory(
            &chain_state,
            MonadEthHistoryParams {
                block_count: Quantity(1),
                newest_block: BlockTags::Number(Quantity(999)),
                reward_percentiles: None,
            },
        )
        .await
        .expect("should get fee history");
        assert_eq!(res.0.oldest_block, 999);
        assert_eq!(res.0.base_fee_per_gas, vec![1_000, 2_000]);
        assert_eq!(res.0.gas_used_ratio, vec![gas_used]);
        assert_eq!(res.0.reward, Some(vec![]));

        let res = monad_eth_feeHistory(
            &chain_state,
            MonadEthHistoryParams {
                block_count: Quantity(2),
                newest_block: BlockTags::Number(Quantity(999)),
                reward_percentiles: None,
            },
        )
        .await
        .expect("should get fee history");
        assert_eq!(res.0.oldest_block, 998);
        assert_eq!(res.0.base_fee_per_gas, vec![5_000, 1_000, 2_000]);
        assert_eq!(res.0.gas_used_ratio, vec![0.0, gas_used]);
        assert_eq!(res.0.reward, Some(vec![]));
    }
}
