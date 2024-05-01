use std::cmp::min;

use alloy_primitives::aliases::{B256, U128, U256, U64};
use alloy_rlp::Decodable;
use log::{debug, trace};
use monad_blockdb::{BlockTableKey, BlockValue, EthTxKey};
use reth_primitives::{BlockHash, TransactionSigned};
use reth_rpc_types::{AccessListItem, Parity, Signature, Transaction};
use serde::Deserialize;
use serde_json::Value;

use crate::{
    blockdb::BlockDbEnv,
    eth_json_types::{
        deserialize_block_tags, deserialize_fixed_data, deserialize_quantity,
        deserialize_unformatted_data, serialize_result, BlockTags, EthHash, Quantity,
        UnformattedData,
    },
    jsonrpc::JsonRpcError,
    triedb::{decode_tx_type, ReceiptDetails, TransactionReceipt, TriedbEnv, TriedbResult},
};

pub fn parse_tx_content(
    value: &BlockValue,
    tx: &TransactionSigned,
    tx_index: u64,
) -> Option<Transaction> {
    // recover transaction signer
    let transaction = tx
        .clone()
        .into_ecrecovered_unchecked()
        .expect("transaction sender should exist");

    // parse fee parameters
    let base_fee = value.block.base_fee_per_gas;
    let gas_price = base_fee
        .and_then(|base_fee| {
            tx.effective_tip_per_gas(Some(base_fee))
                .map(|tip| tip + base_fee as u128)
        })
        .unwrap_or_else(|| tx.max_fee_per_gas());

    // parse signature
    let signature = Signature {
        r: tx.signature().r,
        s: tx.signature().s,
        v: U256::from(tx.signature().odd_y_parity as u8),
        y_parity: Some(Parity(tx.signature().odd_y_parity)),
    };

    // parse access list
    let access_list = Some(
        tx.access_list()
            .unwrap()
            .0
            .iter()
            .map(|item| AccessListItem {
                address: item.address.0.into(),
                storage_keys: item.storage_keys.iter().map(|key| key.0.into()).collect(),
            })
            .collect(),
    );

    let retval = Transaction {
        hash: tx.hash(),
        nonce: U64::from(tx.nonce()),
        from: transaction.signer(),
        to: tx.to(),
        value: tx.value().into(),
        gas_price: Some(U128::from(gas_price)),
        max_fee_per_gas: Some(U128::from(tx.max_fee_per_gas())),
        max_priority_fee_per_gas: tx.max_priority_fee_per_gas().map(U128::from),
        signature: Some(signature),
        gas: U256::from(tx.gas_limit()),
        input: tx.input().clone(),
        chain_id: tx.chain_id().map(U64::from),
        access_list,
        transaction_type: Some(U64::from(tx.tx_type() as u8)),
        block_hash: Some(value.block.hash_slow()),
        block_number: Some(U256::from(value.block.number)),
        transaction_index: Some(U256::from(tx_index)),

        // only relevant for EIP-4844 transactions
        max_fee_per_blob_gas: None,
        blob_versioned_hashes: vec![],
        other: Default::default(),
    };

    Some(retval)
}

pub fn parse_tx_receipt(
    block: &BlockValue,
    rlp_receipt: &mut &[u8],
    block_num: u64,
    tx_index: u64,
) -> Option<TransactionReceipt> {
    // decode transaction type from buffer
    let tx_type = match decode_tx_type(rlp_receipt) {
        Ok(tx_type) => tx_type,
        Err(e) => {
            debug!("tx type decode error: {e}");
            return None::<TransactionReceipt>;
        }
    };

    match ReceiptDetails::decode(rlp_receipt) {
        Ok(r) => {
            let tx = block.block.body.get(tx_index as usize)?;
            let transaction = tx
                .clone()
                .into_ecrecovered_unchecked()
                .expect("transaction sender should exist");
            let base_fee_per_gas = block.block.base_fee_per_gas.unwrap_or_default() as u128;
            // effective gas price is calculated according to eth json rpc specification
            let effective_gas_price = base_fee_per_gas
                + min(
                    tx.max_fee_per_gas() - base_fee_per_gas,
                    tx.max_priority_fee_per_gas().unwrap(),
                );
            let tx_receipt = TransactionReceipt {
                transaction_type: tx_type,
                transaction_hash: Some(tx.hash()),
                transaction_index: U64::from(tx_index),
                block_hash: Some(block.block.hash_slow()),
                block_number: Some(U256::from(block_num)),
                from: transaction.signer(),
                to: tx.to(),
                // TODO: read from triedb to determine whether a contract is deployed
                contract_address: None,
                // TODO: gas_used = cumulative_gas_used[txn_index] - cumulative_gas_used[txn_index-1]
                gas_used: U128::from(21000),
                effective_gas_price: U128::from(effective_gas_price),
                details: r,
                // TODO: EIP4844 fields
                blob_gas_used: None,
                blob_gas_price: None,
            };
            Some(tx_receipt)
        }
        Err(e) => {
            debug!("receipt decode error: {e}");
            None::<TransactionReceipt>
        }
    }
}

#[derive(Deserialize, Debug)]
struct MonadEthSendRawTransactionParams {
    #[serde(deserialize_with = "deserialize_unformatted_data")]
    hex_tx: UnformattedData,
}

// TODO: need to support EIP-4844 transactions
#[allow(non_snake_case)]
pub async fn monad_eth_sendRawTransaction(
    ipc: flume::Sender<TransactionSigned>,
    params: Value,
) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_sendRawTransaction: {params:?}");

    let p: MonadEthSendRawTransactionParams = match serde_json::from_value(params) {
        Ok(s) => s,
        Err(e) => {
            debug!("invalid params {e}");
            return Err(JsonRpcError::invalid_params());
        }
    };

    match TransactionSigned::decode_enveloped(&mut &p.hex_tx.0[..]) {
        Ok(txn) => {
            let hash = txn.hash();

            match flume::Sender::send_async(&ipc, txn).await {
                Ok(_) => Ok(Value::String(hash.to_string())),
                Err(e) => {
                    debug!("mempool ipc send error {:?}", e);
                    Err(JsonRpcError::internal_error())
                }
            }
        }
        Err(e) => {
            debug!("eth txn decode failed {:?}", e);
            Err(JsonRpcError::txn_decode_error())
        }
    }
}

#[derive(Deserialize, Debug)]
struct MonadEthGetTransactionReceiptParams {
    #[serde(deserialize_with = "deserialize_fixed_data")]
    tx_hash: EthHash,
}

#[allow(non_snake_case)]
pub async fn monad_eth_getTransactionReceipt(
    blockdb_env: &BlockDbEnv,
    triedb_env: &TriedbEnv,
    params: Value,
) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_getTransactionReceipt: {params:?}");

    let p: MonadEthGetTransactionReceiptParams = match serde_json::from_value(params) {
        Ok(s) => s,
        Err(e) => {
            debug!("invalid params {e}");
            return Err(JsonRpcError::invalid_params());
        }
    };

    let key = EthTxKey(B256::new(p.tx_hash.0));
    let Some(txn_value) = blockdb_env.get_txn(key).await else {
        return serialize_result(None::<TransactionReceipt>);
    };
    let txn_index = txn_value.transaction_index;

    let Some(block) = blockdb_env.get_block_by_hash(txn_value.block_hash).await else {
        return serialize_result(None::<TransactionReceipt>);
    };
    let block_num = block.block.number;

    match triedb_env.get_receipt(txn_index, block_num).await {
        TriedbResult::Null => serialize_result(None::<TransactionReceipt>),
        TriedbResult::Receipt(rlp_receipt) => {
            let mut rlp_buf = rlp_receipt.as_slice();

            let Some(receipt) = parse_tx_receipt(&block, &mut rlp_buf, block_num, txn_index) else {
                return Err(JsonRpcError::internal_error());
            };
            serialize_result(Some(receipt))
        }
        _ => Err(JsonRpcError::internal_error()),
    }
}

#[derive(Deserialize, Debug)]
struct MonadEthGetTransactionByHashParams {
    #[serde(deserialize_with = "deserialize_fixed_data")]
    tx_hash: EthHash,
}

#[allow(non_snake_case)]
pub async fn monad_eth_getTransactionByHash(
    blockdb_env: &BlockDbEnv,
    params: Value,
) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_getTransactionByHash: {params:?}");

    let p: MonadEthGetTransactionByHashParams = match serde_json::from_value(params) {
        Ok(s) => s,
        Err(e) => {
            debug!("invalid params {e}");
            return Err(JsonRpcError::invalid_params());
        }
    };

    let key = EthTxKey(B256::new(p.tx_hash.0));
    let Some(result) = blockdb_env.get_txn(key).await else {
        return serialize_result(None::<Transaction>);
    };

    let block_key = result.block_hash;
    let block = blockdb_env
        .get_block_by_hash(block_key)
        .await
        .expect("txn was found so its block should exist");

    let transaction = block
        .block
        .body
        .get(result.transaction_index as usize)
        .expect("txn and block found so its index should be correct");

    let retval = parse_tx_content(&block, transaction, result.transaction_index);

    serialize_result(retval)
}

#[derive(Deserialize, Debug)]
struct MonadEthGetTransactionByBlockHashAndIndexParams {
    #[serde(deserialize_with = "deserialize_fixed_data")]
    block_hash: EthHash,
    #[serde(deserialize_with = "deserialize_quantity")]
    index: Quantity,
}

#[allow(non_snake_case)]
pub async fn monad_eth_getTransactionByBlockHashAndIndex(
    blockdb_env: &BlockDbEnv,
    params: Value,
) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_getTransactionByBlockHashAndIndex: {params:?}");

    let p: MonadEthGetTransactionByBlockHashAndIndexParams = match serde_json::from_value(params) {
        Ok(s) => s,
        Err(e) => {
            debug!("invalid params {e}");
            return Err(JsonRpcError::invalid_params());
        }
    };

    let key = BlockTableKey(BlockHash::new(p.block_hash.0));
    let Some(block) = blockdb_env.get_block_by_hash(key).await else {
        return serialize_result(None::<Transaction>);
    };

    let Some(transaction) = block.block.body.get(p.index.0 as usize) else {
        return serialize_result(None::<Transaction>);
    };

    let retval = parse_tx_content(&block, transaction, p.index.0);

    serialize_result(retval)
}

#[derive(Deserialize, Debug)]
struct MonadEthGetTransactionByBlockNumberAndIndexParams {
    #[serde(deserialize_with = "deserialize_block_tags")]
    block_tag: BlockTags,
    #[serde(deserialize_with = "deserialize_quantity")]
    index: Quantity,
}

#[allow(non_snake_case)]
pub async fn monad_eth_getTransactionByBlockNumberAndIndex(
    blockdb_env: &BlockDbEnv,
    params: Value,
) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_getTransactionByBlockNumberAndIndex: {params:?}");

    let p: MonadEthGetTransactionByBlockNumberAndIndexParams = match serde_json::from_value(params)
    {
        Ok(s) => s,
        Err(e) => {
            debug!("invalid params {e}");
            return Err(JsonRpcError::invalid_params());
        }
    };

    let Some(block) = blockdb_env.get_block_by_tag(p.block_tag).await else {
        return serialize_result(None::<Transaction>);
    };

    let Some(transaction) = block.block.body.get(p.index.0 as usize) else {
        return serialize_result(None::<Transaction>);
    };

    let retval = parse_tx_content(&block, transaction, p.index.0);

    serialize_result(retval)
}
