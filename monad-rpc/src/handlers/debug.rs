use alloy_consensus::{Block, BlockBody, Header, ReceiptEnvelope, TxEnvelope};
use alloy_eips::eip2718::Encodable2718;
use alloy_rlp::Encodable;
use monad_archive::prelude::{ArchiveReader, BlockDataReader, IndexReader};
use monad_rpc_docs::rpc;
use monad_triedb_utils::triedb_env::{BlockKey, FinalizedBlockKey, TransactionLocation, Triedb};
use monad_types::SeqNum;
use serde::{Deserialize, Serialize};
use tracing::{error, trace};

use crate::{
    eth_json_types::{BlockTags, EthHash},
    handlers::eth::block::get_block_key_from_tag,
    hex,
    jsonrpc::{JsonRpcError, JsonRpcResult},
};

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct DebugBlockParams {
    block: BlockTags,
}

#[rpc(method = "debug_getRawBlock")]
#[allow(non_snake_case)]
/// Returns an RLP-encoded block.
pub async fn monad_debug_getRawBlock<T: Triedb>(
    triedb_env: &T,
    archive_reader: &Option<ArchiveReader>,
    params: DebugBlockParams,
) -> JsonRpcResult<String> {
    trace!("monad_debug_getRawBlock: {params:?}");
    let block_key = get_block_key_from_tag(triedb_env, params.block);

    let encode_block = |block: Block<TxEnvelope>| {
        let mut res = Vec::new();
        block.encode(&mut res);
        Ok(hex::encode(&res))
    };

    if let Some(header) = triedb_env
        .get_block_header(block_key)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        if let Ok(transactions) = triedb_env.get_transactions(block_key).await {
            let transactions = transactions
                .into_iter()
                .map(|recovered_tx| recovered_tx.tx)
                .collect();
            return encode_block(Block {
                header: header.header,
                body: BlockBody {
                    transactions,
                    ommers: vec![],
                    withdrawals: None,
                },
            });
        }
    };

    if let (Some(archive_reader), BlockKey::Finalized(FinalizedBlockKey(block_num))) =
        (archive_reader, block_key)
    {
        if let Ok(block) = archive_reader
            .get_block_by_number(block_num.0)
            .await
            .inspect_err(|e| {
                error!("Error getting block by number from archive: {e:?}");
            })
        {
            let transactions = block
                .body
                .transactions
                .into_iter()
                .map(|recovered_tx| recovered_tx.tx)
                .collect();
            return encode_block(Block {
                header: block.header,
                body: BlockBody {
                    transactions,
                    ommers: vec![],
                    withdrawals: None,
                },
            });
        }
    }

    Err(JsonRpcError::internal_error("block data not found".into()))
}

#[rpc(method = "debug_getRawHeader")]
#[allow(non_snake_case)]
/// Returns an RLP-encoded header.
pub async fn monad_debug_getRawHeader<T: Triedb>(
    triedb_env: &T,
    archive_reader: &Option<ArchiveReader>,
    params: DebugBlockParams,
) -> JsonRpcResult<String> {
    trace!("monad_debug_getRawHeader: {params:?}");
    let block_key = get_block_key_from_tag(triedb_env, params.block);

    let encode_header = |header: Header| {
        let mut res = Vec::new();
        header.encode(&mut res);
        Ok(hex::encode(&res))
    };

    if let Some(header) = triedb_env
        .get_block_header(block_key)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        return encode_header(header.header);
    };

    if let (Some(archive_reader), BlockKey::Finalized(FinalizedBlockKey(block_num))) =
        (archive_reader, block_key)
    {
        if let Ok(block) = archive_reader
            .get_block_by_number(block_num.0)
            .await
            .inspect_err(|e| {
                error!("Error getting block by number from archive: {e:?}");
            })
        {
            return encode_header(block.header);
        }
    }

    Err(JsonRpcError::internal_error("block data not found".into()))
}

#[derive(Serialize, Debug, schemars::JsonSchema)]
#[serde(transparent)]
pub struct MonadDebugGetRawReceiptsResult {
    receipts: Vec<String>,
}

#[rpc(method = "debug_getRawReceipts")]
#[allow(non_snake_case)]
/// Returns an array of EIP-2718 binary-encoded receipts.
pub async fn monad_debug_getRawReceipts<T: Triedb>(
    triedb_env: &T,
    archive_reader: &Option<ArchiveReader>,
    params: DebugBlockParams,
) -> JsonRpcResult<MonadDebugGetRawReceiptsResult> {
    trace!("monad_debug_getRawReceipts: {params:?}");
    let block_key = get_block_key_from_tag(triedb_env, params.block);

    let encode_receipts = |receipts: Vec<ReceiptEnvelope>| {
        let receipts = receipts
            .into_iter()
            .map(|r| {
                let mut res = Vec::new();
                r.encode(&mut res);
                hex::encode(&res)
            })
            .collect();
        Ok(MonadDebugGetRawReceiptsResult { receipts })
    };

    if let Ok(receipts) = triedb_env.get_receipts(block_key).await {
        let receipts: Vec<ReceiptEnvelope> = receipts
            .into_iter()
            .map(|receipt_with_log_index| receipt_with_log_index.receipt)
            .collect();
        return encode_receipts(receipts);
    };

    if let (Some(archive_reader), BlockKey::Finalized(FinalizedBlockKey(block_num))) =
        (archive_reader, block_key)
    {
        if let Ok(receipts) = archive_reader
            .get_block_receipts(block_num.0)
            .await
            .inspect_err(|e| {
                error!("Error getting block by number from archive: {e:?}");
            })
        {
            let receipts: Vec<ReceiptEnvelope> = receipts
                .into_iter()
                .map(|receipt_with_log_index| receipt_with_log_index.receipt)
                .collect();
            return encode_receipts(receipts);
        }
    }

    Err(JsonRpcError::internal_error("block data not found".into()))
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadDebugGetRawTransactionParams {
    tx_hash: EthHash,
}

#[rpc(method = "debug_getRawTransaction")]
#[allow(non_snake_case)]
/// Returns an array of EIP-2718 binary-encoded transactions.
pub async fn monad_debug_getRawTransaction<T: Triedb>(
    triedb_env: &T,
    archive_reader: &Option<ArchiveReader>,
    params: MonadDebugGetRawTransactionParams,
) -> JsonRpcResult<String> {
    trace!("monad_debug_getRawTransaction: {params:?}");
    let latest_block_key = get_block_key_from_tag(triedb_env, BlockTags::Latest);

    if let Some(TransactionLocation {
        tx_index,
        block_num,
    }) = triedb_env
        .get_transaction_location_by_hash(latest_block_key, params.tx_hash.0)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        let block_key = triedb_env.get_block_key(SeqNum(block_num));
        if let Some(tx) = triedb_env
            .get_transaction(block_key, tx_index)
            .await
            .map_err(JsonRpcError::internal_error)?
        {
            let mut res = Vec::new();
            tx.tx.encode_2718(&mut res);
            return Ok(hex::encode(&res));
        };
    }

    if let Some(archive_reader) = archive_reader {
        if let Ok((tx, _)) = archive_reader
            .get_tx(&params.tx_hash.0.into())
            .await
            .inspect_err(|e| {
                error!("Error getting tx from archive: {e:?}");
            })
        {
            let mut res = Vec::new();
            tx.tx.encode_2718(&mut res);
            return Ok(hex::encode(&res));
        }
    }

    Err(JsonRpcError::internal_error("transaction not found".into()))
}
