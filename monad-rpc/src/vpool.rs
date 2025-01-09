use std::{
    collections::VecDeque,
    sync::{Arc, Weak},
};

use monad_rpc_docs::rpc;
use monad_triedb_utils::triedb_env::{Triedb, TriedbEnv};
use rayon::slice::ParallelSlice;
use reth_primitives::{
    revm_primitives::bitvec::store::BitStore, Address, TransactionSigned,
    TransactionSignedEcRecovered,
};
use scc::{ebr::Guard, HashIndex, HashMap, TreeIndex};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};
use tracing::{error, warn};

use crate::{
    eth_json_types::EthAddress,
    jsonrpc::{JsonRpcError, JsonRpcResult},
};

#[rpc(method = "txpool_content")]
#[allow(non_snake_case)]
pub async fn monad_txpool_content() -> JsonRpcResult<String> {
    Err(JsonRpcError::method_not_supported())
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct TxPoolContentFromParams {
    pub address: EthAddress,
}

#[rpc(method = "txpool_contentFrom", ignore = "v_pool")]
#[allow(non_snake_case)]
pub async fn monad_txpool_contentFrom(
    v_pool: Arc<Mutex<monad_eth_vpool::VirtualPool>>,
    params: TxPoolContentFromParams,
) -> JsonRpcResult<String> {
    Err(JsonRpcError::method_not_supported())
}

#[rpc(method = "txpool_inspect")]
#[allow(non_snake_case)]
pub async fn monad_txpool_inspect() -> JsonRpcResult<String> {
    Err(JsonRpcError::method_not_supported())
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct TxPoolStatus {
    pub pending: usize,
    pub queued: usize,
}

#[rpc(method = "txpool_status", ignore = "v_pool")]
#[allow(non_snake_case)]
pub async fn monad_txpool_status(v_pool: Arc<Mutex<monad_eth_vpool::VirtualPool>>) -> JsonRpcResult<TxPoolStatus> {
    Ok(TxPoolStatus {
        pending: v_pool.lock().await.pending_len(),
        queued: v_pool.lock().await.queued_len(),
    })
}