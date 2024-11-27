use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{FutureExt, Stream};
use monad_triedb_utils::triedb_env::{BlockHeader, Triedb};
use pin_project::pin_project;
use reth_primitives::{Block, TransactionSigned};
use reth_rpc_types::TransactionReceipt;
use tracing::error;

use crate::{block_handlers::block_receipts, jsonrpc::JsonRpcError};

#[pin_project::pin_project(project = StateProj)]
enum State {
    GetBlock(u64),
    GetBlockResult {
        task: std::pin::Pin<
            Box<
                dyn futures::Future<Output = Result<Option<BlockHeader>, JsonRpcError>>
                    + Send
                    + Sync,
            >,
        >,
        height: u64,
    },
    BlockAndReceipts {
        block_header: BlockHeader,
        task: std::pin::Pin<
            Box<
                dyn futures::Future<
                        Output = Result<Vec<(TransactionSigned, TransactionReceipt)>, JsonRpcError>,
                    > + Send
                    + Sync,
            >,
        >,
    },
}

pub trait BlockState: Send {
    fn get_block(
        &self,
        block_num: u64,
    ) -> impl Future<Output = Result<Option<BlockHeader>, JsonRpcError>> + Send + Sync;
    fn get_receipts(
        &self,
        header: BlockHeader,
        block_num: u64,
    ) -> impl Future<Output = Result<Vec<(TransactionSigned, TransactionReceipt)>, JsonRpcError>>
           + Send
           + Sync;
}

#[derive(Clone)]
pub struct TrieDbBlockState<T: Triedb> {
    inner: T,
}

impl<T: Triedb> TrieDbBlockState<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T: Triedb + Send + Sync> BlockState for TrieDbBlockState<T> {
    async fn get_block(&self, block_num: u64) -> Result<Option<BlockHeader>, JsonRpcError> {
        self.inner
            .get_block_header(block_num)
            .await
            .map_err(JsonRpcError::internal_error)
    }

    async fn get_receipts(
        &self,
        header: BlockHeader,
        block_num: u64,
    ) -> Result<Vec<(TransactionSigned, TransactionReceipt)>, JsonRpcError> {
        let transactions = self
            .inner
            .get_transactions(block_num)
            .await
            .map_err(JsonRpcError::internal_error)?;
        let bloom_receipts = self
            .inner
            .get_receipts(block_num)
            .await
            .map_err(JsonRpcError::internal_error)?;
        let receipts =
            block_receipts(&transactions, bloom_receipts, &header.header, header.hash).await?;

        let result: Vec<(TransactionSigned, TransactionReceipt)> =
            transactions.into_iter().zip(receipts).collect();
        Ok(result)
    }
}

#[derive(Clone)]
pub struct MockBlockState {
    blocks: Vec<Block>,
}

impl MockBlockState {
    pub fn new() -> Self {
        Self { blocks: Vec::new() }
    }

    pub fn set_blocks(&mut self, blocks: Vec<Block>) {
        self.blocks = blocks;
    }
}

impl BlockState for MockBlockState {
    async fn get_block(&self, block_num: u64) -> Result<Option<BlockHeader>, JsonRpcError> {
        match self.blocks.get(block_num as usize) {
            Some(b) => Ok(Some(BlockHeader {
                hash: b.hash_slow(),
                header: b.header.clone(),
            })),
            None => Ok(None),
        }
    }

    async fn get_receipts(
        &self,
        _block_header: BlockHeader,
        _block_num: u64,
    ) -> Result<Vec<(TransactionSigned, TransactionReceipt)>, JsonRpcError> {
        Ok(vec![])
    }
}

#[pin_project]
pub struct BlockWatcher<B: BlockState + Clone> {
    triedb: B,
    interval: tokio::time::Interval,
    #[pin]
    state: State,
}

impl<B: BlockState + Clone> BlockWatcher<B> {
    pub fn new(triedb: B, current_height: u64) -> Self {
        Self {
            triedb,
            interval: tokio::time::interval(std::time::Duration::from_millis(500)),
            state: State::GetBlock(current_height),
        }
    }
}

#[derive(Clone, Default)]
pub struct BlockWithReceipts {
    pub block_header: BlockHeader,
    pub transactions: Vec<TransactionSigned>,
    pub receipts: Vec<TransactionReceipt>,
}

impl<B: BlockState + Clone + Send + Sync + 'static> Stream for BlockWatcher<B> {
    type Item = BlockWithReceipts;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut().project();
        let mut state = this.state.as_mut();

        match state.as_mut().project() {
            StateProj::GetBlock(height) => match this.interval.poll_tick(cx) {
                Poll::Ready(_) => {
                    let triedb = this.triedb.clone();
                    let height = *height;
                    let task = async move { triedb.get_block(height).await };
                    let task = Box::pin(task);
                    state.set(State::GetBlockResult { task, height });
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Poll::Pending => Poll::Pending,
            },
            StateProj::GetBlockResult { task, height } => match task.poll_unpin(cx) {
                Poll::Ready(block) => match block {
                    Ok(Some(block_header)) => {
                        let triedb = this.triedb.clone();
                        let height = block_header.header.number;
                        let header = block_header.clone();
                        let task = async move { triedb.get_receipts(header, height).await };
                        let task = Box::pin(task);
                        state.set(State::BlockAndReceipts { block_header, task });
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                    Ok(None) => {
                        let height = *height;
                        state.set(State::GetBlock(height));
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                    Err(e) => {
                        error!("error getting block inside blockwatcher: {e:?}");
                        let height = *height;
                        state.set(State::GetBlock(height));
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                },
                Poll::Pending => Poll::Pending,
            },
            StateProj::BlockAndReceipts { block_header, task } => match task.poll_unpin(cx) {
                Poll::Ready(receipts) => match receipts {
                    Ok(receipts) => {
                        let height = block_header.header.number;
                        let block_header = block_header.clone();
                        state.set(State::GetBlock(height + 1));

                        let (transactions, receipts) = receipts.into_iter().unzip();
                        Poll::Ready(Some(BlockWithReceipts {
                            block_header,
                            transactions,
                            receipts,
                        }))
                    }
                    Err(e) => {
                        error!("error getting receipts inside blockwatcher: {e:?}");
                        let height = block_header.header.number;
                        state.set(State::GetBlock(height));
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                },
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

#[cfg(test)]
mod test {
    use futures::StreamExt;
    use reth_primitives::Header;

    use super::*;

    #[tokio::test]
    async fn test_block_stream() {
        let mut mock = MockBlockState::new();
        let mut blocks = Vec::new();
        for i in 0..3 {
            let block = Block {
                header: Header {
                    number: i,
                    ..Default::default()
                },
                ..Default::default()
            };
            blocks.push(block);
        }
        mock.set_blocks(blocks);
        let mut watcher = BlockWatcher::new(mock, 0);

        for i in 0..3 {
            let block = watcher.next().await;
            assert_eq!(block.unwrap().block_header.header.number, i)
        }
    }
}
