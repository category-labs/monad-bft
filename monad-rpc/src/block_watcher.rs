use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{FutureExt, Stream};
use pin_project::pin_project;
use reth_primitives::Block;
use reth_rpc_types::TransactionReceipt;

#[pin_project::pin_project(project = StateProj)]
enum State {
    GetBlock(u64),
    GetBlockResult {
        task: std::pin::Pin<Box<dyn futures::Future<Output = Block> + Send + Sync>>,
    },
    BlockAndReceipts {
        block: Block,
        task:
            std::pin::Pin<Box<dyn futures::Future<Output = Vec<TransactionReceipt>> + Send + Sync>>,
    },
}

// TODO: remove this trait when triedb contains blocks
pub trait BlockState: Send {
    fn get_block(&self, block_num: u64) -> impl Future<Output = Block> + Send + Sync;
    fn get_receipts(
        &self,
        block_num: u64,
    ) -> impl Future<Output = Vec<TransactionReceipt>> + Send + Sync;
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
    async fn get_block(&self, block_num: u64) -> Block {
        if self.blocks.is_empty() || block_num > self.blocks.len() as u64 {
            Block::default()
        } else {
            self.blocks
                .get(block_num as usize)
                .cloned()
                .unwrap_or_default()
        }
    }

    async fn get_receipts(&self, _block_num: u64) -> Vec<TransactionReceipt> {
        vec![]
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
            interval: tokio::time::interval(std::time::Duration::from_secs(1)),
            state: State::GetBlock(current_height),
        }
    }
}

pub struct BlockWithReceipts {
    block: Block,
    receipts: Vec<TransactionReceipt>,
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
                    state.set(State::GetBlockResult { task });
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Poll::Pending => Poll::Pending,
            },
            StateProj::GetBlockResult { task } => match task.poll_unpin(cx) {
                Poll::Ready(block) => {
                    let triedb = this.triedb.clone();
                    let height = block.header.number;
                    let task = async move { triedb.get_receipts(height).await };
                    let task = Box::pin(task);
                    state.set(State::BlockAndReceipts { block, task });
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Poll::Pending => Poll::Pending,
            },
            StateProj::BlockAndReceipts { block, task } => match task.poll_unpin(cx) {
                Poll::Ready(receipts) => {
                    let height = block.header.number;
                    let block = block.clone();
                    state.set(State::GetBlock(height + 1));
                    Poll::Ready(Some(BlockWithReceipts { block, receipts }))
                }
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
            assert_eq!(block.unwrap().block.number, i)
        }
    }
}
