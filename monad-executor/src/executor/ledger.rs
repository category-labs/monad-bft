use std::{
    collections::HashMap,
    marker::Unpin,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use futures::Stream;
use monad_consensus_types::block::BlockType;
use monad_types::BlockId;

use crate::{Executor, LedgerCommand};

pub struct MockLedger<O: BlockType, E> {
    blockchain: Vec<O>,
    block_index: HashMap<BlockId, usize>,
    ledger_fetch_cb: Option<(BlockId, Box<dyn (FnOnce(Option<O>) -> E) + Send + Sync>)>,
    waker: Option<Waker>,
}

impl<O: BlockType, E> MockLedger<O, E> {
    pub fn ready(&self) -> bool {
        self.ledger_fetch_cb.is_some()
    }
}

impl<O: BlockType, E> Default for MockLedger<O, E> {
    fn default() -> Self {
        Self {
            blockchain: Vec::new(),
            block_index: HashMap::new(),
            ledger_fetch_cb: None,
            waker: None,
        }
    }
}

impl<O: BlockType, E> MockLedger<O, E> {
    pub fn get_blocks(&self) -> &Vec<O> {
        &self.blockchain
    }
}

impl<O: BlockType, E> Executor for MockLedger<O, E> {
    type Command = LedgerCommand<O, E>;
    fn exec(&mut self, commands: Vec<Self::Command>) {
        let mut wake = false;

        for command in commands {
            match command {
                LedgerCommand::LedgerCommit(block) => {
                    // it is assumed the consensus verified that block is valid, thus commiting shouldn't cause issue
                    // only concern might be hash collision
                    self.block_index
                        .insert(block.get_id(), self.blockchain.len());
                    self.blockchain.push(block);
                }
                LedgerCommand::LedgerFetch(block_id, cb) => {
                    self.ledger_fetch_cb = Some((block_id, cb));
                    wake = true;
                }
            }
        }
        if wake {
            if let Some(waker) = self.waker.take() {
                waker.wake()
            };
        }
    }
}

impl<O, E> Stream for MockLedger<O, E>
where
    Self: Unpin,
    O: Clone + BlockType,
{
    type Item = E;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if let Some((block_id, cb)) = this.ledger_fetch_cb.take() {
            return Poll::Ready(Some(cb({
                if let Some(i) = self.block_index.get(&block_id) {
                    Some(self.blockchain[*i].clone())
                } else {
                    None
                }
            })));
        }

        self.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use futures::{FutureExt, StreamExt};
    use monad_testutil::block::MockBlock;

    use crate::{executor::ledger::MockLedger, Executor, LedgerCommand};

    #[derive(Debug, PartialEq, Eq)]
    struct MockLedgerEvent {
        pub block: Option<MockBlock>,
    }

    #[test]
    fn test_basic_stream_functionality() {
        let mut mock_ledger = MockLedger::<MockBlock, MockLedgerEvent>::default();
        assert_eq!(mock_ledger.next().now_or_never(), None); // nothing should be within the pipeline
        let block = MockBlock {
            block_id: monad_types::BlockId(monad_types::Hash([0x00_u8; 32])),
            parent_block_id: monad_types::BlockId(monad_types::Hash([0x01_u8; 32])),
        };
        mock_ledger.exec(vec![LedgerCommand::LedgerCommit(block)]);
        assert_eq!(mock_ledger.next().now_or_never(), None); // ledger commit shouldn't cause any event

        mock_ledger.exec(vec![LedgerCommand::LedgerFetch(
            monad_types::BlockId(monad_types::Hash([0x00_u8; 32])),
            Box::new(|block: Option<MockBlock>| MockLedgerEvent { block }),
        )]);
        let retrieved = mock_ledger.next().now_or_never();
        assert_ne!(retrieved, None); // there should be a response
        let mock_ledger_event = retrieved.unwrap().unwrap().block.unwrap();
        assert_eq!(
            mock_ledger_event.block_id,
            monad_types::BlockId(monad_types::Hash([0x00_u8; 32])),
        );
        assert_eq!(
            mock_ledger_event.parent_block_id,
            monad_types::BlockId(monad_types::Hash([0x01_u8; 32])),
        );
    }

    #[test]
    fn test_seeking_exist() {
        let mut mock_ledger = MockLedger::<MockBlock, MockLedgerEvent>::default();
        assert_eq!(mock_ledger.next().now_or_never(), None); // nothing should be within the pipeline
        mock_ledger.exec(vec![
            LedgerCommand::LedgerCommit(MockBlock {
                block_id: monad_types::BlockId(monad_types::Hash([0x01_u8; 32])),
                parent_block_id: monad_types::BlockId(monad_types::Hash([0x00_u8; 32])),
            }),
            LedgerCommand::LedgerCommit(MockBlock {
                block_id: monad_types::BlockId(monad_types::Hash([0x02_u8; 32])),
                parent_block_id: monad_types::BlockId(monad_types::Hash([0x01_u8; 32])),
            }),
            LedgerCommand::LedgerCommit(MockBlock {
                block_id: monad_types::BlockId(monad_types::Hash([0x03_u8; 32])),
                parent_block_id: monad_types::BlockId(monad_types::Hash([0x02_u8; 32])),
            }),
            LedgerCommand::LedgerCommit(MockBlock {
                block_id: monad_types::BlockId(monad_types::Hash([0x04_u8; 32])),
                parent_block_id: monad_types::BlockId(monad_types::Hash([0x03_u8; 32])),
            }),
        ]);
        assert_eq!(mock_ledger.next().now_or_never(), None); // ledger commit shouldn't cause any event

        mock_ledger.exec(vec![LedgerCommand::LedgerFetch(
            monad_types::BlockId(monad_types::Hash([0x02_u8; 32])),
            Box::new(|block: Option<MockBlock>| MockLedgerEvent { block }),
        )]);
        let retrieved = mock_ledger.next().now_or_never();
        assert_ne!(retrieved, None); // there should be a response
        let mock_ledger_event = retrieved.unwrap().unwrap().block.unwrap();
        assert_eq!(
            mock_ledger_event.block_id,
            monad_types::BlockId(monad_types::Hash([0x02_u8; 32])),
        );
        assert_eq!(
            mock_ledger_event.parent_block_id,
            monad_types::BlockId(monad_types::Hash([0x01_u8; 32])),
        );

        // similarly, calling retrieve again always be viable
        mock_ledger.exec(vec![LedgerCommand::LedgerFetch(
            monad_types::BlockId(monad_types::Hash([0x02_u8; 32])),
            Box::new(|block: Option<MockBlock>| MockLedgerEvent { block }),
        )]);
        let retrieved = mock_ledger.next().now_or_never();
        assert_ne!(retrieved, None); // there should be a response
        let mock_ledger_event = retrieved.unwrap().unwrap().block.unwrap();
        assert_eq!(
            mock_ledger_event.block_id,
            monad_types::BlockId(monad_types::Hash([0x02_u8; 32])),
        );
        assert_eq!(
            mock_ledger_event.parent_block_id,
            monad_types::BlockId(monad_types::Hash([0x01_u8; 32])),
        );
    }
    #[test]
    fn test_seeking_non_exist() {
        let mut mock_ledger = MockLedger::<MockBlock, MockLedgerEvent>::default();
        assert_eq!(mock_ledger.next().now_or_never(), None); // nothing should be within the pipeline

        mock_ledger.exec(vec![
            LedgerCommand::LedgerCommit(MockBlock {
                block_id: monad_types::BlockId(monad_types::Hash([0x01_u8; 32])),
                parent_block_id: monad_types::BlockId(monad_types::Hash([0x00_u8; 32])),
            }),
            LedgerCommand::LedgerCommit(MockBlock {
                block_id: monad_types::BlockId(monad_types::Hash([0x02_u8; 32])),
                parent_block_id: monad_types::BlockId(monad_types::Hash([0x01_u8; 32])),
            }),
            LedgerCommand::LedgerCommit(MockBlock {
                block_id: monad_types::BlockId(monad_types::Hash([0x03_u8; 32])),
                parent_block_id: monad_types::BlockId(monad_types::Hash([0x02_u8; 32])),
            }),
            LedgerCommand::LedgerCommit(MockBlock {
                block_id: monad_types::BlockId(monad_types::Hash([0x04_u8; 32])),
                parent_block_id: monad_types::BlockId(monad_types::Hash([0x03_u8; 32])),
            }),
        ]);
        assert_eq!(mock_ledger.next().now_or_never(), None); // ledger commit shouldn't cause any event

        mock_ledger.exec(vec![LedgerCommand::LedgerFetch(
            monad_types::BlockId(monad_types::Hash([0x10_u8; 32])),
            Box::new(|block: Option<MockBlock>| MockLedgerEvent { block }),
        )]);
        let retrieved = mock_ledger.next().now_or_never();
        assert_ne!(retrieved, None); // there should be a response
        let mock_ledger_event = retrieved.unwrap().unwrap().block;

        assert_eq!(mock_ledger_event, None);
    }
}
