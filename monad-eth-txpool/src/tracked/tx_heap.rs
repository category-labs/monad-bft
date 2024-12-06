use std::collections::{BinaryHeap, VecDeque};

use indexmap::IndexMap;
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_eth_block_policy::{AccountNonceRetrievable, EthValidatedBlock};
use monad_eth_types::EthAddress;

use super::list::TrackedTxList;
use crate::transaction::ValidEthTransaction;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct OrderedTxGroup<'a> {
    tx: &'a ValidEthTransaction,
    virtual_time: u64,
    address: &'a EthAddress,
    queued: VecDeque<&'a ValidEthTransaction>,
}

pub struct TrackedTxHeap<'a> {
    heap: BinaryHeap<OrderedTxGroup<'a>>,
    virtual_time: u64,
}

impl<'a> TrackedTxHeap<'a> {
    pub fn new<SCT>(
        tracked_txs: &'a IndexMap<EthAddress, TrackedTxList>,
        extending_blocks: &Vec<&EthValidatedBlock<SCT>>,
    ) -> Self
    where
        SCT: SignatureCollection,
    {
        let pending_account_nonces = extending_blocks.get_account_nonces();

        let mut heap_vec = Vec::with_capacity(tracked_txs.len());

        for (address, tx_list) in tracked_txs {
            let mut queued = tx_list.get_queued(pending_account_nonces.get(address).cloned());

            let Some(tx) = queued.next() else {
                continue;
            };

            assert_eq!(address, &tx.sender());

            heap_vec.push(OrderedTxGroup {
                tx,
                virtual_time: 0,
                address,
                queued: queued.collect(),
            });
        }

        Self {
            heap: BinaryHeap::from(heap_vec),
            virtual_time: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn addresses<'s>(&'s self) -> impl Iterator<Item = &'a EthAddress> + 's {
        self.heap.iter().map(
            |OrderedTxGroup {
                 tx: _,
                 virtual_time: _,
                 address,
                 queued: _,
             }| *address,
        )
    }

    pub fn drain_in_order_while(
        mut self,
        mut f: impl FnMut(&EthAddress, &ValidEthTransaction) -> TrackedTxHeapDrainAction,
    ) {
        while let Some(OrderedTxGroup {
            tx,
            virtual_time: _,
            address,
            mut queued,
        }) = self.heap.pop()
        {
            match f(address, tx) {
                TrackedTxHeapDrainAction::Skip => {}
                TrackedTxHeapDrainAction::Continue => {
                    if let Some(tx) = queued.pop_front() {
                        self.push(address, tx, queued);
                    }
                }
                TrackedTxHeapDrainAction::Stop => {
                    break;
                }
            }
        }
    }

    #[inline]
    fn push(
        &mut self,
        address: &'a EthAddress,
        tx: &'a ValidEthTransaction,
        queued: VecDeque<&'a ValidEthTransaction>,
    ) {
        assert_eq!(address, &tx.sender());

        self.virtual_time += 1;
        self.heap.push(OrderedTxGroup {
            tx,
            virtual_time: self.virtual_time,
            address,
            queued,
        });
    }
}

pub enum TrackedTxHeapDrainAction {
    Skip,
    Continue,
    Stop,
}
