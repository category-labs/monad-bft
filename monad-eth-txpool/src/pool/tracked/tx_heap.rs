use std::collections::{BinaryHeap, VecDeque};

use alloy_primitives::Address;
use indexmap::IndexMap;
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_block_policy::{AccountNonceRetrievable, EthValidatedBlock};

use super::list::TrackedTxList;
use crate::pool::transaction::ValidEthTransaction;

#[derive(Debug, PartialEq, Eq)]
struct OrderedTxGroup<'a> {
    tx: &'a ValidEthTransaction,
    virtual_time: u64,
    address: &'a Address,
    queued: VecDeque<&'a ValidEthTransaction>,
}

impl PartialOrd for OrderedTxGroup<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedTxGroup<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.tx
            .cmp(other.tx)
            .then_with(|| self.virtual_time.cmp(&other.virtual_time).reverse())
    }
}

pub struct TrackedTxHeap<'a> {
    heap: BinaryHeap<OrderedTxGroup<'a>>,
    virtual_time: u64,
}

impl<'a> TrackedTxHeap<'a> {
    pub fn new<ST, SCT>(
        tracked_txs: &'a IndexMap<Address, TrackedTxList>,
        extending_blocks: &Vec<&EthValidatedBlock<ST, SCT>>,
    ) -> Self
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    {
        let pending_account_nonces = extending_blocks.get_account_nonces();

        let mut heap_vec = Vec::with_capacity(tracked_txs.len());
        let mut virtual_time = 0;

        for (address, tx_list) in tracked_txs {
            let mut queued = tx_list.get_queued(pending_account_nonces.get(address).cloned());

            let Some(tx) = queued.next() else {
                continue;
            };

            assert_eq!(address, tx.signer_ref());

            heap_vec.push(OrderedTxGroup {
                tx,
                virtual_time,
                address,
                queued: queued.collect(),
            });
            virtual_time += 1;
        }

        Self {
            heap: BinaryHeap::from(heap_vec),
            virtual_time,
        }
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn addresses<'s>(&'s self) -> impl Iterator<Item = &'a Address> + 's {
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
        mut f: impl FnMut(&Address, &ValidEthTransaction) -> TrackedTxHeapDrainAction,
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
        address: &'a Address,
        tx: &'a ValidEthTransaction,
        queued: VecDeque<&'a ValidEthTransaction>,
    ) {
        assert_eq!(address, tx.signer_ref());

        self.heap.push(OrderedTxGroup {
            tx,
            virtual_time: self.virtual_time,
            address,
            queued,
        });
        self.virtual_time += 1;
    }
}

pub enum TrackedTxHeapDrainAction {
    Skip,
    Continue,
    Stop,
}
