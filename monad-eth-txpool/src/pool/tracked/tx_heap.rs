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
    cmp::Ordering,
    collections::{BinaryHeap, VecDeque},
};

use alloy_consensus::Transaction;
use alloy_primitives::Address;
use indexmap::IndexMap;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_block_policy::{AccountNonceRetrievable, EthValidatedBlock};
use monad_validator::signature_collection::SignatureCollection;

use super::list::TrackedTxList;
use crate::pool::transaction::ValidEthTransaction;

#[derive(Debug, PartialEq, Eq)]
struct OrderedTx<'a> {
    tx: &'a ValidEthTransaction,
    effective_tip_per_gas: u128,
}

impl<'a> OrderedTx<'a> {
    fn new(tx: &'a ValidEthTransaction, base_fee: u64) -> Option<Self> {
        Some(Self {
            tx,
            effective_tip_per_gas: tx.raw().effective_tip_per_gas(base_fee)?,
        })
    }
}

impl<'a> PartialOrd for OrderedTx<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for OrderedTx<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.effective_tip_per_gas, self.tx.gas_limit())
            .cmp(&(other.effective_tip_per_gas, other.tx.gas_limit()))
    }
}

#[derive(Debug, PartialEq, Eq)]
struct OrderedTxGroup<'a> {
    tx: OrderedTx<'a>,
    virtual_time: u64,
    address: &'a Address,
    queued: VecDeque<OrderedTx<'a>>,
}

impl PartialOrd for OrderedTxGroup<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedTxGroup<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.tx
            .cmp(&other.tx)
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
        base_fee: u64,
    ) -> Self
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    {
        let pending_account_nonces = extending_blocks.get_account_nonces();

        let mut heap_vec = Vec::with_capacity(tracked_txs.len());
        let mut virtual_time = 0;

        for (address, tx_list) in tracked_txs {
            let mut queued = tx_list
                .get_queued(pending_account_nonces.get(address).cloned())
                .map_while(|tx| OrderedTx::new(tx, base_fee));

            let Some(tx) = queued.next() else {
                continue;
            };

            assert_eq!(address, tx.tx.signer_ref());

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
            tx:
                OrderedTx {
                    tx,
                    effective_tip_per_gas: _,
                },
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
    fn push(&mut self, address: &'a Address, tx: OrderedTx<'a>, queued: VecDeque<OrderedTx<'a>>) {
        assert_eq!(address, tx.tx.signer_ref());

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
