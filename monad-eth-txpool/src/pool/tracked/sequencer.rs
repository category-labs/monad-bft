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
    collections::{BTreeMap, BinaryHeap, VecDeque},
};

use alloy_consensus::{transaction::Recovered, Transaction, TxEnvelope};
use alloy_primitives::Address;
use indexmap::IndexMap;
use monad_chain_config::{revision::ChainRevision, ChainConfig};
use monad_consensus_types::block::{AccountBalanceState, BlockPolicyBlockValidator};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_block_policy::{
    nonce_usage::{NonceUsage, NonceUsageRetrievable},
    EthBlockPolicyBlockValidator, EthValidatedBlock,
};
use monad_validator::signature_collection::SignatureCollection;
use rand::seq::SliceRandom;
use tracing::{debug, error, trace, warn};

use super::list::TrackedTxList;
use crate::pool::transaction::{ValidEthRecoveredAuthorization, ValidEthTransaction};

#[derive(Debug, PartialEq, Eq)]
struct OrderedTx<'a> {
    tx: &'a ValidEthTransaction,
    effective_tip_per_gas: u128,
}

impl<'a> OrderedTx<'a> {
    fn new(tx: &'a ValidEthTransaction, base_fee: u64) -> Option<Self> {
        let effective_tip_per_gas = tx.raw().effective_tip_per_gas(base_fee)?;

        Some(Self {
            tx,
            effective_tip_per_gas,
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

pub struct ProposalSequencer<'a> {
    heap: BinaryHeap<OrderedTxGroup<'a>>,
    virtual_time: u64,
}

impl<'a> ProposalSequencer<'a> {
    pub fn new<ST, SCT>(
        tracked_txs: &'a IndexMap<Address, TrackedTxList>,
        extending_blocks: &Vec<&EthValidatedBlock<ST, SCT>>,
        base_fee: u64,
        tx_limit: usize,
    ) -> Self
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    {
        let mut pending_nonce_usages = extending_blocks.get_nonce_usages().into_map();

        let mut heap_vec = Vec::with_capacity(tracked_txs.len());
        let mut virtual_time = 0;

        for (address, tx_list) in tracked_txs {
            let mut queued = tx_list
                .get_queued(pending_nonce_usages.remove(address))
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

        heap_vec.partial_shuffle(&mut rand::thread_rng(), tx_limit);
        heap_vec.truncate(tx_limit);

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

    pub fn build_proposal<CCT, CRT>(
        mut self,
        chain_id: u64,
        tx_limit: usize,
        proposal_gas_limit: u64,
        proposal_byte_limit: u64,
        chain_config: &CCT,
        mut account_balances: BTreeMap<&Address, AccountBalanceState>,
        validator: EthBlockPolicyBlockValidator<CRT>,
    ) -> Proposal
    where
        CCT: ChainConfig<CRT>,
        CRT: ChainRevision,
    {
        let mut proposal = Proposal::default();

        let mut authority_possible_nonce_deltas = BTreeMap::<Address, Vec<u64>>::default();

        'proposal: while proposal.txs.len() < tx_limit {
            let Some(OrderedTxGroup {
                mut tx,
                virtual_time: _,
                address,
                mut queued,
            }) = self.heap.pop()
            else {
                break;
            };

            if let Some(possible_nonce_deltas) = authority_possible_nonce_deltas.remove(address) {
                let new_account_nonce = NonceUsage::Possible(possible_nonce_deltas.into())
                    .apply_to_account_nonce(tx.tx.nonce());

                assert!(tx.tx.nonce() <= new_account_nonce);

                while tx.tx.nonce() < new_account_nonce {
                    let Some(next_tx) = queued.pop_front() else {
                        continue 'proposal;
                    };

                    tx = next_tx;
                }

                if tx.tx.nonce() != new_account_nonce {
                    error!(
                        tx_nonce = tx.tx.nonce(),
                        new_account_nonce,
                        "txpool sequencer authority nonce delta invariant broken"
                    );
                    break 'proposal;
                }

                self.push(address, tx, queued);
                continue;
            }

            if Self::try_add_tx_to_proposal(
                proposal_gas_limit,
                proposal_byte_limit,
                &mut account_balances,
                &validator,
                &mut proposal,
                address,
                tx.tx,
            ) {
                if let Some(next_tx) = queued.pop_front() {
                    self.push(address, next_tx, queued);
                }

                for ValidEthRecoveredAuthorization {
                    authority,
                    authorization,
                } in tx.tx.iter_valid_recovered_authorizations()
                {
                    if authorization.chain_id != 0
                        && authorization.chain_id != chain_config.chain_id()
                    {
                        warn!(
                            tx = ?tx.tx,
                            ?address,
                            ?authority,
                            ?authorization,
                            "txpool looked up nonces for invalid chain id authorization"
                        );
                        continue;
                    }

                    let Some(account_balance) = account_balances.get_mut(authority) else {
                        continue;
                    };

                    account_balance.is_delegated = true;
                    authority_possible_nonce_deltas
                        .entry(*authority)
                        .or_default()
                        .push(authorization.nonce);
                }
            }
        }

        proposal
    }

    #[inline]
    fn try_add_tx_to_proposal<CRT: ChainRevision>(
        proposal_gas_limit: u64,
        proposal_byte_limit: u64,
        account_balances: &mut BTreeMap<&Address, AccountBalanceState>,
        validator: &EthBlockPolicyBlockValidator<CRT>,
        proposal: &mut Proposal,
        address: &Address,
        tx: &ValidEthTransaction,
    ) -> bool {
        if proposal
            .total_gas
            .checked_add(tx.gas_limit())
            .is_none_or(|new_total_gas| new_total_gas > proposal_gas_limit)
        {
            return false;
        }

        let tx_size = tx.size();
        if proposal
            .total_size
            .checked_add(tx_size)
            .is_none_or(|new_total_size| new_total_size > proposal_byte_limit)
        {
            return false;
        }

        if let Err(error) = validator.try_add_transaction(account_balances, tx.raw()) {
            debug!(
                ?error,
                signer = ?tx.raw().signer(),
                gas_limit = ?tx.gas_limit(),
                value = ?tx.raw().value(),
                gas_fee = ?tx.raw().max_fee_per_gas(),
                "insufficient balance");
            return false;
        }

        proposal.total_gas += tx.gas_limit();
        proposal.total_size += tx_size;
        proposal.txs.push(tx.raw().to_owned());

        trace!(txn_hash = ?tx.hash(), "txn included in proposal");

        true
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

#[derive(Default)]
pub(super) struct Proposal {
    pub txs: Vec<Recovered<TxEnvelope>>,
    pub total_gas: u64,
    pub total_size: u64,
}
