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

use alloy_consensus::TxEnvelope;
use monad_types::SeqNum;

use crate::pool::transaction::ValidEthTransaction;

pub struct EthTxPoolForwardableTxs<'pool, I>
where
    I: Iterator<Item = &'pool mut ValidEthTransaction>,
{
    seq_num: SeqNum,
    base_fee: u64,
    iter: I,
}

impl<'pool, I> EthTxPoolForwardableTxs<'pool, I>
where
    I: Iterator<Item = &'pool mut ValidEthTransaction>,
{
    pub(crate) fn new(seq_num: SeqNum, base_fee: u64, iter: I) -> Self {
        Self {
            seq_num,
            base_fee,
            iter,
        }
    }

    pub fn take_forwardable_while<const MIN_SEQNUM_DIFF: u64, const MAX_RETRIES: usize>(
        self,
        mut f: impl FnMut(&TxEnvelope) -> bool,
    ) -> impl Iterator<Item = &'pool TxEnvelope> {
        self.iter
            .take_while(move |tx| f(tx.raw()))
            .filter_map(move |tx| {
                tx.get_if_forwardable::<MIN_SEQNUM_DIFF, MAX_RETRIES>(self.seq_num, self.base_fee)
            })
    }

    pub fn take_all<const MIN_SEQNUM_DIFF: u64, const MAX_RETRIES: usize>(
        self,
    ) -> impl Iterator<Item = &'pool TxEnvelope> {
        self.take_forwardable_while::<MIN_SEQNUM_DIFF, MAX_RETRIES>(|_| true)
    }
}
