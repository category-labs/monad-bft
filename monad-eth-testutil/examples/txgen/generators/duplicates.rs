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

use super::native_transfer_priority_fee;
use crate::prelude::*;

pub struct DuplicateTxGenerator {
    pub(crate) recipient_keys: KeyPool,
    pub(crate) tx_per_sender: usize,
    pub random_priority_fee: bool,
}

impl Generator for DuplicateTxGenerator {
    fn handle_acct_group(
        &mut self,
        accts: &mut [SimpleAccount],
        ctx: &GenCtx,
    ) -> Vec<(TxEnvelope, Address)> {
        let mut rng = SmallRng::from_entropy();
        let mut txs = Vec::with_capacity(self.tx_per_sender * accts.len());

        for sender in accts {
            let to = self.recipient_keys.next_addr(); // change sampling strategy?
            for _ in 0..self.tx_per_sender {
                let priority_fee = if self.random_priority_fee {
                    rng.gen_range(0..1000)
                } else {
                    0
                };
                let tx =
                    native_transfer_priority_fee(sender, to, U256::from(10), priority_fee, ctx);
                txs.push((tx, to));
            }
        }

        txs
    }
}
