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

use alloy_primitives::{Address, Bytes};
use rand::Rng;

use super::*;
use crate::shared::eip7702::EIP7702;

pub struct EIP7702CreateGenerator {
    pub batch_call_contract: EIP7702,
    pub tx_per_sender: usize,
    pub authorizations_per_tx: usize,
}

impl EIP7702CreateGenerator {
    pub fn new(
        batch_call_contract: EIP7702,
        tx_per_sender: usize,
        authorizations_per_tx: usize,
    ) -> Self {
        Self {
            batch_call_contract,
            tx_per_sender,
            authorizations_per_tx,
        }
    }
}

impl Generator for EIP7702CreateGenerator {
    fn handle_acct_group(
        &mut self,
        accts: &mut [SimpleAccount],
        ctx: &GenCtx,
    ) -> Vec<(TxEnvelope, Address)> {
        let mut txs = Vec::with_capacity(self.tx_per_sender * accts.len());
        let mut rng = rand::thread_rng();

        for sender_idx in 0..accts.len() {
            for _ in 0..self.tx_per_sender {
                let mut authorizations = Vec::with_capacity(self.authorizations_per_tx);
                let mut authorized_accounts = Vec::with_capacity(self.authorizations_per_tx);

                let mut available_indices: Vec<usize> =
                    (0..accts.len()).filter(|&i| i != sender_idx).collect();

                if available_indices.is_empty() {
                    debug!(
                        "Not enough accounts to create authorizations (need at least 2 accounts)"
                    );
                    continue;
                }

                for _ in 0..self.authorizations_per_tx {
                    if available_indices.is_empty() {
                        break;
                    }

                    let idx = rng.gen_range(0..available_indices.len());
                    let auth_idx = available_indices.remove(idx);

                    let authority_addr = accts[auth_idx].addr;
                    let authority_key = accts[auth_idx].key.clone();
                    let authority_nonce = accts[auth_idx].nonce;

                    if let Ok(auth) = self.batch_call_contract.create_authorization(
                        &(authority_addr, authority_key),
                        authority_nonce,
                        ctx.chain_id,
                    ) {
                        authorizations.push(auth);
                        authorized_accounts.push(authority_addr);
                        accts[auth_idx].nonce += 1;
                    }
                }

                let calldata = Bytes::from(vec![0u8; 100]);
                let target_account = authorized_accounts
                    .first()
                    .copied()
                    .unwrap_or(Address::ZERO);

                let tx = self.batch_call_contract.create_eip7702_tx(
                    &mut accts[sender_idx],
                    target_account,
                    authorizations,
                    calldata,
                    ctx.base_fee,
                    ctx.chain_id,
                );

                txs.push((tx, target_account));
            }
        }

        txs
    }
}
