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

use alloy_primitives::Address;
use rand::Rng;

use super::*;
use crate::shared::eip7702::EIP7702;

pub struct EIP7702Generator {
    pub batch_call_contract: EIP7702,
    pub tx_per_sender: usize,
    pub total_authorizations: usize,
    pub authorizations_per_tx: usize,
    authorizations: Vec<(Address, alloy_eips::eip7702::SignedAuthorization)>,
    authority_accounts: Vec<Address>,
}

impl EIP7702Generator {
    pub fn new(
        batch_call_contract: EIP7702,
        tx_per_sender: usize,
        total_authorizations: usize,
        authorizations_per_tx: usize,
    ) -> Self {
        Self {
            batch_call_contract,
            tx_per_sender,
            total_authorizations,
            authorizations_per_tx,
            authorizations: Vec::new(),
            authority_accounts: Vec::new(),
        }
    }
}

impl Generator for EIP7702Generator {
    fn handle_acct_group(
        &mut self,
        accts: &mut [SimpleAccount],
        ctx: &GenCtx,
    ) -> Vec<(TxEnvelope, Address)> {
        let mut txs = Vec::with_capacity(self.tx_per_sender * accts.len());
        let mut rng = rand::thread_rng();

        while self.authorizations.len() < self.total_authorizations && !accts.is_empty() {
            let authority_idx = rng.gen_range(0..accts.len());
            let authority = &accts[authority_idx];
            let authority_addr = authority.addr;

            if self.authority_accounts.contains(&authority_addr) {
                continue;
            }

            if let Ok(auth) = self.batch_call_contract.create_authorization(
                &(authority_addr, authority.key.clone()),
                authority.nonce,
                ctx.chain_id,
            ) {
                self.authorizations.push((authority_addr, auth));
                self.authority_accounts.push(authority_addr);

                accts[authority_idx].nonce += 1;
            }
        }

        for sender in accts {
            for _ in 0..self.tx_per_sender {
                if !self.authorizations.is_empty() {
                    let num_auths = self.authorizations_per_tx.min(self.authorizations.len());
                    let mut selected_auths = Vec::with_capacity(num_auths);
                    let mut selected_accounts = Vec::with_capacity(num_auths);

                    let mut available_indices: Vec<usize> =
                        (0..self.authorizations.len()).collect();
                    for _ in 0..num_auths {
                        let idx = rng.gen_range(0..available_indices.len());
                        let auth_idx = available_indices.remove(idx);
                        let (authorized_account, authorization) = &self.authorizations[auth_idx];
                        selected_auths.push(authorization.clone());
                        selected_accounts.push(*authorized_account);
                    }

                    // Use the first authorized account as the target
                    let target_account = selected_accounts[0];

                    let tx = self.batch_call_contract.create_authorization_usage_tx(
                        sender,
                        target_account,
                        ctx.base_fee * 2, // 100% increase
                        ctx.chain_id,
                        ctx.set_tx_gas_limit,
                        ctx.priority_fee,
                    );

                    txs.push((tx, target_account));
                } else {
                    debug!("No authorizations found, falling back to simple contract call");
                    let tx = self.batch_call_contract.create_simple_call_tx(
                        sender,
                        ctx.base_fee * 2, // 100% increase
                        ctx.chain_id,
                        ctx.set_tx_gas_limit,
                        ctx.priority_fee,
                    );
                    txs.push((tx, self.batch_call_contract.addr));
                }
            }
        }

        txs
    }
}
