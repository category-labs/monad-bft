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

use alloy_consensus::TxEip7702;
use alloy_eips::eip7702::Authorization;

use super::*;

// Non-zero placeholder used as the delegation target for self-authorizations.
// The account only needs to be marked delegated; the target does not have to be a
// deployed contract for the consensus-level reserve-balance check.
const DELEGATION_TARGET: Address = Address::repeat_byte(0xDE);
const DELEGATE_GAS_LIMIT: u64 = 100_000;

pub struct ReserveBalanceFailGenerator {
    pub recipient_keys: KeyPool,
    pub num_fail_txs: usize,
    pub delegate_sender: bool,
}

impl Generator for ReserveBalanceFailGenerator {
    fn handle_acct_group(
        &mut self,
        accts: &mut [SimpleAccount],
        ctx: &GenCtx,
    ) -> Vec<(TxEnvelope, Address, crate::shared::private_key::PrivateKey)> {
        accts
            .iter_mut()
            .flat_map(|sender| {
                let max_fee_per_gas = ctx.base_fee * 2;
                let gas_limit = ctx.set_tx_gas_limit.unwrap_or(50_000); // 50k default, override with --set-tx-gas-limit

                if sender.native_bal > U256::from(5_000_000_000_000_000_u64) {
                    warn!(
                        "Sender balance too high for reserve_balance_fail: {}\nEnsure min_native_amount and seed_native_amount are set less than 5*10^15",
                        sender.native_bal);
                }

                let mut txs = Vec::with_capacity(self.num_fail_txs + 2);

                if self.delegate_sender {
                    // Self-authorize: authority == sender. Because the tx's own nonce is
                    // consumed before authorizations are processed, the auth nonce must
                    // be sender.nonce + 1, and both the tx and the auth each bump the
                    // account nonce — so we advance our local view by 2.
                    let auth = Authorization {
                        chain_id: U256::from(ctx.chain_id),
                        address: DELEGATION_TARGET,
                        nonce: sender.nonce + 1,
                    };
                    let auth_sig = sender.key.sign_hash(&auth.signature_hash());
                    let signed_auth = auth.into_signed(auth_sig);

                    let delegate_tx = TxEip7702 {
                        chain_id: ctx.chain_id,
                        nonce: sender.nonce,
                        gas_limit: DELEGATE_GAS_LIMIT,
                        max_fee_per_gas,
                        max_priority_fee_per_gas: ctx.priority_fee.unwrap_or(0),
                        to: sender.addr,
                        value: U256::ZERO,
                        access_list: Default::default(),
                        input: Default::default(),
                        authorization_list: vec![signed_auth],
                    };
                    let sig = sender.key.sign_transaction(&delegate_tx);
                    sender.nonce += 2;

                    txs.push((
                        TxEnvelope::Eip7702(delegate_tx.into_signed(sig)),
                        sender.addr,
                        sender.key.clone(),
                    ));
                }

                txs.extend((0..self.num_fail_txs + 1).map(|_| {
                    let to = self.recipient_keys.next_addr();

                    let tx = TxEip1559 {
                        chain_id: ctx.chain_id,
                        nonce: sender.nonce,
                        gas_limit,
                        max_fee_per_gas,
                        max_priority_fee_per_gas: ctx.priority_fee.unwrap_or(0), // 0 default, override with --priority-fee
                        to: TxKind::Call(to),
                        value: U256::from(1000),
                        access_list: Default::default(),
                        input: Default::default(),
                    };

                    sender.nonce += 1;

                    let sig = sender.key.sign_transaction(&tx);
                    let tx = TxEnvelope::Eip1559(tx.into_signed(sig));

                    (tx, to, sender.key.clone())
                }));

                txs.into_iter()
            })
            .collect()
    }
}
