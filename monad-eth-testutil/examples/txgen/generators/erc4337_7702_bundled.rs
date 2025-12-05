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

use alloy_eips::eip7702::SignedAuthorization;

use super::*;
use crate::shared::{
    erc4337_entrypoint::{EntryPoint, PackedUserOperation},
    simple7702_account::Simple7702Account,
};

pub struct ERC4337_7702BundledGenerator {
    pub entrypoint: EntryPoint,
    pub simple7702account: Simple7702Account, // One global eip-7702 based account contract
    pub bundler_account: Option<SimpleAccount>, // Initialized from first funded account
    pub ops_per_bundle: usize,
    pub tx_per_sender: usize,
    pub paymaster_address: Option<Address>,
    pub transfer_amount: U256,   // Amount to transfer in each UserOp
    pub recipient_keys: KeyPool, // For deterministic recipients
}

impl ERC4337_7702BundledGenerator {
    pub fn new(
        entrypoint: Address,
        simple7702account: Address,
        ops_per_bundle: usize,
        tx_per_sender: usize,
        paymaster_address: Option<Address>,
        recipient_keys: KeyPool,
    ) -> Self {
        Self {
            entrypoint: EntryPoint { addr: entrypoint },
            simple7702account: Simple7702Account {
                addr: simple7702account,
            },
            bundler_account: None,
            ops_per_bundle,
            tx_per_sender,
            paymaster_address,
            transfer_amount: U256::from(100_000_000_000_000_000_000u128), // 100 MON
            recipient_keys,
        }
    }

    fn create_user_op_for_account(
        &self,
        acct: &mut SimpleAccount,
        recipient: Address,
        ctx: &GenCtx,
    ) -> PackedUserOperation {
        // UserOp sender
        let sender = acct.addr;
        // UserOp nonce is account sender, meaning account UserOps are sequential
        let nonce = U256::from(acct.nonce);
        let calldata = self.simple7702account.encode_execute_calldata(
            recipient,
            self.transfer_amount,
            Bytes::new(),
        );
        // no init_code as no contract to deploy
        let init_code = Bytes::new();

        let verification_gas = 100_000u128;
        let call_gas = 50_000u128;
        let pre_verification_gas = 21_000u128;

        // no paymaster in the current iteration
        let paymaster_and_data = Bytes::new();

        let mut user_op = self.entrypoint.create_user_operation(
            sender,
            nonce,
            init_code,
            calldata,
            verification_gas,
            call_gas,
            pre_verification_gas,
            10,
            100_000u128,
            paymaster_and_data,
        );

        let user_op_signature =
            self.entrypoint
                .sign_user_operation(&user_op, &acct.key, ctx.chain_id);

        user_op.signature = user_op_signature;
        acct.nonce += 1;

        user_op
    }

    fn create_authorization_for_account(
        &self,
        acct: &SimpleAccount,
        ctx: &GenCtx,
    ) -> Result<SignedAuthorization> {
        self.simple7702account.create_authorization(
            &(acct.addr, acct.key.clone()),
            acct.nonce,
            ctx.chain_id,
        )
    }
}

impl Generator for ERC4337_7702BundledGenerator {
    fn handle_acct_group(
        &mut self,
        accts: &mut [SimpleAccount],
        ctx: &GenCtx,
    ) -> Vec<(TxEnvelope, Address, PrivateKey)> {
        // Initialize bundler from first funded account
        if self.bundler_account.is_none() {
            if accts.is_empty() {
                error!("Cannot initialize bundler: no accounts available");
                return vec![];
            }
            let bundler = SimpleAccount {
                nonce: accts[0].nonce,
                native_bal: accts[0].native_bal,
                erc20_balances: accts[0].erc20_balances.clone(),
                key: accts[0].key.clone(),
                addr: accts[0].addr,
            };
            info!(
                "Initialized bundler account from first sender: {} (balance: {})",
                bundler.addr, bundler.native_bal
            );
            self.bundler_account = Some(bundler);
        }

        let mut user_ops_with_auths = Vec::new();

        // Generate UserOps + Authorizations for each account, skipping bundler account
        for acct in accts.iter_mut().skip(1) {
            for _ in 0..self.tx_per_sender {
                let recipient = self.recipient_keys.next_addr();

                let auth = match self.create_authorization_for_account(acct, ctx) {
                    Ok(auth) => auth,
                    Err(e) => {
                        error!(
                            "Failed to create authorization for account {}: {}",
                            acct.addr, e
                        );
                        continue;
                    }
                };

                let user_op = self.create_user_op_for_account(acct, recipient, ctx);

                info!(
                    "Created user operation {} for account {} with auth {}",
                    user_op.nonce, acct.addr, auth.nonce
                );

                user_ops_with_auths.push((user_op, auth, acct.addr));
            }
        }

        // Bundle bunch of UserOps into handleOps transactions
        let mut bundle_txs = Vec::new();

        for chunk in user_ops_with_auths.chunks(self.ops_per_bundle) {
            let user_ops: Vec<PackedUserOperation> =
                chunk.iter().map(|(op, _, _)| op.clone()).collect();

            let authorizations: Vec<SignedAuthorization> =
                chunk.iter().map(|(_, auth, _)| auth.clone()).collect();

            let bundler = self
                .bundler_account
                .as_mut()
                .expect("Bundler should be initialized");
            let bundle_tx = self.entrypoint.create_handle_ops_tx_with_7702(
                bundler,
                user_ops,
                authorizations,
                ctx.base_fee,
                ctx.chain_id,
                Some(10_000_000),
                ctx.priority_fee,
            );

            bundle_txs.push((bundle_tx, self.entrypoint.addr, bundler.key.clone()));
        }

        info!(
            "Generated ERC-4337 + EIP-7702 bundles: {} user operations in {} bundles with {} ops per bundle",
            user_ops_with_auths.len(),
            bundle_txs.len(),
            self.ops_per_bundle,
        );

        bundle_txs
    }
}
