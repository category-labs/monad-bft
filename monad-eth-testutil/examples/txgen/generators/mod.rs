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

use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope};
use alloy_primitives::TxKind;
use duplicates::DuplicateTxGenerator;
use ecmul::ECMulGenerator;
use few_to_many::CreateAccountsGenerator;
use high_call_data::HighCallDataTxGenerator;
use many_to_many::ManyToManyGenerator;
use non_deterministic_storage::NonDeterministicStorageTxGenerator;
use reserve_balance::ReserveBalanceGenerator;
use self_destruct::SelfDestructTxGenerator;
use storage_deletes::StorageDeletesTxGenerator;
use system_key_normal::SystemKeyNormalTxGenerator;
use system_spam::SystemTransactionSpamGenerator;
use uniswap::UniswapGenerator;

use crate::{
    config::{DeployedContract, GenMode},
    prelude::*,
    shared::erc20::ERC20,
};

mod duplicates;
mod ecmul;
mod few_to_many;
mod high_call_data;
mod many_to_many;
mod non_deterministic_storage;
mod reserve_balance;
mod self_destruct;
mod storage_deletes;
mod system_key_normal;
mod system_spam;
mod uniswap;

pub fn make_generator(
    traffic_gen: &TrafficGen,
    deployed_contract: DeployedContract,
) -> Result<Box<dyn Generator + Send + Sync>> {
    let recipient_keys = KeyPool::new(traffic_gen.recipients, traffic_gen.recipient_seed);
    let tx_per_sender = traffic_gen.tx_per_sender();
    Ok(match &traffic_gen.gen_mode {
        GenMode::NullGen => Box::new(NullGen),
        GenMode::FewToMany(config) => Box::new(CreateAccountsGenerator {
            recipient_keys,
            tx_type: config.tx_type,
            erc20: deployed_contract.erc20().ok(),
            tx_per_sender,
        }),
        GenMode::ManyToMany(config) => Box::new(ManyToManyGenerator {
            recipient_keys,
            tx_type: config.tx_type,
            tx_per_sender,
            erc20: deployed_contract.erc20().ok(),
        }),
        GenMode::Duplicates => Box::new(DuplicateTxGenerator {
            recipient_keys,
            tx_per_sender,
            random_priority_fee: false,
            tx_type: TxType::Native,
            erc20: None,
        }),
        GenMode::RandomPriorityFee(config) => Box::new(DuplicateTxGenerator {
            recipient_keys,
            tx_per_sender,
            random_priority_fee: true,
            tx_type: config.tx_type,
            erc20: deployed_contract.erc20().ok(),
        }),
        GenMode::HighCallData => Box::new(HighCallDataTxGenerator {
            recipient_keys,
            tx_per_sender,
            gas_limit: 800_000,
        }),
        GenMode::HighCallDataLowGasLimit => Box::new(HighCallDataTxGenerator {
            recipient_keys,
            tx_per_sender,
            gas_limit: 200_000, // raised 2x from 100k
        }),
        GenMode::NonDeterministicStorage => Box::new(NonDeterministicStorageTxGenerator {
            recipient_keys,
            tx_per_sender,
            erc20: deployed_contract.erc20()?,
        }),
        GenMode::StorageDeletes => Box::new(StorageDeletesTxGenerator {
            recipient_keys,
            tx_per_sender,
            erc20: deployed_contract.erc20()?,
        }),
        GenMode::SelfDestructs => Box::new(SelfDestructTxGenerator {
            tx_per_sender,
            contracts: Vec::with_capacity(1000),
        }),
        GenMode::ECMul => Box::new(ECMulGenerator {
            ecmul: deployed_contract.ecmul()?,
            tx_per_sender,
        }),
        GenMode::Uniswap => Box::new(UniswapGenerator {
            uniswap: deployed_contract.uniswap()?,
            tx_per_sender,
        }),
        GenMode::ReserveBalance => Box::new(ReserveBalanceGenerator {
            recipient_keys,
            num_drain_txs: 2,
        }),
        GenMode::SystemSpam(config) => Box::new(SystemTransactionSpamGenerator {
            recipient_keys,
            tx_per_sender,
            system_nonce: 0,
            call_type: config.call_type.clone(),
        }),
        GenMode::SystemKeyNormal => Box::new(SystemKeyNormalTxGenerator {
            recipient_keys,
            tx_per_sender,
            system_nonce: 0,
            random_priority_fee: false,
        }),
        GenMode::SystemKeyNormalRandomPriorityFee => Box::new(SystemKeyNormalTxGenerator {
            recipient_keys,
            tx_per_sender,
            system_nonce: 0,
            random_priority_fee: true,
        }),
    })
}

struct NullGen;
impl Generator for NullGen {
    fn handle_acct_group(
        &mut self,
        _accts: &mut [SimpleAccount],
        _ctx: &GenCtx,
    ) -> Vec<(TxEnvelope, Address)> {
        vec![]
    }
}

pub fn native_transfer(
    from: &mut SimpleAccount,
    to: Address,
    amt: U256,
    ctx: &GenCtx,
) -> TxEnvelope {
    native_transfer_priority_fee(from, to, amt, 0, ctx)
}

pub fn native_transfer_priority_fee(
    from: &mut SimpleAccount,
    to: Address,
    amt: U256,
    priority_fee: u128,
    ctx: &GenCtx,
) -> TxEnvelope {
    let max_fee_per_gas = ctx.base_fee * 2;
    let gas_limit = ctx.set_tx_gas_limit.unwrap_or(42_000); // use override or default
    let priority_fee = ctx.priority_fee.unwrap_or(priority_fee as u64) as u128;
    let tx = TxEip1559 {
        chain_id: ctx.chain_id,
        nonce: from.nonce,
        gas_limit,
        max_fee_per_gas,
        max_priority_fee_per_gas: priority_fee,
        to: TxKind::Call(to),
        value: amt,
        access_list: Default::default(),
        input: Default::default(),
    };

    // update from
    from.nonce += 1;
    from.native_bal = from
        .native_bal
        .checked_sub(amt + U256::from(gas_limit * max_fee_per_gas))
        .unwrap_or(U256::ZERO);

    let sig = from.key.sign_transaction(&tx);
    TxEnvelope::Eip1559(tx.into_signed(sig))
}

pub fn erc20_transfer(
    from: &mut SimpleAccount,
    to: Address,
    amt: U256,
    erc20: &ERC20,
    ctx: &GenCtx,
) -> TxEnvelope {
    let max_fee_per_gas = ctx.base_fee;
    let tx = erc20.construct_transfer(
        &from.key,
        to,
        from.nonce,
        amt,
        max_fee_per_gas,
        ctx.chain_id,
        ctx.set_tx_gas_limit,
        ctx.priority_fee,
    );

    // update from
    from.nonce += 1;
    from.native_bal = from
        .native_bal
        .checked_sub(U256::from(
            ctx.set_tx_gas_limit.unwrap_or(200_000) * max_fee_per_gas,
        ))
        .unwrap_or(U256::ZERO); // updated to use gas limit override
    from.erc20_bal = from.erc20_bal.checked_sub(amt).unwrap_or(U256::ZERO);
    tx
}

pub fn erc20_mint(from: &mut SimpleAccount, erc20: &ERC20, ctx: &GenCtx) -> TxEnvelope {
    let max_fee_per_gas = ctx.base_fee;
    let tx = erc20.construct_mint(
        &from.key,
        from.nonce,
        max_fee_per_gas,
        ctx.chain_id,
        ctx.set_tx_gas_limit,
        ctx.priority_fee,
    );

    // update from
    from.nonce += 1;

    from.native_bal = from
        .native_bal
        .checked_sub(U256::from(
            ctx.set_tx_gas_limit.unwrap_or(200_000) * max_fee_per_gas,
        ))
        .unwrap_or(U256::ZERO); // updated to use gas limit override
    from.erc20_bal += U256::from(10_u128.pow(30)); // todo: current erc20 impl just mints a constant
    tx
}
