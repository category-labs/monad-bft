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

use std::sync::atomic::{AtomicUsize, Ordering};

use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope};
use alloy_primitives::{Address, Bytes, TxKind, U256};
use duplicates::DuplicateTxGenerator;
use ecmul::ECMulGenerator;
use eip7702::EIP7702Generator;
use eip7702_create::EIP7702CreateGenerator;
use extreme_values::ExtremeValuesGenerator;
use few_to_many::CreateAccountsGenerator;
use high_call_data::HighCallDataTxGenerator;
use many_to_many::ManyToManyGenerator;
use nft_sale::NftSaleGenerator;
use non_deterministic_storage::NonDeterministicStorageTxGenerator;
use rand::prelude::*;
use reserve_balance::ReserveBalanceGenerator;
use reserve_balance_fail::ReserveBalanceFailGenerator;
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
mod eip7702;
mod eip7702_create;
mod extreme_values;
mod few_to_many;
mod high_call_data;
mod many_to_many;
mod nft_sale;
mod non_deterministic_storage;
mod reserve_balance;
mod reserve_balance_fail;
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

    let gen_tx_type = |tx_type: TxType, contract_count: usize| -> Result<GenTxType> {
        match tx_type {
            TxType::Native => Ok(GenTxType::Native),
            TxType::ERC20 => Ok(GenTxType::ERC20(ERC20Pool::from_deployed_contract(
                deployed_contract.clone(),
                contract_count,
            )?)),
        }
    };

    Ok(match &traffic_gen.gen_mode {
        GenMode::NullGen => Box::new(NullGen),
        GenMode::FewToMany(config) => Box::new(CreateAccountsGenerator {
            recipient_keys,
            tx_type: gen_tx_type(config.tx_type, config.contract_count)?,
            tx_per_sender,
        }),
        GenMode::ManyToMany(config) => Box::new(ManyToManyGenerator {
            recipient_keys,
            tx_per_sender,
            tx_type: gen_tx_type(config.tx_type, config.contract_count)?,
        }),
        GenMode::Duplicates => Box::new(DuplicateTxGenerator {
            recipient_keys,
            tx_per_sender,
            random_priority_fee: false,
            tx_type: GenTxType::Native,
        }),
        GenMode::RandomPriorityFee(config) => Box::new(DuplicateTxGenerator {
            recipient_keys,
            tx_per_sender,
            random_priority_fee: true,
            tx_type: gen_tx_type(config.tx_type, config.contract_count)?,
        }),
        GenMode::HighCallData(config) => Box::new(HighCallDataTxGenerator {
            recipient_keys,
            tx_per_sender,
            erc20_pool: ERC20Pool::from_deployed_contract(
                deployed_contract,
                config.contract_count,
            )?,
        }),
        GenMode::NonDeterministicStorage(config) => Box::new(NonDeterministicStorageTxGenerator {
            recipient_keys,
            tx_per_sender,
            erc20_pool: ERC20Pool::from_deployed_contract(
                deployed_contract,
                config.contract_count,
            )?,
        }),
        GenMode::StorageDeletes(config) => Box::new(StorageDeletesTxGenerator {
            recipient_keys,
            tx_per_sender,
            erc20_pool: ERC20Pool::from_deployed_contract(
                deployed_contract,
                config.contract_count,
            )?,
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
        GenMode::ReserveBalanceFail(config) => Box::new(ReserveBalanceFailGenerator {
            recipient_keys,
            num_fail_txs: config.num_fail_txs,
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
        GenMode::EIP7702Reuse(config) => Box::new(EIP7702Generator::new(
            deployed_contract.eip7702()?,
            tx_per_sender,
            config.total_authorizations,
            config.authorizations_per_tx,
        )),
        GenMode::EIP7702Create(config) => Box::new(EIP7702CreateGenerator::new(
            deployed_contract.eip7702()?,
            tx_per_sender,
            config.authorizations_per_tx,
        )),
        GenMode::ExtremeValues(config) => Box::new(ExtremeValuesGenerator::new(
            recipient_keys,
            tx_per_sender,
            ERC20Pool::from_deployed_contract(deployed_contract, config.contract_count)?,
        )),
        GenMode::NftSale => Box::new(NftSaleGenerator {
            nft_sale: deployed_contract.nft_sale()?,
            tx_per_sender,
        }),
    })
}

pub enum GenTxType {
    Native,
    ERC20(ERC20Pool),
}

pub struct ERC20Pool {
    pool: Vec<ERC20>,
    index: AtomicUsize,
}

impl ERC20Pool {
    pub fn new(erc20s: Vec<ERC20>) -> Result<Self> {
        if erc20s.is_empty() {
            return Err(eyre::eyre!("ERC20 pool is empty"));
        }
        Ok(Self {
            pool: erc20s,
            index: AtomicUsize::new(0),
        })
    }

    pub fn from_deployed_contract(
        deployed_contract: DeployedContract,
        contract_count: usize,
    ) -> Result<Self> {
        let mut erc20s = deployed_contract.erc20()?;
        if erc20s.len() < contract_count {
            return Err(eyre::eyre!(
                "Not enough ERC20 contracts deployed: {} < {}",
                erc20s.len(),
                contract_count
            ));
        }
        erc20s.resize(
            contract_count,
            ERC20 {
                addr: Address::ZERO,
            },
        );
        Self::new(erc20s)
    }
}

impl ERC20Pool {
    pub fn next_contract(&self) -> &ERC20 {
        let index = self.index.load(Ordering::Acquire);
        let erc20 = self.pool.get(index).expect("ERC20 pool is empty");
        self.index
            .store((index + 1) % self.pool.len(), Ordering::Release);
        erc20
    }
}

struct NullGen;
impl Generator for NullGen {
    fn handle_acct_group(
        &mut self,
        _accts: &mut [SimpleAccount],
        _ctx: &GenCtx,
    ) -> Vec<(TxEnvelope, Address, crate::shared::private_key::PrivateKey)> {
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

pub fn native_transfer_with_params(
    from: &mut SimpleAccount,
    to: Address,
    value: U256,
    nonce: Option<u64>,
    gas_limit: Option<u64>,
    max_fee_per_gas: Option<u128>,
    max_priority_fee_per_gas: Option<u128>,
    input_data: Option<Bytes>,
    ctx: &GenCtx,
) -> TxEnvelope {
    let nonce = nonce.unwrap_or(from.nonce);
    let gas_limit = gas_limit.unwrap_or(ctx.set_tx_gas_limit.unwrap_or(21_000));
    let max_fee_per_gas = max_fee_per_gas.unwrap_or(ctx.base_fee * 2);
    let max_priority_fee_per_gas =
        max_priority_fee_per_gas.unwrap_or(ctx.priority_fee.unwrap_or(0));
    let input_data = input_data.unwrap_or_default();

    let tx = TxEip1559 {
        chain_id: ctx.chain_id,
        nonce,
        gas_limit,
        max_fee_per_gas,
        max_priority_fee_per_gas,
        to: TxKind::Call(to),
        value,
        access_list: Default::default(),
        input: input_data,
    };

    // update from
    if nonce == from.nonce {
        from.nonce += 1;
    } else {
        from.nonce = nonce + 1;
    }
    let gas_cost = U256::from(gas_limit as u128 * max_fee_per_gas);
    from.native_bal = from
        .native_bal
        .checked_sub(value + gas_cost)
        .unwrap_or(U256::ZERO);

    let sig = from.key.sign_transaction(&tx);
    TxEnvelope::Eip1559(tx.into_signed(sig))
}

pub fn native_transfer_priority_fee(
    from: &mut SimpleAccount,
    to: Address,
    amt: U256,
    priority_fee: u128,
    ctx: &GenCtx,
) -> TxEnvelope {
    native_transfer_with_params(
        from,
        to,
        amt,
        None,
        None,
        None,
        Some(priority_fee),
        None,
        ctx,
    )
}

pub fn erc20_transfer(
    from: &mut SimpleAccount,
    to: Address,
    amt: U256,
    erc20: &ERC20,
    ctx: &GenCtx,
) -> TxEnvelope {
    let max_fee_per_gas = ctx.base_fee * 2;
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
        .checked_sub(U256::from(400_000 * max_fee_per_gas))
        .unwrap_or(U256::ZERO); // todo: wire gas correctly, see above comment

    let balance = from.erc20_balances.entry(erc20.addr).or_insert(U256::ZERO);
    *balance = balance.checked_sub(amt).unwrap_or(U256::ZERO);
    tx
}

pub fn erc20_mint(from: &mut SimpleAccount, erc20: &ERC20, ctx: &GenCtx) -> TxEnvelope {
    let max_fee_per_gas = ctx.base_fee * 2;
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
        .checked_sub(U256::from(400_000 * max_fee_per_gas))
        .unwrap_or(U256::ZERO); // todo: wire gas correctly, see above comment

    let mint_amount = U256::from(10_u128.pow(30)); // todo: current erc20 impl just mints a constant
    let balance = from.erc20_balances.entry(erc20.addr).or_insert(U256::ZERO);
    *balance += mint_amount;
    tx
}

pub fn mutate_eip1559_transaction(
    tx: &TxEnvelope,
    original_key: &crate::shared::private_key::PrivateKey,
) -> TxEnvelope {
    let mut rng = rand::thread_rng();

    let TxEnvelope::Eip1559(signed_tx) = tx else {
        error!("mutate_eip1559_transaction called with non-EIP1559 transaction");
        return tx.clone();
    };

    let original_tx = &signed_tx.tx();

    let mut new_tx = TxEip1559 {
        chain_id: original_tx.chain_id,
        nonce: original_tx.nonce,
        gas_limit: original_tx.gas_limit,
        max_fee_per_gas: original_tx.max_fee_per_gas,
        max_priority_fee_per_gas: original_tx.max_priority_fee_per_gas,
        to: original_tx.to,
        value: original_tx.value,
        access_list: original_tx.access_list.clone(),
        input: original_tx.input.clone(),
    };

    // 8 fields total: 7 transaction fields + 1 signature field
    const FIELD_MUTATION_PROB: f64 = 1.0 / 8.0;

    if rng.gen_bool(FIELD_MUTATION_PROB) {
        new_tx.nonce = rng.gen_range(0..=u64::MAX);
    }

    if rng.gen_bool(FIELD_MUTATION_PROB) {
        new_tx.gas_limit = rng.gen_range(0..=u64::MAX);
    }

    if rng.gen_bool(FIELD_MUTATION_PROB) {
        new_tx.max_fee_per_gas = rng.gen_range(0..=u128::MAX);
    }

    if rng.gen_bool(FIELD_MUTATION_PROB) {
        new_tx.max_priority_fee_per_gas = rng.gen_range(0..=u128::MAX);
    }

    if rng.gen_bool(FIELD_MUTATION_PROB) {
        new_tx.to = TxKind::Call(Address::from(rng.gen::<[u8; 20]>()));
    }

    if rng.gen_bool(FIELD_MUTATION_PROB) {
        new_tx.value = U256::from(rng.gen::<u128>());
    }

    if rng.gen_bool(FIELD_MUTATION_PROB) {
        let input_len = rng.gen_range(0..=1000);
        new_tx.input = Bytes::from((0..input_len).map(|_| rng.gen::<u8>()).collect::<Vec<_>>());
    }

    // Mutate signature (sign with wrong key) if selected
    if rng.gen_bool(FIELD_MUTATION_PROB) {
        // Mutate signature by signing with a random key (invalid signature)
        let (_random_addr, random_key) =
            crate::shared::private_key::PrivateKey::new_with_random(&mut rng);
        let sig = random_key.sign_transaction(&new_tx);
        TxEnvelope::Eip1559(new_tx.into_signed(sig))
    } else {
        // Sign with original key (valid signature, but mutated fields)
        let sig = original_key.sign_transaction(&new_tx);
        TxEnvelope::Eip1559(new_tx.into_signed(sig))
    }
}
