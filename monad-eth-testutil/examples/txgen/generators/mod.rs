use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope};
use alloy_primitives::TxKind;
use duplicates::DuplicateTxGenerator;
use ecmul::ECMulGenerator;
use few_to_many::CreateAccountsGenerator;
use high_call_data::HighCallDataTxGenerator;
use many_to_many::ManyToManyGenerator;
use non_deterministic_storage::NonDeterministicStorageTxGenerator;
use self_destruct::SelfDestructTxGenerator;
use storage_deletes::StorageDeletesTxGenerator;
use uniswap::UniswapGenerator;

use crate::{prelude::*, shared::erc20::ERC20, DeployedContract, GeneratorConfig};

mod duplicates;
mod ecmul;
mod few_to_many;
mod high_call_data;
mod many_to_many;
mod non_deterministic_storage;
mod self_destruct;
mod storage_deletes;
mod uniswap;

pub fn make_generator(
    config: &Config,
    deployed_contract: DeployedContract,
) -> Result<Box<dyn Generator + Send + Sync>> {
    let recipient_keys = SeededKeyPool::new(config.recipients, config.recipient_seed);
    let tx_per_sender = config.tx_per_sender();
    Ok(match config.generator_config {
        GeneratorConfig::NullGen => Box::new(NullGen),
        GeneratorConfig::FewToMany { tx_type } => Box::new(CreateAccountsGenerator {
            recipient_keys: SeededKeyPool::new(config.recipients, config.recipient_seed),
            tx_type,
            erc20: deployed_contract.erc20()?,
            tx_per_sender,
        }),
        GeneratorConfig::ManyToMany { tx_type } => Box::new(ManyToManyGenerator {
            recipient_keys,
            tx_type,
            tx_per_sender,
            erc20: deployed_contract.erc20()?,
        }),
        GeneratorConfig::Duplicates => Box::new(DuplicateTxGenerator {
            recipient_keys,
            tx_per_sender,
            random_priority_fee: false,
        }),
        GeneratorConfig::RandomPriorityFee => Box::new(DuplicateTxGenerator {
            recipient_keys,
            tx_per_sender,
            random_priority_fee: true,
        }),
        GeneratorConfig::HighCallData => Box::new(HighCallDataTxGenerator {
            recipient_keys,
            tx_per_sender,
        }),
        GeneratorConfig::NonDeterministicStorage => Box::new(NonDeterministicStorageTxGenerator {
            recipient_keys,
            tx_per_sender,
            erc20: deployed_contract.erc20()?,
        }),
        GeneratorConfig::StorageDeletes => Box::new(StorageDeletesTxGenerator {
            recipient_keys,
            tx_per_sender,
            erc20: deployed_contract.erc20()?,
        }),
        GeneratorConfig::SelfDestructs => Box::new(SelfDestructTxGenerator {
            tx_per_sender,
            contracts: Vec::with_capacity(1000),
        }),
        GeneratorConfig::ECMul => Box::new(ECMulGenerator {
            ecmul: deployed_contract.ecmul()?,
            tx_per_sender,
        }),
        GeneratorConfig::Uniswap => Box::new(UniswapGenerator {
            uniswap: deployed_contract.uniswap()?,
            tx_per_sender,
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
    let tx = TxEip1559 {
        chain_id: 41454,
        nonce: from.nonce,
        gas_limit: 21_000,
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
        .checked_sub(amt + U256::from(21_000 * max_fee_per_gas))
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
    let tx = erc20.construct_transfer(&from.key, to, from.nonce, amt, max_fee_per_gas);

    // update from
    from.nonce += 1;
    from.native_bal = from
        .native_bal
        .checked_sub(U256::from(400_000 * max_fee_per_gas))
        .unwrap_or(U256::ZERO); // todo: wire gas correctly, see above comment
    from.erc20_bal = from.erc20_bal.checked_sub(amt).unwrap_or(U256::ZERO);
    tx
}

pub fn erc20_mint(from: &mut SimpleAccount, erc20: &ERC20, ctx: &GenCtx) -> TxEnvelope {
    let max_fee_per_gas = ctx.base_fee;
    let tx = erc20.construct_mint(&from.key, from.nonce, max_fee_per_gas);

    // update from
    from.nonce += 1;

    from.native_bal = from
        .native_bal
        .checked_sub(U256::from(400_000 * max_fee_per_gas))
        .unwrap_or(U256::ZERO); // todo: wire gas correctly, see above comment
    from.erc20_bal += U256::from(10_u128.pow(30)); // todo: current erc20 impl just mints a constant
    tx
}
