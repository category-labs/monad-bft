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
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{
    aliases::{I24, U24},
    hex::{self, FromHex},
    keccak256, Address, Bytes, TxKind, U160, U256,
};
use alloy_rlp::Encodable;
use alloy_rpc_client::ReqwestClient;
use alloy_rpc_types::TransactionReceipt;
use alloy_sol_macro::sol;
use alloy_sol_types::SolCall;
use eyre::Result;
use serde::Deserialize;

use super::{eth_json_rpc::EthJsonRpc, private_key::PrivateKey};
use crate::{
    shared::erc20::{ensure_contract_deployed, ERC20},
    SimpleAccount,
};

const FACTORY_BYTECODE: &str = include_str!("uniswap_factory_bytecode.txt");
const MANAGER_BYTECODE: &str = include_str!("uniswap_manager_bytecode.txt");
const INITIAL_PRICE: f64 = 300.0;

#[derive(Deserialize, Debug, Clone, Copy)]
#[serde(transparent)]
pub struct Uniswap {
    pub addr: Address,
}

impl Uniswap {
    pub async fn deploy(
        deployer: &(Address, PrivateKey),
        client: &ReqwestClient,
        max_fee_per_gas: u128,
        chain_id: u64,
        _gas_limit: Option<u64>,
    ) -> Result<Self> {
        let nonce = client.get_transaction_count(&deployer.0).await?;

        // deploy factory
        let tx = Self::deploy_factory_tx(nonce, &deployer.1, max_fee_per_gas, chain_id);
        let mut rlp_encoded_tx = Vec::new();
        tx.encode_2718(&mut rlp_encoded_tx);
        let _: String = client
            .request(
                "eth_sendRawTransaction",
                [format!("0x{}", hex::encode(rlp_encoded_tx))],
            )
            .await?;
        let factory_addr = calculate_contract_addr(&deployer.0, nonce);

        // deploy two ERC20 tokens for uniswap pool
        let tx = ERC20::deploy_tx(nonce + 1, &deployer.1, max_fee_per_gas, chain_id);
        let mut rlp_encoded_tx = Vec::new();
        tx.encode_2718(&mut rlp_encoded_tx);
        let _: String = client
            .request(
                "eth_sendRawTransaction",
                [format!("0x{}", hex::encode(rlp_encoded_tx))],
            )
            .await?;
        let token_a_addr = calculate_contract_addr(&deployer.0, nonce + 1);

        let tx = ERC20::deploy_tx(nonce + 2, &deployer.1, max_fee_per_gas, chain_id);
        let mut rlp_encoded_tx = Vec::new();
        tx.encode_2718(&mut rlp_encoded_tx);
        let _: String = client
            .request(
                "eth_sendRawTransaction",
                [format!("0x{}", hex::encode(rlp_encoded_tx))],
            )
            .await?;
        let token_b_addr = calculate_contract_addr(&deployer.0, nonce + 2);

        // create pool
        let tx = Self::create_pool(
            nonce + 3,
            &deployer.1,
            factory_addr,
            token_a_addr,
            token_b_addr,
            max_fee_per_gas,
            chain_id,
        );
        let mut rlp_encoded_tx = Vec::new();
        tx.encode_2718(&mut rlp_encoded_tx);
        let tx_hash: Bytes = client
            .request(
                "eth_sendRawTransaction",
                [format!("0x{}", hex::encode(rlp_encoded_tx))],
            )
            .await?;
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        let receipt: TransactionReceipt = client
            .request("eth_getTransactionReceipt", [tx_hash])
            .await?;
        let receipt_log = receipt
            .inner
            .logs()
            .first()
            .ok_or_else(|| eyre::eyre!("No logs found in receipt"))?;
        // pool address is emitted as the last element in the event
        let log_data = &receipt_log.data().data;
        let pool_addr = Address::from_slice(&log_data[log_data.len() - 20..]);

        // initialize pool
        let tx =
            Self::initialize_pool(nonce + 4, &deployer.1, pool_addr, max_fee_per_gas, chain_id);
        let mut rlp_encoded_tx = Vec::new();
        tx.encode_2718(&mut rlp_encoded_tx);
        let _: String = client
            .request(
                "eth_sendRawTransaction",
                [format!("0x{}", hex::encode(rlp_encoded_tx))],
            )
            .await?;

        // deploy manager
        let tx: TxEnvelope = Self::deploy_manager_tx(
            nonce + 5,
            &deployer.1,
            pool_addr,
            token_a_addr,
            token_b_addr,
            max_fee_per_gas,
            chain_id,
        );
        let mut rlp_encoded_tx = Vec::new();
        tx.encode_2718(&mut rlp_encoded_tx);
        let _: String = client
            .request(
                "eth_sendRawTransaction",
                [format!("0x{}", hex::encode(rlp_encoded_tx))],
            )
            .await?;
        let manager_addr = calculate_contract_addr(&deployer.0, nonce + 5);

        ensure_contract_deployed(client, manager_addr).await?;

        Ok(Self { addr: manager_addr })
    }

    // Deploy UniswapV3Factory
    pub fn deploy_factory_tx(
        nonce: u64,
        deployer: &PrivateKey,
        max_fee_per_gas: u128,
        chain_id: u64,
    ) -> TxEnvelope {
        let input = Bytes::from_hex(FACTORY_BYTECODE).unwrap();
        let tx = TxEip1559 {
            chain_id,
            nonce,
            gas_limit: 20_000_000,
            max_fee_per_gas,
            max_priority_fee_per_gas: 10,
            to: TxKind::Create,
            value: U256::ZERO,
            access_list: Default::default(),
            input,
        };

        let sig = deployer.sign_transaction(&tx);
        TxEnvelope::Eip1559(tx.into_signed(sig))
    }

    // Create a Uniswap pool using the factory
    pub fn create_pool(
        nonce: u64,
        deployer: &PrivateKey,
        factory: Address,
        token_a: Address,
        token_b: Address,
        max_fee_per_gas: u128,
        chain_id: u64,
    ) -> TxEnvelope {
        let input = UniswapV3Factory::createPoolCall {
            tokenA: token_a,
            tokenB: token_b,
            fee: U24::from(500), // uniswap can only take fee of 500 or 3000
        }
        .abi_encode();

        let tx = TxEip1559 {
            chain_id,
            nonce,
            gas_limit: 20_000_000,
            max_fee_per_gas,
            max_priority_fee_per_gas: 10,
            to: TxKind::Call(factory),
            value: U256::ZERO,
            access_list: Default::default(),
            input: input.into(),
        };

        let sig = deployer.sign_transaction(&tx);
        TxEnvelope::Eip1559(tx.into_signed(sig))
    }

    // Helper function to initialize the price of the pool
    pub fn initialize_pool(
        nonce: u64,
        deployer: &PrivateKey,
        pool: Address,
        max_fee_per_gas: u128,
        chain_id: u64,
    ) -> TxEnvelope {
        let input = UniswapV3Pool::initializeCall {
            sqrtPriceX96: calculate_sqrt_price_x96(INITIAL_PRICE),
        }
        .abi_encode();

        let tx = TxEip1559 {
            chain_id,
            nonce,
            gas_limit: 500_000,
            max_fee_per_gas,
            max_priority_fee_per_gas: 10,
            to: TxKind::Call(pool),
            value: U256::ZERO,
            access_list: Default::default(),
            input: input.into(),
        };

        let sig = deployer.sign_transaction(&tx);
        TxEnvelope::Eip1559(tx.into_signed(sig))
    }

    // UniswapV3 requires a contract to interact with the pool, the manager contract is used to do so
    pub fn deploy_manager_tx(
        nonce: u64,
        deployer: &PrivateKey,
        pool: Address,
        token_a: Address,
        token_b: Address,
        max_fee_per_gas: u128,
        chain_id: u64,
    ) -> TxEnvelope {
        let input = Bytes::from_hex(MANAGER_BYTECODE).unwrap();
        let mut extended_input = input.to_vec();
        extended_input.extend(token_a.into_word().to_vec());
        extended_input.extend(token_b.into_word().to_vec());
        extended_input.extend(pool.into_word().to_vec());

        let tx = TxEip1559 {
            chain_id,
            nonce,
            gas_limit: 20_000_000,
            max_fee_per_gas,
            max_priority_fee_per_gas: 10,
            to: TxKind::Create,
            value: U256::ZERO,
            access_list: Default::default(),
            input: extended_input.into(),
        };

        let sig = deployer.sign_transaction(&tx);
        TxEnvelope::Eip1559(tx.into_signed(sig))
    }

    // Helper function to construct a Uniswap transaction
    pub fn construct_tx(
        &self,
        sender: &mut SimpleAccount,
        max_fee_per_gas: u128,
        chain_id: u64,
        gas_limit: Option<u64>,
        priority_fee: Option<u64>,
    ) -> TxEnvelope {
        // price point of 300.0 has a tick value of 57000
        // with tick spacing of 60, lower tick is 10 ticks below current tick
        // upper tick is 10 ticks above current tick
        let input = UniswapV3Manager::mintCall {
            tickLower: I24::from_raw(U24::from(56400)),
            tickUpper: I24::from_raw(U24::from(57600)),
            amount: 100_000_000_000_000_000_000,
            data: Bytes::default(),
        }
        .abi_encode();
        let tx = TxEip1559 {
            chain_id,
            nonce: sender.nonce,
            gas_limit: gas_limit.unwrap_or(400_000), // 400k default, override with --set-tx-gas-limit
            max_fee_per_gas,
            max_priority_fee_per_gas: priority_fee.unwrap_or(0) as u128, // 0 default, override with --priority-fee
            to: TxKind::Call(self.addr),
            value: U256::ZERO,
            access_list: Default::default(),
            input: input.into(),
        };

        let sig = sender.key.sign_transaction(&tx);
        sender.nonce += 1;
        sender.native_bal = sender
            .native_bal
            .checked_sub(U256::from(400_000 * max_fee_per_gas))
            .unwrap_or(U256::ZERO);

        TxEnvelope::Eip1559(tx.into_signed(sig))
    }
}

// Contract interface
sol! {
    contract UniswapV3Factory {
        function createPool(address tokenA, address tokenB, uint24 fee) external;
    }
}

sol! {
    contract UniswapV3Pool {
        function initialize(uint160 sqrtPriceX96) external;
    }
}

sol! {
    contract UniswapV3Manager {
        function swap(
            bool zeroForOne,
            uint256 amountSpecified,
            uint160 sqrtPriceLimitX96,
            bytes calldata data
        ) external;
        function mint(
            int24 tickLower,
            int24 tickUpper,
            uint128 amount,
            bytes calldata data
        ) external;
    }
}

// Helper function for contract deployment
fn calculate_contract_addr(deployer: &Address, nonce: u64) -> Address {
    let mut out = Vec::new();
    let enc: [&dyn Encodable; 2] = [&deployer, &nonce];
    alloy_rlp::encode_list::<_, dyn Encodable>(&enc, &mut out);
    let hash = keccak256(out);
    let (_, contract_address) = hash.as_slice().split_at(12);
    Address::from_slice(contract_address)
}

fn calculate_sqrt_price_x96(price: f64) -> U160 {
    U160::from((price.sqrt() * (2f64.powf(96f64))) as u128)
}
