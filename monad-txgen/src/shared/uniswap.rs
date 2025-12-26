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
use alloy_rpc_types::{TransactionReceipt, TransactionRequest};
use alloy_sol_macro::sol;
use alloy_sol_types::{SolCall, SolConstructor, SolEvent};
use eyre::Result;
use serde::{Deserialize, Serialize};

use crate::shared::{
    ensure_contract_deployed, erc20::ERC20, eth_json_rpc::EthJsonRpc, private_key::PrivateKey,
    weth::WETH, SimpleAccount,
};

const FACTORY_BYTECODE: &str = include_str!("uniswap_factory_bytecode.txt");
const NON_FUNGIBLE_POSITION_MANAGER_BYTECODE: &str =
    include_str!("uniswap_non_fungible_position_manager_bytecode.txt");
const INITIAL_PRICE: f64 = 300.0;

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct Uniswap {
    pub factory_addr: Address,
    pub nonfungible_position_manager_addr: Address,
    pub weth_addr: Address,
    pub token_a_addr: Address,
    pub token_b_addr: Address,
    pub pool_addr: Address,
}

impl Uniswap {
    pub async fn deploy(
        deployer: &(Address, PrivateKey),
        client: &ReqwestClient,
        max_fee_per_gas: u128,
        chain_id: u64,
        _gas_limit: Option<u64>,
    ) -> Result<Self> {
        let mut nonce = client.get_transaction_count(&deployer.0).await?;

        // deploy a weth(wrapped native token) contract
        let weth_addr =
            (WETH::deploy_with_nonce(nonce, deployer, client, max_fee_per_gas, chain_id).await?)
                .addr;
        nonce += 1;
        tracing::info!("weth_addr: {}", weth_addr);
        // deploy uniswap factory
        let factory_addr =
            Self::deploy_factory_tx(client, nonce, deployer, max_fee_per_gas, chain_id).await?;
        nonce += 1;
        tracing::info!("factory_addr: {}", factory_addr);
        // deploy uniswap non-fungible position manager
        let manager_addr = Self::deploy_manager_tx(
            client,
            nonce,
            deployer,
            factory_addr,
            weth_addr,
            max_fee_per_gas,
            chain_id,
        )
        .await?;
        nonce += 1;
        tracing::info!("manager_addr: {}", manager_addr);

        // deploy two ERC20 tokens for uniswap pool
        let token_a_addr =
            Self::deploy_token(client, nonce, deployer, max_fee_per_gas, chain_id).await?;
        nonce += 1;
        tracing::info!("token_a_addr: {}", token_a_addr);
        let token_b_addr: Address =
            Self::deploy_token(client, nonce, deployer, max_fee_per_gas, chain_id).await?;
        nonce += 1;
        tracing::info!("token_b_addr: {}", token_b_addr);

        // create pool
        let pool_addr = Self::create_pool(
            client,
            nonce,
            deployer,
            factory_addr,
            token_a_addr,
            token_b_addr,
            max_fee_per_gas,
            chain_id,
        )
        .await?;
        nonce += 1;
        tracing::info!("pool_addr: {}", pool_addr);

        // initialize pool
        Self::initialize_pool(
            client,
            nonce,
            &deployer.1,
            pool_addr,
            max_fee_per_gas,
            chain_id,
        )
        .await?;

        Ok(Self {
            factory_addr,
            nonfungible_position_manager_addr: manager_addr,
            weth_addr,
            token_a_addr,
            token_b_addr,
            pool_addr,
        })
    }

    pub async fn deploy_factory_tx(
        client: &ReqwestClient,
        nonce: u64,
        deployer: &(Address, PrivateKey),
        max_fee_per_gas: u128,
        chain_id: u64,
    ) -> Result<Address> {
        let input = Bytes::from_hex(FACTORY_BYTECODE).unwrap();
        let tx = TxEip1559 {
            chain_id,
            nonce,
            gas_limit: 30_000_000,
            max_fee_per_gas,
            max_priority_fee_per_gas: 10,
            to: TxKind::Create,
            value: U256::ZERO,
            access_list: Default::default(),
            input,
        };

        let sig = deployer.1.sign_transaction(&tx);
        let tx = TxEnvelope::Eip1559(tx.into_signed(sig));

        let mut rlp_encoded_tx = Vec::new();
        tx.encode_2718(&mut rlp_encoded_tx);
        let _: String = client
            .request(
                "eth_sendRawTransaction",
                [format!("0x{}", hex::encode(rlp_encoded_tx))],
            )
            .await?;
        let factory_addr = calculate_contract_addr(&deployer.0, nonce);
        ensure_contract_deployed(client, factory_addr, tx.tx_hash()).await?;

        Ok(factory_addr)
    }

    pub async fn deploy_manager_tx(
        client: &ReqwestClient,
        nonce: u64,
        deployer: &(Address, PrivateKey),
        factory_address: Address,
        weth9_address: Address,
        max_fee_per_gas: u128,
        chain_id: u64,
    ) -> Result<Address> {
        let bytecode = Bytes::from_hex(NON_FUNGIBLE_POSITION_MANAGER_BYTECODE).unwrap();

        let constructor = NonfungiblePositionManager::constructorCall {
            factory: factory_address,
            weth9: weth9_address,
            tokenDescriptor: Address::ZERO,
        };
        let constructor_args = constructor.abi_encode();
        let mut input = bytecode.to_vec();
        input.extend_from_slice(&constructor_args);

        let tx = TxEip1559 {
            chain_id,
            nonce,
            gas_limit: 30_000_000,
            max_fee_per_gas,
            max_priority_fee_per_gas: 10,
            to: TxKind::Create,
            value: U256::ZERO,
            access_list: Default::default(),
            input: Bytes::from(input),
        };

        let sig = deployer.1.sign_transaction(&tx);
        let tx = TxEnvelope::Eip1559(tx.into_signed(sig));
        let mut rlp_encoded_tx = Vec::new();
        tx.encode_2718(&mut rlp_encoded_tx);
        let _: String = client
            .request(
                "eth_sendRawTransaction",
                [format!("0x{}", hex::encode(rlp_encoded_tx))],
            )
            .await?;
        let manager_addr = calculate_contract_addr(&deployer.0, nonce);

        ensure_contract_deployed(client, manager_addr, tx.tx_hash()).await?;

        Ok(manager_addr)
    }

    async fn deploy_token(
        client: &ReqwestClient,
        nonce: u64,
        deployer: &(Address, PrivateKey),
        max_fee_per_gas: u128,
        chain_id: u64,
    ) -> Result<Address> {
        let tx: TxEnvelope = ERC20::deploy_tx(nonce, &deployer.1, max_fee_per_gas, chain_id);
        let mut rlp_encoded_tx = Vec::new();
        tx.encode_2718(&mut rlp_encoded_tx);
        let _: String = client
            .request(
                "eth_sendRawTransaction",
                [format!("0x{}", hex::encode(rlp_encoded_tx))],
            )
            .await?;
        let token_a_addr = calculate_contract_addr(&deployer.0, nonce);

        ensure_contract_deployed(client, token_a_addr, tx.tx_hash()).await?;

        Ok(token_a_addr)
    }

    // Create a Uniswap pool using the factory
    pub async fn create_pool(
        client: &ReqwestClient,
        nonce: u64,
        deployer: &(Address, PrivateKey),
        factory: Address,
        token_a: Address,
        token_b: Address,
        max_fee_per_gas: u128,
        chain_id: u64,
    ) -> Result<Address> {
        let input = UniswapV3Factory::createPoolCall {
            tokenA: token_a,
            tokenB: token_b,
            fee: U24::from(500),
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

        let sig = deployer.1.sign_transaction(&tx);
        let tx = TxEnvelope::Eip1559(tx.into_signed(sig));

        let mut rlp_encoded_tx = Vec::new();
        tx.encode_2718(&mut rlp_encoded_tx);
        let tx_hash: Bytes = client
            .request(
                "eth_sendRawTransaction",
                [format!("0x{}", hex::encode(rlp_encoded_tx))],
            )
            .await?;

        tokio::time::sleep(std::time::Duration::from_secs(10)).await;

        let receipt: TransactionReceipt = client
            .request("eth_getTransactionReceipt", [tx_hash])
            .await?;

        let pool_created_log = receipt
            .inner
            .logs()
            .iter()
            .find(|log| {
                log.topics().first() == Some(&UniswapV3Factory::PoolCreated::SIGNATURE_HASH)
            })
            .ok_or_else(|| eyre::eyre!("No pool created log found in receipt"))?;

        let event = UniswapV3Factory::PoolCreated::decode_log(&pool_created_log.inner, true)?;
        let pool_address = event.pool;

        Ok(pool_address)
    }

    pub async fn initialize_pool(
        client: &ReqwestClient,
        nonce: u64,
        deployer: &PrivateKey,
        pool: Address,
        max_fee_per_gas: u128,
        chain_id: u64,
    ) -> Result<()> {
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
        let tx = TxEnvelope::Eip1559(tx.into_signed(sig));

        let mut rlp_encoded_tx = Vec::new();
        tx.encode_2718(&mut rlp_encoded_tx);
        let tx_hash: Bytes = client
            .request(
                "eth_sendRawTransaction",
                [format!("0x{}", hex::encode(rlp_encoded_tx))],
            )
            .await?;

        tokio::time::sleep(std::time::Duration::from_secs(10)).await;

        let receipt: TransactionReceipt = client
            .request("eth_getTransactionReceipt", [tx_hash])
            .await?;

        if !receipt.inner.status() {
            return Err(eyre::eyre!("Pool initialization transaction reverted"));
        }

        receipt
            .inner
            .logs()
            .iter()
            .find(|log| log.topics().first() == Some(&UniswapV3Pool::Initialize::SIGNATURE_HASH))
            .inspect(|_| tracing::info!("Pool initialized successfully!"))
            .ok_or_else(|| eyre::eyre!("No initialize event found in receipt"))?;

        Ok(())
    }

    pub fn construct_token_mint_tx(
        &self,
        sender: &mut SimpleAccount,
        token_addr: Address,
        max_fee_per_gas: u128,
        chain_id: u64,
        gas_limit: Option<u64>,
    ) -> TxEnvelope {
        ERC20::construct_mint(
            &ERC20 { addr: token_addr },
            &sender.key,
            sender.nonce,
            max_fee_per_gas,
            chain_id,
            gas_limit,
            Option::Some(10),
        )
    }

    pub fn construct_token_approve_tx(
        &self,
        sender: &mut SimpleAccount,
        token_addr: Address,
        spender: Address,
        max_fee_per_gas: u128,
        chain_id: u64,
        gas_limit: Option<u64>,
    ) -> TxEnvelope {
        ERC20::construct_approve(
            &ERC20 { addr: token_addr },
            &sender.key,
            spender,
            sender.nonce,
            U256::MAX,
            max_fee_per_gas,
            chain_id,
            gas_limit,
            Option::Some(10),
        )
    }

    pub fn construct_add_liquidity_tx(
        &self,
        sender: &mut SimpleAccount,
        non_fungible_position_manager_addr: Address,
        token_0_addr: Address,
        token_1_addr: Address,
        max_fee_per_gas: u128,
        chain_id: u64,
        gas_limit: Option<u64>,
    ) -> TxEnvelope {
        // price point of 300.0 has a tick value of 57000
        // with tick spacing of 60, lower tick is 10 ticks below current tick
        // upper tick is 10 ticks above current tick
        let input = NonfungiblePositionManager::mintCall {
            params: NonfungiblePositionManager::MintParams {
                token0: token_0_addr,
                token1: token_1_addr,
                fee: U24::from(500),
                tickLower: I24::from_raw(U24::from(56400)),
                tickUpper: I24::from_raw(U24::from(57600)),
                amount0Desired: U256::from(100_000_000_000_000_000_000u128),
                amount1Desired: U256::from(100_000_000_000_000_000_000u128),
                amount0Min: U256::from(100_000_000_000_000_000_000u128),
                amount1Min: U256::from(100_000_000_000_000_000_000u128),
                recipient: sender.addr,
                deadline: U256::from(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs()
                        + 3600,
                ),
            },
        }
        .abi_encode();
        let tx = TxEip1559 {
            chain_id,
            nonce: sender.nonce,
            gas_limit: gas_limit.unwrap_or(500_000),
            max_fee_per_gas,
            max_priority_fee_per_gas: 10,
            to: TxKind::Call(non_fungible_position_manager_addr),
            value: U256::ZERO,
            access_list: Default::default(),
            input: input.into(),
        };

        let sig = sender.key.sign_transaction(&tx);
        TxEnvelope::Eip1559(tx.into_signed(sig))
    }

    pub async fn get_pool_address(
        client: &ReqwestClient,
        factory: Address,
        token_a: Address,
        token_b: Address,
    ) -> Result<Address> {
        // Verify pool creation by calling getPool()
        let get_pool_input = UniswapV3Factory::getPoolCall {
            tokenA: token_a,
            tokenB: token_b,
            fee: U24::from(500),
        }
        .abi_encode();

        let call_request = TransactionRequest::default()
            .to(factory)
            .input(get_pool_input.into());

        let pool_bytes: Bytes = client.request("eth_call", (call_request, "latest")).await?;

        let pool_return = UniswapV3Factory::getPoolCall::abi_decode_returns(&pool_bytes, true)
            .map_err(|e| eyre::eyre!("Failed to decode getPool() return: {}", e))?;

        Ok(pool_return._0)
    }

    pub async fn get_owner(client: &ReqwestClient, factory: Address) -> Result<Address> {
        let get_owner_input = UniswapV3Factory::ownerCall {}.abi_encode();
        let call_request = TransactionRequest::default()
            .to(factory)
            .input(get_owner_input.into());

        let owner_bytes: Bytes = client.request("eth_call", (call_request, "latest")).await?;

        let owner_return = UniswapV3Factory::ownerCall::abi_decode_returns(&owner_bytes, true)
            .map_err(|e| eyre::eyre!("Failed to decode owner() return: {}", e))?;

        Ok(owner_return._0)
    }
}

// Contract interface
sol! {
    contract UniswapV3Factory {
        event PoolCreated(
            address indexed token0,
            address indexed token1,
            uint24 indexed fee,
            int24 tickSpacing,
            address pool
        );

        function createPool(address tokenA, address tokenB, uint24 fee) external;
        function getPool(address tokenA, address tokenB, uint24 fee) external view returns (address);
        function owner() external view returns (address);
    }
}

sol! {
    contract UniswapV3Pool {
        event Initialize(uint160 sqrtPriceX96, int24 tick);

        function initialize(uint160 sqrtPriceX96) external;
    }
}

sol! {
    contract NonfungiblePositionManager {
        struct MintParams {
            address token0;
            address token1;
            uint24 fee;
            int24 tickLower;
            int24 tickUpper;
            uint256 amount0Desired;
            uint256 amount1Desired;
            uint256 amount0Min;
            uint256 amount1Min;
            address recipient;
            uint256 deadline;
        }

        constructor(
            address factory,
            address weth9,
            address tokenDescriptor
        );
        function mint(MintParams calldata params)
        external
        payable
        returns (
            uint256 tokenId,
            uint128 liquidity,
            uint256 amount0,
            uint256 amount1
        );

        function positions(uint256 tokenId)
        external
        view
        returns (
            uint96 nonce,
            address operator,
            address token0,
            address token1,
            uint24 fee,
            int24 tickLower,
            int24 tickUpper,
            uint128 liquidity,
            uint256 feeGrowthInside0LastX128,
            uint256 feeGrowthInside1LastX128,
            uint128 tokensOwed0,
            uint128 tokensOwed1
        );
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
