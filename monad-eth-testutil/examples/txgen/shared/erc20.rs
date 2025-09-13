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

use std::time::Duration;

use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope};
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{
    hex::{self, FromHex},
    keccak256, Address, Bytes, TxKind, U256,
};
use alloy_rlp::Encodable;
use alloy_rpc_client::ReqwestClient;
use alloy_sol_macro::sol;
use alloy_sol_types::SolCall;
use eyre::Result;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::time::sleep;
use tracing::info;

use crate::{
    shared::{eth_json_rpc::EthJsonRpc, private_key::PrivateKey},
    SimpleAccount,
};

const BYTECODE: &str = include_str!("erc20_bytecode.txt");

#[derive(Deserialize, Debug, Clone, Copy)]
#[serde(transparent)]
pub struct ERC20 {
    pub addr: Address,
}

pub async fn ensure_contract_deployed(client: &ReqwestClient, addr: Address) -> Result<()> {
    let mut timeout = Duration::from_millis(200);
    for _ in 0..10 {
        info!(
            "Waiting {}ms for contract to be deployed...",
            timeout.as_millis()
        );
        sleep(timeout).await;

        let code = client.get_code(&addr).await?;
        if code != "0x" {
            info!(addr = addr.to_string(), "Deployed contract");
            return Ok(());
        }

        // else exponential backoff
        timeout *= 2;
    }

    Err(eyre::eyre!(
        "Failed to deployed contract {}",
        addr.to_string()
    ))
}

impl ERC20 {
    pub async fn deploy(
        deployer: &(Address, PrivateKey),
        client: &ReqwestClient,
        max_fee_per_gas: u128,
        chain_id: u64,
    ) -> Result<Self> {
        let nonce = client.get_transaction_count(&deployer.0).await?;
        let tx = Self::deploy_tx(nonce, &deployer.1, max_fee_per_gas, chain_id);
        let mut rlp_encoded_tx = Vec::new();
        tx.encode_2718(&mut rlp_encoded_tx);

        // make compiler happy, actually parse string : (
        let _: String = client
            .request(
                "eth_sendRawTransaction",
                [format!("0x{}", hex::encode(rlp_encoded_tx))],
            )
            .await?;

        let addr = calculate_contract_addr(&deployer.0, nonce);
        ensure_contract_deployed(client, addr).await?;
        Ok(ERC20 { addr })
    }

    pub fn deploy_tx_with_gas_limit(
        nonce: u64,
        deployer: &PrivateKey,
        max_fee_per_gas: u128,
        chain_id: u64,
        gas_limit: u64,
    ) -> TxEnvelope {
        let input = Bytes::from_hex(BYTECODE).unwrap();
        let tx = TxEip1559 {
            chain_id,
            nonce,
            gas_limit, // usually around 600k gas
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

    pub fn deploy_tx(
        nonce: u64,
        deployer: &PrivateKey,
        max_fee_per_gas: u128,
        chain_id: u64,
    ) -> TxEnvelope {
        let input = Bytes::from_hex(BYTECODE).unwrap();
        let tx = TxEip1559 {
            chain_id,
            nonce,
            gas_limit: 20_000_000, // usually around 600k gas
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

    pub fn self_destruct_tx(
        &self,
        sender: &mut SimpleAccount,
        max_fee_per_gas: u128,
        chain_id: u64,
    ) -> TxEnvelope {
        self.construct_tx(
            sender,
            IERC20::destroySmartContractCall {},
            max_fee_per_gas,
            chain_id,
        )
    }

    pub fn construct_tx<T: alloy_sol_types::SolCall>(
        &self,
        sender: &mut SimpleAccount,
        input: T,
        max_fee_per_gas: u128,
        chain_id: u64,
    ) -> TxEnvelope {
        let input = input.abi_encode();
        let tx = make_tx(
            sender.nonce,
            &sender.key,
            self.addr,
            U256::ZERO,
            input,
            max_fee_per_gas,
            chain_id,
        );
        sender.nonce += 1;
        tx
    }

    pub fn construct_mint(
        &self,
        from: &PrivateKey,
        nonce: u64,
        max_fee_per_gas: u128,
        chain_id: u64,
    ) -> TxEnvelope {
        let input = IERC20::mintCall {}.abi_encode();
        make_tx(
            nonce,
            from,
            self.addr,
            U256::ZERO,
            input,
            max_fee_per_gas,
            chain_id,
        )
    }

    pub fn construct_transfer(
        &self,
        from: &PrivateKey,
        recipient: Address,
        nonce: u64,
        amount: U256,
        max_fee_per_gas: u128,
        chain_id: u64,
    ) -> TxEnvelope {
        let input = IERC20::transferCall { recipient, amount }.abi_encode();
        make_tx(
            nonce,
            from,
            self.addr,
            U256::ZERO,
            input,
            max_fee_per_gas,
            chain_id,
        )
    }

    pub fn balance_of(&self, account: Address) -> (&'static str, [Value; 1]) {
        let input = IERC20::balanceOfCall { account };
        let call = json!({
            "to": self.addr,
            "data": input.abi_encode()
        });
        ("eth_call", [call])
    }
}

fn make_tx(
    nonce: u64,
    signer: &PrivateKey,
    contract_or_to: Address,
    value: U256,
    input: impl Into<Bytes>,
    max_fee_per_gas: u128,
    chain_id: u64,
) -> TxEnvelope {
    let tx = TxEip1559 {
        chain_id,
        nonce,
        gas_limit: 100_000, // actual gas used around 51k
        max_fee_per_gas,
        max_priority_fee_per_gas: 0,
        to: TxKind::Call(contract_or_to),
        value,
        access_list: Default::default(),
        input: input.into(),
    };
    let sig = signer.sign_transaction(&tx);
    TxEnvelope::Eip1559(tx.into_signed(sig))
}

pub fn calculate_contract_addr(deployer: &Address, nonce: u64) -> Address {
    let mut out = Vec::new();
    let enc: [&dyn Encodable; 2] = [&deployer, &nonce];
    alloy_rlp::encode_list::<_, dyn Encodable>(&enc, &mut out);
    let hash = keccak256(out);
    let (_, contract_address) = hash.as_slice().split_at(12);
    Address::from_slice(contract_address)
}

sol! {
pragma solidity ^0.8.13;

contract IERC20 {
    // constructor(string memory _name, string memory _symbol, uint8 _decimals);
    event Transfer(address indexed from, address indexed to, uint256 value);

    function totalSupply() external view returns (uint256);
    function balanceOf(address account) external view returns (uint256);
    function transfer(address recipient, uint256 amount) external returns (bool);
    function allowance(address owner, address spender) external view returns (uint256);
    function approve(address spender, uint256 amount) external returns (bool);
    function transferFrom(address sender, address recipient, uint256 amount) external returns (bool);

    // custom testing fns
    function mint() external;
    function reset(address addr) external;
    function destroySmartContract() external;
    function transferToFriends(uint256 amount) external;
    function addFriend(address friend) external;
}}
