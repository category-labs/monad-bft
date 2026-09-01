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
    hex::{self, FromHex},
    keccak256, Address, Bytes, TxKind, U256,
};
use alloy_rlp::Encodable;
use alloy_rpc_client::ReqwestClient;
use alloy_rpc_types::TransactionReceipt;
use alloy_sol_macro::sol;
use alloy_sol_types::{SolCall, SolConstructor};
use eyre::Result;
use serde::Deserialize;

use crate::shared::{
    ensure_contract_deployed, erc4337_entrypoint::EntryPoint, eth_json_rpc::EthJsonRpc,
    private_key::PrivateKey,
};

const PAYMASTER_BYTECODE: &str = include_str!("erc4337_paymaster_bytecode.txt");

#[derive(Deserialize, Debug, Clone, Copy)]
#[serde(transparent)]
// Wrapper around the TxgenPaymaster.sol contract
pub struct Paymaster {
    pub addr: Address,
}

impl Paymaster {
    pub async fn deploy(
        deployer: &(Address, PrivateKey),
        client: &ReqwestClient,
        entrypoint: &EntryPoint,
        max_fee_per_gas: u128,
        chain_id: u64,
    ) -> Result<Self> {
        let mut nonce = client.get_transaction_count(&deployer.0).await?;
        let bytecode = Bytes::from_hex(PAYMASTER_BYTECODE).unwrap();

        let constructor = TxgenPaymaster::constructorCall {
            _entryPointAddr: entrypoint.addr,
            _owner: deployer.0,
        };
        let constructor_args = constructor.abi_encode();
        let mut input = bytecode.to_vec();
        input.extend_from_slice(&constructor_args);

        let tx = TxEip1559 {
            chain_id,
            nonce,
            gas_limit: 10_000_000,
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

        let paymaster_addr = calculate_contract_addr(&deployer.0, nonce);
        ensure_contract_deployed(client, paymaster_addr, tx.tx_hash()).await?;
        nonce += 1;

        Self::deposit(
            client,
            paymaster_addr,
            &deployer.1,
            U256::from(100_000_000_000_000_000_000_000_u128), // 100k MON
            nonce,
            chain_id,
            max_fee_per_gas,
        )
        .await?;
        nonce += 1;

        Self::add_stake(
            client,
            paymaster_addr,
            &deployer.1,
            U256::from(100_000_000_000_000_000_000_000_u128), // 100k MON
            1000,
            chain_id,
            nonce,
            max_fee_per_gas,
        )
        .await?;

        Ok(Paymaster {
            addr: paymaster_addr,
        })
    }

    async fn deposit(
        client: &ReqwestClient,
        paymaster_addr: Address,
        sender: &PrivateKey,
        value_to_deposit: U256,
        nonce: u64,
        chain_id: u64,
        max_fee_per_gas: u128,
    ) -> Result<()> {
        let tx = Self::construct_deposit_tx(
            paymaster_addr,
            sender,
            value_to_deposit,
            max_fee_per_gas,
            chain_id,
            nonce,
            Some(10_000_000),
            Some(10),
        );
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

        tracing::info!("Deposit receipt: {:?}", receipt);

        Ok(())
    }

    pub async fn add_stake(
        client: &ReqwestClient,
        paymaster_addr: Address,
        sender: &PrivateKey,
        stake_value: U256,
        unstake_delay_sec: u32,
        chain_id: u64,
        nonce: u64,
        max_fee_per_gas: u128,
    ) -> Result<()> {
        let tx = Self::construct_add_stake_tx(
            paymaster_addr,
            sender,
            stake_value,
            unstake_delay_sec,
            max_fee_per_gas,
            chain_id,
            nonce,
            Some(10_000_000),
            Some(10),
        );
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

        tracing::info!("Add stake receipt: {:?}", receipt);

        Ok(())
    }

    // Helper function to deposit funds to EntryPoint via Paymaster (can only be called by paymaster owner)
    pub fn construct_deposit_tx(
        paymaster_addr: Address,
        sender: &PrivateKey,
        value_to_deposit: U256,
        max_fee_per_gas: u128,
        chain_id: u64,
        nonce: u64,
        gas_limit: Option<u64>,
        priority_fee: Option<u128>,
    ) -> TxEnvelope {
        let calldata = TxgenPaymaster::depositCall {}.abi_encode();

        let gas = gas_limit.unwrap_or(400_000);
        let tx = TxEip1559 {
            chain_id,
            nonce,
            gas_limit: gas,
            max_fee_per_gas,
            max_priority_fee_per_gas: priority_fee.unwrap_or(10),
            to: TxKind::Call(paymaster_addr),
            value: value_to_deposit,
            access_list: Default::default(),
            input: Bytes::from(calldata),
        };

        let sig = sender.sign_transaction(&tx);

        TxEnvelope::Eip1559(tx.into_signed(sig))
    }

    // Helper function for Paymaster to add stake in the EntryPoint (can only be called by paymaster owner)
    pub fn construct_add_stake_tx(
        paymaster_addr: Address,
        sender: &PrivateKey,
        stake_value: U256,
        unstake_delay_sec: u32,
        max_fee_per_gas: u128,
        chain_id: u64,
        nonce: u64,
        gas_limit: Option<u64>,
        priority_fee: Option<u128>,
    ) -> TxEnvelope {
        let calldata = TxgenPaymaster::addStakeCall {
            unstakeDelaySec: unstake_delay_sec,
        }
        .abi_encode();

        let gas = gas_limit.unwrap_or(400_000);
        let tx = TxEip1559 {
            chain_id,
            nonce,
            gas_limit: gas,
            max_fee_per_gas,
            max_priority_fee_per_gas: priority_fee.unwrap_or(10),
            to: TxKind::Call(paymaster_addr),
            value: stake_value,
            access_list: Default::default(),
            input: Bytes::from(calldata),
        };

        let sig = sender.sign_transaction(&tx);

        TxEnvelope::Eip1559(tx.into_signed(sig))
    }
}

sol! {
    contract TxgenPaymaster {
        constructor(address _entryPointAddr, address _owner) public;
        function deposit() public payable;
        function withdrawTo(
            address payable withdrawAddress,
            uint256 amount
        ) public;
        function addStake(uint32 unstakeDelaySec) external payable;
        function unlockStake() external;
        function withdrawStake(address payable withdrawAddress) external;

        function getDeposit() public view returns (uint256);
        function getOwner() public view returns (address);
    }
}

pub fn calculate_contract_addr(deployer: &Address, nonce: u64) -> Address {
    let mut out = Vec::new();
    let enc: [&dyn Encodable; 2] = [&deployer, &nonce];
    alloy_rlp::encode_list::<_, dyn Encodable>(&enc, &mut out);
    let hash = keccak256(out);
    let (_, contract_address) = hash.as_slice().split_at(12);
    Address::from_slice(contract_address)
}
