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
use alloy_sol_macro::sol;
use alloy_sol_types::SolCall;
use eyre::Result;
use serde::Deserialize;

use super::{eth_json_rpc::EthJsonRpc, private_key::PrivateKey};
use crate::{shared::erc20::ensure_contract_deployed, SimpleAccount};

const BYTECODE: &str = include_str!("ecmul_bytecode.txt");

#[derive(Deserialize, Debug, Clone, Copy)]
#[serde(transparent)]
pub struct ECMul {
    pub addr: Address,
}

impl ECMul {
    pub async fn deploy(
        deployer: &(Address, PrivateKey),
        client: &ReqwestClient,
        max_fee_per_gas: u128,
        chain_id: u64,
        _gas_limit: Option<u64>,
    ) -> Result<Self> {
        let nonce = client.get_transaction_count(&deployer.0).await?;
        let tx = Self::deploy_tx(nonce, &deployer.1, max_fee_per_gas, chain_id);
        let mut rlp_encoded_tx = Vec::new();
        tx.encode_2718(&mut rlp_encoded_tx);

        let _: String = client
            .request(
                "eth_sendRawTransaction",
                [format!("0x{}", hex::encode(rlp_encoded_tx))],
            )
            .await?;

        let addr = calculate_contract_addr(&deployer.0, nonce);
        ensure_contract_deployed(client, addr).await?;
        Ok(Self { addr })
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
            gas_limit: 2_000_000,
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

    // Helper function to construct an ECMul transaction
    pub fn construct_tx(
        &self,
        sender: &mut SimpleAccount,
        max_fee_per_gas: u128,
        chain_id: u64,
        gas_limit: Option<u64>,
        priority_fee: Option<u128>,
    ) -> TxEnvelope {
        let input = IECMul::performzksyncECMulsCall {
            iterations: U256::from(200),
        }
        .abi_encode();

        let tx = TxEip1559 {
            chain_id,
            nonce: sender.nonce,
            gas_limit: gas_limit.unwrap_or(2_000_000), // 2M default, override with --set-tx-gas-limit
            max_fee_per_gas,
            max_priority_fee_per_gas: priority_fee.unwrap_or(0), // 0 default, override with --priority-fee
            to: TxKind::Call(self.addr),
            value: U256::ZERO,
            access_list: Default::default(),
            input: input.into(),
        };

        let sig = sender.key.sign_transaction(&tx);
        sender.nonce += 1;
        TxEnvelope::Eip1559(tx.into_signed(sig))
    }
}

// Contract interface
sol! {
    contract IECMul {
        function performECMulDebug(bytes32 x, bytes32 y, bytes32 scalar, uint256 iterations) external;
        function performECMuls(bytes32 x, bytes32 y, bytes32 scalar, uint256 iterations) external;
        function performzksyncECMuls(uint256 iterations) external;
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
