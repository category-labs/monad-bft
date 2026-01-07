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
use eyre::Result;
use serde::Deserialize;

use super::ensure_contract_deployed;
use crate::shared::{eth_json_rpc::EthJsonRpc, private_key::PrivateKey};

const BYTECODE: &str = include_str!("stress_contract_bytecode.txt");

#[derive(Deserialize, Debug, Clone, Copy)]
#[serde(transparent)]
pub struct HeavyWriteContract {
    pub addr: Address,
}

impl HeavyWriteContract {
    pub async fn deploy(
        deployer: &(Address, PrivateKey),
        client: &ReqwestClient,
        max_fee_per_gas: u128,
        chain_id: u64,
    ) -> Result<Self> {
        let nonce = client.get_transaction_count(&deployer.0).await?;

        Self::deploy_with_nonce(nonce, deployer, client, max_fee_per_gas, chain_id).await
    }

    pub async fn deploy_with_nonce(
        nonce: u64,
        deployer: &(Address, PrivateKey),
        client: &ReqwestClient,
        max_fee_per_gas: u128,
        chain_id: u64,
    ) -> Result<Self> {
        let input = Bytes::from_hex(BYTECODE.trim().trim_start_matches("0x")).unwrap();
        let tx = TxEip1559 {
            chain_id,
            nonce,
            gas_limit: 20_000_000,
            max_fee_per_gas,
            max_priority_fee_per_gas: 1_000_000_000, // 1 gwei
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

        let contract_addr = calculate_contract_addr(&deployer.0, nonce);
        ensure_contract_deployed(client, contract_addr, tx.tx_hash()).await?;

        Ok(HeavyWriteContract { addr: contract_addr })
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

sol! {
    contract SpartaGasTest3Writer {
        function spartaWrite(uint256 start) external returns (uint256 matches);
    }
}
