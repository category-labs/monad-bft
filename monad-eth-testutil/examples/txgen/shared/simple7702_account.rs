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
use alloy_eips::{
    eip2718::Encodable2718,
    eip7702::{Authorization, SignedAuthorization},
};
use alloy_primitives::{
    hex::{self, FromHex},
    keccak256, Address, Bytes, TxKind, U256,
};
use alloy_rlp::Encodable;
use alloy_rpc_client::ReqwestClient;
use alloy_sol_macro::sol;
use alloy_sol_types::{SolCall, SolConstructor};
use eyre::Result;
use serde::Deserialize;

use crate::shared::{ensure_contract_deployed, eth_json_rpc::EthJsonRpc, private_key::PrivateKey};

const SIMPLE7702ACCOUNT_BYTECODE: &str = include_str!("erc4337_simple7702account_bytecode.txt");

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(transparent)]
//  Wrapper around the Simple7702Account contract https://github.com/eth-infinitism/account-abstraction/blob/b36a1ed52ae00da6f8a4c8d50181e2877e4fa410/contracts/accounts/Simple7702Account.sol
pub struct Simple7702Account {
    pub addr: Address,
}

impl Simple7702Account {
    pub async fn deploy(
        deployer: &(Address, PrivateKey),
        client: &ReqwestClient,
        entrypoint_addr: Address,
        max_fee_per_gas: u128,
        chain_id: u64,
    ) -> Result<Self> {
        let bytecode = Bytes::from_hex(SIMPLE7702ACCOUNT_BYTECODE).unwrap();

        let constructor = ISimple7702Account::constructorCall {
            anEntryPoint: entrypoint_addr,
        };
        let constructor_args = constructor.abi_encode();
        let mut input = bytecode.to_vec();
        input.extend_from_slice(&constructor_args);

        let nonce = client.get_transaction_count(&deployer.0).await?;
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

        let addr = calculate_contract_addr(&deployer.0, nonce);
        ensure_contract_deployed(client, addr, tx.tx_hash()).await?;
        Ok(Simple7702Account { addr })
    }

    // Create an authorization for an EOA to delegate to this contract(Simple7702Account)
    pub fn create_authorization(
        &self,
        authority: &(Address, PrivateKey),
        nonce: u64,
        chain_id: u64,
    ) -> Result<SignedAuthorization> {
        let authorization = Authorization {
            chain_id,
            address: self.addr,
            nonce,
        };

        let signature = authority.1.sign_hash(&authorization.signature_hash());
        Ok(authorization.into_signed(signature))
    }

    pub fn encode_execute_calldata(&self, dest: Address, value: U256, data: Bytes) -> Bytes {
        let call = ISimple7702Account::executeCall {
            target: dest,
            value,
            data,
        };
        call.abi_encode().into()
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

sol! {
    interface IEntryPoint{}
    interface ISimple7702Account {
        struct Call {
            address target;
            uint256 value;
            bytes data;
        }


        constructor(IEntryPoint anEntryPoint) public;
        function execute(address target, uint256 value, bytes calldata data) external;
        function executeBatch(Call[] calldata calls) external;
    }
}
