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
use alloy_sol_types::{SolCall, SolConstructor};
use eyre::Result;
use serde::Deserialize;

use crate::{
    shared::{ensure_contract_deployed, eth_json_rpc::EthJsonRpc, private_key::PrivateKey},
    SimpleAccount,
};

const NFTSALE_BYTECODE: &str = include_str!("nftsale_bytecode.txt");
const INITIAL_SALE_PRICE: u128 = 1000000000000000000;

#[derive(Deserialize, Debug, Clone, Copy)]
pub struct NftSale {
    pub addr: Address,
    #[serde(skip, default)]
    pub current_sale_price: U256,
}

impl NftSale {
    pub async fn deploy(
        deployer: &(Address, PrivateKey),
        client: &ReqwestClient,
        max_fee_per_gas: u128,
        chain_id: u64,
        _gas_limit: Option<u64>,
    ) -> Result<Self> {
        let nonce = client.get_transaction_count(&deployer.0).await?;

        let tx = Self::deploy_nftsale_tx(nonce, &deployer.1, max_fee_per_gas, chain_id);
        let mut rlp_encoded_tx = Vec::new();
        tx.encode_2718(&mut rlp_encoded_tx);
        let _: String = client
            .request(
                "eth_sendRawTransaction",
                [format!("0x{}", hex::encode(rlp_encoded_tx))],
            )
            .await?;
        let nft_sale_addr = calculate_contract_addr(&deployer.0, nonce);

        // Wait for contract deployment to be confirmed
        ensure_contract_deployed(client, nft_sale_addr, tx.tx_hash()).await?;

        let current_price = Self::get_current_price(client, nft_sale_addr).await?;
        tracing::info!(
            "NFT sale contract deployed at {nft_sale_addr} with initial price {current_price:?}"
        );

        Ok(NftSale {
            addr: nft_sale_addr,
            current_sale_price: current_price,
        })
    }

    pub fn deploy_nftsale_tx(
        nonce: u64,
        deployer: &PrivateKey,
        max_fee_per_gas: u128,
        chain_id: u64,
    ) -> TxEnvelope {
        let bytecode = Bytes::from_hex(NFTSALE_BYTECODE).unwrap();

        let constructor = NFTSale::constructorCall {
            name: "MonadNFT".to_string(),
            symbol: "MNFT".to_string(),
            initialPrice: U256::from(INITIAL_SALE_PRICE),
        };
        let constructor_args = constructor.abi_encode();
        let mut input = bytecode.to_vec();
        input.extend_from_slice(&constructor_args);

        let tx = TxEip1559 {
            chain_id,
            nonce,
            gas_limit: 20_000_000,
            max_fee_per_gas,
            max_priority_fee_per_gas: 10,
            to: TxKind::Create,
            value: U256::ZERO,
            access_list: Default::default(),
            input: Bytes::from(input),
        };

        let sig = deployer.sign_transaction(&tx);
        TxEnvelope::Eip1559(tx.into_signed(sig))
    }

    pub async fn get_current_price(client: &ReqwestClient, nft_sale_addr: Address) -> Result<U256> {
        let calldata = NFTSale::getCurrentPriceCall {}.abi_encode();
        let result: String = client
            .request(
                "eth_call",
                (
                    serde_json::json!({
                        "to": nft_sale_addr,
                        "data": format!("0x{}", hex::encode(&calldata))
                    }),
                    "latest",
                ),
            )
            .await?;

        let price_bytes = Bytes::from_hex(&result)?;
        let current_price = NFTSale::getCurrentPriceCall::abi_decode_returns(&price_bytes)?;

        Ok(current_price)
    }

    pub fn construct_buy_tx(
        &mut self,
        sender: &mut SimpleAccount,
        max_fee_per_gas: u128,
        chain_id: u64,
        gas_limit: Option<u64>,
        priority_fee: Option<u128>,
    ) -> TxEnvelope {
        let calldata = NFTSale::buyCall {}.abi_encode();
        let value = self.current_sale_price + (self.current_sale_price / U256::from(100));

        let gas = gas_limit.unwrap_or(400_000);
        let tx = TxEip1559 {
            chain_id,
            nonce: sender.nonce,
            gas_limit: gas,
            max_fee_per_gas,
            max_priority_fee_per_gas: priority_fee.unwrap_or(0) * 2,
            to: TxKind::Call(self.addr),
            value,
            access_list: Default::default(),
            input: Bytes::from(calldata),
        };

        let sig = sender.key.sign_transaction(&tx);
        sender.nonce += 1;

        // total cost: gas + NFT purchase price
        let gas_cost = U256::from(gas as u128 * max_fee_per_gas);
        let total_cost = gas_cost + value;

        sender.native_bal = sender
            .native_bal
            .checked_sub(total_cost)
            .unwrap_or(U256::ZERO);

        self.current_sale_price = value;

        TxEnvelope::Eip1559(tx.into_signed(sig))
    }

    pub async fn get_owner(client: &ReqwestClient, nft_sale_addr: Address) -> Result<Address> {
        use alloy_sol_types::SolCall;

        let calldata = NFTSale::ownerCall {}.abi_encode();
        let result: String = client
            .request(
                "eth_call",
                (
                    serde_json::json!({
                        "to": nft_sale_addr,
                        "data": format!("0x{}", hex::encode(&calldata))
                    }),
                    "latest",
                ),
            )
            .await?;

        // ABI decode the result
        let return_data = Bytes::from_hex(&result)?;
        let decoded = NFTSale::ownerCall::abi_decode_returns(&return_data)?;

        Ok(decoded)
    }
}

// NFTSale Contract Interface
sol! {
    contract NFTSale {
        constructor(string memory name, string memory symbol, uint256 initialPrice);

        function buy() external payable;
        function getCurrentPrice() external view returns (uint256);
        function getTokenIdCounter() external view returns (uint256);
        function owner() public view virtual returns (address);
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
