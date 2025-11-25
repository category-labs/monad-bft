use std::time::Duration;

use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope};
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{
    hex::{self, FromHex},
    keccak256, Address, Bytes, PrimitiveSignature, TxKind, U256,
};
use alloy_rlp::Encodable;
use alloy_rpc_client::ReqwestClient;
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_macro::sol;
use alloy_sol_types::SolCall;
use eyre::Result;
use serde::Deserialize;
use tokio::time::sleep;
use tracing::info;

// source code in https://github.com/category-labs/monad-integration/main/tests/test_contract_interaction/example.sol
const BYTECODE: &str = include_str!("bytecode.txt");

#[derive(Deserialize, Debug, Clone, Copy)]
#[serde(transparent)]
pub struct Contract {
    pub addr: Address,
}

impl Contract {
    pub async fn deploy_tx(
        client: &ReqwestClient,
        nonce: u64,
        deployer: &PrivateKeySigner,
        chain_id: u64,
    ) -> Result<Self> {
        let input = Bytes::from_hex(BYTECODE).unwrap();
        let tx = TxEip1559 {
            chain_id,
            nonce,
            gas_limit: 2_000_000,
            max_fee_per_gas: 100_000_000_000,
            max_priority_fee_per_gas: 10,
            to: TxKind::Create,
            value: U256::ZERO,
            access_list: Default::default(),
            input,
        };

        let sig = sign_transaction(deployer, &tx);
        let signed_tx = TxEnvelope::Eip1559(tx.into_signed(sig));

        let mut rlp_encoded_tx = Vec::new();
        signed_tx.encode_2718(&mut rlp_encoded_tx);
        let _: String = client
            .request(
                "eth_sendRawTransaction",
                [format!("0x{}", hex::encode(rlp_encoded_tx))],
            )
            .await?;

        let addr = calculate_contract_addr(&deployer.address(), nonce);
        ensure_contract_deployed(client, addr).await?;
        Ok(Contract { addr })
    }

    pub async fn contract_write(
        &self,
        client: &ReqwestClient,
        signer: &PrivateKeySigner,
        nonce: u64,
        chain_id: u64,
        start_index: U256,
    ) -> Result<()> {
        let input = Example::expensiveWriteCall {
            startIndex: start_index,
        }
        .abi_encode();

        let tx = TxEip1559 {
            chain_id,
            nonce,
            gas_limit: 20_000_000,
            max_fee_per_gas: 100_000_000_000,
            max_priority_fee_per_gas: 10,
            to: TxKind::Call(self.addr),
            value: U256::ZERO,
            access_list: Default::default(),
            input: input.into(),
        };
        let sig = sign_transaction(signer, &tx);
        let signed_tx = TxEnvelope::Eip1559(tx.into_signed(sig));

        let mut rlp_encoded_tx = Vec::new();
        signed_tx.encode_2718(&mut rlp_encoded_tx);
        let _: String = client
            .request(
                "eth_sendRawTransaction",
                [format!("0x{}", hex::encode(rlp_encoded_tx))],
            )
            .await?;

        Ok(())
    }
}

pub fn sign_transaction(
    signer: &PrivateKeySigner,
    transaction: &impl SignableTransaction<PrimitiveSignature>,
) -> PrimitiveSignature {
    signer
        .sign_hash_sync(&transaction.signature_hash())
        .expect("signature works")
}

pub fn calculate_contract_addr(deployer: &Address, nonce: u64) -> Address {
    let mut out = Vec::new();
    let enc: [&dyn Encodable; 2] = [&deployer, &nonce];
    alloy_rlp::encode_list::<_, dyn Encodable>(&enc, &mut out);
    let hash = keccak256(out);
    let (_, contract_address) = hash.as_slice().split_at(12);
    Address::from_slice(contract_address)
}

pub async fn ensure_contract_deployed(client: &ReqwestClient, addr: Address) -> Result<()> {
    let mut timeout = Duration::from_millis(200);
    for _ in 0..10 {
        info!(
            "Waiting {}ms for contract to be deployed...",
            timeout.as_millis()
        );
        sleep(timeout).await;

        let code = client
            .request::<_, String>("eth_getCode", (addr, "latest"))
            .await?;

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

sol! {
pragma solidity ^0.8.13;

contract Example {
    function expensiveRead(uint256 num) public view returns (uint256[] memory);
    function expensiveWrite(uint256 startIndex) public;
}}
