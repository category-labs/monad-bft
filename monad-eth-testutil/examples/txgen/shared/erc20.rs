use std::time::Duration;

use alloy_rlp::Encodable;
use alloy_rpc_client::ReqwestClient;
use alloy_sol_macro::sol;
use alloy_sol_types::SolCall;
use eyre::Result;
use reth_primitives::{
    hex::FromHex, keccak256, AccessList, Address, Bytes, Transaction, TransactionKind,
    TransactionSigned, TxEip1559, U256,
};
use serde::Deserialize;
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
    pub async fn deploy(deployer: &(Address, PrivateKey), client: &ReqwestClient) -> Result<Self> {
        let nonce = client.get_transaction_count(&deployer.0).await?;
        let tx = Self::deploy_tx(nonce, &deployer.1);

        // make compiler happy, actually parse string : (
        let _: String = client
            .request("eth_sendRawTransaction", [tx.envelope_encoded()])
            .await?;

        let addr = calculate_contract_addr(&deployer.0, nonce);
        ensure_contract_deployed(client, addr).await?;
        Ok(ERC20 { addr })
    }

    pub fn deploy_tx(nonce: u64, deployer: &PrivateKey) -> TransactionSigned {
        let input = Bytes::from_hex(BYTECODE).unwrap();
        let tx = Transaction::Eip1559(TxEip1559 {
            chain_id: 41454,
            nonce,
            gas_limit: 800_000, // usually around 600k gas
            max_fee_per_gas: 100_000,
            max_priority_fee_per_gas: 10,
            to: TransactionKind::Create,
            value: U256::ZERO.into(),
            access_list: AccessList::default(),
            input,
        });

        let sig = deployer.sign_transaction(&tx);
        TransactionSigned::from_transaction_and_signature(tx, sig)
    }

    pub fn self_destruct_tx(&self, sender: &mut SimpleAccount) -> TransactionSigned {
        self.construct_tx(sender, IERC20::destroySmartContractCall {})
    }

    pub fn construct_tx<T: alloy_sol_types::SolCall>(
        &self,
        sender: &mut SimpleAccount,
        input: T,
    ) -> TransactionSigned {
        let input = input.abi_encode();
        let tx = make_tx(sender.nonce, &sender.key, self.addr, U256::ZERO, input);
        sender.nonce += 1;
        tx
    }

    pub fn construct_mint(&self, from: &PrivateKey, nonce: u64) -> TransactionSigned {
        let input = IERC20::mintCall {}.abi_encode();
        make_tx(nonce, from, self.addr, U256::ZERO, input)
    }

    pub fn construct_transfer(
        &self,
        from: &PrivateKey,
        recipient: Address,
        nonce: u64,
        amount: U256,
    ) -> TransactionSigned {
        let input = IERC20::transferCall { recipient, amount }.abi_encode();
        make_tx(nonce, from, self.addr, U256::ZERO, input)
    }

    // pub fn balance_of(&self, account: Address) -> (&'static str, [Value; 1]) {
    //     let input = IERC20::balanceOfCall { account };
    //     let call = json!({
    //         "to": self.addr,
    //         "data": input.abi_encode()
    //     });
    //     ("eth_call", [call])
    // }
}

fn make_tx(
    nonce: u64,
    signer: &PrivateKey,
    contract_or_to: Address,
    value: U256,
    input: impl Into<Bytes>,
) -> TransactionSigned {
    let tx = Transaction::Eip1559(TxEip1559 {
        chain_id: 41454,
        nonce,
        gas_limit: 200_000, // probably closer to 80k
        max_fee_per_gas: 10_000,
        max_priority_fee_per_gas: 0,
        to: TransactionKind::Call(contract_or_to),
        value: value.into(),
        access_list: AccessList::default(),
        input: input.into(),
    });
    let sig = signer.sign_transaction(&tx);
    TransactionSigned::from_transaction_and_signature(tx, sig)
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
