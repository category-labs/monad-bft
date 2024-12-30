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
    ) -> Result<Self> {
        let nonce = client.get_transaction_count(&deployer.0).await?;
        let tx = Self::deploy_tx(nonce, &deployer.1, max_fee_per_gas);

        let _: String = client
            .request("eth_sendRawTransaction", [tx.envelope_encoded()])
            .await?;

        let addr = calculate_contract_addr(&deployer.0, nonce);
        ensure_contract_deployed(client, addr).await?;
        Ok(Self { addr })
    }

    pub fn deploy_tx(
        nonce: u64,
        deployer: &PrivateKey,
        max_fee_per_gas: u128,
    ) -> TransactionSigned {
        let input = Bytes::from_hex(BYTECODE).unwrap();
        let tx = Transaction::Eip1559(TxEip1559 {
            chain_id: 41454,
            nonce,
            gas_limit: 2_000_000,
            max_fee_per_gas,
            max_priority_fee_per_gas: 10,
            to: TransactionKind::Create,
            value: U256::ZERO.into(),
            access_list: AccessList::default(),
            input,
        });

        let sig = deployer.sign_transaction(&tx);
        TransactionSigned::from_transaction_and_signature(tx, sig)
    }

    // Helper function to construct an ECMul transaction
    pub fn construct_tx(
        &self,
        sender: &mut SimpleAccount,
        max_fee_per_gas: u128,
    ) -> TransactionSigned {
        let input = IECMul::performzksyncECMulsCall {
            iterations: U256::from(200),
        }
        .abi_encode();

        let tx = Transaction::Eip1559(TxEip1559 {
            chain_id: 41454,
            nonce: sender.nonce,
            gas_limit: 2_000_000,
            max_fee_per_gas,
            max_priority_fee_per_gas: 0,
            to: TransactionKind::Call(self.addr),
            value: U256::ZERO.into(),
            access_list: AccessList::default(),
            input: input.into(),
        });

        let sig = sender.key.sign_transaction(&tx);
        sender.nonce += 1;
        TransactionSigned::from_transaction_and_signature(tx, sig)
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
