use std::str::FromStr;

use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope};
use alloy_primitives::{Address, FixedBytes, TxKind, B256, U256};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use monad_chain_config::MONAD_DEVNET_CHAIN_ID;

#[derive(Clone, Debug)]
pub struct Faucet {
    priv_key: PrivateKeySigner,
    nonce: u64,
}

impl Faucet {
    pub fn new() -> Self {
        Self::new_with_pk(
            B256::from_str("0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")
                .unwrap(),
        )
    }

    pub fn new_with_pk(pk: B256) -> Self {
        Self {
            priv_key: PrivateKeySigner::from_bytes(&pk).expect("invalid pk"),
            nonce: 0,
        }
    }

    pub fn fund(&mut self, receiver: [u8; 32]) -> TxEnvelope {
        let tx_receiver = FixedBytes(receiver);
        let tx_receiver_address = PrivateKeySigner::from_bytes(&tx_receiver)
            .unwrap()
            .address();
        self.fund_address(tx_receiver_address)
    }

    pub fn fund_address(&mut self, address: Address) -> TxEnvelope {
        let mon = U256::from(1_000_000_000_000_000_000u64);
        let tx = TxEip1559 {
            chain_id: MONAD_DEVNET_CHAIN_ID,
            nonce: self.nonce,
            gas_limit: 30000,
            max_priority_fee_per_gas: 20,
            max_fee_per_gas: 100_000_000_000,
            to: TxKind::Call(address),
            value: U256::from(10_000) * mon,
            access_list: Default::default(),
            input: vec![0; 10].into(),
        };

        self.nonce += 1;

        let tx_signature = self
            .priv_key
            .sign_hash_sync(&tx.signature_hash())
            .expect("signature works");

        tx.into_signed(tx_signature).into()
    }
}
