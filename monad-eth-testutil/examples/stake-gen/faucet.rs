use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope};
use alloy_primitives::{hex, Address, FixedBytes, TxHash, TxKind, U256};
use alloy_rlp::{BytesMut, Encodable};
use alloy_rpc_client::{ClientBuilder, ReqwestClient};
use alloy_signer::SignerSync;
use alloy_signer_local::LocalSigner;
use secp256k1::SecretKey as SecpPrivateKey;
use url::Url;

use crate::get_transaction_count;

pub struct Faucet {
    pub secp_privkey: SecpPrivateKey,
    pub nonce: u64,

    pub client: ReqwestClient,
    pub chain_id: u64,
}

impl Faucet {
    pub async fn new(faucet_key_hex: &str, rpc_url: Url, chain_id: u64) -> Self {
        let client = ClientBuilder::default().http(rpc_url);
        let faucet_key =
            SecpPrivateKey::from_slice(&hex::decode(faucet_key_hex).expect("invalid faucet key"))
                .expect("invalid faucet key");
        let public_key = &faucet_key
            .public_key(secp256k1::SECP256K1)
            .serialize_uncompressed()[1..];
        let nonce = get_transaction_count(&client, &Address::from_raw_public_key(public_key)).await;

        Faucet {
            secp_privkey: faucet_key,
            nonce,

            client,
            chain_id,
        }
    }

    pub async fn fund_address(&mut self, eth_address: Address, amount: U256) -> Option<TxHash> {
        let tx = TxEip1559 {
            chain_id: self.chain_id,
            nonce: self.nonce,
            gas_limit: 30_000,
            max_fee_per_gas: 100_000_000_000,
            max_priority_fee_per_gas: 0,
            to: TxKind::Call(eth_address),
            value: amount,
            access_list: Default::default(),
            input: Default::default(),
        };

        let signer = LocalSigner::from_bytes(&FixedBytes::<32>::from_slice(
            &self.secp_privkey.secret_bytes(),
        ))
        .unwrap();

        let signature_hash = tx.signature_hash();
        let signature = signer.sign_hash_sync(&signature_hash).unwrap();
        let signed_tx = TxEnvelope::Eip1559(tx.into_signed(signature));

        let mut rlp_encoded_tx = BytesMut::new();
        signed_tx.encode(&mut rlp_encoded_tx);

        match self
            .client
            .request::<_, TxHash>(
                "eth_sendRawTransaction",
                &[format!("0x{}", hex::encode(rlp_encoded_tx))],
            )
            .await
        {
            Ok(tx_hash) => {
                self.nonce += 1;
                Some(tx_hash)
            }
            Err(rpc_error) => {
                println!("Faucet RPC error: {}", rpc_error);
                None
            }
        }
    }
}
