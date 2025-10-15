use std::collections::BTreeMap;

use alloy_primitives::{
    hex::{self, FromHex},
    Address, FixedBytes,
};
use alloy_rpc_client::ReqwestClient;
use alloy_signer::k256::ecdsa::SigningKey;
use blst::min_pk::{PublicKey as BlsPublicKey, SecretKey as BlsPrivateKey};
use monad_types::{Epoch, Stake};
use secp256k1::{PublicKey as SecpPublicKey, SecretKey as SecpPrivateKey};
use serde::{de::Error, Deserialize};

use crate::get_transaction_count;

pub type ValidatorId = u64;
pub type WithdrawalId = u8;

pub struct NodeData {
    pub name: String,

    pub secp_privkey: SecpPrivateKey,
    pub secp_pubkey: SecpPublicKey,

    pub bls_privkey: BlsPrivateKey,
    pub bls_pubkey: BlsPublicKey,

    pub eth_address: Address,
    pub nonce: u64,

    pub val_id: Option<u64>,
    pub current_stake: Stake,

    // pending withdrawals for the node as a delegator
    pub pending_withdrawals: BTreeMap<ValidatorId, BTreeMap<WithdrawalId, Epoch>>,
}

impl std::cmp::PartialEq for NodeData {
    fn eq(&self, other: &Self) -> bool {
        self.secp_pubkey == other.secp_pubkey
    }
}

impl std::cmp::Eq for NodeData {}

impl std::cmp::PartialOrd for NodeData {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.secp_pubkey.partial_cmp(&other.secp_pubkey)
    }
}

impl std::cmp::Ord for NodeData {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl std::fmt::Display for NodeData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.name)
    }
}

impl std::fmt::Debug for NodeData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeData")
            .field("name", &self.name)
            .field(
                "secp_privkey",
                &hex::encode(self.secp_privkey.secret_bytes()),
            )
            .field("secp_pubkey", &hex::encode(self.secp_pubkey.serialize()))
            .field("bls_privkey", &hex::encode(self.bls_privkey.serialize()))
            .field("bls_pubkey", &hex::encode(self.bls_pubkey.compress()))
            .field("stake", &self.current_stake)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for NodeData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawNodeData {
            name: Option<String>,
            secp_privkey: String,
            bls_privkey: String,
        }
        let raw_node_data = RawNodeData::deserialize(deserializer)?;

        let secp_key_hex = raw_node_data.secp_privkey;
        let secp_key_bytes = FixedBytes::<32>::from_hex(secp_key_hex).map_err(D::Error::custom)?;

        let bls_key_hex = raw_node_data.bls_privkey;
        let bls_key_bytes = FixedBytes::<32>::from_hex(bls_key_hex).map_err(D::Error::custom)?;

        Ok(NodeData::new(
            raw_node_data.name,
            secp_key_bytes.into(),
            bls_key_bytes.into(),
        ))
    }
}

impl NodeData {
    pub fn new(
        name: Option<String>,
        secp_privkey_bytes: [u8; 32],
        bls_privkey_bytes: [u8; 32],
    ) -> Self {
        let secp_privkey = SecpPrivateKey::from_slice(&secp_privkey_bytes).expect(&format!(
            "Invalid secp private key: {:?}",
            secp_privkey_bytes
        ));
        let secp_pubkey = secp_privkey.public_key(secp256k1::SECP256K1);
        let bls_privkey = BlsPrivateKey::from_bytes(&bls_privkey_bytes)
            .expect(&format!("Invalid bls private key: {:?}", bls_privkey_bytes));
        let bls_pubkey = bls_privkey.sk_to_pk();

        Self {
            name: name.unwrap_or(hex::encode_prefixed(secp_pubkey.serialize())),

            secp_privkey,
            secp_pubkey,

            bls_privkey,
            bls_pubkey,

            eth_address: Address::from_private_key(
                &SigningKey::from_slice(&secp_privkey_bytes).unwrap(),
            ),
            nonce: 0,

            val_id: None,
            current_stake: Stake::from(0),

            pending_withdrawals: BTreeMap::new(),
        }
    }

    pub async fn update_nonce(&mut self, client: &ReqwestClient) {
        self.nonce = get_transaction_count(client, &self.eth_address).await;
    }
}

pub trait Delegator {
    fn get_nonce(&self) -> u64;
    fn get_nonce_mut(&mut self) -> &mut u64;

    fn get_secp_priv_key(&self) -> SecpPrivateKey;

    fn get_pending_withdrawals(
        &mut self,
    ) -> &mut BTreeMap<ValidatorId, BTreeMap<WithdrawalId, Epoch>>;
}

impl Delegator for NodeData {
    fn get_nonce(&self) -> u64 {
        self.nonce
    }

    fn get_nonce_mut(&mut self) -> &mut u64 {
        &mut self.nonce
    }

    fn get_secp_priv_key(&self) -> SecpPrivateKey {
        self.secp_privkey
    }

    fn get_pending_withdrawals(
        &mut self,
    ) -> &mut BTreeMap<ValidatorId, BTreeMap<WithdrawalId, Epoch>> {
        &mut self.pending_withdrawals
    }
}
