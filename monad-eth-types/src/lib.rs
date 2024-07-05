use ::serde::{Deserialize, Serialize};
use alloy_primitives::{Address, FixedBytes, B256};

pub mod serde;

pub const EMPTY_RLP_TX_LIST: u8 = 0xc0;

pub type Nonce = u64;
pub type Balance = u128;

// FIXME reth types shouldn't be leaked
/// A 20-byte Eth address
#[derive(
    Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize,
)]
pub struct EthAddress(pub Address);

impl EthAddress {
    pub fn from_bytes(bytes: [u8; 20]) -> Self {
        Self(Address(FixedBytes(bytes)))
    }
}

impl AsRef<[u8]> for EthAddress {
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl AsRef<[u8; 20]> for EthAddress {
    fn as_ref(&self) -> &[u8; 20] {
        &self.0 .0 .0
    }
}

#[derive(Debug, Copy, Clone)]
pub struct EthAccount {
    pub nonce: Nonce,
    pub balance: Balance,
    pub code_hash: Option<B256>,
}
