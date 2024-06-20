use alloy_primitives::{Address, FixedBytes};

#[cfg(feature = "serde")]
pub mod serde;

pub const EMPTY_RLP_TX_LIST: u8 = 0xc0;

pub type Nonce = u64;

// FIXME reth types shouldn't be leaked
/// A 20-byte Eth address
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
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
