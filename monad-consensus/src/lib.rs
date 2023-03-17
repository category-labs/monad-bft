pub mod types;

use zerocopy::AsBytes;

type Hash = [u8; 32];

#[repr(transparent)]
#[derive(Copy, Clone, Debug, Default, PartialEq, PartialOrd, AsBytes)]
pub struct Round(u64);

impl AsRef<[u8]> for Round {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

#[repr(transparent)]
#[derive(Copy, Clone, Debug, Default, AsBytes)]
pub struct NodeId(u16);

impl AsRef<[u8]> for NodeId {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

#[repr(transparent)]
#[derive(Copy, Clone, Debug, Default)]
pub struct BlockId(pub Hash);
