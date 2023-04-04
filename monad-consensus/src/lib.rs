pub mod signatures;
pub mod types;
pub mod validation;

use std::ops::Add;
use std::ops::Sub;

use zerocopy::AsBytes;

pub type Hash = [u8; 32];

#[repr(transparent)]
#[derive(Copy, Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd, AsBytes)]
pub struct Round(pub u64);

impl AsRef<[u8]> for Round {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl Add for Round {
    type Output = Self;

    fn add(self, other: Self) -> Self::Output {
        Round(self.0 + other.0)
    }
}

impl Sub for Round {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Round(self.0 - rhs.0)
    }
}

#[repr(transparent)]
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, AsBytes)]
pub struct NodeId(pub u16);

impl AsRef<[u8]> for NodeId {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

#[repr(transparent)]
#[derive(Copy, Clone, Default, PartialEq, Eq, Hash)]
pub struct BlockId(pub Hash);

impl std::fmt::Debug for BlockId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:>02x}{:>02x}..{:>02x}{:>02x}",
            self.0[0], self.0[1], self.0[30], self.0[31]
        )
    }
}
