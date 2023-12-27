pub mod convert;
pub mod epoch_manager;

use std::{
    error::Error,
    io,
    ops::{Add, AddAssign, Rem, Sub, SubAssign},
};

use monad_crypto::{
    hasher::{Hash, Hashable, Hasher},
    secp256k1::PubKey,
};
use serde::Deserialize;
use zerocopy::AsBytes;

/// Consensus round
#[repr(transparent)]
#[derive(Copy, Clone, Eq, Hash, Ord, PartialEq, PartialOrd, AsBytes)]
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

impl AddAssign for Round {
    fn add_assign(&mut self, other: Self) {
        self.0 += other.0
    }
}

impl std::fmt::Debug for Round {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Consensus epoch
///
/// During an epoch, the validator set remain stable: no validator is allowed to
/// stake or unstake until the next epoch
#[repr(transparent)]
#[derive(Copy, Clone, Hash, Eq, PartialEq, PartialOrd, Ord)]
pub struct Epoch(pub u64);

impl Add for Epoch {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}

impl std::fmt::Debug for Epoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Block sequence number
///
/// Consecutive blocks in the same branch have consecutive sequence numbers,
/// meaning a block must extend its parent block's sequence number by 1. Thus,
/// the committed ledger has consecutive sequence numbers, with no holes in
/// between.
#[repr(transparent)]
#[derive(Copy, Clone, Eq, Hash, Ord, PartialEq, PartialOrd, AsBytes)]
pub struct SeqNum(pub u64);

impl AsRef<[u8]> for SeqNum {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl Add for SeqNum {
    type Output = Self;

    fn add(self, other: Self) -> Self::Output {
        SeqNum(self.0 + other.0)
    }
}

impl Sub for SeqNum {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        SeqNum(self.0 - rhs.0)
    }
}

impl AddAssign for SeqNum {
    fn add_assign(&mut self, other: Self) {
        self.0 += other.0
    }
}

impl Rem for SeqNum {
    type Output = SeqNum;

    fn rem(self, rhs: Self) -> Self::Output {
        SeqNum(self.0 % rhs.0)
    }
}

impl std::fmt::Debug for SeqNum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// NodeId is the validator's pubkey identity in the consensus protocol
#[repr(transparent)]
#[derive(Copy, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct NodeId(pub PubKey);

impl std::fmt::Debug for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// BlockId uniquely identifies a block
#[repr(transparent)]
#[derive(Copy, Clone, PartialEq, Eq, Hash)]
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

impl Hashable for BlockId {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(self.0);
    }
}

/// Stake is the amount of tokens the validator deposited for validating
/// privileges and earning transaction fees
#[repr(transparent)]
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Deserialize)]
pub struct Stake(pub i64);

impl Add for Stake {
    type Output = Self;

    fn add(self, other: Self) -> Self::Output {
        Stake(self.0 + other.0)
    }
}

impl Sub for Stake {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Stake(self.0 - rhs.0)
    }
}

impl AddAssign for Stake {
    fn add_assign(&mut self, rhs: Self) {
        self.0 += rhs.0
    }
}

impl SubAssign for Stake {
    fn sub_assign(&mut self, rhs: Self) {
        self.0 -= rhs.0
    }
}

impl std::iter::Sum for Stake {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Stake(0), |a, b| a + b)
    }
}

/// Serialize into S, usually bytes
pub trait Serializable<S> {
    fn serialize(&self) -> S;
}

/// All types can trivially serialize to itself
impl<S: Clone> Serializable<S> for S {
    fn serialize(&self) -> S {
        self.clone()
    }
}

/// Deserialize from S, usually bytes
pub trait Deserializable<S: ?Sized>: Sized {
    type ReadError: Error + Send + Sync;

    fn deserialize(message: &S) -> Result<Self, Self::ReadError>;
}

/// All types can trivially deserialize to itself
impl<S: Clone> Deserializable<S> for S {
    type ReadError = io::Error;

    fn deserialize(message: &S) -> Result<Self, Self::ReadError> {
        Ok(message.clone())
    }
}

// FIXME-4: move to monad-executor-glue after spaghetti fixed
/// RouterTarget specifies the particular node(s) that the router should send
/// the message toward
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RouterTarget {
    Broadcast,
    PointToPoint(NodeId),
}

// FIXME-4: move to monad-executor-glue after spaghetti fixed
/// TimeoutVariant distinguishes the source of the timer scheduled
/// - `Pacemaker`: consensus pacemaker round timeout
/// - `BlockSync`: timeout for a specific blocksync request
#[derive(Hash, Debug, Clone, PartialEq, Eq, Copy)]
pub enum TimeoutVariant {
    Pacemaker,
    BlockSync(BlockId),
}

#[repr(transparent)]
pub struct EnumDiscriminant(pub i32);

impl Hashable for EnumDiscriminant {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(self.0.to_le_bytes());
    }
}
