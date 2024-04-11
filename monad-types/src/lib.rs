pub mod convert;

use std::{
    error::Error,
    io,
    ops::{Add, AddAssign, Div, Rem, Sub, SubAssign},
};

use monad_crypto::{
    certificate_signature::PubKey,
    hasher::{Hash, Hashable, Hasher},
};
use serde::Deserialize;
use zerocopy::AsBytes;

pub const GENESIS_SEQ_NUM: SeqNum = SeqNum(u64::MAX);

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
        SeqNum(self.0.overflowing_add(other.0).0)
    }
}

impl Sub for SeqNum {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        SeqNum(self.0.overflowing_sub(rhs.0).0)
    }
}

impl AddAssign for SeqNum {
    fn add_assign(&mut self, other: Self) {
        self.0 += other.0
    }
}

impl Div for SeqNum {
    type Output = SeqNum;

    fn div(self, rhs: Self) -> Self::Output {
        SeqNum(self.0 / rhs.0)
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

impl SeqNum {
    /// Compute the epoch that the sequence number belong to. It does NOT mean
    /// that the block is proposed in the epoch
    ///
    /// The genesis qc represents a committed genesis block. This function
    /// should never be called on GENESIS_SEQ_NUM
    ///
    /// GENESIS_SEQ_NUM -> undefined
    /// [0, val_set_update_interval] -> Epoch 1
    /// [val_set_update_interval + 1, 2 * val_set_update_interval] -> Epoch 2
    /// ... The first epoch is one block longer than all other ones
    pub fn to_epoch(&self, val_set_update_interval: SeqNum) -> Epoch {
        assert_ne!(self, &GENESIS_SEQ_NUM);
        Epoch((self.0.saturating_sub(1) / val_set_update_interval.0) + 1)
    }

    /// The first epoch starts with SeqNum 0 and end with 100. Every epoch
    /// afterwards starts at SeqNum (X * interval) + 1 and end with (X *
    /// interval + interval)
    pub fn is_epoch_end(&self, val_set_update_interval: SeqNum) -> bool {
        *self % val_set_update_interval == SeqNum(0) && *self != SeqNum(0)
    }

    /// Compute the epoch number of the next block/sequence number
    ///
    /// sn.get_next_block_epoch(interval) == sn.to_epoch(interval) + Epoch(1)
    /// <->
    /// sn.is_epoch_end(interval) == true
    pub fn get_next_block_epoch(&self, val_set_update_interval: SeqNum) -> Epoch {
        (*self + SeqNum(1)).to_epoch(val_set_update_interval)
    }
}

/// NodeId is the validator's pubkey identity in the consensus protocol
#[repr(transparent)]
#[derive(Copy, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct NodeId<P: PubKey> {
    pubkey: P,
}

impl<P: PubKey> std::fmt::Display for NodeId<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.pubkey, f)
    }
}

impl<P: PubKey> NodeId<P> {
    pub fn new(pubkey: P) -> Self {
        Self { pubkey }
    }

    pub fn pubkey(&self) -> P {
        self.pubkey
    }
}

impl<P: PubKey> std::fmt::Debug for NodeId<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.pubkey, f)
    }
}

impl<P: PubKey> Hashable for NodeId<P> {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(self.pubkey.bytes())
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
    type ReadError: Error + Send + Sync + 'static;

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
pub enum RouterTarget<P: PubKey> {
    Broadcast,
    PointToPoint(NodeId<P>),
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

#[cfg(test)]
mod test {
    use test_case::test_case;

    use super::*;

    #[test_case(SeqNum(0), Epoch(1), SeqNum(100); "sn_0_epoch_1")]
    #[test_case(SeqNum(1), Epoch(1), SeqNum(100); "sn_1_epoch_1")]
    #[test_case(SeqNum(100), Epoch(1), SeqNum(100); "sn_100_epoch_1")]
    #[test_case(SeqNum(101), Epoch(2), SeqNum(100); "sn_101_epoch_2")]

    fn test_epoch_conversion(
        seq_num: SeqNum,
        expected_epoch: Epoch,
        val_set_update_interval: SeqNum,
    ) {
        assert_eq!(seq_num.to_epoch(val_set_update_interval), expected_epoch);
    }

    #[test]
    #[should_panic]
    fn test_epoch_conversion_genesis() {
        GENESIS_SEQ_NUM.to_epoch(SeqNum(100));
    }

    #[test]
    fn test_next_epoch() {
        let interval = SeqNum(100);
        let mut seq_num = SeqNum(0);

        while seq_num < SeqNum(interval.0 * 3) {
            // sn.get_next_block_epoch(interval) == sn.to_epoch(interval) + Epoch(1)
            // <->
            // sn.is_epoch_end(interval) == true
            let is_next_epoch =
                seq_num.get_next_block_epoch(interval) == seq_num.to_epoch(interval) + Epoch(1);
            let is_epoch_end = seq_num.is_epoch_end(interval);

            assert_eq!(is_next_epoch, is_epoch_end);

            seq_num += SeqNum(1);
        }
    }
}
