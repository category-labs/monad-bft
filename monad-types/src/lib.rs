pub mod convert;

use std::{
    error::Error,
    io,
    ops::{Add, AddAssign, Sub, SubAssign},
};

use monad_crypto::{hasher::Hash, secp256k1::PubKey};
use monad_proto::{
    error::ProtoError,
    proto::validator_set::{ProtoValidatorSetData, ValidatorMapEntry},
};
use zerocopy::AsBytes;

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

impl std::fmt::Debug for SeqNum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
#[repr(transparent)]
#[derive(Copy, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct NodeId(pub PubKey);

impl std::fmt::Debug for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RouterTarget {
    Broadcast,
    PointToPoint(NodeId),
}

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

#[repr(transparent)]
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
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

pub trait Serializable<S> {
    fn serialize(&self) -> S;
}

impl<S: Clone> Serializable<S> for S {
    fn serialize(&self) -> S {
        self.clone()
    }
}

pub trait Deserializable<S: ?Sized>: Sized {
    type ReadError: Error + Send + Sync;

    fn deserialize(message: &S) -> Result<Self, Self::ReadError>;
}

impl<S: Clone> Deserializable<S> for S {
    type ReadError = io::Error;

    fn deserialize(message: &S) -> Result<Self, Self::ReadError> {
        Ok(message.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatorData(pub Vec<(NodeId, Stake)>);

impl From<&ValidatorData> for ProtoValidatorSetData {
    fn from(value: &ValidatorData) -> Self {
        let vlist = value
            .0
            .iter()
            .map(|(node, stake)| ValidatorMapEntry {
                key: Some(node.into()),
                value: Some(stake.into()),
            })
            .collect::<Vec<ValidatorMapEntry>>();
        Self { validators: vlist }
    }
}

impl TryFrom<ProtoValidatorSetData> for ValidatorData {
    type Error = ProtoError;
    fn try_from(value: ProtoValidatorSetData) -> std::result::Result<Self, Self::Error> {
        let mut vlist = ValidatorData(Vec::new());
        for v in value.validators {
            let a = v
                .key
                .ok_or(Self::Error::MissingRequiredField(
                    "ValildatorMapEntry.key".to_owned(),
                ))?
                .try_into()?;
            let b = v
                .value
                .ok_or(Self::Error::MissingRequiredField(
                    "ValildatorMapEntry.value".to_owned(),
                ))?
                .try_into()?;

            vlist.0.push((a, b));
        }

        Ok(vlist)
    }
}

#[derive(Hash, Debug, Clone, PartialEq, Eq, Copy)]
pub enum TimeoutVariant {
    Pacemaker,
    BlockSync(BlockId),
}
