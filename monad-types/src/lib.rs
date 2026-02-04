// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{
    error::Error,
    fmt::{Debug, Display},
    io,
    ops::{Add, AddAssign, Div, Rem, Sub, SubAssign},
    str::FromStr,
    time::{Duration, Instant},
};

use alloy_primitives::U256;
use alloy_rlp::{
    Decodable, Encodable, RlpDecodable, RlpDecodableWrapper, RlpEncodable, RlpEncodableWrapper,
};
use monad_crypto::certificate_signature::PubKey;
pub use monad_crypto::hasher::Hash;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use zerocopy::IntoBytes;

pub const GENESIS_SEQ_NUM: SeqNum = SeqNum(0);
pub const GENESIS_ROUND: Round = Round(0);

pub const ETHERNET_MTU: u16 = 1500;
pub const DEFAULT_MTU: u16 = ETHERNET_MTU;

const PROTOCOL_VERSION: u32 = 1;

const CLIENT_MAJOR_VERSION: u16 = 0;
const CLIENT_MINOR_VERSION: u16 = 1;

const HASH_VERSION: u16 = 1;
const SERIALIZE_VERSION: u16 = 1;

/// Consensus round
#[repr(transparent)]
#[derive(
    Copy,
    Clone,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    IntoBytes,
    Serialize,
    Deserialize,
    RlpEncodableWrapper,
    RlpDecodableWrapper,
)]
pub struct Round(pub u64);

impl Round {
    pub const MIN: Round = Round(u64::MIN);
    pub const MAX: Round = Round(u64::MAX);

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

pub type Balance = U256;
pub type Nonce = u64;

impl AsRef<[u8]> for Round {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl Add for Round {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Round(
            self.0
                .checked_add(rhs.0)
                .unwrap_or_else(|| panic!("{:?} + {:?}", self.0, rhs.0)),
        )
    }
}

impl Sub for Round {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Round(
            self.0
                .checked_sub(rhs.0)
                .unwrap_or_else(|| panic!("{:?} - {:?}", self.0, rhs.0)),
        )
    }
}

impl AddAssign for Round {
    fn add_assign(&mut self, other: Self) {
        *self = *self + other
    }
}

impl Debug for Round {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

impl Display for Round {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl FromStr for Round {
    type Err = <u64 as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let round = u64::from_str(s)?;
        Ok(Round(round))
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct RoundSpan {
    pub start: Round, // inclusive
    pub end: Round,   // exclusive
}

impl RoundSpan {
    // TODO: The empty RoundSpan is only used in the Group struct
    // representing a validator set. Remove this exception after we
    // refactor out the Group struct.
    pub fn empty(at: Round) -> Self {
        Self { start: at, end: at }
    }

    pub fn new(start: Round, end: Round) -> Option<Self> {
        if start >= end {
            return None;
        }
        Some(Self { start, end })
    }

    pub fn single(start: Round) -> Option<Self> {
        if start >= Round::MAX {
            return None;
        }
        Some(Self {
            start,
            end: start + Round(1),
        })
    }

    pub fn contains(&self, round: Round) -> bool {
        self.start <= round && round < self.end
    }
    pub fn overlaps(&self, other: &RoundSpan) -> bool {
        self.start < other.end && other.start < self.end
    }
}

impl From<RoundSpan> for std::ops::Range<Round> {
    fn from(span: RoundSpan) -> Self {
        span.start..span.end
    }
}

/// Consensus epoch
///
/// During an epoch, the validator set remain stable: no validator is allowed to
/// stake or unstake until the next epoch
#[repr(transparent)]
#[derive(
    Copy,
    Clone,
    Hash,
    Eq,
    PartialEq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    IntoBytes,
    RlpEncodableWrapper,
    RlpDecodableWrapper,
)]
pub struct Epoch(pub u64);

impl Epoch {
    pub const MAX: Epoch = Epoch(u64::MAX);
}

impl AsRef<[u8]> for Epoch {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl Add for Epoch {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Epoch(
            self.0
                .checked_add(rhs.0)
                .unwrap_or_else(|| panic!("{:?} + {:?}", self.0, rhs.0)),
        )
    }
}

impl Sub for Epoch {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Epoch(
            self.0
                .checked_sub(rhs.0)
                .unwrap_or_else(|| panic!("{:?} - {:?}", self.0, rhs.0)),
        )
    }
}

impl Epoch {
    pub fn checked_sub(self, count: usize) -> Option<Self> {
        self.0.checked_sub(count as u64).map(Epoch)
    }
}

impl Debug for Epoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

impl Display for Epoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl FromStr for Epoch {
    type Err = <u64 as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let epoch_num = u64::from_str(s)?;
        Ok(Epoch(epoch_num))
    }
}
/// Block sequence number
///
/// Consecutive blocks in the same branch have consecutive sequence numbers,
/// meaning a block must extend its parent block's sequence number by 1. Thus,
/// the committed ledger has consecutive sequence numbers, with no holes in
/// between.
#[repr(transparent)]
#[derive(
    Copy,
    Clone,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    IntoBytes,
    Serialize,
    Deserialize,
    RlpEncodableWrapper,
    RlpDecodableWrapper,
)]
pub struct SeqNum(pub u64);

impl SeqNum {
    pub const MIN: SeqNum = SeqNum(u64::MIN);
    pub const MAX: SeqNum = SeqNum(u64::MAX);
}

impl AsRef<[u8]> for SeqNum {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl Add for SeqNum {
    type Output = Self;

    fn add(self, other: Self) -> Self::Output {
        SeqNum(
            self.0
                .checked_add(other.0)
                .unwrap_or_else(|| panic!("{:?} + {:?}", self, other)),
        )
    }
}

impl Sub for SeqNum {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        SeqNum(
            self.0
                .checked_sub(rhs.0)
                .unwrap_or_else(|| panic!("{:?} - {:?}", self, rhs)),
        )
    }
}

impl AddAssign for SeqNum {
    fn add_assign(&mut self, other: Self) {
        *self = *self + other;
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

impl SeqNum {
    pub const fn saturating_add(self, other: Self) -> Self {
        Self(self.0.saturating_add(other.0))
    }
}

impl Debug for SeqNum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

impl Display for SeqNum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl FromStr for SeqNum {
    type Err = <u64 as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let seq_num = u64::from_str(s)?;
        Ok(SeqNum(seq_num))
    }
}

impl SeqNum {
    /// Compute the epoch that the sequence number belong to. It does NOT mean
    /// that the block is proposed in the epoch
    ///
    /// [0, epoch_length-1] -> Epoch 1
    /// [epoch_length, (2 * epoch_length)-1] -> Epoch 2
    pub fn to_epoch(&self, epoch_length: SeqNum) -> Epoch {
        Epoch((self.0 / epoch_length.0) + 1)
    }

    /// This tells us what the boundary block of the epoch is. Note that this only indicates when
    /// the next epoch's round is scheduled.
    pub fn is_epoch_end(&self, epoch_length: SeqNum) -> bool {
        *self % epoch_length == epoch_length - SeqNum(1)
    }

    /// Get the epoch number whose validator set is locked by this block. Should
    /// only be called on the boundary block sequence number
    ///
    /// Current design locks the info for epoch n + 1 by the end of epoch n. The
    /// validators have epoch_start_delay to prepare themselves for any duties
    pub fn get_locked_epoch(&self, epoch_length: SeqNum) -> Epoch {
        assert!(self.is_epoch_end(epoch_length));
        (*self).to_epoch(epoch_length) + Epoch(1)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

/// NodeId is the validator's pubkey identity in the consensus protocol
#[repr(transparent)]
#[derive(
    Copy,
    Clone,
    Hash,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
    RlpEncodableWrapper,
    RlpDecodableWrapper,
)]
pub struct NodeId<P: PubKey>(
    #[serde(serialize_with = "serialize_pubkey::<_, P>")]
    #[serde(deserialize_with = "deserialize_pubkey::<_, P>")]
    #[serde(bound = "P:PubKey")]
    #[serde(rename(serialize = "node_id", deserialize = "node_id"))]
    // Outer struct always flatten this struct, thus renaming to node_id
    // TODO now that this is a newtype, do we still need to rename?
    P,
);

impl<P: PubKey> std::fmt::Display for NodeId<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl<P: PubKey> NodeId<P> {
    pub fn new(pubkey: P) -> Self {
        Self(pubkey)
    }

    pub fn pubkey(&self) -> P {
        self.0
    }
}

impl<P: PubKey> Debug for NodeId<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

pub fn serialize_pubkey<S, P>(pubkey: &P, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    P: PubKey,
{
    let hex_str = "0x".to_string() + &hex::encode(pubkey.bytes());
    serializer.serialize_str(&hex_str)
}

pub fn deserialize_pubkey<'de, D, P>(deserializer: D) -> Result<P, D::Error>
where
    D: Deserializer<'de>,
    P: PubKey,
{
    let buf = <String as Deserialize>::deserialize(deserializer)?;

    let hex_str = match buf.strip_prefix("0x") {
        Some(hex_str) => hex_str,
        None => &buf,
    };

    let bytes = hex::decode(hex_str).map_err(<D::Error as serde::de::Error>::custom)?;

    P::from_bytes(&bytes).map_err(<D::Error as serde::de::Error>::custom)
}

/// BlockId uniquely identifies a block
#[repr(transparent)]
#[derive(
    Copy,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    RlpDecodableWrapper,
    RlpEncodableWrapper,
)]
pub struct BlockId(pub Hash);

pub const GENESIS_BLOCK_ID: BlockId = BlockId(Hash([0_u8; 32]));

impl Debug for BlockId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:>02x}{:>02x}..{:>02x}{:>02x}",
            self.0[0], self.0[1], self.0[30], self.0[31]
        )
    }
}

/// Stake is the amount of tokens the validator deposited for validating
/// privileges and earning transaction fees
#[repr(transparent)]
#[derive(
    Debug,
    Copy,
    Clone,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
    RlpEncodableWrapper,
    RlpDecodableWrapper,
)]
pub struct Stake(
    #[serde(serialize_with = "serialize_u256")]
    #[serde(deserialize_with = "deserialize_u256")]
    pub U256,
);

pub fn serialize_u256<S>(num: &U256, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if num == &U256::ZERO {
        serializer.serialize_str("0x0")
    } else {
        let u256_bytes = num.to_be_bytes::<32>();
        let hex_str = "0x".to_string() + hex::encode(u256_bytes).trim_start_matches("0");
        serializer.serialize_str(&hex_str)
    }
}

pub fn deserialize_u256<'de, D>(deserializer: D) -> Result<U256, D::Error>
where
    D: Deserializer<'de>,
{
    match <Value as Deserialize>::deserialize(deserializer)? {
        Value::Number(num) => {
            if let Some(num_u128) = num.as_u128() {
                Ok(U256::from(num_u128))
            } else {
                Err(serde::de::Error::custom(
                    "number either negative or too big",
                ))
            }
        }
        Value::String(string) => {
            U256::from_str(&string).map_err(<D::Error as serde::de::Error>::custom)
        }
        _ => Err(serde::de::Error::custom("invalid u256 type")),
    }
}

impl Stake {
    pub const ZERO: Stake = Stake(U256::ZERO);
    pub const ONE: Stake = Stake(U256::ONE);

    pub fn checked_div(self, divisor: Stake) -> Option<f64> {
        if divisor.0.is_zero() {
            return None;
        }
        Some(self / divisor)
    }
}

impl From<u64> for Stake {
    fn from(value: u64) -> Self {
        Stake(U256::from(value))
    }
}

impl From<U256> for Stake {
    fn from(value: U256) -> Self {
        Stake(value)
    }
}

impl Add for Stake {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Stake(
            self.0
                .checked_add(rhs.0)
                .unwrap_or_else(|| panic!("{:?} + {:?}", self.0, rhs.0)),
        )
    }
}

impl Sub for Stake {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Stake(
            self.0
                .checked_sub(rhs.0)
                .unwrap_or_else(|| panic!("{:?} - {:?}", self.0, rhs.0)),
        )
    }
}

impl AddAssign for Stake {
    fn add_assign(&mut self, rhs: Self) {
        *self = *self + rhs
    }
}

impl SubAssign for Stake {
    fn sub_assign(&mut self, rhs: Self) {
        *self = *self - rhs
    }
}

impl Div<Stake> for Stake {
    type Output = f64;

    fn div(self, rhs: Stake) -> f64 {
        f64::from(self.0) / f64::from(rhs.0)
    }
}

impl std::iter::Sum for Stake {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Stake::ZERO, |a, b| a + b)
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
#[derive(Debug)]
pub enum RouterTarget<P: PubKey> {
    Broadcast(Epoch),
    Raptorcast(Epoch), // sharded raptor-aware broadcast
    PointToPoint(NodeId<P>),
    TcpPointToPoint {
        to: NodeId<P>,
        completion: Option<futures::channel::oneshot::Sender<()>>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub enum UdpPriority {
    High = 0,
    Regular = 1,
}

impl TryFrom<usize> for UdpPriority {
    type Error = &'static str;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(UdpPriority::High),
            1 => Ok(UdpPriority::Regular),
            _ => Err("invalid priority index"),
        }
    }
}

/// Trait for use in tests to populate structs where the value of the fields is not relevant
pub trait DontCare {
    fn dont_care() -> Self;
}

impl<T: Default> DontCare for T {
    fn dont_care() -> Self {
        T::default()
    }
}

pub struct DropTimer<F>
where
    F: Fn(Duration),
{
    start: Instant,
    threshold: Duration,
    trip: F,
}

impl<F> DropTimer<F>
where
    F: Fn(Duration),
{
    pub fn start(threshold: Duration, trip: F) -> Self {
        Self {
            start: Instant::now(),
            threshold,
            trip,
        }
    }
}

impl<F> Drop for DropTimer<F>
where
    F: Fn(Duration),
{
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        if elapsed <= self.threshold {
            return;
        }
        (self.trip)(elapsed)
    }
}

pub trait ExecutionProtocol:
    Debug + Clone + PartialEq + Eq + Send + Sync + Unpin + Encodable + Decodable + Serialize + 'static
{
    /// inputs to execution
    type ProposedHeader: Debug
        + Clone
        + PartialEq
        + Eq
        + Send
        + Sync
        + Unpin
        + Encodable
        + Decodable
        + Serialize
        + for<'de> Deserialize<'de>
        // TODO delete Default once null blocks are gone
        + Default;
    type Body: Debug
        + Clone
        + PartialEq
        + Eq
        + Send
        + Sync
        + Unpin
        + Encodable
        + Decodable
        + Serialize
        + for<'de> Deserialize<'de>
        // TODO delete Default once null blocks are gone
        + Default;

    /// output of execution
    type FinalizedHeader: FinalizedHeader;
}

pub trait FinalizedHeader:
    Debug
    + Clone
    + PartialEq
    + Eq
    + Send
    + Sync
    + Unpin
    + Encodable
    + Decodable
    + Serialize
    + for<'de> Deserialize<'de>
{
    fn seq_num(&self) -> SeqNum;
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Deserialize, Serialize, RlpEncodable, RlpDecodable)]
pub struct MonadVersion {
    pub protocol_version: u32,
    pub client_version_maj: u16,
    pub client_version_min: u16,
    pub hash_version: u16,
    pub serialize_version: u16,
}

impl MonadVersion {
    pub fn version() -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            client_version_maj: CLIENT_MAJOR_VERSION,
            client_version_min: CLIENT_MINOR_VERSION,
            hash_version: HASH_VERSION,
            serialize_version: SERIALIZE_VERSION,
        }
    }
}

/// A `Vec<T>` that enforces a maximum length during RLP deserialization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LimitedVec<T, const N: usize>(pub Vec<T>);

impl<T, const N: usize> Default for LimitedVec<T, N> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl<T, const N: usize> LimitedVec<T, N> {
    pub fn into_inner(self) -> Vec<T> {
        self.0
    }
}

impl<T: Encodable, const N: usize> Encodable for LimitedVec<T, N> {
    fn length(&self) -> usize {
        alloy_rlp::list_length(&self.0)
    }

    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        alloy_rlp::encode_list(&self.0, out);
    }
}

impl<T: Decodable, const N: usize> Decodable for LimitedVec<T, N> {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let payload = &mut alloy_rlp::Header::decode_bytes(buf, true)?;
        let mut vec = Vec::new();
        while !payload.is_empty() {
            if vec.len() >= N {
                return Err(alloy_rlp::Error::Custom("list exceeds maximum length"));
            }
            vec.push(T::decode(payload)?);
        }
        Ok(Self(vec))
    }
}

impl<T, const N: usize> std::ops::Deref for LimitedVec<T, N> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T, const N: usize> std::ops::DerefMut for LimitedVec<T, N> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T, const N: usize> From<Vec<T>> for LimitedVec<T, N> {
    fn from(vec: Vec<T>) -> Self {
        Self(vec)
    }
}

impl<T, const N: usize> IntoIterator for LimitedVec<T, N> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[cfg(test)]
mod test {
    use alloy_rlp::{Decodable, Encodable};
    use serde::de::{
        value::{Error as SerdeError, StrDeserializer, U64Deserializer},
        IntoDeserializer,
    };
    use serde_test::{assert_ser_tokens, Token};
    use test_case::test_case;

    use super::*;

    #[test_case(SeqNum(0), Epoch(1), SeqNum(100); "sn_0_epoch_1")]
    #[test_case(SeqNum(1), Epoch(1), SeqNum(100); "sn_1_epoch_1")]
    #[test_case(SeqNum(99), Epoch(1), SeqNum(100); "sn_99_epoch_1")]
    #[test_case(SeqNum(100), Epoch(2), SeqNum(100); "sn_100_epoch_2")]
    #[test_case(SeqNum(199), Epoch(2), SeqNum(100); "sn_199_epoch_2")]
    #[test_case(SeqNum(200), Epoch(3), SeqNum(100); "sn_200_epoch_3")]

    fn test_epoch_conversion(seq_num: SeqNum, expected_epoch: Epoch, epoch_length: SeqNum) {
        assert_eq!(seq_num.to_epoch(epoch_length), expected_epoch);
    }

    #[test]
    fn test_rlp_block_id() {
        let bid = BlockId(Hash([0xac; 32]));
        let raw = [0xac; 32];

        let mut bid_buf = vec![];
        bid.encode(&mut bid_buf);

        let mut raw_buf = vec![];
        raw.encode(&mut raw_buf);

        assert_eq!(bid_buf, raw_buf);
    }

    #[test]
    fn test_serialize_stake() {
        let zero_stake = Stake::ZERO;
        let expected_tokens = vec![Token::NewtypeStruct { name: "Stake" }, Token::String("0x0")];
        assert_ser_tokens(&zero_stake, &expected_tokens);

        let zero_stake = Stake::ONE;
        let expected_tokens = vec![Token::NewtypeStruct { name: "Stake" }, Token::String("0x1")];
        assert_ser_tokens(&zero_stake, &expected_tokens);

        let mil_stake = Stake::from(1_000_000);
        let expected_tokens = vec![
            Token::NewtypeStruct { name: "Stake" },
            Token::String("0xf4240"),
        ];
        assert_ser_tokens(&mil_stake, &expected_tokens);
    }

    #[test]
    fn test_deserialize_u256() {
        let zero = 0_u64;
        let deserializer: U64Deserializer<SerdeError> = zero.into_deserializer();
        assert_eq!(deserialize_u256(deserializer), Ok(U256::ZERO));

        let one = 1_u64;
        let deserializer: U64Deserializer<SerdeError> = one.into_deserializer();
        assert_eq!(deserialize_u256(deserializer), Ok(U256::ONE));

        let u64_max = u64::MAX;
        let deserializer: U64Deserializer<SerdeError> = u64_max.into_deserializer();
        assert_eq!(deserialize_u256(deserializer), Ok(U256::from(u64_max)));

        let deserializer: StrDeserializer<SerdeError> = "0".into_deserializer();
        assert_eq!(deserialize_u256(deserializer), Ok(U256::ZERO));

        let deserializer: StrDeserializer<SerdeError> = "0x0".into_deserializer();
        assert_eq!(deserialize_u256(deserializer), Ok(U256::ZERO));

        let deserializer: StrDeserializer<SerdeError> = "1".into_deserializer();
        assert_eq!(deserialize_u256(deserializer), Ok(U256::ONE));

        let deserializer: StrDeserializer<SerdeError> = "0x1".into_deserializer();
        assert_eq!(deserialize_u256(deserializer), Ok(U256::ONE));

        let deserializer: StrDeserializer<SerdeError> = "10".into_deserializer();
        assert_eq!(deserialize_u256(deserializer), Ok(U256::from(10)));

        let deserializer: StrDeserializer<SerdeError> = "0xA".into_deserializer();
        assert_eq!(deserialize_u256(deserializer), Ok(U256::from(10)));

        let deserializer: StrDeserializer<SerdeError> = "0x10".into_deserializer();
        assert_eq!(deserialize_u256(deserializer), Ok(U256::from(16)));
    }

    #[test]
    fn test_limited_vec_roundtrip() {
        let original: LimitedVec<u32, 5> = vec![1u32, 2, 3].into();
        let mut buf = Vec::new();
        original.encode(&mut buf);

        let decoded = LimitedVec::<u32, 5>::decode(&mut buf.as_slice()).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_limited_vec_rejects_overflow() {
        let exact: LimitedVec<u32, 100> = vec![1u32, 2, 3].into();
        let mut buf = Vec::new();
        exact.encode(&mut buf);
        let decoded = LimitedVec::<u32, 3>::decode(&mut buf.as_slice()).unwrap();
        assert_eq!(decoded.len(), 3);

        // N+1 elements should fail
        let over: LimitedVec<u32, 100> = vec![1u32, 2, 3, 4].into();
        let mut buf = Vec::new();
        over.encode(&mut buf);
        assert!(LimitedVec::<u32, 3>::decode(&mut buf.as_slice()).is_err());
    }

    #[test]
    fn test_limited_vec_empty() {
        let empty: LimitedVec<u32, 0> = Vec::new().into();
        let mut buf = Vec::new();
        empty.encode(&mut buf);

        let decoded = LimitedVec::<u32, 0>::decode(&mut buf.as_slice()).unwrap();
        assert!(decoded.is_empty());
    }
}
