use std::{
    error::Error,
    fmt::Debug,
    io,
    ops::{Add, AddAssign, Div, Rem, Sub, SubAssign},
    str::FromStr,
    time::{Duration, Instant},
};

use alloy_rlp::{
    Decodable, Encodable, RlpDecodable, RlpDecodableWrapper, RlpEncodable, RlpEncodableWrapper,
};
use monad_crypto::certificate_signature::PubKey;
pub use monad_crypto::hasher::Hash;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use zerocopy::AsBytes;

pub mod convert;

pub const GENESIS_SEQ_NUM: SeqNum = SeqNum(0);
pub const GENESIS_ROUND: Round = Round(0);

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
    AsBytes,
    Serialize,
    Deserialize,
    RlpEncodableWrapper,
    RlpDecodableWrapper,
)]
pub struct Round(pub u64);

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
        self.0.fmt(f)
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
    AsBytes,
    RlpEncodableWrapper,
    RlpDecodableWrapper,
)]
pub struct Epoch(pub u64);

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

impl Debug for Epoch {
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
#[derive(
    Copy,
    Clone,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    AsBytes,
    Deserialize,
    RlpEncodableWrapper,
    RlpDecodableWrapper,
)]
pub struct SeqNum(
    // FIXME get rid of this, we won't have u64::MAX
    /// Some serde libraries e.g. toml represent numbers as i64 so they don't
    /// support serializing u64::MAX, which is used as the genesis qc sequence
    /// number. Converting to string first gets around this limitation
    #[serde(deserialize_with = "deserialize_big_u64")]
    pub u64,
);

impl SeqNum {
    pub const MIN: SeqNum = SeqNum(u64::MIN);
    pub const MAX: SeqNum = SeqNum(u64::MAX);
}

impl Serialize for SeqNum {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0.to_string())
    }
}

fn deserialize_big_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let buf = <std::string::String as Deserialize>::deserialize(deserializer)?;
    u64::from_str(&buf).map_err(<D::Error as serde::de::Error>::custom)
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
        self.0.fmt(f)
    }
}

impl SeqNum {
    /// Compute the epoch that the sequence number belong to. It does NOT mean
    /// that the block is proposed in the epoch
    ///
    /// [0, val_set_update_interval-1] -> Epoch 1
    /// [val_set_update_interval, (2 * val_set_update_interval)-1] -> Epoch 2
    pub fn to_epoch(&self, val_set_update_interval: SeqNum) -> Epoch {
        Epoch((self.0 / val_set_update_interval.0) + 1)
    }

    /// This tells us what the boundary block of the epoch is. Note that this only indicates when
    /// the next epoch's round is scheduled.
    pub fn is_epoch_end(&self, val_set_update_interval: SeqNum) -> bool {
        *self % val_set_update_interval == val_set_update_interval - SeqNum(1)
    }

    /// Get the epoch number whose validator set is locked by this block. Should
    /// only be called on the boundary block sequence number
    ///
    /// Current design locks the info for epoch n + 2 by the end of epoch n. The
    /// validators have an entire epoch to prepare themselves for any duties
    pub fn get_locked_epoch(&self, val_set_update_interval: SeqNum) -> Epoch {
        assert!(self.is_epoch_end(val_set_update_interval));
        (*self).to_epoch(val_set_update_interval) + Epoch(2)
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
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct Stake(pub i64);

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
    Broadcast(Epoch),
    Raptorcast(Epoch), // sharded raptor-aware broadcast
    PointToPoint(NodeId<P>),
    TcpPointToPoint(NodeId<P>),
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
    Debug + Clone + PartialEq + Eq + Send + Sync + Unpin + Encodable + Decodable + 'static
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
        // TODO delete Default once null blocks are gone
        + Default;
    type Body: Debug
        + PartialEq
        + Eq
        + Send
        + Sync
        + Unpin
        + Encodable
        + Decodable
        // TODO delete Default once null blocks are gone
        + Default;

    /// output of execution
    type FinalizedHeader: FinalizedHeader;
}

pub trait FinalizedHeader:
    Debug + Clone + PartialEq + Eq + Send + Sync + Unpin + Encodable + Decodable
{
    fn seq_num(&self) -> SeqNum;
}

pub trait MockableFinalizedHeader: Sized {
    fn from_seq_num(seq_num: SeqNum) -> Self;
}

pub trait MockableProposedHeader: Sized {
    fn create(seq_num: SeqNum, timestamp_ns: u128, mix_hash: [u8; 32]) -> Self;
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

#[cfg(test)]
mod test {
    use alloy_rlp::Encodable;
    use test_case::test_case;

    use super::*;

    #[test_case(SeqNum(0), Epoch(1), SeqNum(100); "sn_0_epoch_1")]
    #[test_case(SeqNum(1), Epoch(1), SeqNum(100); "sn_1_epoch_1")]
    #[test_case(SeqNum(99), Epoch(1), SeqNum(100); "sn_99_epoch_1")]
    #[test_case(SeqNum(100), Epoch(2), SeqNum(100); "sn_100_epoch_2")]
    #[test_case(SeqNum(199), Epoch(2), SeqNum(100); "sn_199_epoch_2")]
    #[test_case(SeqNum(200), Epoch(3), SeqNum(100); "sn_200_epoch_3")]

    fn test_epoch_conversion(
        seq_num: SeqNum,
        expected_epoch: Epoch,
        val_set_update_interval: SeqNum,
    ) {
        assert_eq!(seq_num.to_epoch(val_set_update_interval), expected_epoch);
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
}
