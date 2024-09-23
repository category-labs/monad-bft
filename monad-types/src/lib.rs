pub mod convert;

use std::{
    error::Error,
    io,
    ops::{Add, AddAssign, Div, Rem, Sub, SubAssign},
    str::FromStr,
    time::{Duration, Instant},
};

pub use monad_crypto::hasher::Hash;
use monad_crypto::{
    certificate_signature::PubKey,
    hasher::{Hashable, Hasher},
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use zerocopy::AsBytes;

pub const GENESIS_SEQ_NUM: SeqNum = SeqNum(0);

/// Consensus round
#[repr(transparent)]
#[derive(Copy, Clone, Eq, Hash, Ord, PartialEq, PartialOrd, AsBytes, Serialize, Deserialize)]
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
#[derive(Copy, Clone, Hash, Eq, PartialEq, PartialOrd, Ord, Serialize, Deserialize, AsBytes)]
pub struct Epoch(pub u64);

impl AsRef<[u8]> for Epoch {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

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
#[derive(Copy, Clone, Eq, Hash, Ord, PartialEq, PartialOrd, AsBytes, Deserialize)]
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

impl std::fmt::Debug for SeqNum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl SeqNum {
    /// Compute the epoch that the sequence number belong to. It does NOT mean
    /// that the block is proposed in the epoch
    ///
    /// [0, val_set_update_interval] -> Epoch 1
    /// [val_set_update_interval + 1, 2 * val_set_update_interval] -> Epoch 2
    /// ... The first epoch is one block longer than all other ones
    pub fn to_epoch(&self, val_set_update_interval: SeqNum) -> Epoch {
        Epoch((self.0.saturating_sub(1) / val_set_update_interval.0) + 1)
    }

    /// The first epoch starts with SeqNum 0 and end with 100. Every epoch
    /// afterwards starts at SeqNum (X * interval) + 1 and end with (X *
    /// interval + interval)
    ///
    /// This tells us what the boundary block of the epoch is. Note that this only indicates when
    /// the next epoch's round is scheduled.
    pub fn is_epoch_end(&self, val_set_update_interval: SeqNum) -> bool {
        *self % val_set_update_interval == SeqNum(0) && *self != SeqNum(0)
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
#[derive(Copy, Clone, Hash, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct NodeId<P: PubKey> {
    #[serde(serialize_with = "serialize_pubkey::<_, P>")]
    #[serde(deserialize_with = "deserialize_pubkey::<_, P>")]
    #[serde(bound = "P:PubKey")]
    #[serde(rename(serialize = "node_id", deserialize = "node_id"))]
    // Outer struct always flatten this struct, thus renaming to node_id
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

fn serialize_pubkey<S, P>(pubkey: &P, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    P: PubKey,
{
    let hex_str = "0x".to_string() + &hex::encode(pubkey.bytes());
    serializer.serialize_str(&hex_str)
}

fn deserialize_pubkey<'de, D, P>(deserializer: D) -> Result<P, D::Error>
where
    D: Deserializer<'de>,
    P: PubKey,
{
    let buf = <String as Deserialize>::deserialize(deserializer)?;
    let bytes = if let Some(("", hex_str)) = buf.split_once("0x") {
        hex::decode(hex_str.to_owned()).map_err(<D::Error as serde::de::Error>::custom)?
    } else {
        return Err(<D::Error as serde::de::Error>::custom("Missing hex prefix"));
    };

    P::from_bytes(&bytes).map_err(<D::Error as serde::de::Error>::custom)
}

/// BlockId uniquely identifies a block
#[repr(transparent)]
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
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
    Broadcast(Epoch),
    Raptorcast(Epoch), // sharded raptor-aware broadcast
    PointToPoint(NodeId<P>),
    TcpPointToPoint(NodeId<P>),
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

/// Trait for use in tests to populate structs where the value of the fields is not relevant
pub trait DontCare {
    fn dont_care() -> Self;
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

#[cfg(test)]
mod test {
    use monad_crypto::NopPubKey;
    use serde_json::from_str;
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
    fn test_round_arithmetic() {
        let r1 = Round(1);
        let r2 = Round(2);
        let mut r3 = Round(3);

        assert!(r1.0 == 1);
        assert!(r2.0 == 2);
        assert!(r1 + r2 == Round(3));

        r3 += r1;
        assert!(r3 == Round(4));
    }

    #[test]
    fn test_seqnum_arithmetic() {
        let s1 = SeqNum(8);
        let s2 = SeqNum(4);
        assert!(s1 / s2 == SeqNum(2));
    }

    #[test]
    fn test_stake_arithmetic() {
        let s1 = Stake(1);
        let s2 = Stake(2);
        assert!(s1 + s2 == Stake(3));
        assert!(s2 - s1 == Stake(1));

        let mut s3 = Stake(3);
        s3 -= s1;
        assert!(s3 == Stake(2));
    }

    #[test]
    fn test_epoch_calcs() {
        let val_set_update_interval = SeqNum(100);

        let seq_num = SeqNum(100);
        assert!(seq_num.is_epoch_end(val_set_update_interval));
        assert!(seq_num.get_locked_epoch(val_set_update_interval) == Epoch(3));

        let seq_num = SeqNum(200);
        assert!(seq_num.is_epoch_end(val_set_update_interval));
        assert!(seq_num.get_locked_epoch(val_set_update_interval) == Epoch(4));
    }

    #[test]
    #[should_panic]
    fn test_locked_epoch_non_epoch_end() {
        let val_set_update_interval = SeqNum(100);
        let seq_num = SeqNum(150);
        seq_num.get_locked_epoch(val_set_update_interval);
    }

    #[test]
    #[should_panic]
    fn test_locked_epoch_genesis() {
        let val_set_update_interval = SeqNum(100);
        let seq_num = SeqNum(0);
        seq_num.get_locked_epoch(val_set_update_interval);
    }

    #[test]
    fn test_deserialize_missing_hex_prefix() {
        let json = r#"{"node_id": "0011223344556677"}"#;
        let result = from_str::<NodeId<NopPubKey>>(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_serialize_pubkey() {
        let test_bytes = [3u8; 32];
        let test_pubkey = NopPubKey::from_bytes(&test_bytes).unwrap();
        let node_id = NodeId::new(test_pubkey);

        let serialized = Serializable::serialize(&node_id);
        assert_eq!(serialized, node_id);
    }

    #[test]
    fn test_deserialize_pubkey() {
        let test_bytes = [3u8; 32];
        let test_pubkey = NopPubKey::from_bytes(&test_bytes).unwrap();

        let deserialized = NopPubKey::deserialize(&test_pubkey).unwrap();
        assert_eq!(deserialized, NopPubKey::from_bytes(&test_bytes).unwrap());
    }

    #[test]
    fn test_drop_timer_drops() {
        let timer = DropTimer::start(Duration::from_millis(10), |_| {
            panic!();
        });
    }

    #[test]
    #[should_panic]
    fn test_drop_time_elapses() {
        let timer = DropTimer::start(Duration::from_millis(10), |_| {
            panic!();
        });
        std::thread::sleep(Duration::from_millis(20));
    }
}
