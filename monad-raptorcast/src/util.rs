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
    cell::OnceCell,
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt,
    net::SocketAddr,
    num::NonZero,
    ops::Range,
    rc::Rc,
};

use bytes::Bytes;
use fixed::{types::extra::U11, FixedU16};
use iset::IntervalMap;
use monad_crypto::{
    certificate_signature::{CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey},
    hasher::{Hasher, HasherType},
};
use monad_types::{Epoch, NodeId, Round, RoundSpan};
use monad_validator::validator_set::{ValidatorSet, ValidatorSetType as _};

use crate::udp::GroupId;

// Argument for raptorcast send
#[derive(Debug, Clone, Copy)]
pub enum BuildTarget<'a, PT: PubKey> {
    // broadcast a message to the validators where each validator gets
    // the full chunks of the raptor-coded message
    Broadcast(&'a ValidatorSet<PT>),
    // raptorcast to the validators, chunks distributed by their
    // proportion of stakes.
    Raptorcast(&'a ValidatorSet<PT>),
    // unicast message as raptor-coded chunks to a single recipient
    PointToPoint(&'a NodeId<PT>),
    // raptorcast to a set of full nodes, assuming equal stake
    // distribution
    FullNodeRaptorCast(&'a SecondaryGroup<PT>),
}

impl<'a, PT: PubKey> BuildTarget<'a, PT> {
    pub fn iter(&self) -> Box<dyn Iterator<Item = &NodeId<PT>> + '_> {
        match self {
            BuildTarget::Broadcast(valset) => Box::new(valset.get_members().keys()),
            BuildTarget::Raptorcast(valset) => Box::new(valset.get_members().keys()),
            BuildTarget::PointToPoint(node_id) => Box::new(std::iter::once(*node_id)),
            BuildTarget::FullNodeRaptorCast(group) => Box::new(group.iter()),
        }
    }
}

pub fn compute_hash<PT>(id: &NodeId<PT>) -> NodeIdHash
where
    PT: PubKey,
{
    let full_hash = compute_full_hash(&id.pubkey().bytes());
    HexBytes(full_hash.0[..20].try_into().expect("20 bytes"))
}

pub fn compute_app_message_hash(app_msg: &[u8]) -> AppMessageHash {
    let full_hash = compute_full_hash(app_msg);
    HexBytes(full_hash.0[..20].try_into().expect("20 bytes"))
}

pub fn compute_full_hash(bytes: &[u8]) -> monad_crypto::hasher::Hash {
    let mut hasher = HasherType::new();
    hasher.update(bytes);
    hasher.hash()
}

#[derive(Copy, Clone, Hash, Eq, Ord, PartialEq, PartialOrd)]
pub struct HexBytes<const N: usize>(pub [u8; N]);
impl<const N: usize> std::fmt::Debug for HexBytes<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "0x")?;
        for byte in self.0 {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

impl<const N: usize> HexBytes<N> {
    pub fn as_slice(&self) -> &[u8; N] {
        &self.0
    }
}

pub type NodeIdHash = HexBytes<20>;
pub type AppMessageHash = HexBytes<20>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BroadcastMode {
    Primary,
    Secondary,
    Unspecified,
}

// Invariance: The group must be non-empty.
#[derive(Debug, Clone, Default)]
pub struct SecondaryGroup<PT: PubKey> {
    full_nodes: BTreeSet<NodeId<PT>>,
}

impl<PT: PubKey> SecondaryGroup<PT> {
    // SAFETY: The caller must ensure that full_nodes set is non-empty.
    pub fn new_unchecked(full_nodes: BTreeSet<NodeId<PT>>) -> Self {
        Self { full_nodes }
    }

    pub fn new(full_nodes: BTreeSet<NodeId<PT>>) -> Option<Self> {
        if full_nodes.is_empty() {
            return None;
        }
        Some(Self { full_nodes })
    }

    pub fn iter(&self) -> impl Iterator<Item = &NodeId<PT>> + '_ {
        self.full_nodes.iter()
    }

    pub fn len(&self) -> NonZero<usize> {
        NonZero::new(self.full_nodes.len()).unwrap()
    }

    pub fn is_member(&self, node_id: &NodeId<PT>) -> bool {
        self.full_nodes.contains(node_id)
    }
}

#[derive(Debug, Clone)]
pub struct SecondaryGroupAssignment<PT: PubKey> {
    publisher_id: NodeId<PT>,
    round_span: RoundSpan,
    group: SecondaryGroup<PT>,
}

impl<PT: PubKey> SecondaryGroupAssignment<PT> {
    pub fn new(publisher_id: NodeId<PT>, round_span: RoundSpan, group: SecondaryGroup<PT>) -> Self {
        Self {
            publisher_id,
            round_span,
            group,
        }
    }

    pub fn group(&self) -> &SecondaryGroup<PT> {
        &self.group
    }

    pub fn is_member(&self, node_id: &NodeId<PT>) -> bool {
        self.group.is_member(node_id)
    }

    pub fn publisher_id(&self) -> &NodeId<PT> {
        &self.publisher_id
    }

    pub fn round_span(&self) -> &RoundSpan {
        &self.round_span
    }
}

// An interval map from RoundSpan to SecondaryGroup.
//
// Invariance: Each group's round span must be non-overlapping.
pub struct SecondaryGroupMap<PT: PubKey> {
    group_map: IntervalMap<Round, SecondaryGroup<PT>>,
}

impl<PT: PubKey> Default for SecondaryGroupMap<PT> {
    fn default() -> Self {
        Self {
            group_map: IntervalMap::new(),
        }
    }
}

impl<PT: PubKey> SecondaryGroupMap<PT> {
    pub fn get(&self, round: Round) -> Option<&SecondaryGroup<PT>> {
        let mut iter = self.group_map.overlap(round);
        let (_range, v) = iter.next()?;
        Some(v)
    }

    pub fn get_current_or_next(&self, round: Round) -> Option<&SecondaryGroup<PT>> {
        if let Some(group) = self.get(round) {
            return Some(group);
        }
        let (span, group) = self.group_map.smallest()?;
        if round < span.start {
            return Some(group);
        }
        None
    }

    #[must_use]
    // the caller should check the return value to ensure the group is successfully inserted.
    // returns None if there is an overlap with existing groups.
    pub fn try_insert(&mut self, round_span: RoundSpan, group: SecondaryGroup<PT>) -> Option<()> {
        let round_span: Range<_> = round_span.into();
        if self.group_map.has_overlap(round_span.clone()) {
            return None;
        }
        self.group_map.insert(round_span, group);
        Some(())
    }

    // cull groups that ends before or at round_cap
    pub fn delete_expired(&mut self, round_cap: Round) {
        while let Some((range, _)) = self.group_map.smallest() {
            if range.end <= round_cap {
                self.group_map.remove(range);
            } else {
                break;
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.group_map.is_empty()
    }
}

// The membership of a full node in multiple SecondaryGroupMaps, each
// lead by a validator.
pub struct FullNodeGroupMap<PT: PubKey> {
    map: HashMap<NodeId<PT>, SecondaryGroupMap<PT>>,
}

impl<PT: PubKey> Default for FullNodeGroupMap<PT> {
    fn default() -> Self {
        Self {
            map: Default::default(),
        }
    }
}

impl<PT: PubKey> FullNodeGroupMap<PT> {
    pub fn get_group_map(&self, publisher_id: &NodeId<PT>) -> Option<&SecondaryGroupMap<PT>> {
        self.map.get(publisher_id)
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    #[must_use]
    pub fn try_insert(&mut self, assignment: SecondaryGroupAssignment<PT>) -> Option<()> {
        let group_map = self.map.entry(assignment.publisher_id).or_default();
        group_map.try_insert(assignment.round_span, assignment.group)
    }

    // cull groups that ends before or at round_cap
    pub fn delete_expired(&mut self, round_cap: Round) {
        self.map.retain(|_publisher_id, group_map| {
            group_map.delete_expired(round_cap);
            !group_map.is_empty()
        });
    }
}

// This type is definitionally an alias of the epoch-validators
// map. The semantic of ValidatorGroupMap restricted to the context a
// usage as a group map.
pub type ValidatorGroupMap<PT> = BTreeMap<Epoch, ValidatorSet<PT>>;

// The raptorcast group for a given group_id for the purpose of
// checking the validity of sender and for rebroadcasting.
pub enum BroadcastGroup<'a, PT: PubKey> {
    ValidatorGroup {
        self_id: &'a NodeId<PT>,
        author: &'a NodeId<PT>,
        group: &'a ValidatorSet<PT>,
    },
    FullNodeGroup {
        self_id: &'a NodeId<PT>,
        publisher: &'a NodeId<PT>,
        group: &'a SecondaryGroup<PT>,
    },
}

#[derive(Debug)]
pub enum BroadcastGroupError {
    // The specified group_id does not correspond to any known group.
    GroupNotFound,
    // The author is not a member of the specified group.
    InvalidAuthor,
}

impl<'a, PT: PubKey> BroadcastGroup<'a, PT> {
    // Return a known valid group of the given group_id.
    //
    // Note: the self_id is not required to be a member of the
    // group. As a result, non-validator nodes can be allowed to
    // receive/decode messages from a valid group. We can add this
    // restriction in the future if needed.
    pub fn from_group_id(
        group_id: GroupId,
        self_id: &'a NodeId<PT>,
        // The signer the raptorcast message, not necessarily the sender.
        author: &'a NodeId<PT>,
        validator_group_map: &'a ValidatorGroupMap<PT>,
        full_node_group_map: &'a FullNodeGroupMap<PT>,
    ) -> Result<Self, BroadcastGroupError> {
        match group_id {
            GroupId::Primary(epoch) => {
                let group = validator_group_map
                    .get(&epoch)
                    .ok_or(BroadcastGroupError::GroupNotFound)?;
                if !group.is_member(author) {
                    return Err(BroadcastGroupError::InvalidAuthor);
                }
                Ok(BroadcastGroup::ValidatorGroup {
                    self_id,
                    author,
                    group,
                })
            }

            GroupId::Secondary(round) => {
                let publisher = author;
                let group = full_node_group_map
                    .get_group_map(publisher)
                    .ok_or(BroadcastGroupError::GroupNotFound)?
                    // FIXME: should use `get` method. `get_current_or_next`
                    // implements the old behavior, which can be hit if full node is
                    // upgraded before the validator. It'll be removed after the
                    // upgrade
                    .get_current_or_next(round)
                    .ok_or(BroadcastGroupError::GroupNotFound)?;
                Ok(BroadcastGroup::FullNodeGroup {
                    self_id,
                    publisher,
                    group,
                })
            }
        }
    }

    pub fn is_sender_valid(&self, sender: &NodeId<PT>) -> bool {
        match self {
            // For Primary RC, the sender can be any one of the validators.
            BroadcastGroup::ValidatorGroup { group, .. } => group.is_member(sender),
            // For Secondary RC, the sender must be either the
            // publisher or someone in the group.
            BroadcastGroup::FullNodeGroup {
                group, publisher, ..
            } => sender == *publisher || group.is_member(sender),
        }
    }

    pub fn should_rebroadcast(&self, is_first_hop_recipient: bool) -> bool {
        // In both primary and secondary raptorcast, each chunk's
        // recipient is responsible for broadcast the chunk to all
        // peers.
        match self {
            BroadcastGroup::ValidatorGroup { self_id, group, .. } => {
                group.is_member(self_id) && is_first_hop_recipient
            }
            BroadcastGroup::FullNodeGroup { self_id, group, .. } => {
                group.is_member(self_id) && is_first_hop_recipient
            }
        }
    }

    // The caller must ensure should_rebroadcast(recipient) returns
    // true for the chunk.
    pub fn rebroadcasting_peers(&self) -> Box<dyn Iterator<Item = &NodeId<PT>> + '_> {
        match self {
            BroadcastGroup::ValidatorGroup {
                self_id,
                group,
                author,
            } => {
                let peers = group
                    .get_members()
                    .keys()
                    .filter(|nid| *nid != *self_id && *nid != *author);
                Box::new(peers)
            }
            BroadcastGroup::FullNodeGroup { self_id, group, .. } => {
                // Note: publisher is expected to not belong to the
                // group for secondary RC.
                let peers = group.iter().filter(|nid| *nid != *self_id);
                Box::new(peers)
            }
        }
    }
}

// Similar to std::iter::Extend trait but implemented for FnMut as
// well.
pub trait Collector<T> {
    fn push(&mut self, item: T);
    fn reserve(&mut self, _additional: usize) {}
}

impl<T> Collector<T> for Vec<T> {
    fn push(&mut self, item: T) {
        Vec::push(self, item)
    }

    fn reserve(&mut self, additional: usize) {
        Vec::reserve(self, additional)
    }
}

impl<F, T> Collector<T> for F
where
    F: FnMut(T),
{
    fn push(&mut self, item: T) {
        self(item)
    }
}

// expect collecting no message.
pub struct EmptyCollector;
impl<T> Collector<T> for EmptyCollector {
    fn push(&mut self, _item: T) {
        tracing::debug!("Unexpected message collected in EmptyCollector");
    }
}

// discards all collected messages.
pub struct BlackholeCollector;
impl<T> Collector<T> for BlackholeCollector {
    fn push(&mut self, _item: T) {}
}

// a database of socket addresses for nodes.
pub trait PeerAddrLookup<PT: PubKey> {
    fn lookup(&self, node_id: &NodeId<PT>) -> Option<SocketAddr>;
}

impl<PT: PubKey> PeerAddrLookup<PT> for std::collections::HashMap<NodeId<PT>, SocketAddr> {
    fn lookup(&self, node_id: &NodeId<PT>) -> Option<SocketAddr> {
        self.get(node_id).copied()
    }
}

// a specified socket address for a single node
#[derive(Debug, Clone, Copy)]
pub struct KnownSocketAddr<'a, PT: PubKey>(pub &'a NodeId<PT>, pub SocketAddr);

impl<PT: PubKey> PeerAddrLookup<PT> for KnownSocketAddr<'_, PT> {
    fn lookup(&self, node_id: &NodeId<PT>) -> Option<SocketAddr> {
        if self.0 == node_id {
            Some(self.1)
        } else {
            None
        }
    }
}

impl<PT: PubKey, F> PeerAddrLookup<PT> for F
where
    F: Fn(&NodeId<PT>) -> Option<SocketAddr>,
{
    fn lookup(&self, node_id: &NodeId<PT>) -> Option<SocketAddr> {
        self(node_id)
    }
}

impl<PT: PubKey, PL1, PL2> PeerAddrLookup<PT> for (&PL1, &PL2)
where
    PL1: PeerAddrLookup<PT>,
    PL2: PeerAddrLookup<PT>,
{
    fn lookup(&self, node_id: &NodeId<PT>) -> Option<SocketAddr> {
        self.0.lookup(node_id).or_else(|| self.1.lookup(node_id))
    }
}

impl<PT, T> PeerAddrLookup<PT> for std::sync::Arc<T>
where
    PT: PubKey,
    T: PeerAddrLookup<PT>,
{
    fn lookup(&self, node_id: &NodeId<PT>) -> Option<SocketAddr> {
        self.as_ref().lookup(node_id)
    }
}

/// Used in RaptorCast instance to lookup peer addresses with the peer discovery driver.
impl<ST: CertificateSignatureRecoverable, PD> PeerAddrLookup<CertificateSignaturePubKey<ST>>
    for std::sync::Mutex<monad_peer_discovery::driver::PeerDiscoveryDriver<PD>>
where
    PD: monad_peer_discovery::PeerDiscoveryAlgo<SignatureType = ST>,
{
    fn lookup(&self, node_id: &NodeId<CertificateSignaturePubKey<ST>>) -> Option<SocketAddr> {
        let guard = self.lock().ok()?;
        guard.lookup(node_id)
    }
}

impl<ST: CertificateSignatureRecoverable, PD> PeerAddrLookup<CertificateSignaturePubKey<ST>>
    for monad_peer_discovery::driver::PeerDiscoveryDriver<PD>
where
    PD: monad_peer_discovery::PeerDiscoveryAlgo<SignatureType = ST>,
{
    fn lookup(&self, node_id: &NodeId<CertificateSignaturePubKey<ST>>) -> Option<SocketAddr> {
        self.get_addr(node_id)
    }
}

// A cheaply cloned wrapper around a node_id with lazily-calculated
// hash and a lazily-lookedup socket address.
//
// Change to Arc if we need parallel processing.
#[derive(Clone)]
pub struct Recipient<PT: PubKey>(Rc<RecipientInner<PT>>);

impl<PT: PubKey> std::hash::Hash for Recipient<PT> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.node_hash().hash(state);
    }
}

impl<PT: PubKey> PartialEq for Recipient<PT> {
    fn eq(&self, other: &Self) -> bool {
        self.node_hash() == other.node_hash()
    }
}
impl<PT: PubKey> Eq for Recipient<PT> {}

impl<PT: PubKey> std::fmt::Debug for Recipient<PT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<node-{}>", &hex::encode(&self.node_hash()[..6]))?;
        if let Some(addr) = self.0.addr.get() {
            if let Some(addr) = addr {
                write!(f, "@{}", addr)?;
            } else {
                write!(f, "@<unknown>")?;
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
struct RecipientInner<PT: PubKey> {
    node_id: NodeId<PT>,
    node_hash: OnceCell<[u8; 20]>,
    addr: OnceCell<Option<SocketAddr>>,
}

impl Recipient<monad_crypto::NopPubKey> {
    // only used for testing
    #[cfg(test)]
    pub fn dummy(addr: Option<SocketAddr>) -> Self {
        let mut bytes = format!("{:?}", addr).into_bytes();
        bytes.resize(32, 0u8);

        let pubkey = monad_crypto::NopPubKey::from_bytes(&bytes).expect("pubkey");
        let recipient = Self::new(NodeId::new(pubkey));
        recipient.0.addr.set(addr).expect("addr not set");
        recipient
    }
}

impl<PT: PubKey> Recipient<PT> {
    pub fn new(node_id: NodeId<PT>) -> Self {
        let node_hash = OnceCell::new();
        let addr = OnceCell::new();
        let inner = RecipientInner {
            node_id,
            node_hash,
            addr,
        };
        Self(Rc::new(inner))
    }

    pub fn node_id(&self) -> &NodeId<PT> {
        &self.0.node_id
    }

    pub(crate) fn node_hash(&self) -> &[u8; 20] {
        self.0
            .node_hash
            .get_or_init(|| compute_hash(&self.0.node_id).0)
    }

    // Expect `lookup` or `set_addr` performed earlier, otherwise panic.
    #[allow(unused)]
    pub(crate) fn get_addr(&self) -> Option<SocketAddr> {
        *self.0.addr.get().expect("get addr called before lookup")
    }

    pub fn lookup(&self, handle: &(impl PeerAddrLookup<PT> + ?Sized)) -> &Option<SocketAddr> {
        self.0.addr.get_or_init(|| {
            let addr = handle.lookup(&self.0.node_id);
            if addr.is_none() {
                tracing::warn!("raptorcast: unknown address for node {}", self.0.node_id);
            }
            addr
        })
    }
}

#[cfg(test)]
pub struct DummyPeerLookup;

#[cfg(test)]
impl PeerAddrLookup<monad_crypto::NopPubKey> for DummyPeerLookup {
    fn lookup(&self, _node_id: &NodeId<monad_crypto::NopPubKey>) -> Option<SocketAddr> {
        panic!("recipient addr should be self contained")
    }
}

#[derive(Debug, Clone)]
pub struct UdpMessage<PT: PubKey> {
    pub recipient: Recipient<PT>,
    pub payload: Bytes,
    pub stride: usize,
}

// Represented as a fixed-point number with 11 fractional bits.
// Range: 0 to ~31.9995, Increments: ~0.000488
#[derive(Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct Redundancy(FixedU16<U11>);

impl Redundancy {
    pub const ZERO: Self = Self(FixedU16::ZERO);
    pub const MIN: Self = Self(FixedU16::MIN);
    pub const MAX: Self = Self(FixedU16::MAX);

    #[allow(unused)]
    const BITS: u32 = 16;
    const FRAC_BITS: u32 = 11;
    #[allow(unused)]
    const DELTA: Self = Self(FixedU16::DELTA);
    const MAX_MULTIPLIER: usize = usize::MAX / (u16::MAX as usize);

    // guaranteed to be lossless for num in [0,32).
    pub const fn from_u8(num: u8) -> Self {
        assert!((num as u16) <= u16::MAX >> Self::FRAC_BITS);
        Redundancy(FixedU16::from_bits((num as u16) << Self::FRAC_BITS))
    }

    // may round to the nearest representable number when needed
    pub fn from_f32(num: f32) -> Option<Self> {
        FixedU16::checked_from_num(num).map(Redundancy)
    }

    pub fn to_f32(&self) -> f32 {
        self.0.to_num()
    }

    pub const fn scale(&self, base: usize) -> Option<usize> {
        if base > Self::MAX_MULTIPLIER {
            return None;
        }
        let Some(scaled) = (self.0.to_bits() as usize).checked_mul(base) else {
            return None;
        };
        Some(scaled.div_ceil(1 << Self::FRAC_BITS))
    }
}

impl fmt::Debug for Redundancy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.to_f32().fmt(f)
    }
}

pub fn unix_ts_ms_now() -> u64 {
    std::time::UNIX_EPOCH
        .elapsed()
        .expect("time went backwards")
        .as_millis()
        .try_into()
        .expect("unix epoch doesn't fit in u64")
}

#[cfg(test)]
mod tests {
    use monad_crypto::certificate_signature::CertificateSignaturePubKey;
    use monad_secp::SecpSignature;
    use monad_testutil::signing::get_key;
    use monad_types::Stake;
    use monad_validator::validator_set::ValidatorSet;

    use super::*;
    use crate::udp::GroupId;
    type ST = SecpSignature;
    type PT = CertificateSignaturePubKey<ST>;

    // Creates a node id that we can refer to just from its seed
    fn nid(seed: u64) -> NodeId<PT> {
        let key_pair = get_key::<ST>(seed);
        let pub_key = key_pair.pubkey();
        NodeId::new(pub_key)
    }

    fn make_validator_set(node_ids: &[NodeId<PT>]) -> ValidatorSet<PT> {
        assert!(!node_ids.is_empty());
        let members = node_ids.iter().map(|id| (*id, Stake::ONE)).collect();
        ValidatorSet::new_unchecked(members)
    }

    #[test]
    fn test_valid_redundancy_range() {
        assert_eq!(Redundancy::MIN.to_f32(), 0.0);
        assert_eq!(Redundancy::MAX.to_f32(), 31.999512);
        assert_eq!(Redundancy::DELTA.to_f32(), 0.00048828125);
        assert_eq!(Redundancy::BITS, 16);

        assert_eq!(Redundancy::from_f32(2.5).map(|r| r.to_f32()), Some(2.5));
        assert_eq!(
            Redundancy::from_f32(2.1).map(|r| r.to_f32()),
            Some(2.1000977)
        );

        assert_eq!(Redundancy::from_u8(31).scale(100), Some(3100));
        assert_eq!(Redundancy::from_u8(1).scale(100), Some(100));
        assert_eq!(Redundancy::from_u8(2).scale(100), Some(200));
        assert_eq!(Redundancy::from_f32(2.5).unwrap().scale(100), Some(250));

        assert_eq!(Redundancy::from_u8(0).scale(100), Some(0));
        assert_eq!(Redundancy::MAX.scale(100), Some(3200));

        assert_eq!(
            Redundancy::MAX.scale(Redundancy::MAX_MULTIPLIER),
            // +1 because Redundancy::MAX is fractional, and the
            // resultant gets rounded up
            Some((usize::MAX >> Redundancy::FRAC_BITS) + 1)
        );
        assert_eq!(Redundancy::MAX.scale(Redundancy::MAX_MULTIPLIER + 1), None);

        assert!((u16::MAX as usize)
            .checked_mul(Redundancy::MAX_MULTIPLIER)
            .is_some());
        assert!((u16::MAX as usize)
            .checked_mul(Redundancy::MAX_MULTIPLIER + 1)
            .is_none());
    }

    #[test]
    fn test_known_socket_addr() {
        let node1 = nid(1);
        let node2 = nid(2);
        let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();

        let lookup = KnownSocketAddr(&node1, addr);

        assert_eq!(lookup.lookup(&node1), Some(addr));
        assert_eq!(lookup.lookup(&node2), None);
    }

    #[test]
    fn test_peer_addr_lookup_closure() {
        let node1 = nid(1);
        let node2 = nid(2);
        let addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();

        let lookup_fn = |node_id: &NodeId<PT>| {
            if node_id == &node1 {
                Some(addr)
            } else {
                None
            }
        };

        assert_eq!(lookup_fn.lookup(&node1), Some(addr));
        assert_eq!(lookup_fn.lookup(&node2), None);
    }

    #[test]
    fn test_secondary_group() {
        // membership check
        let group = SecondaryGroup::new([nid(1), nid(2), nid(3)].into_iter().collect()).unwrap();
        assert!(group.is_member(&nid(1)));
        assert!(group.is_member(&nid(2)));
        assert!(group.is_member(&nid(3)));
        assert!(!group.is_member(&nid(4)));
        assert_eq!(group.len().get(), 3);

        // invariance
        let group = SecondaryGroup::<PT>::new([].into_iter().collect());
        assert!(group.is_none());
    }

    #[test]
    fn test_secondary_group_assignment_accessors() {
        let self_id = nid(1);
        let publisher_id = nid(10);
        let round_span = RoundSpan::new(Round(5), Round(10)).unwrap();
        let group = SecondaryGroup::new([self_id, nid(2), nid(3)].into_iter().collect()).unwrap();
        let assignment = SecondaryGroupAssignment::new(publisher_id, round_span, group);

        assert_eq!(assignment.publisher_id(), &publisher_id);
        assert_eq!(assignment.round_span(), &round_span);
        assert!(assignment.is_member(&self_id));
        assert!(assignment.is_member(&nid(2)));
        assert!(!assignment.is_member(&nid(10))); // publisher not in group
    }

    #[test]
    fn test_secondary_group_map_insert_and_get() {
        let mut map = SecondaryGroupMap::<PT>::default();

        let group1 = SecondaryGroup::new([nid(1), nid(2)].into_iter().collect()).unwrap();
        let round_span1 = RoundSpan::new(Round(1), Round(5)).unwrap();

        let group2 = SecondaryGroup::new([nid(3), nid(4)].into_iter().collect()).unwrap();
        let round_span2 = RoundSpan::new(Round(10), Round(15)).unwrap();

        assert!(map.try_insert(round_span1, group1).is_some());
        assert!(map.try_insert(round_span2, group2).is_some());

        // get group at round 3 (within [1, 5))
        let grp = map.get(Round(3)).unwrap();
        assert!(grp.is_member(&nid(1)));

        // get group at round 12 (within [10, 15))
        let grp = map.get(Round(12)).unwrap();
        assert!(grp.is_member(&nid(3)));

        // no group at round 7 (gap between groups)
        assert!(map.get(Round(7)).is_none());
    }

    #[test]
    fn test_secondary_group_map_overlap() {
        let mut map = SecondaryGroupMap::<PT>::default();

        let group1 = SecondaryGroup::new([nid(1), nid(2)].into_iter().collect()).unwrap();
        let round_span1 = RoundSpan::new(Round(1), Round(10)).unwrap();

        let group2 = SecondaryGroup::new([nid(3), nid(4)].into_iter().collect()).unwrap();
        let round_span2 = RoundSpan::new(Round(5), Round(15)).unwrap(); // overlaps

        assert!(map.try_insert(round_span1, group1).is_some());
        assert!(map.try_insert(round_span2, group2).is_none()); // rejected
    }

    #[test]
    fn test_secondary_group_map_delete_expired() {
        let mut map = SecondaryGroupMap::<PT>::default();

        let group1 = SecondaryGroup::new([nid(1)].into_iter().collect()).unwrap();
        let round_span1 = RoundSpan::new(Round(1), Round(5)).unwrap();

        let group2 = SecondaryGroup::new([nid(2)].into_iter().collect()).unwrap();
        let round_span2 = RoundSpan::new(Round(5), Round(10)).unwrap();

        let group3 = SecondaryGroup::new([nid(3)].into_iter().collect()).unwrap();
        let round_span3 = RoundSpan::new(Round(10), Round(15)).unwrap();

        assert!(map.try_insert(round_span1, group1).is_some());
        assert!(map.try_insert(round_span2, group2).is_some());
        assert!(map.try_insert(round_span3, group3).is_some());

        // Delete groups ending at or before Round(5)
        map.delete_expired(Round(5));

        // group [1, 5) should be deleted
        assert!(map.get(Round(3)).is_none());
        // group [5, 10) should still exist
        assert!(map.get(Round(7)).is_some());
        // group [10, 15) should still exist
        assert!(map.get(Round(12)).is_some());

        // delete groups ending at or before Round(10)
        map.delete_expired(Round(10));
        assert!(map.get(Round(7)).is_none());
        assert!(map.get(Round(12)).is_some());
    }

    #[test]
    fn test_full_node_group_map_insert_and_get() {
        let mut map = FullNodeGroupMap::<PT>::default();

        let publisher1 = nid(100);
        let publisher2 = nid(200);

        let assignment1 = SecondaryGroupAssignment::new(
            publisher1,
            RoundSpan::new(Round(1), Round(5)).unwrap(),
            SecondaryGroup::new([nid(1), nid(2)].into_iter().collect()).unwrap(),
        );

        let assignment2 = SecondaryGroupAssignment::new(
            publisher2,
            RoundSpan::new(Round(1), Round(5)).unwrap(),
            SecondaryGroup::new([nid(3), nid(4)].into_iter().collect()).unwrap(),
        );

        assert!(map.try_insert(assignment1).is_some());
        assert!(map.try_insert(assignment2).is_some());

        assert_eq!(map.len(), 2);
        assert!(!map.is_empty());

        // get group map for publisher1
        let group_map = map.get_group_map(&publisher1).unwrap();
        let grp = group_map.get(Round(3)).unwrap();
        assert!(grp.is_member(&nid(1)));

        // get group map for publisher2
        let group_map = map.get_group_map(&publisher2).unwrap();
        let grp = group_map.get(Round(3)).unwrap();
        assert!(grp.is_member(&nid(3)));

        // unknown publisher returns None
        assert!(map.get_group_map(&nid(999)).is_none());
    }

    #[test]
    fn test_full_node_group_map_delete_expired() {
        let mut map = FullNodeGroupMap::<PT>::default();

        let publisher = nid(100);

        let assignment1 = SecondaryGroupAssignment::new(
            publisher,
            RoundSpan::new(Round(1), Round(5)).unwrap(),
            SecondaryGroup::new([nid(1)].into_iter().collect()).unwrap(),
        );

        let assignment2 = SecondaryGroupAssignment::new(
            publisher,
            RoundSpan::new(Round(10), Round(15)).unwrap(),
            SecondaryGroup::new([nid(2)].into_iter().collect()).unwrap(),
        );

        assert!(map.try_insert(assignment1).is_some());
        assert!(map.try_insert(assignment2).is_some());

        // delete expired at Round(5)
        map.delete_expired(Round(5));

        let group_map = map.get_group_map(&publisher).unwrap();
        assert!(group_map.get(Round(3)).is_none());
        assert!(group_map.get(Round(12)).is_some());

        // delete expired at Round(15) - remove entire publisher entry
        map.delete_expired(Round(15));
        assert!(map.is_empty());
    }

    #[test]
    fn test_broadcast_group_from_primary_group_id() {
        let self_id = nid(1);
        let author = nid(2);
        let validator_ids = [nid(1), nid(2), nid(3)];

        let mut validator_group_map = ValidatorGroupMap::<PT>::new();
        validator_group_map.insert(Epoch(1), make_validator_set(&validator_ids));
        let full_node_group_map = FullNodeGroupMap::<PT>::default();

        let group = BroadcastGroup::from_group_id(
            GroupId::Primary(Epoch(1)),
            &self_id,
            &author,
            &validator_group_map,
            &full_node_group_map,
        )
        .unwrap();

        assert!(matches!(group, BroadcastGroup::ValidatorGroup { .. }));
    }

    #[test]
    fn test_broadcast_group_from_primary_group_not_found() {
        let self_id = nid(1);
        let author = nid(2);

        let validator_group_map = ValidatorGroupMap::<PT>::new();
        let full_node_group_map = FullNodeGroupMap::<PT>::default();

        // Epoch not found
        let result = BroadcastGroup::from_group_id(
            GroupId::Primary(Epoch(99)),
            &self_id,
            &author,
            &validator_group_map,
            &full_node_group_map,
        );

        assert!(matches!(result, Err(BroadcastGroupError::GroupNotFound)));
    }

    #[test]
    fn test_broadcast_group_from_primary_invalid_author() {
        let self_id = nid(1);
        let author = nid(99); // not in validator set
        let validator_ids = [nid(1), nid(2), nid(3)];

        let mut validator_group_map = ValidatorGroupMap::<PT>::new();
        validator_group_map.insert(Epoch(1), make_validator_set(&validator_ids));

        let full_node_group_map = FullNodeGroupMap::<PT>::default();

        let result = BroadcastGroup::from_group_id(
            GroupId::Primary(Epoch(1)),
            &self_id,
            &author,
            &validator_group_map,
            &full_node_group_map,
        );

        assert!(matches!(result, Err(BroadcastGroupError::InvalidAuthor)));
    }

    #[test]
    fn test_broadcast_group_from_secondary_group_id() {
        let self_id = nid(1);
        let publisher = nid(100);

        let validator_group_map = ValidatorGroupMap::<PT>::new();
        let mut full_node_group_map = FullNodeGroupMap::<PT>::default();

        let assignment = SecondaryGroupAssignment::new(
            publisher,
            RoundSpan::new(Round(1), Round(10)).unwrap(),
            SecondaryGroup::new([self_id, nid(2), nid(3)].into_iter().collect()).unwrap(),
        );
        full_node_group_map
            .try_insert(assignment)
            .expect("no overlaps");

        // Valid secondary group lookup
        let group = BroadcastGroup::from_group_id(
            GroupId::Secondary(Round(5)),
            &self_id,
            &publisher, // author is publisher for secondary
            &validator_group_map,
            &full_node_group_map,
        )
        .unwrap();

        assert!(matches!(group, BroadcastGroup::FullNodeGroup { .. }));
    }

    #[test]
    fn test_broadcast_group_from_secondary_publisher_not_found() {
        let self_id = nid(1);
        let unknown_publisher = nid(999);

        let validator_group_map = ValidatorGroupMap::<PT>::new();
        let full_node_group_map = FullNodeGroupMap::<PT>::default();

        let result = BroadcastGroup::from_group_id(
            GroupId::Secondary(Round(5)),
            &self_id,
            &unknown_publisher,
            &validator_group_map,
            &full_node_group_map,
        );

        assert!(matches!(result, Err(BroadcastGroupError::GroupNotFound)));
    }

    #[test]
    fn test_broadcast_group_from_secondary_round_not_found() {
        let self_id = nid(1);
        let publisher = nid(100);

        let validator_group_map = ValidatorGroupMap::<PT>::new();
        let mut full_node_group_map = FullNodeGroupMap::<PT>::default();

        let assignment = SecondaryGroupAssignment::new(
            publisher,
            RoundSpan::new(Round(1), Round(10)).unwrap(),
            SecondaryGroup::new([self_id, nid(2)].into_iter().collect()).unwrap(),
        );
        full_node_group_map
            .try_insert(assignment)
            .expect("no overlaps");

        // Round 50 is outside [1, 10)
        let result = BroadcastGroup::from_group_id(
            GroupId::Secondary(Round(50)),
            &self_id,
            &publisher,
            &validator_group_map,
            &full_node_group_map,
        );

        assert!(matches!(result, Err(BroadcastGroupError::GroupNotFound)));
    }

    #[test]
    fn test_broadcast_group_is_sender_valid_validator() {
        let self_id = nid(1);
        let author = nid(2);
        let validator_ids = [nid(1), nid(2), nid(3)];

        let mut validator_group_map = ValidatorGroupMap::<PT>::new();
        validator_group_map.insert(Epoch(1), make_validator_set(&validator_ids));

        let full_node_group_map = FullNodeGroupMap::<PT>::default();

        let group = BroadcastGroup::from_group_id(
            GroupId::Primary(Epoch(1)),
            &self_id,
            &author,
            &validator_group_map,
            &full_node_group_map,
        )
        .unwrap();

        // Any validator is a valid sender
        assert!(group.is_sender_valid(&nid(1)));
        assert!(group.is_sender_valid(&nid(2)));
        assert!(group.is_sender_valid(&nid(3)));
        // Non-validator is not a valid sender
        assert!(!group.is_sender_valid(&nid(99)));
    }

    #[test]
    fn test_broadcast_group_is_sender_valid_fullnode() {
        let self_id = nid(1);
        let publisher = nid(100);

        let validator_group_map = ValidatorGroupMap::<PT>::new();
        let mut full_node_group_map = FullNodeGroupMap::<PT>::default();

        let assignment = SecondaryGroupAssignment::new(
            publisher,
            RoundSpan::new(Round(1), Round(10)).unwrap(),
            SecondaryGroup::new([self_id, nid(2), nid(3)].into_iter().collect()).unwrap(),
        );
        full_node_group_map
            .try_insert(assignment)
            .expect("no overlaps");

        let group = BroadcastGroup::from_group_id(
            GroupId::Secondary(Round(5)),
            &self_id,
            &publisher,
            &validator_group_map,
            &full_node_group_map,
        )
        .unwrap();

        // Publisher is valid sender
        assert!(group.is_sender_valid(&publisher));
        // Group members are valid senders
        assert!(group.is_sender_valid(&self_id));
        assert!(group.is_sender_valid(&nid(2)));
        assert!(group.is_sender_valid(&nid(3)));
        // Non-member, non-publisher is not valid
        assert!(!group.is_sender_valid(&nid(99)));
    }

    #[test]
    fn test_broadcast_group_should_rebroadcast() {
        let self_id = nid(1);
        let author = nid(2);
        let validator_ids = [nid(1), nid(2), nid(3)];

        let mut validator_group_map = ValidatorGroupMap::<PT>::new();
        validator_group_map.insert(Epoch(1), make_validator_set(&validator_ids));

        let full_node_group_map = FullNodeGroupMap::<PT>::default();

        let group = BroadcastGroup::from_group_id(
            GroupId::Primary(Epoch(1)),
            &self_id,
            &author,
            &validator_group_map,
            &full_node_group_map,
        )
        .unwrap();

        // self_id is a member, first hop -> should rebroadcast
        assert!(group.should_rebroadcast(true));
        // self_id is a member, not first hop -> should not rebroadcast
        assert!(!group.should_rebroadcast(false));

        let self_id = nid(99); // not in validator set
        let group = BroadcastGroup::from_group_id(
            GroupId::Primary(Epoch(1)),
            &self_id,
            &author,
            &validator_group_map,
            &full_node_group_map,
        )
        .unwrap();

        // self_id is not a member -> should not rebroadcast
        assert!(!group.should_rebroadcast(true));
        assert!(!group.should_rebroadcast(false));
    }

    #[test]
    fn test_broadcast_group_rebroadcasting_peers_validator() {
        let self_id = nid(1);
        let author = nid(2);
        let validator_ids = [nid(1), nid(2), nid(3), nid(4)];

        let mut validator_group_map = ValidatorGroupMap::<PT>::new();
        validator_group_map.insert(Epoch(1), make_validator_set(&validator_ids));

        let full_node_group_map = FullNodeGroupMap::<PT>::default();

        let group = BroadcastGroup::from_group_id(
            GroupId::Primary(Epoch(1)),
            &self_id,
            &author,
            &validator_group_map,
            &full_node_group_map,
        )
        .unwrap();

        let peers: Vec<_> = group.rebroadcasting_peers().cloned().collect();

        // Should exclude self_id (nid(1)) and author (nid(2))
        assert_eq!(peers.len(), 2);
        assert!(peers.contains(&nid(3)));
        assert!(peers.contains(&nid(4)));
        assert!(!peers.contains(&self_id));
        assert!(!peers.contains(&author));
    }

    #[test]
    fn test_broadcast_group_rebroadcasting_peers_fullnode() {
        let self_id = nid(1);
        let publisher = nid(100);

        let validator_group_map = ValidatorGroupMap::<PT>::new();
        let mut full_node_group_map = FullNodeGroupMap::<PT>::default();

        let assignment = SecondaryGroupAssignment::new(
            publisher,
            RoundSpan::new(Round(1), Round(10)).unwrap(),
            SecondaryGroup::new([self_id, nid(2), nid(3)].into_iter().collect()).unwrap(),
        );
        full_node_group_map
            .try_insert(assignment)
            .expect("no overlaps");

        let group = BroadcastGroup::from_group_id(
            GroupId::Secondary(Round(5)),
            &self_id,
            &publisher,
            &validator_group_map,
            &full_node_group_map,
        )
        .unwrap();

        let peers: Vec<_> = group.rebroadcasting_peers().cloned().collect();

        // Should exclude self_id (nid(1)), publisher is not in group
        assert_eq!(peers.len(), 2);
        assert!(peers.contains(&nid(2)));
        assert!(peers.contains(&nid(3)));
        assert!(!peers.contains(&self_id));
    }
}
