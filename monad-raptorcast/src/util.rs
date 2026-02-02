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
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    fmt,
    net::SocketAddr,
    num::NonZero,
    rc::Rc,
};

use bytes::Bytes;
use fixed::{types::extra::U11, FixedU16};
use itertools::Itertools;
use monad_crypto::{
    certificate_signature::{CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey},
    hasher::{Hasher, HasherType},
};
use monad_types::{Epoch, NodeId, Round, RoundSpan, Stake};
use monad_validator::validator_set::{ValidatorSet, ValidatorSetType as _};
use tracing::{debug, warn};

use crate::udp::GroupId;

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
}

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

// This represents a raptorcast group abstracted over the use cases below:
// 1) Validator->Validator raptorcast recv & re-broadcast
// 2) Validator->FullNode raptorcast recv & re-broadcast
// 3) Validator->FullNode raptorcast send (when initiating proposals)
// Validator->Validator send group is presented by EpochValidators instead, as
// that contains stake info per validator.
#[derive(Clone, PartialEq, Eq)] // For some reason Default doesn't work
pub struct Group<PT: PubKey> {
    // The node_id of the validator publishing to full-nodes.
    validator_id: Option<NodeId<PT>>,
    round_span: RoundSpan,
    sorted_other_peers: Vec<NodeId<PT>>, // Excludes self
}

// Keyed by starting round
type GroupQueue<PT> = BTreeMap<Round, Group<PT>>;

// Groups in a GroupQueue should be sorted by start round, earliest round first
impl<PT: PubKey> Ord for Group<PT> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Compare fields other than round_span.start as well, to make the
        // ordering more consistent and predictable
        other
            .round_span
            .start
            .cmp(&self.round_span.start)
            .then_with(|| other.round_span.end.cmp(&self.round_span.end))
            .then_with(|| other.validator_id.cmp(&self.validator_id))
    }
}

impl<PT: PubKey> PartialOrd for Group<PT> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<PT: PubKey> fmt::Debug for Group<PT> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Group")
            .field("start", &self.round_span.start.0)
            .field("end", &self.round_span.end.0)
            .field("other_peers", &self.sorted_other_peers.len())
            .finish()
    }
}

impl<PT: PubKey> Group<PT> {
    // For the use case where we re-raptorcast to validators
    pub fn new_validator_group(all_peers: Vec<NodeId<PT>>, self_id: &NodeId<PT>) -> Self {
        // We will call `check_author_node_id()` often, so sorting here will
        // allow us to use binary search instead of linear search.
        let sorted_other_peers: Vec<_> = all_peers
            .into_iter()
            .filter(|peer| peer != self_id)
            .sorted()
            .collect();
        Self {
            validator_id: None,
            round_span: RoundSpan::empty(Round::MIN), // unused for validator group
            sorted_other_peers,
        }
    }

    // For the use case where we re-raptorcast to full-nodes
    // Validators Raptorcasting to full-nodes should set `self_id` == `validator_id`
    // Note that the user is responsible for checking that self_id exists in `all_peers`.
    // This is specially important when a client receives a `ConfirmGroup`
    // message over the network from a (rogue?) validator.
    pub fn new_fullnode_group(
        all_peers: Vec<NodeId<PT>>,
        self_id: &NodeId<PT>,
        validator_id: NodeId<PT>,
        round_span: RoundSpan,
    ) -> Self {
        // We will call `check_author_node_id()` often, so sorting here will
        // allow us to use binary search instead of linear search.
        let mut sorted_other_peers = all_peers;
        if self_id != &validator_id {
            // The validator won't find its own nodeid among the peers
            // Swap self_id in `all_peers` with the last element, then pop.
            let self_index = sorted_other_peers
                .iter()
                .position(|peer| peer == self_id)
                .expect(
                    "Could not find own node id when instantiating a \
                        Raptorcast group for full-nodes",
                );
            sorted_other_peers.swap_remove(self_index);
        }
        sorted_other_peers.sort(); // Groups recv over network are already sorted, though
        Self {
            validator_id: Some(validator_id),
            round_span,
            sorted_other_peers,
        }
    }

    // For bandwidth calculation and for calculating number of packets when
    // originator segments app messages into raptorcast chunks.
    pub fn size_excl_self(&self) -> usize {
        self.sorted_other_peers.len()
    }

    pub fn get_validator_id(&self) -> &NodeId<PT> {
        // Only set when re-raptorcasting to full-nodes
        self.validator_id.as_ref().expect("Validator ID is not set")
    }

    pub fn iter_peers(&self) -> impl Iterator<Item = &NodeId<PT>> + '_ {
        self.sorted_other_peers.iter()
    }

    #[cfg(test)]
    pub fn get_other_peers(&self) -> &Vec<NodeId<PT>> {
        &self.sorted_other_peers
    }

    pub fn get_round_span(&self) -> &RoundSpan {
        &self.round_span
    }

    fn empty_iterator(&self) -> GroupIterator<'_, PT> {
        GroupIterator {
            group: self,
            num_consumed: usize::MAX,
            author_id_ix: usize::MAX,
            start_ix: 0,
        }
    }

    // Returns a safe iterator suitable for (re-) raptorcasting to full-nodes.
    // Argument `seed` is used for avoiding always assigning chunks for small
    // proposals to the same node.
    // The iteration will start from index `seed % self.sorted_other_peers.len()`.
    // Yields NodeIds.
    pub fn iter_skip_self_and_author(
        &self,
        author_id: &NodeId<PT>,
        seed: usize,
    ) -> GroupIterator<'_, PT> {
        // Hint for the index of author_id within self.sorted_other_peers.
        // We want to skip it when iterating the peers for broadcasting.
        let author_id_ix = if let Some(root_vid) = self.validator_id {
            // Case for full-node raptorcasting. Lets check that the author_id
            // (in the inbound message) is the same as expected for this group.
            // Note that AuthorID is a validator and we will not find it among
            // the full-node ids in the group.
            if author_id != &root_vid {
                warn!(
                    "Author {} does not match raptorcast group validator id {}",
                    author_id, root_vid
                );
                return self.empty_iterator();
            }
            usize::MAX
        } else {
            // Case for validator-to-validator raptorcasting.
            // We are a validator and we are re-raptorcasting to full-nodes.
            // We do a scan for author ID upfront because we don't want to yield
            // any nodeID before we know for sure the author_id is among them.
            let maybe_pos_author_id = self.sorted_other_peers.binary_search(author_id);
            if maybe_pos_author_id.is_err() {
                warn!("Author {} is not a member of raptorcast group", author_id);
                return self.empty_iterator();
            }
            maybe_pos_author_id.unwrap()
        };
        // Avoid div by zero and also overflow when adding `num_consumed` later`
        let start_ix = if self.sorted_other_peers.is_empty() {
            0
        } else {
            seed % self.sorted_other_peers.len()
        };
        GroupIterator {
            group: self,
            num_consumed: 0,
            author_id_ix,
            start_ix,
        }
    }

    // There are cases where we need to check that the source node is valid
    // before we get to call iter_skip_self_and_author()
    pub fn check_author_node_id(&self, author_id: &NodeId<PT>) -> bool {
        if let Some(root_vid) = self.validator_id {
            // Case for full-node raptorcasting
            let good = &root_vid == author_id;
            if !good {
                debug!(?author_id, ?root_vid, "check_author_node_id (fn) failed");
            }
            good
        } else {
            // Case for validator-to-validator
            let good = self.sorted_other_peers.binary_search(author_id).is_ok();
            if !good {
                debug!(?author_id, ?self.sorted_other_peers, "check_author_node_id (v2v) failed");
            }
            good
        }
    }
}

// The responsibility of this class is to simply skip self and author node_id
// without copying or rebuilding vectors.
// Intended to be used in the recv leg of re-raptorcasting to validators, or for
// both the recv & send leg of (re-) raptorcasting to fullnodes, as these do
// not need Stake information for each validator.
pub struct GroupIterator<'a, PT: PubKey> {
    group: &'a Group<PT>,
    num_consumed: usize,
    author_id_ix: usize,
    start_ix: usize,
}

impl<'a, PT: PubKey> Iterator for GroupIterator<'a, PT> {
    type Item = &'a NodeId<PT>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.num_consumed < self.group.sorted_other_peers.len() {
            let index = (self.num_consumed + self.start_ix) % self.group.sorted_other_peers.len();
            self.num_consumed += 1;
            if index != self.author_id_ix {
                return Some(&self.group.sorted_other_peers[index]);
            }
        }
        None
    }
}

// This is an abstraction of a peer list that interfaces receive-side of RaptorCast
// The send side, i.e. initiating a RaptorCast proposal, is represented with
// struct `EpochValidators` instead.
#[derive(Debug)]
pub struct ReBroadcastGroupMap<PT: PubKey> {
    // When iterating nodeIds in a raptorcast group, this node id is always skipped
    our_node_id: NodeId<PT>,

    // For Validator->validator re-raptorcasting
    validator_map: BTreeMap<Epoch, Group<PT>>,

    // For Validator->fullnode re-raptorcasting: each entry is keyed by
    // validator NodeId. GroupQueue is a BTreeMap keyed by starting round
    fullnode_map: BTreeMap<NodeId<PT>, GroupQueue<PT>>,
}

impl<PT: PubKey> ReBroadcastGroupMap<PT> {
    pub fn new(our_node_id: NodeId<PT>) -> Self {
        Self {
            our_node_id,
            validator_map: BTreeMap::new(),
            fullnode_map: BTreeMap::new(),
        }
    }

    // For UdpState::handle_message() so it can drop inbound messages early,
    // before calling iterate_rebroadcast_peers().
    pub fn check_source(
        &self,
        msg_group_id: GroupId,
        author_node_id: &NodeId<PT>,
        src_addr: &SocketAddr,
    ) -> bool {
        match msg_group_id {
            GroupId::Primary(msg_epoch) => {
                // validator to validator raptorcast
                if let Some(group) = self.validator_map.get(&msg_epoch) {
                    let author_found = group.check_author_node_id(author_node_id);
                    if !author_found {
                        debug!(?author_node_id, ?self.validator_map, ?src_addr,
                            "Validator author for v2v group not found in validator_map");
                    }
                    author_found
                } else {
                    debug!(?msg_epoch, ?self.validator_map, "Epoch not found in validator_map");
                    false
                }
            }
            GroupId::Secondary(msg_round) => {
                // Source node id (validator) is already the key to the map, so
                // we don't need to look into the group itself.
                if let Some(groups) = self.fullnode_map.get(author_node_id) {
                    if let Some((_start_round, group)) = groups.range(..=msg_round).next_back() {
                        if group.get_round_span().contains(msg_round) {
                            return true;
                        }
                    }
                    debug!(
                        ?msg_round,
                        ?author_node_id,
                        ?self.fullnode_map,
                        ?src_addr,
                        "msg_round not found for validator's round span");
                    // FIXME: returning true for backward compatibility,
                    // to be removed after upgrade
                    return true;
                } else {
                    debug!(?author_node_id, ?self.fullnode_map, ?src_addr,
                        "Validator author for v2fn group not found in fullnode_map");
                }
                false
            }
        }
    }

    // Intended to be used by UdpState::handle_message()
    // When receiving a raptorcast chunk, this method will help determine which
    // peers (validators or full-nodes) to re-broadcast chunks to, given the
    // inbound chunk's epoch field (for validator-to-validator raptorcasting)
    // and author field (for validator-to-fullnodes raptorcasting)
    pub fn iterate_rebroadcast_peers(
        &self,
        msg_group_id: GroupId,
        msg_author: &NodeId<PT>, // skipped when iterating RaptorCast group
    ) -> Option<GroupIterator<'_, PT>> {
        let rebroadcast_group = match msg_group_id {
            GroupId::Primary(msg_epoch) => self.validator_map.get(&msg_epoch)?,
            GroupId::Secondary(msg_round) => {
                let group_queue = self.fullnode_map.get(msg_author)?;

                // FIXME: or_else defaults to the old behavior, which can be hit
                // if full node is upgraded before the validator. It'll be
                // removed after the upgrade
                group_queue
                    .range(..=msg_round)
                    .next_back()
                    .filter(|(_start_round, group)| group.get_round_span().contains(msg_round))
                    .or_else(|| group_queue.first_key_value())
                    .map(|(_key, group)| group)?
            }
        };

        if rebroadcast_group.size_excl_self() == 0 {
            // If there's no other peers in the group, then there's no one to broadcast to
            return None;
        }

        // this validates author
        Some(rebroadcast_group.iter_skip_self_and_author(msg_author, 0))
    }

    // As Validator: When we get an AddEpochValidatorSet.
    pub fn push_group_validator_set(
        &mut self,
        validator_set: Vec<(NodeId<PT>, Stake)>,
        epoch: Epoch,
    ) {
        let (all_peers, _validator_stakes): (Vec<_>, Vec<_>) = validator_set.into_iter().unzip();
        let new_group = Group::new_validator_group(all_peers, &self.our_node_id);
        if let Some(existing_group) = self.validator_map.get(&epoch) {
            assert_eq!(existing_group, &new_group);
            warn!("duplicate validator set update (this is safe but unexpected)")
        } else {
            let replaced = self.validator_map.insert(epoch, new_group);
            assert!(replaced.is_none());
        }
    }

    // As Full-node: When secondary RaptorCast instance (Client) sends us a Group<>
    pub fn push_group_fullnodes(&mut self, group: Group<PT>) {
        let vid = *group.get_validator_id();
        let prev_group_queue_from_vid = format!("{:?}", self.fullnode_map.get(&vid));
        self.fullnode_map
            .entry(vid)
            .or_default()
            .insert(group.round_span.start, group);
        debug!(?vid, ?prev_group_queue_from_vid, "RaptorCast Group insert",);
    }

    pub fn delete_expired_groups(&mut self, curr_epoch: Epoch, curr_round: Round) {
        // Keep current and future* groups.
        // Note: normally the client will only send as groups that are
        // currently active, but it is possible for the client to send us a
        // group scheduled for the future when we (the non-dedicated full-node)
        // aren't received proposals yet and hence do not know what the current
        // round is.
        for (_vid, group_queue) in self.fullnode_map.iter_mut() {
            group_queue.retain(|_start_round, group| curr_round < group.round_span.end);
        }
        self.fullnode_map
            .retain(|_vid, group_queue| !group_queue.is_empty());

        // clear old validator map
        self.validator_map
            .retain(|key, _| *key + Epoch(1) >= curr_epoch);

        debug!(
            epoch=?curr_epoch,
            round=?curr_round,
            validator_map_len=?self.validator_map.len(),
            fullnode_map_len=?self.fullnode_map.len(),
            "RaptorCast delete_expired_groups",
        );
    }

    pub fn get_fullnode_map(&self) -> BTreeMap<NodeId<PT>, Group<PT>> {
        let mut res: BTreeMap<_, _> = BTreeMap::new();
        for (vid, group_queue) in &self.fullnode_map {
            if let Some((_start_round, group)) = group_queue.first_key_value() {
                res.insert(*vid, group.clone());
            }
        }
        res
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
    use std::collections::HashSet;

    use monad_crypto::certificate_signature::CertificateSignaturePubKey;
    use monad_secp::SecpSignature;
    use monad_testutil::signing::get_key;

    use super::*;
    type ST = SecpSignature;
    type PT = CertificateSignaturePubKey<ST>;

    // Creates a node id that we can refer to just from its seed
    fn nid(seed: u64) -> NodeId<PT> {
        let key_pair = get_key::<ST>(seed);
        let pub_key = key_pair.pubkey();
        NodeId::new(pub_key)
    }

    #[test]
    fn test_fullnode_iterator_self_on() {
        let group = Group::<PT>::new_fullnode_group(
            vec![nid(0), nid(1), nid(2)],
            &nid(1), // self_id
            nid(3),  // validator
            RoundSpan::new(Round(3), Round(8)).unwrap(),
        );
        assert_eq!(group.size_excl_self(), 2);
        assert_eq!(group.get_validator_id(), &nid(3));
        assert_eq!(group.get_other_peers(), &vec![nid(0), nid(2)]);
        assert_eq!(
            group.get_round_span(),
            &RoundSpan::new(Round(3), Round(8)).unwrap()
        );
        let empty_iter: Vec<_> = group.empty_iterator().collect();
        assert!(empty_iter.is_empty());

        let it: Vec<_> = group
            .iter_skip_self_and_author(&nid(3), 0)
            .cloned()
            .collect();
        assert_eq!(&it, &vec![nid(0), nid(2)]);

        // Calling a second time should not change anything
        let it: Vec<_> = group
            .iter_skip_self_and_author(&nid(3), 0)
            .cloned()
            .collect();
        assert_eq!(&it, &vec![nid(0), nid(2)]);

        // Invalid author id: should return empty iterator
        let it: Vec<_> = group
            .iter_skip_self_and_author(&nid(5), 0)
            .cloned()
            .collect();
        assert!(it.is_empty());
    }

    #[test]
    fn test_fullnode_iterator_self_off() {
        let group = Group::<PT>::new_fullnode_group(
            vec![nid(0), nid(1), nid(2)],
            &nid(3), // self_id
            nid(3),  // validator id
            RoundSpan::new(Round(3), Round(8)).unwrap(),
        );
        assert_eq!(group.size_excl_self(), 3);
        assert_eq!(group.get_validator_id(), &nid(3));
        assert_eq!(group.get_other_peers(), &vec![nid(0), nid(1), nid(2)]);
        assert_eq!(
            group.get_round_span(),
            &RoundSpan::new(Round(3), Round(8)).unwrap()
        );
        let empty_iter: Vec<_> = group.empty_iterator().collect();
        assert!(empty_iter.is_empty());

        let it: Vec<_> = group
            .iter_skip_self_and_author(&nid(3), 0)
            .cloned()
            .collect();
        assert_eq!(&it, &vec![nid(0), nid(1), nid(2)]);

        // Invalid author id: should return empty iterator
        let it: Vec<_> = group
            .iter_skip_self_and_author(&nid(5), 0)
            .cloned()
            .collect();
        assert!(it.is_empty());
    }

    #[test]
    fn test_fullnode_iterator_only_self() {
        let group = Group::<PT>::new_fullnode_group(
            vec![nid(1)],
            &nid(1), // self_id
            nid(3),  // validator id
            RoundSpan::new(Round(3), Round(8)).unwrap(),
        );
        assert_eq!(group.size_excl_self(), 0);
        assert_eq!(group.get_validator_id(), &nid(3));
        assert_eq!(group.get_other_peers(), &vec![]);
        assert_eq!(
            group.get_round_span(),
            &RoundSpan::new(Round(3), Round(8)).unwrap()
        );
        let empty_iter: Vec<_> = group.empty_iterator().collect();
        assert!(empty_iter.is_empty());

        let it: Vec<_> = group
            .iter_skip_self_and_author(&nid(3), 0)
            .cloned()
            .collect();
        assert!(it.is_empty());
        // Invalid author id: should return empty iterator
        let it: Vec<_> = group
            .iter_skip_self_and_author(&nid(5), 0)
            .cloned()
            .collect();
        assert!(it.is_empty());
    }

    #[test]
    fn test_validator_iterator_self_on() {
        let group = Group::<PT>::new_validator_group(
            vec![nid(0), nid(1), nid(2)],
            &nid(1), // self_id
        );
        assert_eq!(group.size_excl_self(), 2);
        assert_eq!(group.get_other_peers(), &vec![nid(0), nid(2)]);
        assert_eq!(group.get_round_span(), &RoundSpan::empty(Round::MIN));
        let empty_iter: Vec<_> = group.empty_iterator().collect();
        assert!(empty_iter.is_empty());

        // Invalid author id: should return empty iterator
        let it: Vec<_> = group
            .iter_skip_self_and_author(&nid(5), 0)
            .cloned()
            .collect();
        assert!(it.is_empty());

        let it: Vec<_> = group
            .iter_skip_self_and_author(&nid(2), 0)
            .cloned()
            .collect();
        assert_eq!(&it, &vec![nid(0)]);
    }

    #[test]
    fn test_iterator_rand() {
        let group = Group::<PT>::new_fullnode_group(
            vec![nid(0), nid(1), nid(2), nid(3), nid(4)],
            &nid(0),  // self_id
            nid(100), // validator id
            RoundSpan::new(Round(3), Round(8)).unwrap(),
        );

        // Non-"randomized" iteration (but sorted)
        let it: Vec<_> = group
            .iter_skip_self_and_author(&nid(100), 0)
            .cloned()
            .collect();
        let mut org_nodes = vec![nid(1), nid(2), nid(3), nid(4)];
        org_nodes.sort();
        assert_eq!(&it, &org_nodes);

        // "Randomized" iterations
        let mut permutations_seen = HashSet::<Vec<NodeId<PT>>>::new();
        for seed in 1..10 {
            let it: Vec<_> = group
                .iter_skip_self_and_author(&nid(100), seed)
                .cloned()
                .collect();
            permutations_seen.insert(it);
        }

        // Verify that we have seen a few permutations, ensuring that we won't
        // always assign chunks for small proposals to the same node.
        assert!(permutations_seen.len() >= 4);
    }

    #[test]
    #[should_panic]
    fn test_no_validator_id_in_validator_group() {
        let group = Group::<PT>::new_validator_group(
            vec![nid(0), nid(1), nid(2)],
            &nid(1), // self_id
        );
        group.get_validator_id(); // should panic
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
}
