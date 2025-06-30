use std::{collections::BTreeMap, fmt};

use itertools::Itertools;
use monad_crypto::{
    certificate_signature::{CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey},
    hasher::{Hasher, HasherType},
};
use monad_types::{Epoch, NodeId, Round, RoundSpan, Stake};

#[derive(Clone, Default)]
pub struct EpochValidators<ST>
where
    ST: CertificateSignatureRecoverable,
{
    pub validators: BTreeMap<NodeId<CertificateSignaturePubKey<ST>>, Validator>,
}

impl<ST> EpochValidators<ST>
where
    ST: CertificateSignatureRecoverable,
{
    /// Returns a view of the validator set without a given node. On ValidatorsView being dropped,
    /// the validator set is reverted back to normal.
    pub fn view_without(
        &mut self,
        without: Vec<&NodeId<CertificateSignaturePubKey<ST>>>,
    ) -> ValidatorsView<ST> {
        let mut removed = Vec::new();
        for without in without {
            if let Some(removed_validator) = self.validators.remove(without) {
                removed.push((*without, removed_validator));
            }
        }
        ValidatorsView {
            view: &mut self.validators,
            removed,
        }
    }
}

#[derive(Debug)]
pub struct ValidatorsView<'a, ST>
where
    ST: CertificateSignatureRecoverable,
{
    view: &'a mut BTreeMap<NodeId<CertificateSignaturePubKey<ST>>, Validator>,
    removed: Vec<(NodeId<CertificateSignaturePubKey<ST>>, Validator)>,
}
impl<ST> ValidatorsView<'_, ST>
where
    ST: CertificateSignatureRecoverable,
{
    pub fn view(&self) -> &BTreeMap<NodeId<CertificateSignaturePubKey<ST>>, Validator> {
        self.view
    }
}

impl<ST> Drop for ValidatorsView<'_, ST>
where
    ST: CertificateSignatureRecoverable,
{
    fn drop(&mut self) {
        while let Some((without, removed_validator)) = self.removed.pop() {
            self.view.insert(without, removed_validator);
        }
    }
}

#[derive(Debug, Clone)]
pub struct FullNodes<P: PubKey> {
    pub list: Vec<NodeId<P>>,
}

impl<P: PubKey> Default for FullNodes<P> {
    fn default() -> Self {
        Self {
            list: Default::default(),
        }
    }
}

impl<P: PubKey> FullNodes<P> {
    pub fn new(nodes: Vec<NodeId<P>>) -> Self {
        Self { list: nodes }
    }

    pub fn view(&self) -> FullNodesView<P> {
        FullNodesView(&self.list)
    }
}

#[derive(Debug, Clone)]
pub struct FullNodesView<'a, P: PubKey>(&'a Vec<NodeId<P>>);

impl<P: PubKey> FullNodesView<'_, P> {
    pub fn view(&self) -> &Vec<NodeId<P>> {
        self.0
    }
}

#[derive(Debug, Clone)]
pub struct Validator {
    pub stake: Stake,
}

// Argument for raptorcast send
#[derive(Debug)]
pub enum BuildTarget<'a, ST: CertificateSignatureRecoverable> {
    Broadcast(
        // validator stakes for given epoch_no, not including self
        // this MUST NOT BE EMPTY
        ValidatorsView<'a, ST>,
    ),
    Raptorcast(
        (
            // validator stakes for given epoch_no, not including self
            // this MUST NOT BE EMPTY
            // Contains Stake information per validator node id
            ValidatorsView<'a, ST>,
            // Dedicated full-nodes (rather than priority nodes)
            FullNodesView<'a, CertificateSignaturePubKey<ST>>,
        ),
    ),
    // sharded raptor-aware broadcast
    PointToPoint(&'a NodeId<CertificateSignaturePubKey<ST>>),
    // Group should not be empty after excluding self node Id
    FullNodeRaptorCast(&'a Group<ST>),
}

pub fn compute_hash<PT>(id: &NodeId<PT>) -> NodeIdHash
where
    PT: PubKey,
{
    let mut hasher = HasherType::new();
    hasher.update(id.pubkey().bytes());
    HexBytes(hasher.hash().0[..20].try_into().expect("20 bytes"))
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

pub type NodeIdHash = HexBytes<20>;
pub type AppMessageHash = HexBytes<20>;

// This represents a raptorcast group abstracted over the use cases below:
// 1) Validator->Validator raptorcast recv & re-broadcast
// 2) Validator->FullNode raptorcast recv & re-broadcast
// 3) Validator->FullNode raptorcast send (when initiating proposals)
// Validator->Validator send group is presented by EpochValidators instead, as
// that contains stake info per validator.
#[derive(Clone, PartialEq, Eq)] // For some reason Default doesn't work
pub struct Group<ST>
where
    ST: CertificateSignatureRecoverable,
{
    // The node_id of the validator publishing to full-nodes.
    validator_id: Option<NodeId<CertificateSignaturePubKey<ST>>>,
    round_span: RoundSpan,
    sorted_other_peers: Vec<NodeId<CertificateSignaturePubKey<ST>>>, // Excludes self
}

impl<ST> fmt::Debug for Group<ST>
where
    ST: CertificateSignatureRecoverable,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Group")
            .field("start", &self.round_span.start.0)
            .field("end", &self.round_span.end.0)
            .field("other_peers", &self.sorted_other_peers.len())
            .finish()
    }
}

// the trait `Default` is not implemented for `ST`
impl<ST> Default for Group<ST>
where
    ST: CertificateSignatureRecoverable,
{
    fn default() -> Self {
        Self {
            validator_id: None,
            round_span: RoundSpan::default(),
            sorted_other_peers: Vec::new(),
        }
    }
}

impl<ST> Group<ST>
where
    ST: CertificateSignatureRecoverable,
{
    // For the use case where we re-raptorcast to validators
    pub fn new_validator_group(
        all_peers: Vec<NodeId<CertificateSignaturePubKey<ST>>>,
        self_id: &NodeId<CertificateSignaturePubKey<ST>>,
    ) -> Self {
        // We will call `check_author_node_id()` often, so sorting here will
        // allow us to use binary search instead of linear search.
        let sorted_other_peers: Vec<_> = all_peers
            .into_iter()
            .filter(|peer| peer != self_id)
            .sorted()
            .collect();
        Self {
            validator_id: None,
            round_span: RoundSpan::default(),
            sorted_other_peers,
        }
    }

    // For the use case where we re-raptorcast to full-nodes
    // Validators Raptorcasting to full-nodes should set `self_id` == `validator_id`
    // Note that the user is responsible for checking that self_id exists in `all_peers`.
    // This is specially important when a client receives a `ConfirmGroup`
    // message over the network from a (rogue?) validator.
    pub fn new_fullnode_group(
        all_peers: Vec<NodeId<CertificateSignaturePubKey<ST>>>,
        self_id: &NodeId<CertificateSignaturePubKey<ST>>,
        validator_id: NodeId<CertificateSignaturePubKey<ST>>,
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

    pub fn get_validator_id(&self) -> &NodeId<CertificateSignaturePubKey<ST>> {
        // Only set when re-raptorcasting to full-nodes
        self.validator_id.as_ref().expect("Validator ID is not set")
    }

    #[cfg(test)]
    pub fn get_other_peers(&self) -> &Vec<NodeId<CertificateSignaturePubKey<ST>>> {
        &self.sorted_other_peers
    }

    pub fn get_round_span(&self) -> &RoundSpan {
        &self.round_span
    }

    fn empty_iterator(&self) -> GroupIterator<ST> {
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
        author_id: &NodeId<CertificateSignaturePubKey<ST>>,
        seed: usize,
    ) -> GroupIterator<ST> {
        // Hint for the index of author_id within self.sorted_other_peers.
        // We want to skip it when iterating the peers for broadcasting.
        let author_id_ix = if let Some(root_vid) = self.validator_id {
            // Case for full-node raptorcasting. Lets check that the author_id
            // (in the inbound message) is the same as expected for this group.
            // Note that AuthorID is a validator and we will not find it among
            // the full-node ids in the group.
            if author_id != &root_vid {
                tracing::warn!(
                    "Author {} does not match raptorcast group validator id {}",
                    author_id,
                    root_vid
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
                tracing::warn!("Author {} is not a member of raptorcast group", author_id);
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
    pub fn check_author_node_id(&self, author_id: &NodeId<CertificateSignaturePubKey<ST>>) -> bool {
        if let Some(root_vid) = self.validator_id {
            // Case for full-node raptorcasting
            &root_vid == author_id
        } else {
            // Case for validator-to-validator
            self.sorted_other_peers.binary_search(author_id).is_ok()
        }
    }
}

// The responsibility of this class is to simply skip self and author node_id
// without copying or rebuilding vectors.
// Intended to be used in the recv leg of re-raptorcasting to validators, or for
// both the recv & send leg of (re-) raptorcasting to fullnodes, as these do
// not need Stake information for each validator.
pub struct GroupIterator<'a, ST>
where
    ST: CertificateSignatureRecoverable,
{
    group: &'a Group<ST>,
    num_consumed: usize,
    author_id_ix: usize,
    start_ix: usize,
}

impl<'a, ST> Iterator for GroupIterator<'a, ST>
where
    ST: CertificateSignatureRecoverable,
{
    type Item = &'a NodeId<CertificateSignaturePubKey<ST>>;

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
pub struct ReBroadcastGroupMap<ST>
where
    ST: CertificateSignatureRecoverable,
{
    // When iterating nodeIds in a raptorcast group, this node id is always skipped
    our_node_id: NodeId<CertificateSignaturePubKey<ST>>,

    // For Validator->validator re-raptorcasting
    validator_map: BTreeMap<Epoch, Group<ST>>,

    // For Validator->fullnode re-raptorcasting
    fullnode_map: BTreeMap<NodeId<CertificateSignaturePubKey<ST>>, Group<ST>>,

    // Regarding re-raptorcasting from primary instance, if this.is_fullnode =
    // = true, then we are a full-node re-raptorcasting to other full-nodes.
    // = false, then we are a validator re-raptorcasting to other validators.
    is_fullnode: bool,
}

impl<ST> ReBroadcastGroupMap<ST>
where
    ST: CertificateSignatureRecoverable,
{
    pub fn new(our_node_id: NodeId<CertificateSignaturePubKey<ST>>, is_fullnode: bool) -> Self {
        Self {
            our_node_id,
            validator_map: BTreeMap::new(),
            fullnode_map: BTreeMap::new(),
            is_fullnode,
        }
    }

    // For UdpState::handle_message() so it can drop inbound messages early,
    // before calling iterate_rebroadcast_peers().
    pub fn check_source(
        &self,
        msg_epoch: Epoch,
        author_node_id: &NodeId<CertificateSignaturePubKey<ST>>,
    ) -> bool {
        if self.is_fullnode {
            // Source node id (validator) is already the key to the map, so
            // we don't need to look into the group itself.
            return self.fullnode_map.contains_key(author_node_id);
        }
        if let Some(group) = self.validator_map.get(&msg_epoch) {
            group.check_author_node_id(author_node_id)
        } else {
            false
        }
    }

    // Intended to be used by UdpState::handle_message()
    // When receiving a raptorcast chunk, this method will help determine which
    // peers (validators or full-nodes) to re-broadcast chunks to, given the
    // inbound chunk's epoch field (for validator-to-validator raptorcasting)
    // and author field (for validator-to-fullnodes raptorcasting)
    pub fn iterate_rebroadcast_peers(
        &self,
        msg_epoch: Epoch, // for validator-to-validator re-raptorcasting only
        msg_author: &NodeId<CertificateSignaturePubKey<ST>>, // skipped when iterating RaptorCast group
    ) -> Option<GroupIterator<ST>> {
        let maybe_group = if self.is_fullnode {
            self.fullnode_map.get(msg_author)
        } else {
            self.validator_map.get(&msg_epoch)
        };
        if let Some(group) = maybe_group {
            // If the group excluding self only has the author, then there's
            // nobody to re-broadcast to
            if group.size_excl_self() == 1 {
                return None;
            }
            return Some(group.iter_skip_self_and_author(msg_author, 0)); // this validates author
        }
        None
    }

    // As Validator: When we get an AddEpochValidatorSet.
    pub fn push_group_validator_set(
        &mut self,
        validator_set: Vec<(NodeId<CertificateSignaturePubKey<ST>>, Stake)>,
        epoch: Epoch,
    ) {
        let (all_peers, _validator_stakes): (Vec<_>, Vec<_>) = validator_set.into_iter().unzip();
        let new_group = Group::new_validator_group(all_peers, &self.our_node_id);
        if let Some(existing_group) = self.validator_map.get(&epoch) {
            assert_eq!(existing_group, &new_group);
            tracing::warn!("duplicate validator set update (this is safe but unexpected)")
        } else {
            let replaced = self.validator_map.insert(epoch, new_group);
            assert!(replaced.is_none());
        }
    }

    // As Full-node: When secondary RaptorCast instance (Client) sends us a Group<>
    pub fn push_group_fullnodes(&mut self, group: Group<ST>) {
        assert!(self.is_fullnode);
        if let Some(old_grp) = self
            .fullnode_map
            .insert(*group.get_validator_id(), group.clone())
        {
            tracing::trace!(new_group=?group, old_group=?old_grp, "Group replace");
        } else {
            tracing::trace!(new_group=?group, "Group insert");
        }
    }

    pub fn delete_expired_groups(&mut self, curr_epoch: Epoch, curr_round: Round) {
        let old_count;
        let new_count;
        if self.is_fullnode {
            old_count = self.fullnode_map.len();
            self.fullnode_map
                .retain(|_, group| group.round_span.contains(curr_round));
            new_count = self.fullnode_map.len();
        } else {
            old_count = self.validator_map.len();
            self.validator_map
                .retain(|key, _| *key + Epoch(1) >= curr_epoch);
            new_count = self.validator_map.len();
        }
        tracing::trace!(
            epoch=?curr_epoch,
            round=?curr_round,
            ?old_count,
            ?new_count,
            "RaptorCast delete_expired_groups",
        );
    }

    #[cfg(test)]
    pub fn get_fullnode_map(&self) -> &BTreeMap<NodeId<CertificateSignaturePubKey<ST>>, Group<ST>> {
        &self.fullnode_map
    }
}

// Represented as a fixed-point number with 11 fractional bits.
//
// +-------------------------------------+
// | Bit 15 ...................... Bit 0 |
// | [ I I I I I F F F F F F F F F F F ] |
// |   (5 bits)  (11 bits)               |
// +-------------------------------------+
//
// I = Integer part (bits 15..11)
// F = Fractional part (bits 10..0)
// Value = Integer + Fraction / 2^11
// Range: 0 to ~31.9995, Increments: ~0.000488
#[derive(Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct Redundancy(u16);

impl Redundancy {
    pub const ZERO: Self = Self(0);
    pub const BITS: u32 = 16;
    pub const FRAC_BITS: u32 = 11;
    pub const MIN: Self = Self(u16::MIN);
    pub const MAX: Self = Self(u16::MAX);
    pub const DELTA: Self = Self(1);
    pub const MAX_MULTIPLIER: usize = usize::MAX / (u16::MAX as usize);

    // guaranteed to be lossless for num in [0,32).
    pub const fn from_u8(num: u8) -> Self {
        assert!((num as u16) <= u16::MAX >> Self::FRAC_BITS);
        Redundancy((num as u16) << Self::FRAC_BITS)
    }

    // may round to the nearest representable number when needed
    pub fn from_f32(num: f32) -> Option<Self> {
        let scaled = (num * (1u32 << Self::FRAC_BITS) as f32).round() as u32;

        if scaled > u16::MAX as u32 {
            None
        } else {
            Some(Redundancy(scaled as u16))
        }
    }

    pub fn to_f32(&self) -> f32 {
        self.0 as f32 / (1u32 << Self::FRAC_BITS) as f32
    }

    pub fn scale(&self, base: usize) -> Option<usize> {
        if base > Self::MAX_MULTIPLIER {
            return None;
        }
        let scaled = (self.0 as usize).checked_mul(base)?;
        Some(scaled.div_ceil(1 << Self::FRAC_BITS))
    }
}

impl fmt::Debug for Redundancy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.to_f32().fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use monad_secp::SecpSignature;
    use monad_testutil::signing::get_key;

    use super::*;
    type ST = SecpSignature;
    type PubKeyType = CertificateSignaturePubKey<ST>;

    // Creates a node id that we can refer to just from its seed
    fn nid(seed: u64) -> NodeId<PubKeyType> {
        let key_pair = get_key::<ST>(seed);
        let pub_key = key_pair.pubkey();
        NodeId::new(pub_key)
    }

    #[test]
    fn test_fullnode_iterator_self_on() {
        let group = Group::<ST>::new_fullnode_group(
            vec![nid(0), nid(1), nid(2)],
            &nid(1), // self_id
            nid(3),  // validator
            RoundSpan::new(Round(3), Round(8)),
        );
        assert_eq!(group.size_excl_self(), 2);
        assert_eq!(group.get_validator_id(), &nid(3));
        assert_eq!(group.get_other_peers(), &vec![nid(0), nid(2)]);
        assert_eq!(group.get_round_span(), &RoundSpan::new(Round(3), Round(8)));
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
        let group = Group::<ST>::new_fullnode_group(
            vec![nid(0), nid(1), nid(2)],
            &nid(3), // self_id
            nid(3),  // validator id
            RoundSpan::new(Round(3), Round(8)),
        );
        assert_eq!(group.size_excl_self(), 3);
        assert_eq!(group.get_validator_id(), &nid(3));
        assert_eq!(group.get_other_peers(), &vec![nid(0), nid(1), nid(2)]);
        assert_eq!(group.get_round_span(), &RoundSpan::new(Round(3), Round(8)));
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
        let group = Group::<ST>::new_fullnode_group(
            vec![nid(1)],
            &nid(1), // self_id
            nid(3),  // validator id
            RoundSpan::new(Round(3), Round(8)),
        );
        assert_eq!(group.size_excl_self(), 0);
        assert_eq!(group.get_validator_id(), &nid(3));
        assert_eq!(group.get_other_peers(), &vec![]);
        assert_eq!(group.get_round_span(), &RoundSpan::new(Round(3), Round(8)));
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
        let group = Group::<ST>::new_validator_group(
            vec![nid(0), nid(1), nid(2)],
            &nid(1), // self_id
        );
        assert_eq!(group.size_excl_self(), 2);
        assert_eq!(group.get_other_peers(), &vec![nid(0), nid(2)]);
        assert_eq!(group.get_round_span(), &RoundSpan::default());
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
        let group = Group::<ST>::new_fullnode_group(
            vec![nid(0), nid(1), nid(2), nid(3), nid(4)],
            &nid(0),  // self_id
            nid(100), // validator id
            RoundSpan::new(Round(3), Round(8)),
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
        let mut permutations_seen = HashSet::<Vec<NodeId<CertificateSignaturePubKey<ST>>>>::new();
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
        let group = Group::<ST>::new_validator_group(
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
}
