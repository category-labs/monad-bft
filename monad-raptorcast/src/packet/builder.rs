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

use std::sync::Arc;

use bytes::Bytes;
use monad_crypto::certificate_signature::{
    CertificateKeyPair as _, CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_dataplane::udp::DEFAULT_SEGMENT_SIZE;
use monad_types::NodeId;
use rand::Rng;

use super::{
    assembler::{self, build_header, AssembleMode, BroadcastType, PacketLayout},
    assigner::{self, ChunkAssignment},
    BuildError, ChunkAssigner, UdpMessage,
};
use crate::{
    message::MAX_MESSAGE_SIZE,
    udp::{
        GroupId, MAX_MERKLE_TREE_DEPTH, MAX_NUM_PACKETS, MAX_REDUNDANCY, MAX_SEGMENT_LENGTH,
        MIN_CHUNK_LENGTH, MIN_MERKLE_TREE_DEPTH,
    },
    util::{compute_app_message_hash, unix_ts_ms_now, BuildTarget, Redundancy},
};

pub const DEFAULT_MERKLE_TREE_DEPTH: u8 = 6;

type Result<T, E = BuildError> = std::result::Result<T, E>;

enum MaybeArc<'a, T> {
    Ref(&'a T),
    Arc(Arc<T>),
}

impl<T> From<Arc<T>> for MaybeArc<'_, T> {
    fn from(arc: Arc<T>) -> Self {
        MaybeArc::Arc(arc)
    }
}

impl<'a, T> From<&'a T> for MaybeArc<'a, T> {
    fn from(r: &'a T) -> Self {
        MaybeArc::Ref(r)
    }
}

impl<T> Clone for MaybeArc<'_, T> {
    fn clone(&self) -> Self {
        match self {
            MaybeArc::Ref(r) => MaybeArc::Ref(r),
            MaybeArc::Arc(a) => MaybeArc::Arc(a.clone()),
        }
    }
}

impl<'a, T> AsRef<T> for MaybeArc<'a, T> {
    fn as_ref(&self) -> &T {
        match self {
            MaybeArc::Ref(r) => r,
            MaybeArc::Arc(a) => a.as_ref(),
        }
    }
}

#[derive(Clone, Copy)]
enum TimestampMode {
    Fixed(u64),
    RealTime,
}

pub struct MessageBuilder<'key, ST>
where
    ST: CertificateSignatureRecoverable,
{
    // support both owned or borrowed keys
    key: MaybeArc<'key, ST::KeyPairType>,

    // required fields
    group_id: Option<GroupId>,
    redundancy: Option<Redundancy>,

    // optional fields
    unix_ts_ms: TimestampMode,
    segment_size: usize,
    merkle_tree_depth: u8,
    assemble_mode: AssembleMode,
}

impl<'key, ST> Clone for MessageBuilder<'key, ST>
where
    ST: CertificateSignatureRecoverable,
{
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            group_id: self.group_id,
            redundancy: self.redundancy,
            unix_ts_ms: self.unix_ts_ms,
            segment_size: self.segment_size,
            merkle_tree_depth: self.merkle_tree_depth,
            assemble_mode: self.assemble_mode,
        }
    }
}

impl<'key, ST> MessageBuilder<'key, ST>
where
    ST: CertificateSignatureRecoverable,
{
    #[allow(private_bounds)]
    pub fn new<K>(key: K) -> Self
    where
        K: Into<MaybeArc<'key, ST::KeyPairType>>,
    {
        let segment_size = DEFAULT_SEGMENT_SIZE as usize;
        let merkle_tree_depth = DEFAULT_MERKLE_TREE_DEPTH;
        let key = key.into();

        Self {
            key,

            // default fields
            redundancy: None,
            group_id: None,
            unix_ts_ms: TimestampMode::RealTime,

            // optional fields
            assemble_mode: AssembleMode::default(),
            segment_size,
            merkle_tree_depth,
        }
    }

    // ----- Field filling methods -----
    pub fn segment_size(mut self, size: impl Into<usize>) -> Self {
        self.segment_size = size.into();
        self
    }

    pub fn redundancy(mut self, redundancy: Redundancy) -> Self {
        self.redundancy = Some(redundancy);
        self
    }

    pub fn group_id(mut self, group_id: GroupId) -> Self {
        self.group_id = Some(group_id);
        self
    }

    pub fn unix_ts_ms(mut self, unix_ts_ms: impl Into<u64>) -> Self {
        self.unix_ts_ms = TimestampMode::Fixed(unix_ts_ms.into());
        self
    }

    // we currently don't use non-standard merkle_tree_depth
    #[cfg_attr(not(test), expect(unused))]
    pub fn merkle_tree_depth(mut self, depth: u8) -> Self {
        self.merkle_tree_depth = depth;
        self
    }

    // we currently don't use any non-standard assemble mode.
    #[expect(unused)]
    pub fn assemble_mode(mut self, mode: AssembleMode) -> Self {
        self.assemble_mode = mode;
        self
    }

    // ----- Convenience methods for modifying the builder -----
    pub fn set_group_id(&mut self, group_id: GroupId) {
        self.group_id = Some(group_id);
    }

    // ----- Prepare override builder -----
    pub fn prepare(&self) -> PreparedMessageBuilder<'_, 'key, ST> {
        PreparedMessageBuilder {
            base: self,
            group_id: None,
        }
    }

    // ----- Delegated build methods -----
    pub fn build_into<C>(
        &self,
        app_message: &[u8],
        build_target: &BuildTarget<CertificateSignaturePubKey<ST>>,
        collector: &mut C,
    ) -> Result<()>
    where
        C: super::Collector<UdpMessage<CertificateSignaturePubKey<ST>>>,
    {
        self.prepare()
            .build_into(app_message, build_target, collector)
    }

    pub fn build_vec(
        &self,
        app_message: &[u8],
        build_target: &BuildTarget<CertificateSignaturePubKey<ST>>,
    ) -> Result<Vec<UdpMessage<CertificateSignaturePubKey<ST>>>> {
        self.prepare().build_vec(app_message, build_target)
    }
}

pub struct PreparedMessageBuilder<'base, 'key, ST>
where
    ST: CertificateSignatureRecoverable,
{
    base: &'base MessageBuilder<'key, ST>,

    // Add extra override fields as needed
    group_id: Option<GroupId>,
}

impl<'base, 'key, ST> PreparedMessageBuilder<'base, 'key, ST>
where
    ST: CertificateSignatureRecoverable,
{
    // ----- Setters for overrides -----
    pub fn group_id(mut self, group_id: GroupId) -> Self {
        self.group_id = Some(group_id);
        self
    }

    // ----- Parameter validation methods -----
    fn unwrap_group_id(&self) -> Result<GroupId> {
        if let Some(group_id) = self.group_id {
            return Ok(group_id);
        }
        let group_id = self
            .base
            .group_id
            .expect("group_id must be set before building");
        Ok(group_id)
    }
    fn unwrap_unix_ts_ms(&self) -> Result<u64> {
        let unix_ts_ms = match self.base.unix_ts_ms {
            TimestampMode::Fixed(ts) => ts,
            TimestampMode::RealTime => unix_ts_ms_now(),
        };
        Ok(unix_ts_ms)
    }
    fn unwrap_redundancy(&self) -> Result<Redundancy> {
        let redundancy = self
            .base
            .redundancy
            .expect("redundancy must be set before building");

        if redundancy > MAX_REDUNDANCY {
            return Err(BuildError::RedundancyTooHigh);
        }
        Ok(redundancy)
    }

    fn unwrap_merkle_tree_depth(&self) -> Result<u8> {
        let depth = self.base.merkle_tree_depth;
        if depth < MIN_MERKLE_TREE_DEPTH {
            return Err(BuildError::MerkleTreeTooShallow);
        } else if depth > MAX_MERKLE_TREE_DEPTH {
            return Err(BuildError::MerkleTreeTooDeep);
        }

        Ok(depth)
    }

    fn unwrap_segment_size(&self) -> Result<usize> {
        let segment_size = self.base.segment_size;
        debug_assert!(segment_size <= MAX_SEGMENT_LENGTH);
        let min_segment_size_for_depth =
            PacketLayout::calc_segment_len(MIN_CHUNK_LENGTH, self.base.merkle_tree_depth);
        debug_assert!(segment_size >= min_segment_size_for_depth);

        Ok(segment_size)
    }

    fn checked_message_len(&self, len: usize) -> Result<usize> {
        if len > MAX_MESSAGE_SIZE {
            return Err(BuildError::AppMessageTooLarge);
        }
        Ok(len)
    }

    fn check_assignment(
        &self,
        assignment: &ChunkAssignment<CertificateSignaturePubKey<ST>>,
        app_msg_len: usize, // only used for logging
    ) -> Result<()> {
        if assignment.is_empty() {
            tracing::warn!(?app_msg_len, "no chunk generated");
            return Ok(());
        }

        if assignment.total_chunks() > MAX_NUM_PACKETS {
            return Err(BuildError::TooManyChunks);
        }

        Ok(())
    }

    // ----- Helper methods -----
    fn calc_num_symbols(&self, layout: PacketLayout, app_message_len: usize) -> Result<usize> {
        let redundancy = self.unwrap_redundancy()?;
        let num_symbols = layout
            .calc_num_symbols(app_message_len, redundancy)
            .ok_or(BuildError::TooManyChunks)?;
        if num_symbols > MAX_NUM_PACKETS {
            return Err(BuildError::TooManyChunks);
        }

        Ok(num_symbols)
    }

    fn build_header(
        &self,
        merkle_tree_depth: u8,
        layout: PacketLayout,
        broadcast_type: BroadcastType,
        app_message_hash: &[u8; 20],
        app_message_len: usize,
    ) -> Result<Bytes> {
        let group_id = self.unwrap_group_id()?;
        let unix_ts_ms = self.unwrap_unix_ts_ms()?;

        let header_buf = build_header(
            0, // version
            broadcast_type,
            merkle_tree_depth,
            group_id,
            unix_ts_ms,
            app_message_hash,
            app_message_len,
        )?;

        debug_assert_eq!(header_buf.len(), layout.header_sans_signature_range().len());

        Ok(header_buf)
    }

    fn make_assigner<PT: PubKey>(
        build_target: &BuildTarget<PT>,
        self_node_id: &NodeId<PT>,
        app_message_hash: &[u8; 20],
        rng: &mut impl Rng,
    ) -> Box<dyn ChunkAssigner<PT>>
    where
        ST: CertificateSignatureRecoverable,
    {
        use assigner::{Partitioned, Replicated, StakeBasedWithRC};
        match build_target {
            BuildTarget::PointToPoint(to) => Box::new(Replicated::from_unicast(**to)),
            BuildTarget::Broadcast(nodes) => {
                let assigner = Replicated::from_broadcast(nodes.iter().copied().collect());
                Box::new(assigner)
            }
            BuildTarget::Raptorcast(validators) => {
                let seed =
                    StakeBasedWithRC::<CertificateSignaturePubKey<ST>>::seed_from_app_message_hash(
                        app_message_hash,
                    );
                let sorted_validators = StakeBasedWithRC::shuffle_validators(validators, seed);
                let assigner = StakeBasedWithRC::from_validator_set(sorted_validators);
                Box::new(assigner)
            }
            BuildTarget::FullNodeRaptorCast(group) => {
                let seed = rng.gen::<usize>();
                let nodes = group
                    .iter_skip_self_and_author(self_node_id, seed)
                    .copied()
                    .collect();
                Box::new(Partitioned::from_homogeneous_peers(nodes))
            }
        }
    }

    // ----- Build methods -----
    pub fn build_into<C>(
        &self,
        app_message: &[u8],
        build_target: &BuildTarget<CertificateSignaturePubKey<ST>>,
        collector: &mut C,
    ) -> Result<()>
    where
        C: super::Collector<super::UdpMessage<CertificateSignaturePubKey<ST>>>,
    {
        // figure out the layout of the packet
        let segment_size = self.unwrap_segment_size()?;
        let depth = self.unwrap_merkle_tree_depth()?;
        let layout = PacketLayout::new(segment_size, depth);

        // compute app message hash
        let app_message_hash = compute_app_message_hash(app_message).0;

        // select chunk assignment algorithm based on build target
        let rng = &mut rand::thread_rng();
        let self_node_id = NodeId::new(self.base.key.as_ref().pubkey());
        let assigner = Self::make_assigner(build_target, &self_node_id, &app_message_hash, rng);

        // calculate the number of symbols needed for assignment
        let app_message_len = self.checked_message_len(app_message.len())?;
        let num_symbols = self.calc_num_symbols(layout, app_message_len)?;

        // assign the chunks to recipients
        let assemble_mode = self.base.assemble_mode;
        let order = assemble_mode.expected_chunk_order();
        let mut assignment = assigner.assign_chunks(num_symbols, order)?;
        assignment.ensure_order(order);
        self.check_assignment(&assignment, app_message_len)?;

        // build the shared header
        let header = self.build_header(
            depth,
            layout,
            broadcast_type_from_build_target(build_target),
            &app_message_hash,
            app_message.len(),
        )?;

        // assemble the chunks's headers and content
        assembler::assemble::<ST>(
            self.base.key.as_ref(),
            layout,
            app_message,
            &header,
            assignment,
            assemble_mode,
            collector,
        )?;

        Ok(())
    }

    pub fn build_vec(
        &self,
        app_message: &[u8],
        build_target: &BuildTarget<CertificateSignaturePubKey<ST>>,
    ) -> Result<Vec<UdpMessage<CertificateSignaturePubKey<ST>>>> {
        let mut packets = Vec::new();
        self.build_into(app_message, build_target, &mut packets)?;
        Ok(packets)
    }
}

fn broadcast_type_from_build_target<PT>(build_target: &BuildTarget<'_, PT>) -> BroadcastType
where
    PT: PubKey,
{
    match build_target {
        BuildTarget::Raptorcast { .. } => BroadcastType::Primary,
        BuildTarget::FullNodeRaptorCast { .. } => BroadcastType::Secondary,
        _ => BroadcastType::Unspecified,
    }
}
