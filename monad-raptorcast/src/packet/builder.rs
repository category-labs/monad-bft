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
use rand::{CryptoRng, Rng};

use super::{
    assigner::{self, ChunkAssignment},
    regular::{self, build_header, AssembleMode, PacketLayout},
    BuildError, ChunkAssigner,
};
use crate::{
    message::MAX_MESSAGE_SIZE,
    packet::{assigner::StakeBasedWithRC, deterministic},
    udp::{GroupId, MAX_NUM_PACKETS},
    util::{
        compute_app_message_hash, unix_ts_ms_now, BroadcastMode, BuildTarget, Collector,
        Redundancy, UdpMessage,
    },
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

    // ----- Prepare override builder -----
    pub fn prepare(&self) -> PreparedMessageBuilder<'_, 'key, ST> {
        PreparedMessageBuilder {
            base: self,
            broadcast_mode: None,
        }
    }
}

pub struct PreparedMessageBuilder<'base, 'key, ST>
where
    ST: CertificateSignatureRecoverable,
{
    base: &'base MessageBuilder<'key, ST>,

    // Add extra override fields as needed
    broadcast_mode: Option<BroadcastMode>,
}

impl<'base, 'key, ST> PreparedMessageBuilder<'base, 'key, ST>
where
    ST: CertificateSignatureRecoverable,
{
    // ----- Setters for overrides -----
    #[cfg_attr(not(test), expect(unused))]
    pub fn set_broadcast_mode(mut self, broadcast_mode: BroadcastMode) -> Self {
        self.broadcast_mode = Some(broadcast_mode);
        self
    }

    // ----- Parameter validation methods -----
    fn unwrap_unix_ts_ms(&self) -> Result<u64> {
        let unix_ts_ms = match self.base.unix_ts_ms {
            TimestampMode::Fixed(ts) => ts,
            TimestampMode::RealTime => unix_ts_ms_now(),
        };
        Ok(unix_ts_ms)
    }
    fn unwrap_redundancy(&self) -> Result<Redundancy> {
        // TODO: refactor the validations on the parameters to be
        // dependent on the packet layout corresponding to the
        // broadcast mode. e.g. Deterministic RC has a fixed
        // redundancy, auto-calculated tree depth.
        let redundancy = self
            .base
            .redundancy
            .expect("redundancy must be set before building");

        if redundancy > regular::MAX_REDUNDANCY {
            return Err(BuildError::RedundancyTooHigh);
        }
        Ok(redundancy)
    }

    fn unwrap_merkle_tree_depth(&self) -> Result<u8> {
        let depth = self.base.merkle_tree_depth;
        if depth < regular::MIN_MERKLE_TREE_DEPTH {
            return Err(BuildError::MerkleTreeTooShallow);
        } else if depth > regular::MAX_MERKLE_TREE_DEPTH {
            return Err(BuildError::MerkleTreeTooDeep);
        }

        Ok(depth)
    }

    fn unwrap_segment_size(&self) -> Result<usize> {
        let segment_size = self.base.segment_size;
        debug_assert!(segment_size <= regular::MAX_SEGMENT_LENGTH);
        let min_segment_size_for_depth =
            PacketLayout::calc_segment_len(regular::MIN_CHUNK_LENGTH, self.base.merkle_tree_depth);
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
        broadcast_mode: BroadcastMode,
        group_id: GroupId,
        app_message_hash: &[u8; 20],
        app_message_len: usize,
    ) -> Result<Bytes> {
        let unix_ts_ms = self.unwrap_unix_ts_ms()?;

        let header_buf = build_header(
            broadcast_mode,
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
        build_target: &BuildTarget<'_, PT>,
        self_node_id: &NodeId<PT>,
        app_message_hash: &[u8; 20],
        rng: &mut (impl Rng + CryptoRng),
    ) -> Box<dyn ChunkAssigner<PT>>
    where
        ST: CertificateSignatureRecoverable,
    {
        use assigner::{Partitioned, Replicated, StakeBasedWithRC};
        match build_target {
            BuildTarget::PointToPoint { recipient, .. } => {
                let assigner = Replicated::from_broadcast(
                    [*recipient]
                        .into_iter()
                        .filter(|node_id| *node_id != self_node_id)
                        .cloned(),
                );
                Box::new(assigner)
            }
            BuildTarget::Broadcast(nodes) => {
                let assigner = Replicated::from_broadcast(
                    nodes
                        .iter()
                        .map(|(n, _)| n)
                        .filter(|node_id| *node_id != self_node_id)
                        .cloned(),
                );
                Box::new(assigner)
            }
            BuildTarget::Raptorcast(validators) => {
                let mut validators = validators
                    .iter()
                    .filter(|(node_id, _stake)| *node_id != self_node_id)
                    .map(|(node_id, stake)| (*node_id, *stake))
                    .collect::<Vec<_>>();

                let seed = derive_seed_regular(app_message_hash);
                StakeBasedWithRC::shuffle_validators(&mut validators, seed);
                let assigner = StakeBasedWithRC::from_validator_set(validators);
                Box::new(assigner)
            }
            BuildTarget::FullNodeRaptorCast(group) => {
                let seed = rng.gen::<usize>();
                let mut members: Vec<_> = group.iter().cloned().collect();
                randomize_secondary_group_nodes(&mut members, seed);
                Box::new(Partitioned::from_homogeneous_peers(members))
            }
        }
    }

    // ----- Build methods -----
    pub fn build_into<C>(
        &self,
        app_message: &[u8],
        build_target: &BuildTarget<'_, CertificateSignaturePubKey<ST>>,
        collector: &mut C,
    ) -> Result<()>
    where
        C: Collector<UdpMessage<CertificateSignaturePubKey<ST>>>,
    {
        let broadcast_mode = self
            .broadcast_mode
            .unwrap_or_else(|| broadcast_mode_from_build_target(build_target));

        match broadcast_mode {
            BroadcastMode::DeterministicPrimary(_) => {
                self.build_deterministic_into(app_message, build_target, broadcast_mode, collector)
            }
            BroadcastMode::Primary | BroadcastMode::Secondary | BroadcastMode::Unspecified => {
                self.build_regular_into(app_message, build_target, broadcast_mode, collector)
            }
        }
    }

    pub fn build_deterministic_into<C>(
        &self,
        app_message: &[u8],
        build_target: &BuildTarget<'_, CertificateSignaturePubKey<ST>>,
        broadcast_mode: BroadcastMode,
        collector: &mut C,
    ) -> Result<()>
    where
        C: Collector<UdpMessage<CertificateSignaturePubKey<ST>>>,
    {
        let BroadcastMode::DeterministicPrimary(round) = broadcast_mode else {
            panic!("deterministic raptorcast requires BroadcastMode::DeterministicPrimary");
        };
        let BuildTarget::Raptorcast(group) = build_target else {
            panic!("deterministic raptorcast requires Raptorcast as BuildTarget");
        };
        if app_message.len() > MAX_MESSAGE_SIZE {
            return Err(BuildError::AppMessageTooLarge);
        }

        // deterministic chunk assignment
        let app_message_hash = compute_app_message_hash(app_message).0;
        let seed = derive_seed_deterministic(&app_message_hash, round);
        let mut validators = group
            .iter()
            .map(|(node_id, stake)| (*node_id, *stake))
            .collect::<Vec<_>>();
        StakeBasedWithRC::shuffle_validators(&mut validators, seed);

        let layout = deterministic::PacketLayout::new(app_message.len(), validators.len());

        let assigner = StakeBasedWithRC::from_validator_set(validators);
        let num_base_symbols = layout.calc_num_symbols();
        let assignment = assigner.assign_chunks(num_base_symbols, None)?;
        self.check_assignment(&assignment, app_message.len())?;

        // header fields
        let group_id = build_target.group_id();
        let unix_ts_ms = self.unwrap_unix_ts_ms()?;

        // build header & assemble the chunks
        deterministic::build::<ST>(
            self.base.key.as_ref(),
            layout,
            broadcast_mode,
            group_id,
            unix_ts_ms,
            app_message,
            assignment,
            collector,
        )?;

        Ok(())
    }

    pub fn build_regular_into<C>(
        &self,
        app_message: &[u8],
        build_target: &BuildTarget<'_, CertificateSignaturePubKey<ST>>,
        broadcast_mode: BroadcastMode,
        collector: &mut C,
    ) -> Result<()>
    where
        C: Collector<UdpMessage<CertificateSignaturePubKey<ST>>>,
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
            broadcast_mode,
            build_target.group_id(),
            &app_message_hash,
            app_message.len(),
        )?;

        // assemble the chunks's headers and content
        regular::assemble::<ST>(
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
        build_target: &BuildTarget<'_, CertificateSignaturePubKey<ST>>,
    ) -> Result<Vec<UdpMessage<CertificateSignaturePubKey<ST>>>> {
        let mut packets = Vec::new();
        self.build_into(app_message, build_target, &mut packets)?;
        Ok(packets)
    }
}

fn broadcast_mode_from_build_target<PT>(build_target: &BuildTarget<'_, PT>) -> BroadcastMode
where
    PT: PubKey,
{
    match build_target {
        BuildTarget::Raptorcast { .. } => BroadcastMode::Primary,
        BuildTarget::FullNodeRaptorCast { .. } => BroadcastMode::Secondary,
        BuildTarget::Broadcast(_) | BuildTarget::PointToPoint { .. } => BroadcastMode::Unspecified,
    }
}

// introduce variation in the group member ordering
fn randomize_secondary_group_nodes<PT: PubKey>(group: &mut [NodeId<PT>], seed: usize) {
    let n = seed % group.len();
    group.rotate_left(n);
}

// Derive the seed used to shuffling validator set for deterministic raptorcast
pub fn derive_seed_deterministic(
    app_message_hash: &[u8; 20],
    round: monad_types::Round,
) -> [u8; 32] {
    let mut padded_seed = [0u8; 32];
    padded_seed[..20].copy_from_slice(app_message_hash);
    padded_seed[20..28].copy_from_slice(&round.0.to_le_bytes());
    padded_seed
}

// Derive the seed used to shuffling validator set for regular raptorcast
pub fn derive_seed_regular(app_message_hash: &[u8; 20]) -> [u8; 32] {
    let mut padded_seed = [0u8; 32];
    padded_seed[..20].copy_from_slice(app_message_hash);
    padded_seed
}
