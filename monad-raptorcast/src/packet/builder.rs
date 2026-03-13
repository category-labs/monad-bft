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

use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_dataplane::udp::DEFAULT_SEGMENT_SIZE;

use super::{regular, BuildError};
use crate::{
    message::MAX_MESSAGE_SIZE,
    util::{unix_ts_ms_now, BuildTarget, Collector, Redundancy, UdpMessage},
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

    // ----- Parameter validation methods -----
    fn unwrap_unix_ts_ms(&self) -> Result<u64> {
        let unix_ts_ms = match self.unix_ts_ms {
            TimestampMode::Fixed(ts) => ts,
            TimestampMode::RealTime => unix_ts_ms_now(),
        };
        Ok(unix_ts_ms)
    }
    fn unwrap_redundancy(&self) -> Result<Redundancy> {
        let redundancy = self
            .redundancy
            .expect("redundancy must be set before building");

        if redundancy > regular::MAX_REDUNDANCY {
            return Err(BuildError::RedundancyTooHigh);
        }
        Ok(redundancy)
    }

    fn unwrap_merkle_tree_depth(&self) -> Result<u8> {
        let depth = self.merkle_tree_depth;
        if depth < regular::MIN_MERKLE_TREE_DEPTH {
            return Err(BuildError::MerkleTreeTooShallow);
        } else if depth > regular::MAX_MERKLE_TREE_DEPTH {
            return Err(BuildError::MerkleTreeTooDeep);
        }

        Ok(depth)
    }

    fn unwrap_segment_size(&self) -> Result<usize> {
        let segment_size = self.segment_size;
        debug_assert!(segment_size <= regular::MAX_SEGMENT_LENGTH);
        let min_segment_size_for_depth = regular::PacketLayout::calc_segment_len(
            regular::MIN_CHUNK_LENGTH,
            self.merkle_tree_depth,
        );
        debug_assert!(segment_size >= min_segment_size_for_depth);

        Ok(segment_size)
    }

    fn checked_message_len(&self, len: usize) -> Result<usize> {
        if len > MAX_MESSAGE_SIZE {
            return Err(BuildError::AppMessageTooLarge);
        }
        Ok(len)
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
        let segment_size = self.unwrap_segment_size()?;
        let depth = self.unwrap_merkle_tree_depth()?;
        let redundancy = self.unwrap_redundancy()?;
        let unix_ts_ms = self.unwrap_unix_ts_ms()?;
        self.checked_message_len(app_message.len())?;

        let layout = regular::PacketLayout::new(segment_size, depth);

        regular::build_into::<ST, C>(
            self.key.as_ref(),
            layout,
            redundancy,
            unix_ts_ms,
            app_message,
            build_target,
            collector,
        )
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
