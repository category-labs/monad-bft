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

use std::{hash::Hash, marker::PhantomData};

use bytes::{BufMut, Bytes, BytesMut};
use monad_executor::ExecutorMetricsChain;
use monad_leanudp::{
    Config, DecodeError, DecodeOutcome, Decoder, EncodeError, Encoder, FragmentPolicy,
    IdentityScore, PacketHeader, SystemClock,
};
use monad_peer_score::PeerStatus;
use thiserror::Error;
use zerocopy::IntoBytes;

pub trait AuthPacketFramer<P> {
    type Decoded;
    type Error: std::fmt::Debug;

    fn frame(&mut self, payload: Bytes) -> Result<impl Iterator<Item = Bytes>, Self::Error>;

    fn deframe(
        &mut self,
        public_key: P,
        packet: Bytes,
    ) -> Result<Option<Self::Decoded>, Self::Error>;

    fn metrics(&self) -> ExecutorMetricsChain<'_> {
        ExecutorMetricsChain::default()
    }
}

pub struct PeerScoreAdapter<S> {
    score_reader: S,
}

impl<S> PeerScoreAdapter<S> {
    pub fn new(score_reader: S) -> Self {
        Self { score_reader }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NopScore<N>(PhantomData<N>);

impl<N> NopScore<N> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<N> monad_peer_score::IdentityScore for NopScore<N> {
    type Identity = N;

    fn score(&self, _identity: &Self::Identity) -> PeerStatus {
        PeerStatus::Unknown
    }
}

impl<N, S> IdentityScore for PeerScoreAdapter<S>
where
    N: Hash + Eq + Send + Sync,
    S: monad_peer_score::IdentityScore<Identity = N>,
{
    type Identity = N;

    fn score(&self, identity: &Self::Identity) -> FragmentPolicy {
        if self.score_reader.score(identity).is_promoted() {
            FragmentPolicy::Prioritized
        } else {
            FragmentPolicy::Regular
        }
    }
}

#[derive(Debug, Error)]
pub enum LeanUdpFramingError {
    #[error(transparent)]
    Encode(#[from] EncodeError),
    #[error(transparent)]
    Decode(#[from] DecodeError),
}

pub struct LeanUdpFramer<N, S>
where
    N: Hash + Eq + Clone + Ord + Send + Sync,
    S: monad_peer_score::IdentityScore<Identity = N>,
{
    encoder: Encoder,
    decoder: Decoder<N, PeerScoreAdapter<S>, SystemClock>,
    config: Config,
}

impl<N, S> LeanUdpFramer<N, S>
where
    N: Hash + Eq + Clone + Ord + Send + Sync,
    S: monad_peer_score::IdentityScore<Identity = N>,
{
    pub fn new(score_reader: S, config: Config) -> Self {
        let peer_score = PeerScoreAdapter::new(score_reader);
        let (encoder, decoder) = config.clone().build(peer_score);

        Self {
            encoder,
            decoder,
            config,
        }
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn metrics(&self) -> ExecutorMetricsChain<'_> {
        ExecutorMetricsChain::default()
            .push(self.encoder.executor_metrics())
            .push(self.decoder.executor_metrics())
    }
}

impl<P, N, S> AuthPacketFramer<P> for LeanUdpFramer<N, S>
where
    P: Into<N>,
    N: Hash + Eq + Clone + Ord + Send + Sync,
    S: monad_peer_score::IdentityScore<Identity = N>,
{
    type Decoded = Bytes;
    type Error = LeanUdpFramingError;

    fn frame(&mut self, payload: Bytes) -> Result<impl Iterator<Item = Bytes>, Self::Error> {
        let fragments = self.encoder.fragment(payload)?;

        Ok(fragments.map(move |(header, data)| {
            let mut buf = BytesMut::with_capacity(PacketHeader::SIZE + data.len());
            buf.put_slice(header.as_bytes());
            buf.put_slice(&data);
            buf.freeze()
        }))
    }

    fn deframe(
        &mut self,
        public_key: P,
        packet: Bytes,
    ) -> Result<Option<Self::Decoded>, Self::Error> {
        let identity = public_key.into();
        match self.decoder.decode(identity, packet)? {
            DecodeOutcome::Pending => Ok(None),
            DecodeOutcome::Complete(payload) => Ok(Some(payload)),
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain<'_> {
        LeanUdpFramer::metrics(self)
    }
}
