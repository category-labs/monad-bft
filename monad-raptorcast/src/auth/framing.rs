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

    pub fn set_dedicated_identities(&mut self, identities: impl IntoIterator<Item = N>) {
        self.decoder.set_dedicated_identities(identities);
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use monad_leanudp::{
        metrics::{
            COUNTER_LEANUDP_DECODE_FRAGMENTS_DEDICATED, COUNTER_LEANUDP_DECODE_FRAGMENTS_PRIORITY,
            COUNTER_LEANUDP_DECODE_FRAGMENTS_REGULAR, GAUGE_LEANUDP_POOL_DEDICATED_MESSAGES,
            GAUGE_LEANUDP_POOL_PRIORITY_MESSAGES, GAUGE_LEANUDP_POOL_REGULAR_MESSAGES,
        },
        DecodeError,
    };
    use monad_peer_score::{PeerStatus, Score};

    use super::*;

    struct TestScore {
        promoted: BTreeSet<u64>,
    }

    impl monad_peer_score::IdentityScore for TestScore {
        type Identity = u64;

        fn score(&self, identity: &Self::Identity) -> PeerStatus {
            if self.promoted.contains(identity) {
                PeerStatus::Promoted(Score::ONE)
            } else {
                PeerStatus::Unknown
            }
        }
    }

    fn fragmented_message(framer: &mut LeanUdpFramer<u64, TestScore>, fill: u8) -> Vec<Bytes> {
        let payload = Bytes::from(vec![fill; framer.config().max_fragment_payload]);
        <LeanUdpFramer<u64, TestScore> as AuthPacketFramer<u64>>::frame(framer, payload)
            .unwrap()
            .collect()
    }

    #[test]
    fn validator_traffic_uses_dedicated_pools() {
        let config = Config {
            max_priority_messages: 1,
            max_regular_messages: 1,
            max_messages_per_identity: 1,
            max_messages_per_dedicated_identity: 1,
            max_fragment_payload: 64,
            ..Config::default()
        };
        let mut framer = LeanUdpFramer::new(
            TestScore {
                promoted: [2].into(),
            },
            config,
        );
        framer.set_dedicated_identities([3, 4]);

        let regular = fragmented_message(&mut framer, b'R');
        let priority = fragmented_message(&mut framer, b'P');
        assert_eq!(
            framer.deframe(1u64, regular[0].clone()).unwrap(),
            None,
            "regular pool should contain an incomplete message",
        );
        assert_eq!(
            framer.deframe(2u64, priority[0].clone()).unwrap(),
            None,
            "priority pool should contain an incomplete message",
        );

        let validator = fragmented_message(&mut framer, b'V');
        assert_eq!(framer.deframe(3u64, validator[0].clone()).unwrap(), None);

        let same_validator = fragmented_message(&mut framer, b'X');
        assert!(matches!(
            framer.deframe(3u64, same_validator[0].clone()),
            Err(LeanUdpFramingError::Decode(
                DecodeError::IdentityLimitExceeded { max: 1 }
            ))
        ));

        let other_validator = fragmented_message(&mut framer, b'W');
        assert_eq!(
            framer.deframe(4u64, other_validator[0].clone()).unwrap(),
            None,
            "each validator should have an independent allowance",
        );

        let mut outcome = None;
        for fragment in validator.into_iter().skip(1) {
            outcome = framer.deframe(3u64, fragment).unwrap();
        }
        assert_eq!(
            outcome,
            Some(Bytes::from(vec![
                b'V';
                framer.config().max_fragment_payload
            ]))
        );

        let metrics = framer.decoder.metrics();
        assert_eq!(metrics.gauge(GAUGE_LEANUDP_POOL_REGULAR_MESSAGES).get(), 1);
        assert_eq!(metrics.gauge(GAUGE_LEANUDP_POOL_PRIORITY_MESSAGES).get(), 1);
        assert_eq!(
            metrics.gauge(GAUGE_LEANUDP_POOL_DEDICATED_MESSAGES).get(),
            1
        );
        assert_eq!(
            metrics
                .gauge(COUNTER_LEANUDP_DECODE_FRAGMENTS_REGULAR)
                .get(),
            1
        );
        assert_eq!(
            metrics
                .gauge(COUNTER_LEANUDP_DECODE_FRAGMENTS_PRIORITY)
                .get(),
            1
        );
        assert!(
            metrics
                .gauge(COUNTER_LEANUDP_DECODE_FRAGMENTS_DEDICATED)
                .get()
                > 2
        );
    }
}
