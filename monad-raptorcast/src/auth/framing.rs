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
    collections::VecDeque,
    hash::Hash,
    marker::PhantomData,
    time::{Duration, Instant},
};

use bytes::Bytes;
use monad_executor::ExecutorMetricsChain;
use monad_leanudp::{
    Acknowledgement, Clock, Config, DecodeError, DecodeOutcome, Decoder, EncodeError, Encoder,
    FragmentPolicy, IdentityScore, Packet, RetrySender, SystemClock,
};
use monad_peer_score::PeerStatus;
use monad_types::UdpPriority;
use thiserror::Error;

pub trait AuthPacketFramer<P> {
    type Decoded;
    type Error: std::fmt::Debug;

    fn frame(
        &mut self,
        peer: P,
        payload: Bytes,
        priority: UdpPriority,
        retry_interval: Duration,
    ) -> Result<(u16, Vec<Bytes>), Self::Error>;

    fn mark_sent(&mut self, _peer: &P, _msg_id: u16) {}
    fn cancel(&mut self, _peer: &P, _msg_id: u16) {}
    fn mark_connected(&mut self, _peer: &P) {}
    fn waiting_peers(&self) -> Vec<P> {
        Vec::new()
    }
    fn next_deadline(&self) -> Option<Instant> {
        None
    }
    fn poll_outbound(&mut self) -> Vec<(P, UdpPriority, Bytes)> {
        Vec::new()
    }

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

pub struct LeanUdpFramer<N, S, C = SystemClock>
where
    N: Hash + Eq + Clone + Ord + Send + Sync,
    S: monad_peer_score::IdentityScore<Identity = N>,
    C: Clock,
{
    encoder: Encoder,
    decoder: Decoder<N, PeerScoreAdapter<S>, C>,
    sender: RetrySender<N, UdpPriority>,
    acknowledgements: VecDeque<(N, Bytes)>,
    clock: C,
    config: Config,
}

impl<N, S> LeanUdpFramer<N, S>
where
    N: Hash + Eq + Clone + Ord + Send + Sync,
    S: monad_peer_score::IdentityScore<Identity = N>,
{
    pub fn new(score_reader: S, config: Config) -> Self {
        Self::with_clock(score_reader, config, SystemClock)
    }
}

impl<N, S, C> LeanUdpFramer<N, S, C>
where
    N: Hash + Eq + Clone + Ord + Send + Sync,
    S: monad_peer_score::IdentityScore<Identity = N>,
    C: Clock,
{
    pub fn with_clock(score_reader: S, config: Config, clock: C) -> Self {
        let peer_score = PeerScoreAdapter::new(score_reader);
        let (encoder, decoder) = config.clone().build_with_clock(peer_score, clock.clone());
        let sender = RetrySender::new(config.retry.clone());
        Self {
            encoder,
            decoder,
            sender,
            acknowledgements: VecDeque::new(),
            clock,
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
            .push(self.sender.metrics())
    }
}

impl<N, S, C> AuthPacketFramer<N> for LeanUdpFramer<N, S, C>
where
    N: Hash + Eq + Clone + Ord + Send + Sync,
    S: monad_peer_score::IdentityScore<Identity = N>,
    C: Clock,
{
    type Decoded = Bytes;
    type Error = LeanUdpFramingError;

    fn frame(
        &mut self,
        peer: N,
        payload: Bytes,
        priority: UdpPriority,
        retry_interval: Duration,
    ) -> Result<(u16, Vec<Bytes>), Self::Error> {
        let fragments = self.encoder.fragment(payload)?;
        let msg_id = fragments.msg_id();
        self.sender.track(
            peer,
            fragments.clone(),
            priority,
            retry_interval,
            self.clock.now(),
        )?;
        Ok((
            msg_id,
            (0..fragments.fragment_count() as u16)
                .map(move |seq| fragments.packet(seq).expect("valid fragment"))
                .collect(),
        ))
    }

    fn mark_sent(&mut self, peer: &N, msg_id: u16) {
        self.sender.mark_sent(peer, msg_id, self.clock.now());
    }
    fn cancel(&mut self, peer: &N, msg_id: u16) {
        self.sender.cancel(peer, msg_id);
    }
    fn mark_connected(&mut self, peer: &N) {
        self.sender.mark_connected(peer, self.clock.now());
    }
    fn waiting_peers(&self) -> Vec<N> {
        self.sender.waiting_peers()
    }
    fn next_deadline(&self) -> Option<Instant> {
        self.sender
            .next_deadline()
            .into_iter()
            .chain(self.decoder.next_deadline())
            .min()
    }
    fn poll_outbound(&mut self) -> Vec<(N, UdpPriority, Bytes)> {
        self.acknowledgements.extend(
            self.decoder
                .poll_acknowledgements()
                .into_iter()
                .map(|(peer, ack)| (peer, ack.encode())),
        );
        let mut packets: Vec<_> = self
            .acknowledgements
            .drain(..)
            .map(|(peer, packet)| (peer, UdpPriority::High, packet))
            .collect();
        packets.extend(self.sender.poll_retries(self.clock.now()));
        packets
    }

    fn deframe(
        &mut self,
        identity: N,
        packet: Bytes,
    ) -> Result<Option<Self::Decoded>, Self::Error> {
        let packet = Packet::try_from(packet)?;
        if let Some(ack) = Acknowledgement::from_packet(&packet)? {
            self.sender.acknowledge(&identity, ack, self.clock.now());
            return Ok(None);
        }
        let (outcome, ack) = self.decoder.decode_with_ack(identity.clone(), packet)?;
        if let Some(ack) = ack {
            self.acknowledgements.push_back((identity, ack.encode()));
        }
        match outcome {
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
        <LeanUdpFramer<u64, TestScore> as AuthPacketFramer<u64>>::frame(
            framer,
            0,
            payload,
            UdpPriority::Regular,
            Duration::from_millis(500),
        )
        .unwrap()
        .1
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
