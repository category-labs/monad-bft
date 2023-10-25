use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    fmt::Debug,
    mem,
    time::Duration,
};

use monad_executor_glue::PeerId;
use monad_tracing_counter::inc_count;
use rand::{prelude::SliceRandom, Rng};
use rand_chacha::{rand_core::SeedableRng, ChaCha20Rng, ChaChaRng};

const UNIQUE_ID: usize = 0;
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ID(usize, PeerId);

impl ID {
    pub fn new(peer_id: PeerId) -> Self {
        Self(UNIQUE_ID, peer_id)
    }

    pub fn as_non_unique(mut self, identifier: usize) -> Self {
        assert_ne!(identifier, UNIQUE_ID);
        self.0 = identifier;
        self
    }

    pub fn get_identifier(&self) -> &usize {
        &self.0
    }

    pub fn get_peer_id(&self) -> &PeerId {
        &self.1
    }

    pub fn is_unique(&self) -> bool {
        self.0 == UNIQUE_ID
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkMessage<M> {
    pub from: ID,
    pub to: ID,
    pub message: M,

    /// absolute time
    pub from_tick: Duration,
}

impl<M: Eq> Ord for LinkMessage<M> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.from_tick, self.from, self.to).cmp(&(other.from_tick, other.from, other.to))
    }
}
impl<M> PartialOrd for LinkMessage<M>
where
    Self: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/**
 * TransformerStream carries (Duration, LinkedMessages) that emitted
 * after a transformer with Transform Trait complete their transform
 *
 * Content of a Continue Stream will get processed further
 *
 * Content of a Complete Stream will no longer get processed
 *
 */
type StreamMessage<M> = (Duration, LinkMessage<M>);
pub enum TransformerStream<M> {
    Continue(Vec<StreamMessage<M>>),
    Complete(Vec<StreamMessage<M>>),
}

pub trait Transformer<M> {
    #[must_use]
    /// note that the output Duration should be a delay, not an absolute time
    // TODO smallvec? resulting Vec will almost always be len 1
    fn transform(&mut self, message: LinkMessage<M>) -> TransformerStream<M>;

    fn min_external_delay(&self) -> Option<Duration> {
        None
    }

    fn boxed(self) -> Box<dyn Transformer<M>>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}
/// adds constant latency
#[derive(Clone, Debug)]
pub struct LatencyTransformer(pub Duration);
impl<M> Transformer<M> for LatencyTransformer {
    fn transform(&mut self, message: LinkMessage<M>) -> TransformerStream<M> {
        TransformerStream::Continue(vec![(self.0, message)])
    }

    fn min_external_delay(&self) -> Option<Duration> {
        Some(self.0)
    }
}

/// adds constant latency (parametrizable cap) to each link determined by xor(peer_id_1, peer_id_2)
#[derive(Clone, Debug)]
pub struct XorLatencyTransformer(pub Duration);
impl<M> Transformer<M> for XorLatencyTransformer {
    fn transform(&mut self, message: LinkMessage<M>) -> TransformerStream<M> {
        let mut ck: u8 = 0;
        for b in message.from.get_peer_id().0.bytes() {
            ck ^= b;
        }
        for b in message.to.get_peer_id().0.bytes() {
            ck ^= b;
        }
        TransformerStream::Continue(vec![(self.0.mul_f32(ck as f32 / u8::MAX as f32), message)])
    }
}

/// adds random latency to each message up to a cap
#[derive(Clone, Debug)]
pub struct RandLatencyTransformer {
    gen: ChaChaRng,
    max_latency: u64,
}

impl RandLatencyTransformer {
    pub fn new(seed: u64, max_latency: u64) -> Self {
        RandLatencyTransformer {
            gen: ChaChaRng::seed_from_u64(seed),
            max_latency,
        }
    }

    pub fn next_latency(&mut self) -> Duration {
        let s = self.gen.gen_range(1..self.max_latency);

        Duration::from_millis(s)
    }
}

impl<M> Transformer<M> for RandLatencyTransformer {
    fn transform(&mut self, message: LinkMessage<M>) -> TransformerStream<M> {
        TransformerStream::Continue(vec![(self.next_latency(), message)])
    }
}
#[derive(Clone)]
pub struct PartitionTransformer(pub HashSet<ID>);

impl<M> Transformer<M> for PartitionTransformer {
    fn transform(&mut self, message: LinkMessage<M>) -> TransformerStream<M> {
        if self.0.contains(&message.from) || self.0.contains(&message.to) {
            TransformerStream::Continue(vec![(Duration::ZERO, message)])
        } else {
            TransformerStream::Complete(vec![(Duration::ZERO, message)])
        }
    }
}

impl Debug for PartitionTransformer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionTransformer")
            .field("peers", &self.0)
            .finish()
    }
}

#[derive(Clone, Debug)]

pub struct DropTransformer();

impl<M> Transformer<M> for DropTransformer {
    fn transform(&mut self, _: LinkMessage<M>) -> TransformerStream<M> {
        TransformerStream::Complete(vec![])
    }
}

#[derive(Clone, Debug)]
pub struct PeriodicTransformer {
    pub start: Duration,
    pub end: Duration,
}

impl PeriodicTransformer {
    /// [start, end)
    pub fn new(start: Duration, end: Duration) -> Self {
        assert!(start <= end);
        PeriodicTransformer { start, end }
    }
}

impl<M> Transformer<M> for PeriodicTransformer {
    fn transform(&mut self, message: LinkMessage<M>) -> TransformerStream<M> {
        if message.from_tick < self.start || message.from_tick >= self.end {
            TransformerStream::Complete(vec![(Duration::ZERO, message)])
        } else {
            TransformerStream::Continue(vec![(Duration::ZERO, message)])
        }
    }
}
#[derive(Clone, Debug)]
pub enum TransformerReplayOrder {
    Forward,
    Reverse,
    Random(u64),
}
#[derive(Clone)]
pub struct ReplayTransformer<M> {
    pub filtered_msgs: Vec<LinkMessage<M>>,
    pub end: Duration,
    pub order: TransformerReplayOrder,
}

impl<M> ReplayTransformer<M> {
    /// [Duration::ZERO, end)
    pub fn new(end: Duration, order: TransformerReplayOrder) -> Self {
        ReplayTransformer {
            filtered_msgs: Vec::new(),
            end,
            order,
        }
    }
}

impl<M> Debug for ReplayTransformer<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplayTransformer")
            .field("end", &self.end)
            .field("order", &self.order)
            .finish()
    }
}

impl<M> Transformer<M> for ReplayTransformer<M> {
    fn transform(&mut self, message: LinkMessage<M>) -> TransformerStream<M> {
        if message.from_tick >= self.end {
            if self.filtered_msgs.is_empty() {
                return TransformerStream::Continue(vec![(Duration::ZERO, message)]);
            }

            self.filtered_msgs.push(message);
            let mut result = mem::take(&mut self.filtered_msgs);
            let msgs = match self.order {
                TransformerReplayOrder::Forward => result,
                TransformerReplayOrder::Reverse => {
                    result.reverse();
                    result
                }
                TransformerReplayOrder::Random(seed) => {
                    let mut gen = ChaChaRng::seed_from_u64(seed);
                    result.shuffle(&mut gen);
                    result
                }
            };

            return TransformerStream::Continue(
                std::iter::repeat(Duration::ZERO).zip(msgs).collect(),
            );
        }

        self.filtered_msgs.push(message);
        TransformerStream::Continue(Vec::new())
    }
}

#[derive(Debug, Clone)]
pub enum GenericTransformer<M> {
    Latency(LatencyTransformer),
    XorLatency(XorLatencyTransformer),
    RandLatency(RandLatencyTransformer),
    Partition(PartitionTransformer),
    Drop(DropTransformer),
    Periodic(PeriodicTransformer),
    Replay(ReplayTransformer<M>),
}

impl<M> Transformer<M> for GenericTransformer<M> {
    fn transform(&mut self, message: LinkMessage<M>) -> TransformerStream<M> {
        match self {
            GenericTransformer::Latency(t) => t.transform(message),
            GenericTransformer::XorLatency(t) => t.transform(message),
            GenericTransformer::RandLatency(t) => t.transform(message),
            GenericTransformer::Partition(t) => t.transform(message),
            GenericTransformer::Drop(t) => t.transform(message),
            GenericTransformer::Periodic(t) => t.transform(message),
            GenericTransformer::Replay(t) => t.transform(message),
        }
    }

    fn min_external_delay(&self) -> Option<Duration> {
        match self {
            GenericTransformer::Latency(t) => {
                <LatencyTransformer as Transformer<M>>::min_external_delay(t)
            }
            GenericTransformer::XorLatency(t) => {
                <XorLatencyTransformer as Transformer<M>>::min_external_delay(t)
            }
            GenericTransformer::RandLatency(t) => {
                <RandLatencyTransformer as Transformer<M>>::min_external_delay(t)
            }
            GenericTransformer::Partition(t) => {
                <PartitionTransformer as Transformer<M>>::min_external_delay(t)
            }
            GenericTransformer::Drop(t) => {
                <DropTransformer as Transformer<M>>::min_external_delay(t)
            }
            GenericTransformer::Periodic(t) => {
                <PeriodicTransformer as Transformer<M>>::min_external_delay(t)
            }
            GenericTransformer::Replay(t) => t.min_external_delay(),
        }
    }
}

pub type GenericTransformerPipeline<M> = Vec<GenericTransformer<M>>;

#[derive(Debug, Clone)]
pub struct BytesSplitterTransformer {
    rng: ChaCha20Rng,
    buffers: BTreeMap<ID, VecDeque<LinkMessage<Vec<u8>>>>,
}

impl Default for BytesSplitterTransformer {
    fn default() -> Self {
        Self {
            rng: ChaCha20Rng::from_seed([0_u8; 32]),
            buffers: Default::default(),
        }
    }
}

impl BytesSplitterTransformer {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Transformer<Vec<u8>> for BytesSplitterTransformer {
    fn transform(&mut self, message: LinkMessage<Vec<u8>>) -> TransformerStream<Vec<u8>> {
        let entry = self.buffers.entry(message.to).or_default();
        entry.push_back(message);

        let split_idx = self.rng.gen_range(0..entry.len());

        let mut base_message = entry
            .drain(0..split_idx)
            .reduce(|mut base_message, mut merge_message| {
                base_message.message.append(&mut merge_message.message);
                base_message
            })
            .unwrap_or({
                let split_message = entry
                    .front()
                    .expect("must be at least 1 element (split_idx)");
                LinkMessage {
                    from: split_message.from,
                    to: split_message.to,
                    from_tick: split_message.from_tick,
                    message: Vec::new(),
                }
            });
        let split_message = entry
            .front_mut()
            .expect("must be at least 1 element (split_idx)");

        let message_split_idx = self.rng.gen_range(1..=split_message.message.len());
        base_message
            .message
            .extend(split_message.message.drain(0..message_split_idx));

        if split_message.message.is_empty() {
            entry.pop_front();
        }

        TransformerStream::Continue(vec![(Duration::ZERO, base_message)])
    }
}

#[derive(Clone, Debug)]
struct BwWindow {
    // sliding window of (msg.from_tick, msg.bit_len) sent in the last second
    window: VecDeque<(Duration, usize)>,
    total_bits: usize,
}

impl Default for BwWindow {
    fn default() -> Self {
        Self {
            window: Default::default(),
            total_bits: 0,
        }
    }
}

impl BwWindow {
    fn advance_to(&mut self, now: Duration) {
        while self
            .window
            .front()
            .map_or(false, |&(tick, _)| tick + Duration::from_secs(1) < now)
        {
            let (_, bitlen) = self.window.pop_front().unwrap();
            self.total_bits -= bitlen;
        }
    }

    fn try_push(&mut self, bps_limit: usize, msg: &LinkMessage<Vec<u8>>) -> bool {
        // assert msgs are increasing in from_tick
        assert!(self
            .window
            .back()
            .map_or(true, |&(tick, _)| msg.from_tick >= tick));
        let bit_len = msg.message.len() * 8;
        if self.total_bits + bit_len > bps_limit {
            false
        } else {
            self.total_bits += bit_len;
            self.window.push_back((msg.from_tick, bit_len));
            true
        }
    }
}

#[derive(Clone, Debug)]
pub struct BwTransformer {
    // unit: bits per second
    upload_bps: usize,
    // upload bandwidth window
    window: BwWindow,
}

impl BwTransformer {
    pub fn new(upload_mbps: usize) -> Self {
        Self {
            upload_bps: upload_mbps * 1024 * 1024,
            window: BwWindow::default(),
        }
    }
}

impl Transformer<Vec<u8>> for BwTransformer {
    fn transform(&mut self, message: LinkMessage<Vec<u8>>) -> TransformerStream<Vec<u8>> {
        self.window.advance_to(message.from_tick);

        if self.window.try_push(self.upload_bps, &message) {
            TransformerStream::Continue(vec![(Duration::ZERO, message)])
        } else {
            inc_count!(bwtransfomer_dropped_msg);
            TransformerStream::Complete(vec![])
        }
    }
}

#[derive(Debug, Clone)]
pub enum BytesTransformer {
    Latency(LatencyTransformer),
    XorLatency(XorLatencyTransformer),
    RandLatency(RandLatencyTransformer),
    Partition(PartitionTransformer),
    Drop(DropTransformer),
    Periodic(PeriodicTransformer),
    Replay(ReplayTransformer<Vec<u8>>),

    BytesSplitter(BytesSplitterTransformer),
    Bw(BwTransformer),
}

impl Transformer<Vec<u8>> for BytesTransformer {
    fn transform(&mut self, message: LinkMessage<Vec<u8>>) -> TransformerStream<Vec<u8>> {
        match self {
            BytesTransformer::Latency(t) => t.transform(message),
            BytesTransformer::XorLatency(t) => t.transform(message),
            BytesTransformer::RandLatency(t) => t.transform(message),
            BytesTransformer::Partition(t) => t.transform(message),
            BytesTransformer::Drop(t) => t.transform(message),
            BytesTransformer::Periodic(t) => t.transform(message),
            BytesTransformer::Replay(t) => t.transform(message),
            BytesTransformer::BytesSplitter(t) => t.transform(message),
            BytesTransformer::Bw(t) => t.transform(message),
        }
    }

    fn min_external_delay(&self) -> Option<Duration> {
        match self {
            BytesTransformer::Latency(t) => {
                <LatencyTransformer as Transformer<Vec<u8>>>::min_external_delay(t)
            }
            BytesTransformer::XorLatency(t) => {
                <XorLatencyTransformer as Transformer<Vec<u8>>>::min_external_delay(t)
            }
            BytesTransformer::RandLatency(t) => {
                <RandLatencyTransformer as Transformer<Vec<u8>>>::min_external_delay(t)
            }
            BytesTransformer::Partition(t) => {
                <PartitionTransformer as Transformer<Vec<u8>>>::min_external_delay(t)
            }
            BytesTransformer::Drop(t) => {
                <DropTransformer as Transformer<Vec<u8>>>::min_external_delay(t)
            }
            BytesTransformer::Periodic(t) => {
                <PeriodicTransformer as Transformer<Vec<u8>>>::min_external_delay(t)
            }
            BytesTransformer::Replay(t) => t.min_external_delay(),
            BytesTransformer::BytesSplitter(t) => t.min_external_delay(),
            BytesTransformer::Bw(t) => t.min_external_delay(),
        }
    }
}

pub type BytesTransformerPipeline = Vec<BytesTransformer>;
#[cfg(feature = "monad_test")]
pub mod monad_test {
    use std::{collections::BTreeMap, time::Duration};

    use itertools::Itertools;
    use monad_consensus::messages::consensus_message::ConsensusMessage;
    use monad_consensus_types::{
        message_signature::MessageSignature, signature_collection::SignatureCollection,
    };
    use monad_executor_glue::PeerId;
    use monad_state::MonadMessage;
    use monad_types::Round;

    use super::{
        LatencyTransformer, LinkMessage, RandLatencyTransformer, Transformer, TransformerStream,
        XorLatencyTransformer, ID,
    };
    use crate::transformer::UNIQUE_ID;

    #[derive(Debug, Clone)]
    pub struct TwinsTransformer {
        // PeerId -> All Duplicate
        dups: BTreeMap<PeerId, Vec<usize>>,
        // Round -> Deliver target
        partition: BTreeMap<Round, Vec<ID>>,
        // when rounds cannot be found within partition
        default_part: Vec<ID>,
        // block sync and associated message is dropped by default
        ban_block_sync: bool,
    }

    impl TwinsTransformer {
        pub fn new(
            dups: BTreeMap<PeerId, Vec<usize>>,
            partition: BTreeMap<Round, Vec<ID>>,
            default_part: Vec<ID>,
            ban_block_sync: bool,
        ) -> Self {
            Self {
                dups,
                partition,
                default_part,
                ban_block_sync,
            }
        }
    }

    enum TwinsCapture {
        Spread(PeerId),         // spread to all target given PeerId
        Process(PeerId, Round), // spread to all target given PeerId and Round
        Drop,                   // Drop the message
    }
    impl<ST, SCT> Transformer<MonadMessage<ST, SCT>> for TwinsTransformer
    where
        ST: MessageSignature,
        SCT: SignatureCollection,
    {
        fn transform(
            &mut self,
            message: LinkMessage<MonadMessage<ST, SCT>>,
        ) -> TransformerStream<MonadMessage<ST, SCT>> {
            let LinkMessage {
                to: ID(dup_identifier, pid),
                from,
                message,
                from_tick,
            } = message;
            // only process messages that came in as default_id and spread to non-unique-ids
            assert_eq!(dup_identifier, UNIQUE_ID);

            let spy: &ConsensusMessage<SCT> = message.spy_internal();
            let capture = match spy {
                ConsensusMessage::Proposal(p) => TwinsCapture::Process(pid, p.block.round),
                ConsensusMessage::Vote(v) => TwinsCapture::Process(pid, v.vote.vote_info.round),
                // timeout naturally spread because liveness
                ConsensusMessage::Timeout(_) => TwinsCapture::Spread(pid),
                ConsensusMessage::RequestBlockSync(_) | ConsensusMessage::BlockSync(_) => {
                    if self.ban_block_sync {
                        TwinsCapture::Drop
                    } else {
                        TwinsCapture::Spread(pid)
                    }
                }
            };

            match capture {
                TwinsCapture::Drop => TransformerStream::Complete(vec![]),
                TwinsCapture::Spread(pid) => {
                    let duplicate = self.dups.get(&pid).expect(
                        "PeerId to duplicate mapping provided to TwinTransformer is incomplete",
                    );
                    TransformerStream::Continue(
                        duplicate
                            .iter()
                            .map(|id| {
                                (
                                    Duration::ZERO,
                                    LinkMessage {
                                        from,
                                        to: ID::new(pid).as_non_unique(*id),
                                        message: message.clone(),
                                        from_tick,
                                    },
                                )
                            })
                            .collect_vec(),
                    )
                }
                TwinsCapture::Process(pid, round) => {
                    // round unspecified is naturally treated as drop
                    let round_dups = self.partition.get(&round).unwrap_or(&self.default_part);

                    TransformerStream::Continue(
                        round_dups
                            .iter()
                            .filter_map(|id| {
                                if *id.get_peer_id() == pid {
                                    let msg = LinkMessage {
                                        from,
                                        to: *id,
                                        message: message.clone(),
                                        from_tick,
                                    };
                                    Some((Duration::ZERO, msg))
                                } else {
                                    None
                                }
                            })
                            .collect::<Vec<_>>(),
                    )
                }
            }
        }
    }

    #[derive(Debug, Clone)]
    pub enum MonadMessageTransformer {
        Latency(LatencyTransformer),
        XorLatency(XorLatencyTransformer),
        RandLatency(RandLatencyTransformer),
        Twins(TwinsTransformer),
    }

    impl<ST, SCT> Transformer<MonadMessage<ST, SCT>> for MonadMessageTransformer
    where
        ST: MessageSignature,
        SCT: SignatureCollection,
    {
        fn transform(
            &mut self,
            message: LinkMessage<MonadMessage<ST, SCT>>,
        ) -> TransformerStream<MonadMessage<ST, SCT>> {
            match self {
                MonadMessageTransformer::Latency(t) => t.transform(message),
                MonadMessageTransformer::XorLatency(t) => t.transform(message),
                MonadMessageTransformer::RandLatency(t) => t.transform(message),
                MonadMessageTransformer::Twins(t) => t.transform(message),
            }
        }

        fn min_external_delay(&self) -> Option<Duration> {
            match self {
                MonadMessageTransformer::Latency(t) => {
                    <LatencyTransformer as Transformer<MonadMessage<ST, SCT>>>::min_external_delay(t)
                }
                MonadMessageTransformer::XorLatency(t) => {
                    <XorLatencyTransformer as Transformer<MonadMessage<ST, SCT>>>::min_external_delay(t)
                }
                MonadMessageTransformer::RandLatency(t) => <RandLatencyTransformer as Transformer<
                    MonadMessage<ST, SCT>,
                >>::min_external_delay(t),
                MonadMessageTransformer::Twins(t) => {
                    <TwinsTransformer as Transformer<MonadMessage<ST, SCT>>>::min_external_delay(t)
                }
            }
        }
    }

    pub type MonadMessageTransformerPipeline = Vec<MonadMessageTransformer>;
}

/**
 * pipeline consist of transformers that goes through all the output
 * you can also use multiple pipelines to filter target for unique needs
 * */
pub trait Pipeline<M> {
    #[must_use]
    fn process(&mut self, message: LinkMessage<M>) -> Vec<(Duration, LinkMessage<M>)>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;

    /// pipeline must always emit delays >= min_delay for EXTERNAl messages
    /// min_external_delay MUST be > 0
    fn min_external_delay(&self) -> Duration;
}

// unlike regular transformer, pipeline's job is simply organizing various form of transformer and feed them through
impl<T, M> Pipeline<M> for Vec<T>
where
    T: Transformer<M>,
{
    fn process(&mut self, message: LinkMessage<M>) -> Vec<(Duration, LinkMessage<M>)> {
        let mut complete_message = vec![];
        let mut remain_message = vec![(Duration::ZERO, message)];
        for layer in self {
            let mut new_round_message = vec![];
            for (base_duration, message) in remain_message {
                match (*layer).transform(message) {
                    TransformerStream::Continue(c) => {
                        new_round_message.extend(
                            c.into_iter().map(move |(duration, message)| {
                                (base_duration + duration, message)
                            }),
                        );
                    }
                    TransformerStream::Complete(c) => {
                        complete_message.extend(
                            c.into_iter().map(move |(duration, message)| {
                                (base_duration + duration, message)
                            }),
                        );
                    }
                }
            }
            remain_message = new_round_message;
        }
        complete_message.append(&mut remain_message);
        complete_message
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn min_external_delay(&self) -> Duration {
        let delay = self.iter().map_while(T::min_external_delay).sum();
        assert!(delay > Duration::ZERO, "min_external_delay must be > 0");
        delay
    }
}

#[cfg(test)]
mod test {
    use std::{cmp::max, collections::HashSet, time::Duration};

    use monad_executor_glue::PeerId;
    use monad_testutil::signing::create_keys;
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha20Rng;

    use super::{GenericTransformer, LatencyTransformer, Pipeline, Transformer, ID};
    use crate::{
        transformer::{
            BytesSplitterTransformer, DropTransformer, LinkMessage, PartitionTransformer,
            PeriodicTransformer, RandLatencyTransformer, ReplayTransformer, TransformerStream,
            XorLatencyTransformer,
        },
        utils::test_tool::*,
    };

    #[test]
    fn test_latency_transformer() {
        let mut t = LatencyTransformer(Duration::from_secs(1));
        let m = get_mock_message();
        let TransformerStream::Continue(c) = t.transform(m.clone()) else {
            panic!("latency_transformer returned wrong type")
        };

        assert_eq!(c.len(), 1);
        assert!(c[0].0 == Duration::from_secs(1));
        assert!(c[0].1 == m);
    }

    #[test]
    fn test_xorlatency_transformer() {
        let mut t = XorLatencyTransformer(Duration::from_secs(1));
        let m = get_mock_message();
        let TransformerStream::Continue(c) = t.transform(m.clone()) else {
            panic!("xorlatency_transformer returned wrong type")
        };

        assert_eq!(c.len(), 1);
        // instead of verifying the random algorithm's correctness,
        // verifying the message is not touched
        // and feed through the right channel is more useful
        assert!(c[0].1 == m);
    }

    #[test]
    fn test_randlatency_transformer() {
        let mut t = RandLatencyTransformer::new(1, 30);
        let m = get_mock_message();
        let TransformerStream::Continue(c) = t.transform(m.clone()) else {
            panic!("randlatency_transformer returned wrong type")
        };

        assert_eq!(c.len(), 1);
        assert!(c[0].0 >= Duration::from_millis(1));
        assert!(c[0].0 <= Duration::from_millis(30));
        assert!(c[0].1 == m);
    }

    #[test]
    fn test_partition_transformer() {
        let keys = create_keys(2);
        let mut peers = HashSet::new();
        peers.insert(ID::new(PeerId(keys[0].pubkey())));
        let mut t = PartitionTransformer(peers.clone());
        let m = get_mock_message();
        let TransformerStream::Continue(c) = t.transform(m.clone()) else {
            panic!("partition_transformer returned wrong type")
        };

        assert_eq!(c.len(), 1);
        assert!(c[0].0 == Duration::ZERO);
        assert!(c[0].1 == m);

        let peers = HashSet::new();
        let mut t = PartitionTransformer(peers);
        let TransformerStream::Complete(c) = t.transform(m.clone()) else {
            panic!("partition_transformer returned wrong type")
        };

        assert_eq!(c.len(), 1);
        assert!(c[0].0 == Duration::ZERO);
        assert!(c[0].1 == m);
    }

    #[test]
    fn test_drop_transformer() {
        let mut t = DropTransformer();
        let m = get_mock_message();
        let TransformerStream::Complete(c) = t.transform(m) else {
            panic!("drop_transformer returned wrong type")
        };
        assert_eq!(c.len(), 0);
    }

    #[test]
    fn test_periodic_transformer() {
        let mut t = PeriodicTransformer::new(Duration::from_millis(2), Duration::from_millis(7));
        for idx in 0..2 {
            let mut m = get_mock_message();
            m.from_tick = Duration::from_millis(idx);
            let TransformerStream::Complete(c) = t.transform(m.clone()) else {
                panic!("periodic_transformer returned wrong type")
            };
            assert_eq!(c.len(), 1);
            assert!(c[0].1 == m);
        }

        for idx in 2..7 {
            let mut m = get_mock_message();
            m.from_tick = Duration::from_millis(idx);
            let TransformerStream::Continue(c) = t.transform(m.clone()) else {
                panic!("periodic_transformer returned wrong type")
            };

            assert_eq!(c.len(), 1);
            assert!(c[0].1 == m);
        }
        for idx in 7..1000 {
            let mut m = get_mock_message();
            m.from_tick = Duration::from_millis(idx);
            let TransformerStream::Complete(c) = t.transform(m.clone()) else {
                panic!("periodic_transformer returned wrong type")
            };

            assert_eq!(c.len(), 1);
            assert!(c[0].1 == m);
        }
    }

    #[test]
    fn test_replay_transformer() {
        // we are mostly interested in the burst behaviur of replay
        let mut t = ReplayTransformer::new(
            Duration::from_millis(6),
            crate::transformer::TransformerReplayOrder::Forward,
        );
        for idx in 0..6 {
            let mut mock_message = get_mock_message();
            mock_message.from_tick = Duration::from_millis(idx);
            let TransformerStream::Continue(c) = t.transform(mock_message) else {
                panic!("replay_transformer returned wrong type")
            };
            assert_eq!(c.len(), 0);
        }

        let mut mock_message = get_mock_message();
        mock_message.from_tick = Duration::from_millis(6);
        let TransformerStream::Continue(c) = t.transform(mock_message) else {
            panic!("replay_transformer returned wrong type")
        };

        assert_eq!(c.len(), 7);
        for (idx, (_, m)) in c.iter().enumerate().take(7) {
            assert_eq!(m.from_tick, Duration::from_millis(idx as u64))
        }

        for idx in 7..1000 {
            let mut mock_message = get_mock_message();
            mock_message.from_tick = Duration::from_millis(idx);
            let TransformerStream::Continue(c) = t.transform(mock_message) else {
                panic!("replay_transformer returned wrong type")
            };
            assert_eq!(c.len(), 1);
        }
    }

    #[test]
    fn test_bytes_splitter_transformer() {
        let keys = create_keys(2);

        let mut t = BytesSplitterTransformer::new();

        let mut sent_stream = Vec::new();
        let mut received_stream = Vec::new();
        let mut max_received_message_len = 0;

        let mut rng = ChaCha20Rng::from_seed([0_u8; 32]);

        const MESSAGE_LEN: usize = 32;
        const NUM_MESSAGES_SENT: u64 = 100;

        for idx in 0..NUM_MESSAGES_SENT {
            let bytes: Vec<u8> = (0..MESSAGE_LEN).map(|_| rng.gen()).collect();
            sent_stream.extend(bytes.iter().copied());
            let TransformerStream::Continue(mut c) = t.transform(LinkMessage {
                from: ID::new(PeerId(keys[0].pubkey())),
                to: ID::new(PeerId(keys[1].pubkey())),
                message: bytes,
                from_tick: Duration::from_millis(idx),
            }) else {
                panic!("bytes_splitter_transformer returned wrong type")
            };
            for (_, message) in &mut c {
                max_received_message_len = max(max_received_message_len, message.message.len());
                received_stream.append(&mut message.message);
            }
        }

        assert!(max_received_message_len > MESSAGE_LEN);
        assert!(!received_stream.is_empty());
        assert!(sent_stream
            .into_iter()
            .zip(received_stream.into_iter())
            .all(|(sent_byte, received_byte)| sent_byte == received_byte));
    }

    #[test]
    fn test_twins_transformer() {
        use std::collections::BTreeMap;

        use itertools::Itertools;
        use monad_types::Round;

        use crate::transformer::monad_test::TwinsTransformer;

        let keys = create_keys(2);

        let pid = PeerId(keys[0].pubkey());
        let dups = (1..4)
            .map(|i| ID::new(pid).as_non_unique(i))
            .collect::<Vec<_>>();
        let default_id: ID = ID::new(PeerId(keys[0].pubkey()));
        let mut pid_to_dups = BTreeMap::new();
        pid_to_dups.insert(pid, (1..4).collect_vec());
        let mut filter = BTreeMap::new();
        filter.insert(Round(1), dups.iter().take(2).copied().collect());
        filter.insert(Round(2), dups.iter().skip(1).take(2).copied().collect());

        let mut t = TwinsTransformer::new(pid_to_dups.clone(), filter.clone(), vec![], true);

        for i in 3..30 {
            // messages that is not part of the specified round result in default
            for msg in vec![
                fake_proposal_message(&keys[0], Round(i)),
                fake_vote_message(&keys[0], Round(i)),
            ] {
                let TransformerStream::Continue(c) = t.transform(LinkMessage {
                    from: default_id,
                    to: default_id,
                    message: msg,
                    from_tick: Duration::ZERO,
                }) else {
                    panic!("twins_transformer returned wrong type")
                };
                assert_eq!(c.len(), 0);
            }
        }

        // timeout message gets spread regardless of rounds
        let TransformerStream::Continue(c) = t.transform(LinkMessage {
            from: default_id,
            to: default_id,
            message: fake_timeout_message(&keys[0]),
            from_tick: Duration::ZERO,
        }) else {
            panic!("twins_transformer returned wrong type")
        };
        assert_eq!(c.len(), 3);
        for (
            i,
            (
                t,
                LinkMessage {
                    from,
                    to,
                    message,
                    from_tick,
                },
            ),
        ) in c.into_iter().enumerate()
        {
            assert_eq!(t, Duration::ZERO);
            assert_eq!(from, default_id);
            assert_eq!(to, dups[i]);
            assert_eq!(message, fake_timeout_message(&keys[0]));
            assert_eq!(from_tick, Duration::ZERO)
        }

        // on round 1, message sent to correct pid split into 2 new messages
        for msg in vec![
            fake_proposal_message(&keys[0], Round(1)),
            fake_vote_message(&keys[0], Round(1)),
        ] {
            let TransformerStream::Continue(c) = t.transform(LinkMessage {
                from: default_id,
                to: default_id,
                message: msg.clone(),
                from_tick: Duration::ZERO,
            }) else {
                panic!("twins_transformer returned wrong type")
            };
            assert_eq!(c.len(), 2);

            for (
                i,
                (
                    t,
                    LinkMessage {
                        from,
                        to,
                        message,
                        from_tick,
                    },
                ),
            ) in c.into_iter().enumerate()
            {
                assert_eq!(t, Duration::ZERO);
                assert_eq!(from, default_id);
                assert_eq!(to, dups[i]);
                assert_eq!(message, msg);
                assert_eq!(from_tick, Duration::ZERO)
            }
        }

        // on round 2, message sent to correct pid split into 2 new messages (later 2 dups)
        for msg in vec![
            fake_proposal_message(&keys[0], Round(2)),
            fake_vote_message(&keys[0], Round(2)),
        ] {
            let TransformerStream::Continue(c) = t.transform(LinkMessage {
                from: default_id,
                to: default_id,
                message: msg.clone(),
                from_tick: Duration::ZERO,
            }) else {
                panic!("twins_transformer returned wrong type")
            };
            assert_eq!(c.len(), 2);

            for (
                i,
                (
                    t,
                    LinkMessage {
                        from,
                        to,
                        message,
                        from_tick,
                    },
                ),
            ) in c.into_iter().enumerate()
            {
                assert_eq!(t, Duration::ZERO);
                assert_eq!(from, default_id);
                assert_eq!(to, dups[i + 1]);
                assert_eq!(message, msg);
                assert_eq!(from_tick, Duration::ZERO)
            }
        }

        // Sending Message to any round Key that is not logged will have no impact
        let wrong_id: ID = ID::new(PeerId(keys[1].pubkey()));

        for r in 0..30 {
            for msg in vec![
                fake_proposal_message(&keys[1], Round(r)),
                fake_vote_message(&keys[1], Round(r)),
            ] {
                let stream = t.transform(LinkMessage {
                    from: wrong_id,
                    to: wrong_id,
                    message: msg.clone(),
                    from_tick: Duration::ZERO,
                });
                // no matter stage is drop triggered, it should be empty always
                match stream {
                    TransformerStream::Complete(c) => assert_eq!(c.len(), 0),
                    TransformerStream::Continue(c) => assert_eq!(c.len(), 0),
                };
            }
        }

        // throwing it block sync message should get rejected

        for msg in vec![
            fake_request_block_sync(&keys[1]),
            fake_block_sync(&keys[1]),
            fake_request_block_sync(&keys[0]),
            fake_block_sync(&keys[0]),
        ] {
            let TransformerStream::Complete(c) = t.transform(LinkMessage {
                from: default_id,
                to: default_id,
                message: msg,
                from_tick: Duration::ZERO,
            }) else {
                panic!("twins_transformer returned wrong type")
            };
            assert_eq!(c.len(), 0);
        }

        // however if we enable block_sync then it should be broadcasted
        t = TwinsTransformer::new(pid_to_dups, filter, vec![], false);
        for msg in vec![
            fake_request_block_sync(&keys[1]),
            fake_block_sync(&keys[1]),
            fake_request_block_sync(&keys[0]),
            fake_block_sync(&keys[0]),
        ] {
            let TransformerStream::Continue(c) = t.transform(LinkMessage {
                from: default_id,
                to: default_id,
                message: msg,
                from_tick: Duration::ZERO,
            }) else {
                panic!("twins_transformer returned wrong type")
            };
            assert_eq!(c.len(), 3);
        }
    }

    #[test]
    fn test_pipeline_basic_flow() {
        let mut pipe = vec![LatencyTransformer(Duration::from_millis(30))];

        let mock_message = get_mock_message();
        // try to feed some message through, only some basic latency should be added to everything
        let result = pipe.process(mock_message.clone());
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, Duration::from_millis(30));
        assert!(result[0].1 == mock_message);
    }

    #[test]
    fn test_pipeline_complex_flow() {
        let keys = create_keys(5);
        let mut peers = HashSet::new();
        peers.insert(ID::new(PeerId(keys[0].pubkey())));

        let mut pipe = vec![
            GenericTransformer::Latency(LatencyTransformer(Duration::from_millis(30))),
            GenericTransformer::Partition(PartitionTransformer(peers)),
            GenericTransformer::Periodic(PeriodicTransformer::new(
                Duration::from_millis(2),
                Duration::from_millis(7),
            )),
            GenericTransformer::Latency(LatencyTransformer(Duration::from_millis(30))),
        ];
        for idx in 0..1000 {
            let mut mock_message = get_mock_message();
            mock_message.from = ID::new(PeerId(keys[3].pubkey()));
            mock_message.from_tick = Duration::from_millis(idx);
            let result = pipe.process(mock_message.clone());
            assert_eq!(result.len(), 1);
            // since its not part of the id, it doesn't get selected and filter
            assert_eq!(result[0].0, Duration::from_millis(30));
            assert!(result[0].1 == mock_message);
        }

        // first 2 message should not trigger extra mili
        for idx in 0..2 {
            let mut mock_message = get_mock_message();
            mock_message.from_tick = Duration::from_millis(idx);
            let result = pipe.process(mock_message.clone());
            assert_eq!(result.len(), 1);
            // since its part of the id, it get selected and filter, thus 30 extra mili
            assert_eq!(result[0].0, Duration::from_millis(30));
            assert!(result[0].1 == mock_message);
        }
        // follow by 5 message that get extra delay
        for idx in 2..7 {
            let mut mock_message = get_mock_message();
            mock_message.from_tick = Duration::from_millis(idx);
            let result = pipe.process(mock_message.clone());
            assert_eq!(result.len(), 1);
            // since its part of the id, it get selected and filter, thus 30 extra mili
            assert_eq!(result[0].0, Duration::from_millis(60));
            assert!(result[0].1 == mock_message);
        }
        for idx in 7..1000 {
            let mut mock_message = get_mock_message();
            mock_message.from_tick = Duration::from_millis(idx);
            let result = pipe.process(mock_message.clone());
            assert_eq!(result.len(), 1);
            // since its part of the id, it get selected and filter, thus 30 extra mili
            assert_eq!(result[0].0, Duration::from_millis(30));
            assert!(result[0].1 == mock_message);
        }
    }
}
