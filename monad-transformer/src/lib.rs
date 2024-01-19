use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    fmt::Debug,
    marker::PhantomData,
    mem,
    time::Duration,
};

use bytes::{Buf, Bytes};
use bytes_utils::SegmentedBuf;
use monad_crypto::certificate_signature::PubKey;
use monad_tracing_counter::inc_count;
use monad_types::NodeId;
use rand::{prelude::SliceRandom, Rng};
use rand_chacha::{rand_core::SeedableRng, ChaCha20Rng, ChaChaRng};

pub const UNIQUE_ID: usize = 0;
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ID<PT: PubKey>(usize, NodeId<PT>);

impl<PT: PubKey> ID<PT> {
    pub fn new(peer_id: NodeId<PT>) -> Self {
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

    pub fn get_peer_id(&self) -> &NodeId<PT> {
        &self.1
    }

    pub fn is_unique(&self) -> bool {
        self.0 == UNIQUE_ID
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkMessage<PT: PubKey, M> {
    pub from: ID<PT>,
    pub to: ID<PT>,
    pub message: M,

    /// absolute time
    pub from_tick: Duration,
}

impl<PT: PubKey, M: Eq> Ord for LinkMessage<PT, M> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.from_tick, self.from, self.to).cmp(&(other.from_tick, other.from, other.to))
    }
}
impl<PT: PubKey, M> PartialOrd for LinkMessage<PT, M>
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
type StreamMessage<PT, M> = (Duration, LinkMessage<PT, M>);
pub enum TransformerStream<PT: PubKey, M> {
    Continue(Vec<StreamMessage<PT, M>>),
    Complete(Vec<StreamMessage<PT, M>>),
}

pub trait Transformer<M> {
    type NodeIdPubKey: PubKey;

    #[must_use]
    /// note that the output Duration should be a delay, not an absolute time
    // TODO-3 smallvec? resulting Vec will almost always be len 1
    fn transform(
        &mut self,
        message: LinkMessage<Self::NodeIdPubKey, M>,
    ) -> TransformerStream<Self::NodeIdPubKey, M>;

    fn min_external_delay(&self) -> Option<Duration> {
        None
    }

    fn boxed(self) -> Box<dyn Transformer<M, NodeIdPubKey = Self::NodeIdPubKey>>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}
/// adds constant latency
#[derive(Clone, Debug)]
pub struct LatencyTransformer<PT: PubKey>(pub Duration, PhantomData<PT>);

impl<PT: PubKey> LatencyTransformer<PT> {
    pub fn new(latency: Duration) -> Self {
        Self(latency, PhantomData)
    }
}

impl<PT: PubKey, M> Transformer<M> for LatencyTransformer<PT> {
    type NodeIdPubKey = PT;
    fn transform(&mut self, message: LinkMessage<PT, M>) -> TransformerStream<PT, M> {
        TransformerStream::Continue(vec![(self.0, message)])
    }

    fn min_external_delay(&self) -> Option<Duration> {
        Some(self.0)
    }
}

/// adds constant latency (parametrizable cap) to each link determined by xor(peer_id_1, peer_id_2)
#[derive(Clone, Debug)]
pub struct XorLatencyTransformer<PT: PubKey>(pub Duration, PhantomData<PT>);

impl<PT: PubKey> XorLatencyTransformer<PT> {
    pub fn new(max_latency: Duration) -> Self {
        Self(max_latency, PhantomData)
    }
}

impl<PT: PubKey, M> Transformer<M> for XorLatencyTransformer<PT> {
    type NodeIdPubKey = PT;
    fn transform(&mut self, message: LinkMessage<PT, M>) -> TransformerStream<PT, M> {
        let mut ck: u8 = 0;
        for b in message.from.get_peer_id().pubkey().bytes() {
            ck ^= b;
        }
        for b in message.to.get_peer_id().pubkey().bytes() {
            ck ^= b;
        }
        TransformerStream::Continue(vec![(self.0.mul_f32(ck as f32 / u8::MAX as f32), message)])
    }
}

/// adds random latency to each message up to a cap
#[derive(Clone, Debug)]
pub struct RandLatencyTransformer<PT: PubKey> {
    gen: ChaChaRng,
    max_latency: u64,
    _phantom: PhantomData<PT>,
}

impl<PT: PubKey> RandLatencyTransformer<PT> {
    pub fn new(seed: u64, max_latency: u64) -> Self {
        RandLatencyTransformer {
            gen: ChaChaRng::seed_from_u64(seed),
            max_latency,
            _phantom: PhantomData,
        }
    }

    pub fn next_latency(&mut self) -> Duration {
        let s = self.gen.gen_range(1..self.max_latency);

        Duration::from_millis(s)
    }
}

impl<PT: PubKey, M> Transformer<M> for RandLatencyTransformer<PT> {
    type NodeIdPubKey = PT;
    fn transform(&mut self, message: LinkMessage<PT, M>) -> TransformerStream<PT, M> {
        TransformerStream::Continue(vec![(self.next_latency(), message)])
    }
}
#[derive(Clone)]
pub struct PartitionTransformer<PT: PubKey>(pub HashSet<ID<PT>>);

impl<PT: PubKey, M> Transformer<M> for PartitionTransformer<PT> {
    type NodeIdPubKey = PT;
    fn transform(&mut self, message: LinkMessage<PT, M>) -> TransformerStream<PT, M> {
        if self.0.contains(&message.from) || self.0.contains(&message.to) {
            TransformerStream::Continue(vec![(Duration::ZERO, message)])
        } else {
            TransformerStream::Complete(vec![(Duration::ZERO, message)])
        }
    }
}

impl<PT: PubKey> Debug for PartitionTransformer<PT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionTransformer")
            .field("peers", &self.0)
            .finish()
    }
}

#[derive(Clone, Debug)]

pub struct DropTransformer<PT: PubKey>(PhantomData<PT>);

impl<PT: PubKey> Default for DropTransformer<PT> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<PT: PubKey> DropTransformer<PT> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<PT: PubKey, M> Transformer<M> for DropTransformer<PT> {
    type NodeIdPubKey = PT;
    fn transform(&mut self, _: LinkMessage<PT, M>) -> TransformerStream<PT, M> {
        TransformerStream::Complete(vec![])
    }
}

#[derive(Clone, Debug)]
pub struct PeriodicTransformer<PT: PubKey> {
    pub start: Duration,
    pub end: Duration,

    _phantom: PhantomData<PT>,
}

impl<PT: PubKey> PeriodicTransformer<PT> {
    /// [start, end)
    pub fn new(start: Duration, end: Duration) -> Self {
        assert!(start <= end);
        PeriodicTransformer {
            start,
            end,
            _phantom: PhantomData,
        }
    }
}

impl<PT: PubKey, M> Transformer<M> for PeriodicTransformer<PT> {
    type NodeIdPubKey = PT;
    fn transform(&mut self, message: LinkMessage<PT, M>) -> TransformerStream<PT, M> {
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
pub struct ReplayTransformer<PT: PubKey, M> {
    pub filtered_msgs: Vec<LinkMessage<PT, M>>,
    pub end: Duration,
    pub order: TransformerReplayOrder,
}

impl<PT: PubKey, M> ReplayTransformer<PT, M> {
    /// [Duration::ZERO, end)
    pub fn new(end: Duration, order: TransformerReplayOrder) -> Self {
        ReplayTransformer {
            filtered_msgs: Vec::new(),
            end,
            order,
        }
    }
}

impl<PT: PubKey, M> Debug for ReplayTransformer<PT, M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplayTransformer")
            .field("end", &self.end)
            .field("order", &self.order)
            .finish()
    }
}

impl<PT: PubKey, M> Transformer<M> for ReplayTransformer<PT, M> {
    type NodeIdPubKey = PT;
    fn transform(&mut self, message: LinkMessage<PT, M>) -> TransformerStream<PT, M> {
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
pub enum GenericTransformer<PT: PubKey, M> {
    Latency(LatencyTransformer<PT>),
    XorLatency(XorLatencyTransformer<PT>),
    RandLatency(RandLatencyTransformer<PT>),
    Partition(PartitionTransformer<PT>),
    Drop(DropTransformer<PT>),
    Periodic(PeriodicTransformer<PT>),
    Replay(ReplayTransformer<PT, M>),
}

impl<PT: PubKey, M> Transformer<M> for GenericTransformer<PT, M> {
    type NodeIdPubKey = PT;
    fn transform(&mut self, message: LinkMessage<PT, M>) -> TransformerStream<PT, M> {
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
                <LatencyTransformer<PT> as Transformer<M>>::min_external_delay(t)
            }
            GenericTransformer::XorLatency(t) => {
                <XorLatencyTransformer<PT> as Transformer<M>>::min_external_delay(t)
            }
            GenericTransformer::RandLatency(t) => {
                <RandLatencyTransformer<PT> as Transformer<M>>::min_external_delay(t)
            }
            GenericTransformer::Partition(t) => {
                <PartitionTransformer<PT> as Transformer<M>>::min_external_delay(t)
            }
            GenericTransformer::Drop(t) => {
                <DropTransformer<PT> as Transformer<M>>::min_external_delay(t)
            }
            GenericTransformer::Periodic(t) => {
                <PeriodicTransformer<PT> as Transformer<M>>::min_external_delay(t)
            }
            GenericTransformer::Replay(t) => t.min_external_delay(),
        }
    }
}

pub type GenericTransformerPipeline<PT, M> = Vec<GenericTransformer<PT, M>>;

#[derive(Debug, Clone)]
pub struct BytesSplitterTransformer<PT: PubKey> {
    rng: ChaCha20Rng,
    buffers: BTreeMap<ID<PT>, VecDeque<LinkMessage<PT, Bytes>>>,
}

impl<PT: PubKey> Default for BytesSplitterTransformer<PT> {
    fn default() -> Self {
        Self {
            rng: ChaCha20Rng::from_seed([0_u8; 32]),
            buffers: Default::default(),
        }
    }
}

impl<PT: PubKey> BytesSplitterTransformer<PT> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<PT: PubKey> Transformer<Bytes> for BytesSplitterTransformer<PT> {
    type NodeIdPubKey = PT;
    fn transform(&mut self, message: LinkMessage<PT, Bytes>) -> TransformerStream<PT, Bytes> {
        let entry = self.buffers.entry(message.to).or_default();
        entry.push_back(message);

        let split_idx = self.rng.gen_range(0..entry.len());

        let accumulator = {
            let split_message = entry
                .front()
                .expect("must be at least 1 element (split_idx)");
            LinkMessage {
                from: split_message.from,
                to: split_message.to,
                from_tick: split_message.from_tick,
                message: SegmentedBuf::new(),
            }
        };

        let mut base_message =
            entry
                .drain(0..split_idx)
                .fold(accumulator, |mut base_message, merge_message| {
                    base_message.message.push(merge_message.message);
                    base_message
                });
        let split_message = entry
            .front_mut()
            .expect("must be at least 1 element (split_idx)");

        let message_split_idx = self.rng.gen_range(1..=split_message.message.len());
        base_message
            .message
            .push(split_message.message.copy_to_bytes(message_split_idx));

        if split_message.message.is_empty() {
            entry.pop_front();
        }

        let base_message = LinkMessage {
            from: base_message.from,
            to: base_message.to,
            from_tick: base_message.from_tick,
            message: base_message
                .message
                .copy_to_bytes(base_message.message.remaining()),
        };

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
    fn advance_to(&mut self, now: Duration, sampling_period: Duration) {
        while self
            .window
            .front()
            .map_or(false, |&(tick, _)| tick + sampling_period < now)
        {
            let (_, bitlen) = self.window.pop_front().unwrap();
            self.total_bits -= bitlen;
        }
    }

    fn try_push<PT: PubKey>(&mut self, burst_size: usize, msg: &LinkMessage<PT, Bytes>) -> bool {
        // assert msgs are increasing in from_tick
        assert!(self
            .window
            .back()
            .map_or(true, |&(tick, _)| msg.from_tick >= tick));
        let bit_len = msg.message.len() * 8;
        if self.total_bits + bit_len > burst_size {
            false
        } else {
            self.total_bits += bit_len;
            self.window.push_back((msg.from_tick, bit_len));
            true
        }
    }
}

#[derive(Clone, Debug)]
pub struct BwTransformer<PT: PubKey> {
    // number of bits allowed in the sampling period
    burst_size: usize,
    // sampling period
    sampling_period: Duration,
    // upload bandwidth window
    window: BwWindow,

    _phantom: PhantomData<PT>,
}

impl<PT: PubKey> BwTransformer<PT> {
    pub fn new(upload_mbps: usize, sampling_period: Duration) -> Self {
        Self {
            burst_size: upload_mbps * 1024 * 1024 * sampling_period.as_micros() as usize
                / Duration::from_secs(1).as_micros() as usize,
            sampling_period,
            window: BwWindow::default(),
            _phantom: PhantomData,
        }
    }
}

impl<PT: PubKey> Transformer<Bytes> for BwTransformer<PT> {
    type NodeIdPubKey = PT;
    fn transform(&mut self, message: LinkMessage<PT, Bytes>) -> TransformerStream<PT, Bytes> {
        self.window
            .advance_to(message.from_tick, self.sampling_period);

        if self.window.try_push(self.burst_size, &message) {
            TransformerStream::Continue(vec![(Duration::ZERO, message)])
        } else {
            inc_count!(bwtransformer_dropped_msg);
            TransformerStream::Complete(vec![])
        }
    }
}

#[derive(Debug, Clone)]
pub struct PacerTransformer<PT: PubKey> {
    upload_mbps: usize,
    burst_bits: usize,

    // in absolute time
    last_scheduled_ts: Duration,

    _phantom: PhantomData<PT>,
}

impl<PT: PubKey> PacerTransformer<PT> {
    pub fn new(upload_mbps: usize, burst_bits: usize) -> Self {
        assert!(burst_bits >= 8);
        Self {
            upload_mbps,
            burst_bits,

            last_scheduled_ts: Duration::ZERO,

            _phantom: PhantomData,
        }
    }
}

impl<PT: PubKey> Transformer<Bytes> for PacerTransformer<PT> {
    type NodeIdPubKey = PT;
    fn transform(&mut self, mut message: LinkMessage<PT, Bytes>) -> TransformerStream<PT, Bytes> {
        // The goal of PacerTransformer is to pace messages at a configurable rate. Given a
        // message_1 that's sent at from_tick_1, the delays to generate for each message chunk is
        // straightforward.
        //
        // What's more complicated is determining how much to delay a second message_2 that's sent
        // at from_tick_2. The first chunk of message_2 needs to be scheduled after the last chunk
        // of message_1 is scheduled. The following calculation solves for delay_2. It assumes that
        // prior transformer layers always emit constant latency.
        //
        ///// last_scheduled_ts reprents the absolute time that the last message chunk of message_1
        ///// was sent at
        // last_scheduled_ts == from_tick_1 + latency + delay_1
        //
        ///// delay_offset represents the offset needed to recover last_scheduled_ts given
        ///// from_tick_2
        // last_scheduled_ts == from_tick_2 + latency + delay_offset
        //
        //// solve for delay_offset
        // from_tick_1 + delay_1 = from_tick_2 + delay_offset
        // from_tick_1 - from_tick_2 + delay_1 = delay_offset
        //
        //
        ///// extra_delay is the amount we want to delay the first chunk of message_2 from the last
        ///// message chunk of message_1
        // new_scheduled_ts == last_scheduled_ts + extra_delay
        //
        ///// delay_2 is the final amount of delay the transformer needs to emit to achieve the
        ///// desired new_scheduled_ts with the desired extra_delay
        // new_scheduled_ts == from_tick_2 + latency + delay_2
        //
        //
        ///// solve for delay_2
        // last_scheduled_ts + extra_delay = from_tick_2 + latency + delay_2
        // delay_2 = last_scheduled_ts + extra_delay - from_tick_2 - latency
        // delay_2 = (from_tick_2 + latency + delay_offset) + extra_delay - from_tick_2 - latency
        // delay_2 = delay_offset + extra_delay
        // delay_2 = (from_tick_1 - from_tick_2 + delay_1) + extra_delay
        let schedule_delay = self
            .last_scheduled_ts
            .checked_sub(message.from_tick)
            .unwrap_or(Duration::ZERO);

        let mut stream_messages: Vec<StreamMessage<_, _>> = Vec::new();
        let mut idx = 0;
        while !message.message.is_empty() {
            let chunk = message
                .message
                .copy_to_bytes((self.burst_bits / 8).min(message.message.remaining()));

            let sub_message = LinkMessage {
                from: message.from,
                to: message.to,
                message: chunk,

                from_tick: message.from_tick,
            };
            let extra_delay =
                idx as f64 * (self.burst_bits as f64 / (self.upload_mbps * 1_000_000) as f64);
            let delay_2 = schedule_delay + Duration::from_secs_f64(extra_delay);
            stream_messages.push((delay_2, sub_message));
            idx += 1;
        }

        if let Some((delay_1, _)) = stream_messages.last() {
            self.last_scheduled_ts = message.from_tick + *delay_1;
        }

        TransformerStream::Continue(stream_messages)
    }
}

#[derive(Debug, Clone)]
pub enum BytesTransformer<PT: PubKey> {
    Latency(LatencyTransformer<PT>),
    XorLatency(XorLatencyTransformer<PT>),
    RandLatency(RandLatencyTransformer<PT>),
    Partition(PartitionTransformer<PT>),
    Drop(DropTransformer<PT>),
    Periodic(PeriodicTransformer<PT>),
    Replay(ReplayTransformer<PT, Bytes>),

    BytesSplitter(BytesSplitterTransformer<PT>),
    Bw(BwTransformer<PT>),
    Pacer(PacerTransformer<PT>),
}

impl<PT: PubKey> Transformer<Bytes> for BytesTransformer<PT> {
    type NodeIdPubKey = PT;
    fn transform(&mut self, message: LinkMessage<PT, Bytes>) -> TransformerStream<PT, Bytes> {
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
            BytesTransformer::Pacer(t) => t.transform(message),
        }
    }

    fn min_external_delay(&self) -> Option<Duration> {
        match self {
            BytesTransformer::Latency(t) => {
                <LatencyTransformer<PT> as Transformer<Bytes>>::min_external_delay(t)
            }
            BytesTransformer::XorLatency(t) => {
                <XorLatencyTransformer<PT> as Transformer<Bytes>>::min_external_delay(t)
            }
            BytesTransformer::RandLatency(t) => {
                <RandLatencyTransformer<PT> as Transformer<Bytes>>::min_external_delay(t)
            }
            BytesTransformer::Partition(t) => {
                <PartitionTransformer<PT> as Transformer<Bytes>>::min_external_delay(t)
            }
            BytesTransformer::Drop(t) => {
                <DropTransformer<PT> as Transformer<Bytes>>::min_external_delay(t)
            }
            BytesTransformer::Periodic(t) => {
                <PeriodicTransformer<PT> as Transformer<Bytes>>::min_external_delay(t)
            }
            BytesTransformer::Replay(t) => t.min_external_delay(),
            BytesTransformer::BytesSplitter(t) => t.min_external_delay(),
            BytesTransformer::Bw(t) => t.min_external_delay(),
            BytesTransformer::Pacer(t) => t.min_external_delay(),
        }
    }
}

pub type BytesTransformerPipeline<PT> = Vec<BytesTransformer<PT>>;

/**
 * pipeline consist of transformers that goes through all the output
 * you can also use multiple pipelines to filter target for unique needs
 * */
pub trait Pipeline<M> {
    type NodeIdPubKey: PubKey;
    #[must_use]
    fn process(
        &mut self,
        message: LinkMessage<Self::NodeIdPubKey, M>,
    ) -> Vec<(Duration, LinkMessage<Self::NodeIdPubKey, M>)>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;

    /// pipeline must always emit delays >= min_delay for EXTERNAl messages
    /// min_external_delay MUST be > 0
    fn min_external_delay(&self) -> Duration;
}

impl<T: Pipeline<M> + ?Sized, M> Pipeline<M> for Box<T> {
    type NodeIdPubKey = T::NodeIdPubKey;

    fn process(
        &mut self,
        message: LinkMessage<Self::NodeIdPubKey, M>,
    ) -> Vec<(Duration, LinkMessage<Self::NodeIdPubKey, M>)> {
        (**self).process(message)
    }

    fn len(&self) -> usize {
        (**self).len()
    }

    fn is_empty(&self) -> bool {
        (**self).is_empty()
    }

    fn min_external_delay(&self) -> Duration {
        (**self).min_external_delay()
    }
}

// unlike regular transformer, pipeline's job is simply organizing various form of transformer and feed them through
impl<T, M> Pipeline<M> for Vec<T>
where
    T: Transformer<M>,
{
    type NodeIdPubKey = T::NodeIdPubKey;
    fn process(
        &mut self,
        message: LinkMessage<T::NodeIdPubKey, M>,
    ) -> Vec<(Duration, LinkMessage<T::NodeIdPubKey, M>)> {
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
