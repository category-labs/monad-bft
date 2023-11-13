use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    fmt::Debug,
    mem,
    time::Duration,
};

use monad_tracing_counter::inc_count;
use monad_types::NodeId;
use rand::{prelude::SliceRandom, Rng};
use rand_chacha::{rand_core::SeedableRng, ChaCha20Rng, ChaChaRng};

pub const UNIQUE_ID: usize = 0;
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ID(usize, NodeId);

impl ID {
    pub fn new(peer_id: NodeId) -> Self {
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

    pub fn get_peer_id(&self) -> &NodeId {
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
    // TODO-3 smallvec? resulting Vec will almost always be len 1
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

    fn try_push(&mut self, burst_size: usize, msg: &LinkMessage<Vec<u8>>) -> bool {
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
pub struct BwTransformer {
    // number of bits allowed in the sampling period
    burst_size: usize,
    // sampling period
    sampling_period: Duration,
    // upload bandwidth window
    window: BwWindow,
}

impl BwTransformer {
    pub fn new(upload_mbps: usize, sampling_period: Duration) -> Self {
        Self {
            burst_size: upload_mbps * 1024 * 1024 * sampling_period.as_micros() as usize
                / Duration::from_secs(1).as_micros() as usize,
            sampling_period,
            window: BwWindow::default(),
        }
    }
}

impl Transformer<Vec<u8>> for BwTransformer {
    fn transform(&mut self, message: LinkMessage<Vec<u8>>) -> TransformerStream<Vec<u8>> {
        self.window
            .advance_to(message.from_tick, self.sampling_period);

        if self.window.try_push(self.burst_size, &message) {
            TransformerStream::Continue(vec![(Duration::ZERO, message)])
        } else {
            inc_count!(bwtransfomer_dropped_msg);
            TransformerStream::Complete(vec![])
        }
    }
}

#[derive(Debug, Clone)]
pub struct PacerTransformer {
    upload_mbps: usize,
    burst_bits: usize,

    // in absolute time
    last_scheduled_ts: Duration,
}

impl PacerTransformer {
    pub fn new(upload_mbps: usize, burst_bits: usize) -> Self {
        assert!(burst_bits >= 8);
        Self {
            upload_mbps,
            burst_bits,

            last_scheduled_ts: Duration::ZERO,
        }
    }
}

impl Transformer<Vec<u8>> for PacerTransformer {
    fn transform(&mut self, message: LinkMessage<Vec<u8>>) -> TransformerStream<Vec<u8>> {
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
        let stream_messages: Vec<StreamMessage<_>> = message
            .message
            .chunks(self.burst_bits / 8)
            .enumerate()
            .map(|(idx, chunk)| {
                let message = LinkMessage {
                    from: message.from,
                    to: message.to,
                    message: chunk.to_vec(),

                    from_tick: message.from_tick,
                };
                let extra_delay =
                    idx as f64 * (self.burst_bits as f64 / (self.upload_mbps * 1_000_000) as f64);
                let delay_2 = schedule_delay + Duration::from_secs_f64(extra_delay);
                (delay_2, message)
            })
            .collect();
        if let Some((delay_1, _)) = stream_messages.last() {
            self.last_scheduled_ts = message.from_tick + *delay_1;
        }

        TransformerStream::Continue(stream_messages)
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
    Pacer(PacerTransformer),
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
            BytesTransformer::Pacer(t) => t.transform(message),
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
            BytesTransformer::Pacer(t) => t.min_external_delay(),
        }
    }
}

pub type BytesTransformerPipeline = Vec<BytesTransformer>;

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
