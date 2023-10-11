use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    fmt::Debug,
    mem,
    time::Duration,
};

use rand::{prelude::SliceRandom, Rng};
use rand_chacha::{rand_core::SeedableRng, ChaCha20Rng, ChaChaRng};

use crate::mock::RouterSchedulerID;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkMessage<ID, M> {
    pub from: ID,
    pub to: ID,
    pub message: M,

    /// absolute time
    pub from_tick: Duration,
}

impl<ID, M: Eq> Ord for LinkMessage<ID, M>
where
    ID: RouterSchedulerID,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.from_tick, self.from, self.to).cmp(&(other.from_tick, other.from, other.to))
    }
}
impl<ID, M> PartialOrd for LinkMessage<ID, M>
where
    ID: RouterSchedulerID,
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
type StreamMessage<ID, M> = (Duration, LinkMessage<ID, M>);
pub enum TransformerStream<ID, M> {
    Continue(Vec<StreamMessage<ID, M>>),
    Complete(Vec<StreamMessage<ID, M>>),
}

pub trait Transformer<ID, M> {
    #[must_use]
    /// note that the output Duration should be a delay, not an absolute time
    // TODO smallvec? resulting Vec will almost always be len 1
    fn transform(&mut self, message: LinkMessage<ID, M>) -> TransformerStream<ID, M>;

    fn min_external_delay(&self) -> Option<Duration> {
        None
    }

    fn boxed(self) -> Box<dyn Transformer<ID, M>>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

/// adds constant latency
#[derive(Clone, Debug)]
pub struct LatencyTransformer(pub Duration);
impl<ID, M> Transformer<ID, M> for LatencyTransformer {
    fn transform(&mut self, message: LinkMessage<ID, M>) -> TransformerStream<ID, M> {
        TransformerStream::Continue(vec![(self.0, message)])
    }

    fn min_external_delay(&self) -> Option<Duration> {
        Some(self.0)
    }
}

/// adds constant latency (parametrizable cap) to each link determined by xor(peer_id_1, peer_id_2)
#[derive(Clone, Debug)]
pub struct XorLatencyTransformer(pub Duration);
impl<ID, M> Transformer<ID, M> for XorLatencyTransformer
where
    ID: RouterSchedulerID,
{
    fn transform(&mut self, message: LinkMessage<ID, M>) -> TransformerStream<ID, M> {
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

impl<ID, M> Transformer<ID, M> for RandLatencyTransformer {
    fn transform(&mut self, message: LinkMessage<ID, M>) -> TransformerStream<ID, M> {
        TransformerStream::Continue(vec![(self.next_latency(), message)])
    }
}
#[derive(Clone)]
pub struct PartitionTransformer<ID>(pub HashSet<ID>);

impl<ID, M> Transformer<ID, M> for PartitionTransformer<ID>
where
    ID: RouterSchedulerID,
{
    fn transform(&mut self, message: LinkMessage<ID, M>) -> TransformerStream<ID, M> {
        if self.0.contains(&message.from) || self.0.contains(&message.to) {
            TransformerStream::Continue(vec![(Duration::ZERO, message)])
        } else {
            TransformerStream::Complete(vec![(Duration::ZERO, message)])
        }
    }
}

impl<ID> Debug for PartitionTransformer<ID>
where
    ID: RouterSchedulerID,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionTransformer")
            .field("peers", &self.0)
            .finish()
    }
}

#[derive(Clone, Debug)]

pub struct DropTransformer();

impl<ID, M> Transformer<ID, M> for DropTransformer {
    fn transform(&mut self, _: LinkMessage<ID, M>) -> TransformerStream<ID, M> {
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

impl<ID, M> Transformer<ID, M> for PeriodicTransformer {
    fn transform(&mut self, message: LinkMessage<ID, M>) -> TransformerStream<ID, M> {
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
pub struct ReplayTransformer<ID, M> {
    pub filtered_msgs: Vec<LinkMessage<ID, M>>,
    pub end: Duration,
    pub order: TransformerReplayOrder,
}

impl<ID, M> ReplayTransformer<ID, M> {
    /// [Duration::ZERO, end)
    pub fn new(end: Duration, order: TransformerReplayOrder) -> Self {
        ReplayTransformer {
            filtered_msgs: Vec::new(),
            end,
            order,
        }
    }
}

impl<ID, M> Debug for ReplayTransformer<ID, M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplayTransformer")
            .field("end", &self.end)
            .field("order", &self.order)
            .finish()
    }
}

impl<ID, M> Transformer<ID, M> for ReplayTransformer<ID, M> {
    fn transform(&mut self, message: LinkMessage<ID, M>) -> TransformerStream<ID, M> {
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
pub enum GenericTransformer<ID, M>
where
    ID: RouterSchedulerID,
{
    Latency(LatencyTransformer),
    XorLatency(XorLatencyTransformer),
    RandLatency(RandLatencyTransformer),
    Partition(PartitionTransformer<ID>),
    Drop(DropTransformer),
    Periodic(PeriodicTransformer),
    Replay(ReplayTransformer<ID, M>),
}

impl<ID, M> Transformer<ID, M> for GenericTransformer<ID, M>
where
    ID: RouterSchedulerID,
{
    fn transform(&mut self, message: LinkMessage<ID, M>) -> TransformerStream<ID, M> {
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
                <LatencyTransformer as Transformer<ID, M>>::min_external_delay(t)
            }
            GenericTransformer::XorLatency(t) => {
                <XorLatencyTransformer as Transformer<ID, M>>::min_external_delay(t)
            }
            GenericTransformer::RandLatency(t) => {
                <RandLatencyTransformer as Transformer<ID, M>>::min_external_delay(t)
            }
            GenericTransformer::Partition(t) => {
                <PartitionTransformer<ID> as Transformer<ID, M>>::min_external_delay(t)
            }
            GenericTransformer::Drop(t) => {
                <DropTransformer as Transformer<ID, M>>::min_external_delay(t)
            }
            GenericTransformer::Periodic(t) => {
                <PeriodicTransformer as Transformer<ID, M>>::min_external_delay(t)
            }
            GenericTransformer::Replay(t) => t.min_external_delay(),
        }
    }
}

pub type GenericTransformerPipeline<ID, M> = Vec<GenericTransformer<ID, M>>;

#[derive(Debug, Clone)]
pub struct BytesSplitterTransformer<ID> {
    rng: ChaCha20Rng,
    buffers: BTreeMap<ID, VecDeque<LinkMessage<ID, Vec<u8>>>>,
}

impl<ID> Default for BytesSplitterTransformer<ID> {
    fn default() -> Self {
        Self {
            rng: ChaCha20Rng::from_seed([0_u8; 32]),
            buffers: Default::default(),
        }
    }
}

impl<ID> BytesSplitterTransformer<ID> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<ID> Transformer<ID, Vec<u8>> for BytesSplitterTransformer<ID>
where
    ID: RouterSchedulerID,
{
    fn transform(&mut self, message: LinkMessage<ID, Vec<u8>>) -> TransformerStream<ID, Vec<u8>> {
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

#[derive(Debug, Clone)]
pub enum BytesTransformer<ID>
where
    ID: RouterSchedulerID,
{
    Latency(LatencyTransformer),
    XorLatency(XorLatencyTransformer),
    RandLatency(RandLatencyTransformer),
    Partition(PartitionTransformer<ID>),
    Drop(DropTransformer),
    Periodic(PeriodicTransformer),
    Replay(ReplayTransformer<ID, Vec<u8>>),

    BytesSplitter(BytesSplitterTransformer<ID>),
}

impl<ID> Transformer<ID, Vec<u8>> for BytesTransformer<ID>
where
    ID: RouterSchedulerID,
{
    fn transform(&mut self, message: LinkMessage<ID, Vec<u8>>) -> TransformerStream<ID, Vec<u8>> {
        match self {
            BytesTransformer::Latency(t) => t.transform(message),
            BytesTransformer::XorLatency(t) => t.transform(message),
            BytesTransformer::RandLatency(t) => t.transform(message),
            BytesTransformer::Partition(t) => t.transform(message),
            BytesTransformer::Drop(t) => t.transform(message),
            BytesTransformer::Periodic(t) => t.transform(message),
            BytesTransformer::Replay(t) => t.transform(message),
            BytesTransformer::BytesSplitter(t) => t.transform(message),
        }
    }

    fn min_external_delay(&self) -> Option<Duration> {
        match self {
            BytesTransformer::Latency(t) => {
                <LatencyTransformer as Transformer<ID, Vec<u8>>>::min_external_delay(t)
            }
            BytesTransformer::XorLatency(t) => {
                <XorLatencyTransformer as Transformer<ID, Vec<u8>>>::min_external_delay(t)
            }
            BytesTransformer::RandLatency(t) => {
                <RandLatencyTransformer as Transformer<ID, Vec<u8>>>::min_external_delay(t)
            }
            BytesTransformer::Partition(t) => {
                <PartitionTransformer<ID> as Transformer<ID, Vec<u8>>>::min_external_delay(t)
            }
            BytesTransformer::Drop(t) => {
                <DropTransformer as Transformer<ID, Vec<u8>>>::min_external_delay(t)
            }
            BytesTransformer::Periodic(t) => {
                <PeriodicTransformer as Transformer<ID, Vec<u8>>>::min_external_delay(t)
            }
            BytesTransformer::Replay(t) => t.min_external_delay(),
            BytesTransformer::BytesSplitter(t) => t.min_external_delay(),
        }
    }
}

pub type BytesTransformerPipeline<ID> = Vec<BytesTransformer<ID>>;

/**
 * pipeline consist of transformers that goes through all the output
 * you can also use multiple pipelines to filter target for unique needs
 * */
pub trait Pipeline<ID, M> {
    #[must_use]
    fn process(&mut self, message: LinkMessage<ID, M>) -> Vec<(Duration, LinkMessage<ID, M>)>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;

    /// pipeline must always emit delays >= min_delay for EXTERNAl messages
    /// min_external_delay MUST be > 0
    fn min_external_delay(&self) -> Duration;
}

// unlike regular transformer, pipeline's job is simply organizing various form of transformer and feed them through
impl<T, ID, M> Pipeline<ID, M> for Vec<T>
where
    T: Transformer<ID, M>,
{
    fn process(&mut self, message: LinkMessage<ID, M>) -> Vec<(Duration, LinkMessage<ID, M>)> {
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

    use super::{GenericTransformer, LatencyTransformer, Pipeline, Transformer};
    use crate::transformer::{
        BytesSplitterTransformer, DropTransformer, LinkMessage, PartitionTransformer,
        PeriodicTransformer, RandLatencyTransformer, ReplayTransformer, TransformerStream,
        XorLatencyTransformer,
    };

    /// FIXME these should take in from/to/from_tick as params, not have defaults
    fn get_mock_message() -> LinkMessage<PeerId, String> {
        let keys = create_keys(2);
        LinkMessage {
            from: PeerId(keys[0].pubkey()),
            to: PeerId(keys[1].pubkey()),
            message: "Dummy Message".to_string(),
            from_tick: Duration::from_millis(10),
        }
    }

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
        peers.insert(PeerId(keys[0].pubkey()));
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
                from: PeerId(keys[0].pubkey()),
                to: PeerId(keys[1].pubkey()),
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
        peers.insert(PeerId(keys[0].pubkey()));

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
            mock_message.from = PeerId(keys[3].pubkey());
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
        // and then you nver get extra mili
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
