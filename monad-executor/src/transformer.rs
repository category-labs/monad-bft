use std::{
    collections::HashSet,
    fmt::Debug,
    mem,
    ops::{Index, IndexMut},
    time::Duration,
};

use rand::{prelude::SliceRandom, Rng};
use rand_chacha::{rand_core::SeedableRng, ChaChaRng};

use crate::{mock_swarm::LinkMessage, PeerId};

/**
 * TransformerStream carries (Duration, LinkedMessages) that emited
 * after a transformer with Transform Trait complete their transform
 *
 * remain_message indicate messages that should
 * still get processed in the future (potentially by more transformer).
 *
 * complete_messages indicate messages that are finished transforming
 * and do not wish to be processed further.
 *
 */
pub struct TransformerStream<M> {
    remain_messages: Vec<(Duration, LinkMessage<M>)>,
    complete_messages: Vec<(Duration, LinkMessage<M>)>,
}

impl<M> TransformerStream<M> {
    fn new(
        remain_messages: Vec<(Duration, LinkMessage<M>)>,
        complete_messages: Vec<(Duration, LinkMessage<M>)>,
    ) -> Self {
        TransformerStream {
            remain_messages,
            complete_messages,
        }
    }
}

pub trait Transform<M> {
    #[must_use]
    /// note that the output Duration should be a delay, not an absolute time
    // TODO smallvec? resulting Vec will almost always be len 1
    fn transform(&mut self, message: LinkMessage<M>) -> TransformerStream<M>;

    fn boxed(self) -> Box<dyn Transform<M>>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

/// adds constant latency
#[derive(Clone, Debug)]
pub struct LatencyTransformer(pub Duration);
impl<M> Transform<M> for LatencyTransformer {
    fn transform(&mut self, message: LinkMessage<M>) -> TransformerStream<M> {
        TransformerStream::new(vec![(self.0, message)], vec![])
    }
}

/// adds constant latency (parametrizable cap) to each link determined by xor(peer_id_1, peer_id_2)
#[derive(Clone, Debug)]
pub struct XorLatencyTransformer(pub Duration);
impl<M> Transform<M> for XorLatencyTransformer {
    fn transform(&mut self, message: LinkMessage<M>) -> TransformerStream<M> {
        let mut ck: u8 = 0;
        for b in message.from.0.bytes() {
            ck ^= b;
        }
        for b in message.to.0.bytes() {
            ck ^= b;
        }
        TransformerStream::new(
            vec![(self.0.mul_f32(ck as f32 / u8::MAX as f32), message)],
            vec![],
        )
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

impl<M> Transform<M> for RandLatencyTransformer {
    fn transform(&mut self, message: LinkMessage<M>) -> TransformerStream<M> {
        TransformerStream::new(vec![(self.next_latency(), message)], vec![])
    }
}
#[derive(Clone)]
pub struct PartitionTransformer(pub HashSet<PeerId>);

impl<M> Transform<M> for PartitionTransformer {
    fn transform(&mut self, message: LinkMessage<M>) -> TransformerStream<M> {
        if self.0.contains(&message.from) || self.0.contains(&message.to) {
            TransformerStream::new(vec![(Duration::ZERO, message)], vec![])
        } else {
            TransformerStream::new(vec![], vec![(Duration::ZERO, message)])
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

impl<M> Transform<M> for DropTransformer {
    fn transform(&mut self, _: LinkMessage<M>) -> TransformerStream<M> {
        TransformerStream::new(vec![], vec![])
    }
}

#[derive(Clone, Debug)]
pub struct PeriodicTranformer {
    pub start: u32,    // when period start
    pub duration: u32, // how long does it last
    cnt: u32,          // monotonically inreasing counter
}

impl PeriodicTranformer {
    pub fn new(start: u32, duration: u32) -> Self {
        PeriodicTranformer {
            start,
            duration,
            cnt: 0,
        }
    }
}

impl<M> Transform<M> for PeriodicTranformer {
    fn transform(&mut self, message: LinkMessage<M>) -> TransformerStream<M> {
        self.cnt += 1;
        if self.cnt < self.start || self.cnt >= self.start + self.duration {
            TransformerStream::new(vec![], vec![(Duration::ZERO, message)])
        } else {
            TransformerStream::new(vec![(Duration::ZERO, message)], vec![])
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
    pub cnt: u32,
    pub cnt_limit: u32,
    pub order: TransformerReplayOrder,
}

impl<M> ReplayTransformer<M> {
    pub fn new(cnt_limit: u32, order: TransformerReplayOrder) -> Self {
        ReplayTransformer {
            filtered_msgs: Vec::new(),
            cnt: 0,
            cnt_limit,
            order,
        }
    }
}

impl<M> Debug for ReplayTransformer<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplayTransformer")
            .field("cnt", &self.cnt)
            .field("cnt_limit", &self.cnt_limit)
            .field("order", &self.order)
            .finish()
    }
}

impl<M> Transform<M> for ReplayTransformer<M> {
    fn transform(&mut self, message: LinkMessage<M>) -> TransformerStream<M> {
        if self.cnt > self.cnt_limit {
            return TransformerStream::new(vec![(Duration::ZERO, message)], vec![]);
        }

        self.cnt += 1;
        let mut output = Vec::new();
        self.filtered_msgs.push(message);

        if self.cnt > self.cnt_limit {
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

            output.extend(std::iter::repeat(Duration::ZERO).zip(msgs));
        }

        TransformerStream::new(output, vec![])
    }
}

#[derive(Debug, Clone)]
pub enum Transformer<M> {
    Latency(LatencyTransformer),
    XorLatency(XorLatencyTransformer),
    RandLatency(RandLatencyTransformer),
    Partition(PartitionTransformer),
    Drop(DropTransformer),
    Periodic(PeriodicTranformer),
    Replay(ReplayTransformer<M>),
}

impl<M> Transform<M> for Transformer<M> {
    fn transform(&mut self, message: LinkMessage<M>) -> TransformerStream<M> {
        match self {
            Transformer::Latency(t) => t.transform(message),
            Transformer::XorLatency(t) => t.transform(message),
            Transformer::RandLatency(t) => t.transform(message),
            Transformer::Partition(t) => t.transform(message),
            Transformer::Drop(t) => t.transform(message),
            Transformer::Periodic(t) => t.transform(message),
            Transformer::Replay(t) => t.transform(message),
        }
    }
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
}
#[derive(Clone, Debug)]
pub struct TransformerPipeline<M> {
    transformers: Vec<Transformer<M>>,
}

impl<M> TransformerPipeline<M> {
    pub fn new(transformers: Vec<Transformer<M>>) -> Self {
        TransformerPipeline { transformers }
    }
}

#[macro_export]
macro_rules! xfmr_pipe {
    ($($x:expr),+) => {
        TransformerPipeline::new(vec![$($x), *])
    };
}

impl<M> Index<usize> for TransformerPipeline<M> {
    type Output = Transformer<M>;
    fn index(&self, i: usize) -> &Self::Output {
        assert!(i < self.transformers.len());
        &self.transformers[i]
    }
}

impl<M> IndexMut<usize> for TransformerPipeline<M> {
    fn index_mut(&mut self, i: usize) -> &mut Self::Output {
        assert!(i < self.transformers.len());
        &mut self.transformers[i]
    }
}

// unlike regular transformer, pipeline's job is simply organizing various form of transformer and feed them through
impl<M> Pipeline<M> for TransformerPipeline<M> {
    fn process(&mut self, message: LinkMessage<M>) -> Vec<(Duration, LinkMessage<M>)> {
        let mut complete_message = vec![];
        let mut remain_message = vec![(Duration::ZERO, message)];
        for layer in &mut self.transformers {
            let mut new_round_message = vec![];
            for (base_duration, message) in remain_message {
                let TransformerStream {
                    remain_messages: r,
                    complete_messages: c,
                } = (*layer).transform(message);
                new_round_message.extend(
                    r.into_iter()
                        .map(move |(duration, message)| (base_duration + duration, message)),
                );

                complete_message.extend(
                    c.into_iter()
                        .map(move |(duration, message)| (base_duration + duration, message)),
                );
            }
            remain_message = new_round_message;
        }
        complete_message.append(&mut remain_message);
        complete_message
    }

    fn len(&self) -> usize {
        self.transformers.len()
    }

    fn is_empty(&self) -> bool {
        self.transformers.len() == 0
    }
}
#[cfg(test)]

mod test {
    use std::{collections::HashSet, time::Duration};

    use monad_testutil::signing::create_keys;

    use super::{LatencyTransformer, Pipeline, Transform, Transformer, TransformerPipeline};
    use crate::{
        mock_swarm::LinkMessage,
        transformer::{
            DropTransformer, PartitionTransformer, PeriodicTranformer, RandLatencyTransformer,
            ReplayTransformer, TransformerStream, XorLatencyTransformer,
        },
        PeerId,
    };

    fn get_mock_message() -> LinkMessage<String> {
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
        let TransformerStream {
            remain_messages: r,
            complete_messages: c,
        } = t.transform(m.clone());

        assert_eq!(r.len(), 1);
        assert_eq!(c.len(), 0);
        assert!(r[0].0 == Duration::from_secs(1));
        assert!(r[0].1 == m);
    }

    #[test]
    fn test_xorlatency_transformer() {
        let mut t = XorLatencyTransformer(Duration::from_secs(1));
        let m = get_mock_message();
        let TransformerStream {
            remain_messages: r,
            complete_messages: c,
        } = t.transform(m.clone());

        assert_eq!(r.len(), 1);
        assert_eq!(c.len(), 0);
        // instead of verifying the random algorithm's correctness,
        // verifying the message is not touched
        // and feed through the right channel is more useful
        assert!(r[0].1 == m);
    }

    #[test]
    fn test_randlatency_transformer() {
        let mut t = RandLatencyTransformer::new(1, 30);
        let m = get_mock_message();
        let TransformerStream {
            remain_messages: r,
            complete_messages: c,
        } = t.transform(m.clone());

        assert_eq!(r.len(), 1);
        assert_eq!(c.len(), 0);
        assert!(r[0].0 >= Duration::from_millis(1));
        assert!(r[0].0 <= Duration::from_millis(30));
        assert!(r[0].1 == m);
    }

    #[test]
    fn test_partition_transformer() {
        let keys = create_keys(2);
        let mut peers = HashSet::new();
        peers.insert(PeerId(keys[0].pubkey()));
        let mut t = PartitionTransformer(peers.clone());
        let m = get_mock_message();
        let TransformerStream {
            remain_messages: r,
            complete_messages: c,
        } = t.transform(m.clone());

        assert_eq!(r.len(), 1);
        assert_eq!(c.len(), 0);
        assert!(r[0].0 == Duration::ZERO);
        assert!(r[0].1 == m);

        let peers = HashSet::new();
        let mut t = PartitionTransformer(peers);
        let TransformerStream {
            remain_messages: r,
            complete_messages: c,
        } = t.transform(m.clone());

        assert_eq!(r.len(), 0);
        assert_eq!(c.len(), 1);
        assert!(c[0].0 == Duration::ZERO);
        assert!(c[0].1 == m);
    }

    #[test]
    fn test_drop_transformer() {
        let keys = create_keys(2);
        let mut peers = HashSet::new();
        peers.insert(PeerId(keys[0].pubkey()));
        let mut t = DropTransformer();
        let m = get_mock_message();
        let TransformerStream {
            remain_messages: r,
            complete_messages: c,
        } = t.transform(m);

        assert_eq!(r.len(), 0);
        assert_eq!(c.len(), 0);
    }

    #[test]
    fn test_periodic_transformer() {
        let keys = create_keys(2);
        let mut peers = HashSet::new();
        peers.insert(PeerId(keys[0].pubkey()));
        let mut t = PeriodicTranformer::new(3, 5);
        let m = get_mock_message();
        for _ in 0..2 {
            let TransformerStream {
                remain_messages: r,
                complete_messages: c,
            } = t.transform(m.clone());

            assert_eq!(r.len(), 0);
            assert_eq!(c.len(), 1);
            assert!(c[0].1 == m);
        }

        for _ in 0..5 {
            let TransformerStream {
                remain_messages: r,
                complete_messages: c,
            } = t.transform(m.clone());

            assert_eq!(r.len(), 1);
            assert_eq!(c.len(), 0);
            assert!(r[0].1 == m);
        }
        for _ in 0..1000 {
            let TransformerStream {
                remain_messages: r,
                complete_messages: c,
            } = t.transform(m.clone());

            assert_eq!(r.len(), 0);
            assert_eq!(c.len(), 1);
            assert!(c[0].1 == m);
        }
    }

    #[test]
    fn test_replay_transformer() {
        let keys = create_keys(2);
        let mut peers = HashSet::new();
        peers.insert(PeerId(keys[0].pubkey()));
        // we are mostly interested in the burst behaviur of replay
        let mut t = ReplayTransformer::new(5, crate::transformer::TransformerReplayOrder::Forward);
        let m = get_mock_message();
        for _ in 0..5 {
            let TransformerStream {
                remain_messages: r,
                complete_messages: c,
            } = t.transform(m.clone());
            assert_eq!(r.len(), 0);
            assert_eq!(c.len(), 0);
        }

        let TransformerStream {
            remain_messages: r,
            complete_messages: c,
        } = t.transform(m.clone());
        assert_eq!(r.len(), 6);
        assert_eq!(c.len(), 0);

        for _ in 0..1000 {
            let TransformerStream {
                remain_messages: r,
                complete_messages: c,
            } = t.transform(m.clone());
            assert_eq!(r.len(), 1);
            assert_eq!(c.len(), 0);
        }
    }

    #[test]
    fn test_pipeline_basic_flow() {
        let mut pipe = xfmr_pipe![Transformer::Latency(LatencyTransformer(
            Duration::from_millis(30)
        ))];

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

        let mut pipe = xfmr_pipe![
            Transformer::Latency(LatencyTransformer(Duration::from_millis(30))),
            Transformer::Partition(PartitionTransformer(peers)),
            Transformer::Periodic(PeriodicTranformer::new(3, 5)),
            Transformer::Latency(LatencyTransformer(Duration::from_millis(30)))
        ];
        for _ in 0..1000 {
            let mut mock_message = get_mock_message();
            mock_message.from = PeerId(keys[3].pubkey());
            let result = pipe.process(mock_message.clone());
            assert_eq!(result.len(), 1);
            // since its not part of the id, it doesn't get selected and filter
            assert_eq!(result[0].0, Duration::from_millis(30));
            assert!(result[0].1 == mock_message);
        }
        // not the previous message never made it to periodic, thus it doesn't get trigger the cnt

        // first 2 message should not trigger extra mili
        for _ in 0..2 {
            let mock_message = get_mock_message();
            let result = pipe.process(mock_message.clone());
            assert_eq!(result.len(), 1);
            // since its part of the id, it get selected and filter, thus 30 extra mili
            assert_eq!(result[0].0, Duration::from_millis(30));
            assert!(result[0].1 == mock_message);
        }
        // follow by 5 message that get extra delay
        for _ in 0..5 {
            let mock_message = get_mock_message();
            let result = pipe.process(mock_message.clone());
            assert_eq!(result.len(), 1);
            // since its part of the id, it get selected and filter, thus 30 extra mili
            assert_eq!(result[0].0, Duration::from_millis(60));
            assert!(result[0].1 == mock_message);
        }
        // and then you nver get extra mili
        for _ in 0..1000 {
            let mock_message = get_mock_message();
            let result = pipe.process(mock_message.clone());
            assert_eq!(result.len(), 1);
            // since its part of the id, it get selected and filter, thus 30 extra mili
            assert_eq!(result[0].0, Duration::from_millis(30));
            assert!(result[0].1 == mock_message);
        }
    }
}
