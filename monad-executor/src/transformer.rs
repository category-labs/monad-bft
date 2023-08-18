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

pub struct DropTransformer {}

impl<M> Transform<M> for DropTransformer {
    fn transform(&mut self, _: LinkMessage<M>) -> TransformerStream<M> {
        TransformerStream::new(vec![], vec![])
    }
}

#[derive(Clone, Debug)]
pub struct PeriodicTranformer {
    pub start: u32,    // when period start
    pub duration: u32, // how long does it last
    pub cnt: u32,      // monotonically inreasing counter
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
            return TransformerStream::new(vec![], vec![]);
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
 * pipeline consist of a of transformer that goes through all the output
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
