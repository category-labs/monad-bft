use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use futures::StreamExt;
use monad_crypto::secp256k1::PubKey;

use crate::{executor::mock::MockExecutor, Executor, Message, PeerId, State};

pub enum LinkMessageType<M: Message> {
    Message(M),
    Ack(M::Id),
}

pub struct LinkMessage<M: Message> {
    pub from: PeerId,
    pub to: PeerId,
    pub message: LinkMessageType<M>,

    /// absolute time
    pub from_tick: Duration,
}

pub trait Transformer<M: Message> {
    #[must_use]
    /// note that the output Duration should be a delay, not an absolute time
    // TODO smallvec? resulting Vec will almost always be len 1
    fn transform(&mut self, message: LinkMessage<M>) -> Vec<(Duration, LinkMessage<M>)>;

    fn boxed(self) -> Box<dyn Transformer<M>>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

/// adds constant latency
#[derive(Clone)]
pub struct LatencyTransformer(pub Duration);
impl<M: Message> Transformer<M> for LatencyTransformer {
    fn transform(&mut self, message: LinkMessage<M>) -> Vec<(Duration, LinkMessage<M>)> {
        vec![(self.0, message)]
    }
}

/// adds constant latency (parametrizable cap) to each link determined by xor(peer_id_1, peer_id_2)
#[derive(Clone)]
pub struct XorLatencyTransformer(pub Duration);
impl<M: Message> Transformer<M> for XorLatencyTransformer {
    fn transform(&mut self, message: LinkMessage<M>) -> Vec<(Duration, LinkMessage<M>)> {
        let mut ck: u8 = 0;
        for b in message.from.0.bytes() {
            ck ^= b;
        }
        for b in message.to.0.bytes() {
            ck ^= b;
        }
        vec![(self.0.mul_f32(ck as f32 / u8::MAX as f32), message)]
    }
}

/// blacklists given nodes
#[derive(Clone)]
pub struct BlacklistTransformer(pub HashSet<PeerId>);
impl<M: Message> Transformer<M> for BlacklistTransformer {
    fn transform(&mut self, message: LinkMessage<M>) -> Vec<(Duration, LinkMessage<M>)> {
        let mut output = Vec::new();
        if !self.0.contains(&message.from) && !self.0.contains(&message.to) {
            output.push((Duration::ZERO, message))
        }
        output
    }
}

// using dynamic dispatch so that we can change these at runtime... such as in monad-viz
type LayerTransformer<M> = Vec<Box<dyn Transformer<M>>>;

impl<M: Message> Transformer<M> for LayerTransformer<M> {
    fn transform(&mut self, message: LinkMessage<M>) -> Vec<(Duration, LinkMessage<M>)> {
        self.iter_mut().fold(
            // accumulator is transformed set of messages before/after each layer
            vec![(Duration::ZERO, message)],
            |messages, layer| {
                messages
                    .into_iter()
                    .flat_map(|(base_duration, message)| {
                        // transform each message by applying the layer to each
                        layer
                            .transform(message)
                            .into_iter()
                            .map(move |(duration, message)| (base_duration + duration, message))
                    })
                    .collect()
            },
        )
    }
}

pub struct Nodes<S, T>
where
    S: State,
    T: Transformer<S::Message>,
{
    states: HashMap<PeerId, (MockExecutor<S>, S)>,
    transformer: T,
}

impl<S, T> Nodes<S, T>
where
    S: State,
    T: Transformer<S::Message>,

    MockExecutor<S>: Unpin,
{
    pub fn new(peers: Vec<(PubKey, S::Config)>, transformer: T) -> Self {
        assert!(!peers.is_empty());

        let mut states = HashMap::new();

        for (pubkey, config) in peers {
            let mut executor: MockExecutor<S> = MockExecutor::default();
            let (state, init_commands) = S::init(config);
            executor.exec(init_commands);
            states.insert(PeerId(pubkey), (executor, state));
        }

        let mut nodes = Self {
            states,
            transformer,
        };

        for peer_id in nodes.states.keys().cloned().collect::<Vec<_>>() {
            nodes.simulate_peer(&peer_id, Duration::from_secs(0));
        }

        nodes
    }

    pub fn next_tick(&self) -> Option<Duration> {
        self.states
            .iter()
            .filter_map(|(_, (executor, _))| {
                let tick = executor.peek_event_tick()?;
                Some(tick)
            })
            .min()
    }

    pub fn step(&mut self) -> Option<(Duration, PeerId, S::Event)> {
        if let Some((id, executor, state, tick)) = self
            .states
            .iter_mut()
            .filter_map(|(id, (executor, state))| {
                let tick = executor.peek_event_tick()?;
                Some((id, executor, state, tick))
            })
            .min_by_key(|(_, _, _, tick)| *tick)
        {
            let id = *id;
            let event = futures::executor::block_on(executor.next()).unwrap();
            let commands = state.update(event.clone());

            executor.exec(commands);

            self.simulate_peer(&id, tick);

            Some((tick, id, event))
        } else {
            None
        }
    }

    fn simulate_peer(&mut self, peer_id: &PeerId, tick: Duration) {
        let outbounds = {
            let mut outbounds = Vec::new();
            let (mut executor, state) = self.states.remove(peer_id).unwrap();

            while let Some((to, outbound_message)) = executor.receive_message() {
                let transformed = self.transformer.transform(LinkMessage {
                    from: *peer_id,
                    to,
                    message: LinkMessageType::Message(outbound_message.into()),

                    from_tick: tick,
                });

                outbounds.extend(transformed.into_iter());
            }
            while let Some((to, message_id)) = executor.receive_ack() {
                let transformed = self.transformer.transform(LinkMessage {
                    from: *peer_id,
                    to,
                    message: LinkMessageType::Ack(message_id),

                    from_tick: tick,
                });

                outbounds.extend(transformed.into_iter());
            }

            self.states.insert(*peer_id, (executor, state));
            outbounds
        };

        for (
            delay,
            LinkMessage {
                from,
                to,
                message,
                from_tick,
            },
        ) in outbounds
        {
            let to_state = &mut self.states.get_mut(&to).unwrap().0;

            match message {
                LinkMessageType::Message(m) => {
                    to_state.send_message(tick + delay, from, m, from_tick)
                }
                LinkMessageType::Ack(m_id) => {
                    to_state.send_ack(tick + delay, from, m_id, from_tick)
                }
            }
        }
    }

    pub fn states(&self) -> &HashMap<PeerId, (MockExecutor<S>, S)> {
        &self.states
    }
}
