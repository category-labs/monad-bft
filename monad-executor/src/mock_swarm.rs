use std::{collections::HashMap, time::Duration};

use futures::StreamExt;
use monad_crypto::secp256k1::PubKey;

use crate::{executor::mock::MockExecutor, Executor, PeerId, State};

pub struct Nodes<S: State, L: Fn(&PeerId, &PeerId) -> Duration> {
    states: HashMap<PeerId, (MockExecutor<S::Event, S::Message>, S)>,
    compute_latency: L,
}

impl<S: State, L: Fn(&PeerId, &PeerId) -> Duration> Nodes<S, L> {
    pub fn new(peers: Vec<(PubKey, S::Config)>, compute_latency: L) -> Self {
        assert!(!peers.is_empty());

        let mut states = HashMap::new();

        for (pubkey, config) in peers {
            let mut executor: MockExecutor<S::Event, S::Message> = MockExecutor::default();
            let (state, init_commands) = S::init(config);
            executor.exec(init_commands);
            states.insert(PeerId(pubkey), (executor, state));
        }

        let mut nodes = Self {
            states,
            compute_latency,
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
        let (mut executor, state) = self.states.remove(peer_id).unwrap();

        while let Some((to, outbound_message)) = executor.receive_message() {
            let to_state = if &to == peer_id {
                &mut executor
            } else {
                &mut self.states.get_mut(&to).unwrap().0
            };
            to_state.send_message(
                tick + (self.compute_latency)(peer_id, &to),
                *peer_id,
                outbound_message,
                tick,
            );
        }
        while let Some((to, message_id)) = executor.receive_ack() {
            let to_state = if &to == peer_id {
                &mut executor
            } else {
                &mut self.states.get_mut(&to).unwrap().0
            };
            to_state.send_ack(
                tick + (self.compute_latency)(peer_id, &to),
                *peer_id,
                message_id,
                tick,
            );
        }

        self.states.insert(*peer_id, (executor, state));
    }

    pub fn states(&self) -> &HashMap<PeerId, (MockExecutor<S::Event, S::Message>, S)> {
        &self.states
    }
}
