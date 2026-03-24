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

use std::collections::{HashSet, VecDeque};

use monad_crypto::certificate_signature::PubKey;
use monad_types::NodeId;

pub(crate) const VALIDATOR_SESSION_PREWARM_CONNECTS_PER_TICK: usize = 10;

pub(crate) trait SessionPrewarmBackend<PT: PubKey> {
    fn has_session(&self, validator_id: &NodeId<PT>) -> bool;
    fn connect(&mut self, validator_id: &NodeId<PT>) -> bool;
    fn flush(&mut self);
}

pub(crate) struct ValidatorSessionPrewarmer<PT: PubKey> {
    pending: VecDeque<NodeId<PT>>,
    existing: HashSet<NodeId<PT>>,
}

impl<PT: PubKey> Default for ValidatorSessionPrewarmer<PT> {
    fn default() -> Self {
        Self {
            pending: VecDeque::new(),
            existing: HashSet::new(),
        }
    }
}

impl<PT: PubKey> ValidatorSessionPrewarmer<PT> {
    pub(crate) fn clear(&mut self) {
        self.pending.clear();
        self.existing.clear();
    }

    pub(crate) fn reset(&mut self, validator_ids: impl IntoIterator<Item = NodeId<PT>>) {
        self.clear();

        let mut seen = HashSet::new();
        self.pending.extend(
            validator_ids
                .into_iter()
                .filter(|validator_id| seen.insert(*validator_id)),
        );
    }

    pub(crate) fn tick<B>(&mut self, backend: &mut B)
    where
        B: SessionPrewarmBackend<PT>,
    {
        self.recheck_existing(backend);
        self.promote_established_pending(backend);

        let mut connect_attempts = 0;
        let pending_len = self.pending.len();

        // Cap connect attempts per tick so session prewarming cannot starve the rest of the
        // raptorcast executor when many validators are still pending.
        for _ in 0..pending_len {
            if connect_attempts == VALIDATOR_SESSION_PREWARM_CONNECTS_PER_TICK {
                break;
            }

            let validator_id = self
                .pending
                .pop_front()
                .expect("pending length computed from the queue");

            if backend.has_session(&validator_id) {
                self.existing.insert(validator_id);
                continue;
            }

            if backend.connect(&validator_id) {
                connect_attempts += 1;
            }

            self.pending.push_back(validator_id);
        }

        if connect_attempts > 0 {
            backend.flush();
        }
    }

    fn recheck_existing<B>(&mut self, backend: &mut B)
    where
        B: SessionPrewarmBackend<PT>,
    {
        let mut disconnected = Vec::new();
        self.existing.retain(|validator_id| {
            let has_session = backend.has_session(validator_id);
            if !has_session {
                disconnected.push(*validator_id);
            }
            has_session
        });
        self.pending.extend(disconnected);
    }

    fn promote_established_pending<B>(&mut self, backend: &mut B)
    where
        B: SessionPrewarmBackend<PT>,
    {
        let pending_len = self.pending.len();
        for _ in 0..pending_len {
            let validator_id = self
                .pending
                .pop_front()
                .expect("pending length computed from the queue");

            if backend.has_session(&validator_id) {
                self.existing.insert(validator_id);
            } else {
                self.pending.push_back(validator_id);
            }
        }
    }

    #[cfg(test)]
    fn pending_ids(&self) -> Vec<NodeId<PT>> {
        self.pending.iter().copied().collect()
    }

    #[cfg(test)]
    fn existing_ids(&self) -> HashSet<NodeId<PT>> {
        self.existing.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        net::{Ipv4Addr, SocketAddr, SocketAddrV4},
        sync::Arc,
    };

    use monad_secp::{KeyPair, PubKey};
    use monad_types::NodeId;

    use super::{
        SessionPrewarmBackend, ValidatorSessionPrewarmer,
        VALIDATOR_SESSION_PREWARM_CONNECTS_PER_TICK,
    };

    #[derive(Default)]
    struct FakeBackend {
        sessions: HashSet<NodeId<PubKey>>,
        connectable: HashSet<NodeId<PubKey>>,
        connects: Vec<NodeId<PubKey>>,
        flushes: usize,
        _addrs: HashMap<NodeId<PubKey>, SocketAddr>,
    }

    impl FakeBackend {
        fn with_addrs(ids: &[NodeId<PubKey>]) -> Self {
            Self {
                connectable: ids.iter().copied().collect(),
                _addrs: ids
                    .iter()
                    .enumerate()
                    .map(|(i, id)| {
                        (
                            *id,
                            SocketAddr::V4(SocketAddrV4::new(
                                Ipv4Addr::LOCALHOST,
                                1_000 + i as u16,
                            )),
                        )
                    })
                    .collect(),
                ..Default::default()
            }
        }
    }

    impl SessionPrewarmBackend<PubKey> for FakeBackend {
        fn has_session(&self, validator_id: &NodeId<PubKey>) -> bool {
            self.sessions.contains(validator_id)
        }

        fn connect(&mut self, validator_id: &NodeId<PubKey>) -> bool {
            if self.connectable.contains(validator_id) {
                self.connects.push(*validator_id);
                true
            } else {
                false
            }
        }

        fn flush(&mut self) {
            self.flushes += 1;
        }
    }

    fn validator_id(seed: u8) -> NodeId<PubKey> {
        let keypair = Arc::new(KeyPair::from_bytes(&mut [seed; 32]).unwrap());
        NodeId::new(keypair.pubkey())
    }

    #[test]
    fn reset_replaces_epoch_queue() {
        let a = validator_id(1);
        let b = validator_id(2);
        let c = validator_id(3);

        let mut prewarmer = ValidatorSessionPrewarmer::default();
        prewarmer.reset([a, b]);
        prewarmer.reset([c]);

        assert_eq!(prewarmer.pending_ids(), vec![c]);
        assert!(prewarmer.existing_ids().is_empty());
    }

    #[test]
    fn tick_limits_connects_and_round_robins_pending_queue() {
        let ids: Vec<_> = (1..=12).map(validator_id).collect();
        let mut prewarmer = ValidatorSessionPrewarmer::default();
        prewarmer.reset(ids.iter().copied());

        let mut backend = FakeBackend::with_addrs(&ids);
        prewarmer.tick(&mut backend);

        assert_eq!(
            backend.connects,
            ids[..VALIDATOR_SESSION_PREWARM_CONNECTS_PER_TICK].to_vec()
        );
        assert_eq!(backend.flushes, 1);
        assert_eq!(
            prewarmer.pending_ids(),
            vec![
                ids[10], ids[11], ids[0], ids[1], ids[2], ids[3], ids[4], ids[5], ids[6], ids[7],
                ids[8], ids[9],
            ]
        );
    }

    #[test]
    fn tick_moves_established_sessions_between_pending_and_existing() {
        let a = validator_id(1);
        let b = validator_id(2);

        let mut prewarmer = ValidatorSessionPrewarmer::default();
        prewarmer.reset([a, b]);

        let mut backend = FakeBackend::with_addrs(&[a, b]);
        backend.sessions.insert(a);

        prewarmer.tick(&mut backend);
        assert_eq!(backend.connects, vec![b]);
        assert_eq!(prewarmer.pending_ids(), vec![b]);
        assert_eq!(prewarmer.existing_ids(), HashSet::from([a]));

        backend.sessions.insert(b);
        backend.connects.clear();

        prewarmer.tick(&mut backend);
        assert!(backend.connects.is_empty());
        assert!(prewarmer.pending_ids().is_empty());
        assert_eq!(prewarmer.existing_ids(), HashSet::from([a, b]));

        backend.sessions.remove(&a);
        prewarmer.tick(&mut backend);

        assert_eq!(backend.connects, vec![a]);
        assert_eq!(prewarmer.pending_ids(), vec![a]);
        assert_eq!(prewarmer.existing_ids(), HashSet::from([b]));
    }
}
