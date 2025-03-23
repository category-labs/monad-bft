use std::{collections::BTreeMap, time::Duration};

use monad_peer_discovery::PeerDiscoveryBuilder;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::{NodeBuilder, Nodes, PeerDiscSwarmRelation};

pub struct PeerDiscSwarmBuilder<S, B>
where
    S: PeerDiscSwarmRelation,
{
    pub builders: Vec<NodeBuilder<S, B>>,
    pub seed: u64,
}

impl<S, B> PeerDiscSwarmBuilder<S, B>
where
    S: PeerDiscSwarmRelation,
    B: PeerDiscoveryBuilder<PeerDiscoveryAlgoType = S::PeerDiscoveryAlgoType>,
{
    pub fn build(self) -> Nodes<S> {
        let _init_span_entered =
            tracing::trace_span!("init", tick = format!("{:?}", Duration::ZERO)).entered();
        let mut states = BTreeMap::new();
        for builder in self.builders {
            let id = builder.id;
            let _node_span_entered = tracing::trace_span!("node", id = format!("{}", id)).entered();
            let node = builder.build();
            states.insert(id, node);
        }

        let mut rng = ChaCha8Rng::seed_from_u64(self.seed);

        Nodes {
            states,
            tick: Default::default(),
            rng: ChaCha8Rng::seed_from_u64(rng.r#gen()),
            current_seed: rng.r#gen(),
        }
    }
}
