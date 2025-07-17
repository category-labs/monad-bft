use monad_crypto::certificate_signature::PubKey;
use monad_types::Round;
use serde::{Deserialize, Serialize};

use super::fullnode::FullNodeConfig;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub enum SecondaryRaptorCastModeConfig {
    Client,
    Publisher,
    None, // Disables secondary raptorcast
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct FullNodeRaptorCastConfig<P: PubKey> {
    pub mode: SecondaryRaptorCastModeConfig,

    #[serde(bound = "P:PubKey")]
    pub full_nodes_prioritized: FullNodeConfig<P>,
    pub raptor10_fullnode_redundancy_factor: f32, // validator -> full-nodes

    // RaptorCastConfigSecondaryPublisher::GroupSchedulingConfig
    pub max_group_size: usize,
    pub round_span: Round,
    pub invite_lookahead: Round,
    pub max_invite_wait: Round,
    pub deadline_round_dist: Round,
    pub init_empty_round_span: Round,

    // RaptorCastConfigSecondaryClient
    pub bandwidth_cost_per_group_member: u64,
    pub bandwidth_capacity: u64,
    pub invite_future_dist_min: Round,
    pub invite_future_dist_max: Round,
    pub invite_accept_heartbeat_ms: u64,
}
