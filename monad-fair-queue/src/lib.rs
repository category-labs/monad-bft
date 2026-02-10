pub mod metrics;
mod queue;

use std::fmt::{Debug, Display};

pub use monad_peer_score::{IdentityScore, PeerStatus, Score};
pub use queue::{FairQueue, FairQueueBuilder, FairQueueMetrics};

#[derive(Debug, Clone, thiserror::Error)]
pub enum PushError<Id: Debug + Display> {
    #[error("per-id limit exceeded for {id}: {limit}")]
    PerIdLimitExceeded { id: Id, limit: usize },
    #[error("queue full: {size}/{max_size}")]
    Full { size: usize, max_size: usize },
}
