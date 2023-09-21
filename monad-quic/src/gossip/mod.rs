use std::{error::Error, time::Duration};

use monad_executor_glue::{PeerId, RouterTarget};

pub mod mock;
#[cfg(test)]
mod testutil;

pub enum GossipEvent {
    /// Send gossip_message to peer
    Send(PeerId, Vec<u8>), // send gossip_message

    /// Emit app_message to executor (NOTE: not gossip_message)
    Emit(PeerId, Vec<u8>),
}

/// Gossip describes WHAT gossip messages get delivered (given application-level messages)
/// Gossip converts:
/// - outbound application messages to outbound gossip messages (tag whatever necessary metadata)
/// - inbound gossip messages to inbound application messages + outbound gossip messages
///
/// NOTE that this must gracefully handle outbound application to self (should immediately Emit, not Send)
///
/// `message` and `gossip_message` are both typed as bytes intentionally, because that's the atomic
/// unit of transfer.
pub trait Gossip {
    type Config;
    type MessageId;

    fn new(config: Self::Config) -> Self;
    fn send(&mut self, time: Duration, to: RouterTarget, message: &[u8]) -> Self::MessageId;
    fn forget(&mut self, time: Duration, to: RouterTarget, message_id: Self::MessageId);
    fn handle_gossip_message(&mut self, time: Duration, from: PeerId, gossip_message: &[u8]);

    fn peek_tick(&self) -> Option<Duration>;
    fn poll(&mut self, time: Duration) -> Option<GossipEvent>;
}

// placeholder
pub type GossipError = Box<dyn Error>;
