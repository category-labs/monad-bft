use std::{error::Error, time::Duration};

use monad_executor_glue::{PeerId, RouterTarget};

pub mod gossipsub;
pub mod mock;
#[cfg(test)]
mod testutil;

pub enum GossipEvent<GM> {
    /// Send gossip_message to peer
    Send(PeerId, GM), // send gossip_message

    /// Emit app_message to executor (NOTE: not gossip_message)
    Emit(PeerId, Vec<u8>),
}

impl<GM> GossipEvent<GM> {
    fn map<U, F>(self, f: F) -> GossipEvent<U>
    where
        F: FnOnce(GM) -> U,
    {
        match self {
            GossipEvent::Send(peer_id, gossip_message) => {
                GossipEvent::Send(peer_id, f(gossip_message))
            }
            GossipEvent::Emit(peer_id, message) => GossipEvent::Emit(peer_id, message),
        }
    }
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

    fn new(config: Self::Config) -> Self;
    fn send(&mut self, time: Duration, to: RouterTarget, message: &[u8]);
    fn handle_gossip_message(&mut self, time: Duration, from: PeerId, gossip_message: &[u8]);

    fn peek_tick(&self) -> Option<Duration>;
    fn poll(&mut self, time: Duration) -> Option<GossipEvent<Vec<u8>>>;
}

// placeholder
pub type GossipError = Box<dyn Error>;
