use std::{ops::DerefMut, pin::Pin, time::Duration};

use monad_types::{NodeId, RouterTarget};

pub mod gossipsub;
pub mod mock;
#[cfg(test)]
mod testutil;

pub enum GossipEvent<GM> {
    /// Send gossip_message to peer
    Send(NodeId, GM), // send gossip_message

    /// Emit app_message to executor (NOTE: not gossip_message)
    Emit(NodeId, Vec<u8>),
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
    fn send(&mut self, time: Duration, to: RouterTarget, message: &[u8]);
    fn handle_gossip_message(&mut self, time: Duration, from: NodeId, gossip_message: &[u8]);

    fn peek_tick(&self) -> Option<Duration>;
    fn poll(&mut self, time: Duration) -> Option<GossipEvent<Vec<u8>>>;

    fn boxed<'a>(self) -> BoxGossip<'a>
    where
        Self: Sized + Send + Unpin + 'a,
    {
        Box::pin(self)
    }
}

impl<G: Gossip + ?Sized> Gossip for Box<G> {
    fn send(&mut self, time: Duration, to: RouterTarget, message: &[u8]) {
        (**self).send(time, to, message)
    }

    fn handle_gossip_message(&mut self, time: Duration, from: NodeId, gossip_message: &[u8]) {
        (**self).handle_gossip_message(time, from, gossip_message)
    }

    fn peek_tick(&self) -> Option<Duration> {
        (**self).peek_tick()
    }

    fn poll(&mut self, time: Duration) -> Option<GossipEvent<Vec<u8>>> {
        (**self).poll(time)
    }
}

impl<P> Gossip for Pin<P>
where
    P: DerefMut,
    P::Target: Gossip + Unpin,
{
    fn send(&mut self, time: Duration, to: RouterTarget, message: &[u8]) {
        Pin::get_mut(Pin::as_mut(self)).send(time, to, message)
    }

    fn handle_gossip_message(&mut self, time: Duration, from: NodeId, gossip_message: &[u8]) {
        Pin::get_mut(Pin::as_mut(self)).handle_gossip_message(time, from, gossip_message)
    }

    fn peek_tick(&self) -> Option<Duration> {
        Pin::get_ref(Pin::as_ref(self)).peek_tick()
    }

    fn poll(&mut self, time: Duration) -> Option<GossipEvent<Vec<u8>>> {
        Pin::get_mut(Pin::as_mut(self)).poll(time)
    }
}

pub type BoxGossip<'a> = Pin<Box<dyn Gossip + Send + Unpin + 'a>>;
