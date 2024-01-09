use std::{ops::DerefMut, pin::Pin, time::Duration};

use bytes::Bytes;
use bytes_utils::SegmentedBuf;
use monad_types::{NodeId, RouterTarget};

pub mod broadcasttree;
mod connection_manager;
pub use connection_manager::{ConnectionManager, ConnectionManagerEvent};
pub mod gossipsub;
pub mod mock;
pub mod testutil;

type GossipMessage = Bytes;
type FragmentedGossipMessage = SegmentedBuf<Bytes>;
/// We don't use a more sophisticated `impl Buf` type here, because prost anyway only supports
/// zero-copy deserialization on Bytes
///
/// Further reading:
/// - https://github.com/tokio-rs/prost/blob/907e9f6fbf72262f52333459bbfb27224da1ad72/src/encoding.rs#L988C40-L988C40
/// - https://github.com/tokio-rs/prost/pull/190
type AppMessage = Bytes;

#[derive(Debug)]
pub enum GossipEvent {
    /// Send gossip_message to peer
    /// Delivery is not guaranteed (connection may sever at any point)
    /// Framing of each individual message is guaranteed
    Send(NodeId, FragmentedGossipMessage),

    /// Emit app_message to executor (NOTE: not gossip_message)
    Emit(NodeId, AppMessage),
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
    /// Ask Gossip implementation to send given application message
    /// There are no delivery guarantees; the message may be dropped
    /// - for example, if the Gossip implementation can't find a valid Gossip::connected() path to
    ///   the recipient, this application_message will always be dropped, and a RequestConnect
    ///   event may be emitted
    fn send(&mut self, time: Duration, to: RouterTarget, message: AppMessage);

    /// Handle gossip_message received from peer
    fn handle_gossip_message(
        &mut self,
        time: Duration,
        from: NodeId,
        gossip_message: GossipMessage,
    );

    fn peek_tick(&self) -> Option<Duration>;
    fn poll(&mut self, time: Duration) -> Option<GossipEvent>;

    fn boxed<'a>(self) -> BoxGossip<'a>
    where
        Self: Sized + Send + Unpin + 'a,
    {
        Box::pin(self)
    }
}

impl<G: Gossip + ?Sized> Gossip for Box<G> {
    fn send(&mut self, time: Duration, to: RouterTarget, message: AppMessage) {
        (**self).send(time, to, message)
    }

    fn handle_gossip_message(
        &mut self,
        time: Duration,
        from: NodeId,
        gossip_message: GossipMessage,
    ) {
        (**self).handle_gossip_message(time, from, gossip_message)
    }

    fn peek_tick(&self) -> Option<Duration> {
        (**self).peek_tick()
    }

    fn poll(&mut self, time: Duration) -> Option<GossipEvent> {
        (**self).poll(time)
    }
}

impl<P> Gossip for Pin<P>
where
    P: DerefMut,
    P::Target: Gossip + Unpin,
{
    fn send(&mut self, time: Duration, to: RouterTarget, message: AppMessage) {
        Pin::get_mut(Pin::as_mut(self)).send(time, to, message)
    }

    fn handle_gossip_message(
        &mut self,
        time: Duration,
        from: NodeId,
        gossip_message: GossipMessage,
    ) {
        Pin::get_mut(Pin::as_mut(self)).handle_gossip_message(time, from, gossip_message)
    }

    fn peek_tick(&self) -> Option<Duration> {
        Pin::get_ref(Pin::as_ref(self)).peek_tick()
    }

    fn poll(&mut self, time: Duration) -> Option<GossipEvent> {
        Pin::get_mut(Pin::as_mut(self)).poll(time)
    }
}

pub type BoxGossip<'a> = Pin<Box<dyn Gossip + Send + Unpin + 'a>>;
