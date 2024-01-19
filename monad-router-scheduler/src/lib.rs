use std::{
    collections::{BTreeSet, VecDeque},
    marker::PhantomData,
    time::Duration,
};

use monad_crypto::certificate_signature::PubKey;
use monad_types::{NodeId, RouterTarget};

#[derive(Debug)]
pub enum RouterEvent<PT: PubKey, InboundMessage, TransportMessage> {
    Rx(NodeId<PT>, InboundMessage),
    Tx(NodeId<PT>, TransportMessage),
}

pub trait RouterSchedulerBuilder {
    type RouterScheduler: RouterScheduler;
    fn build(self) -> Self::RouterScheduler;
}

/// RouterScheduler describes HOW gossip messages get delivered
pub trait RouterScheduler {
    type NodeIdPublicKey: PubKey;

    // Transport level message type (usually bytes)
    type TransportMessage;

    // Application level data
    type InboundMessage;
    type OutboundMessage;

    fn process_inbound(
        &mut self,
        time: Duration,
        from: NodeId<Self::NodeIdPublicKey>,
        message: Self::TransportMessage,
    );
    fn send_outbound(
        &mut self,
        time: Duration,
        to: RouterTarget<Self::NodeIdPublicKey>,
        message: Self::OutboundMessage,
    );

    fn peek_tick(&self) -> Option<Duration>;
    fn step_until(
        &mut self,
        until: Duration,
    ) -> Option<RouterEvent<Self::NodeIdPublicKey, Self::InboundMessage, Self::TransportMessage>>;
}

impl<T: RouterScheduler + ?Sized> RouterScheduler for Box<T> {
    type NodeIdPublicKey = T::NodeIdPublicKey;
    type TransportMessage = T::TransportMessage;
    type InboundMessage = T::InboundMessage;
    type OutboundMessage = T::OutboundMessage;

    fn process_inbound(
        &mut self,
        time: Duration,
        from: NodeId<Self::NodeIdPublicKey>,
        message: Self::TransportMessage,
    ) {
        (**self).process_inbound(time, from, message)
    }

    fn send_outbound(
        &mut self,
        time: Duration,
        to: RouterTarget<Self::NodeIdPublicKey>,
        message: Self::OutboundMessage,
    ) {
        (**self).send_outbound(time, to, message)
    }

    fn peek_tick(&self) -> Option<Duration> {
        (**self).peek_tick()
    }

    fn step_until(
        &mut self,
        until: Duration,
    ) -> Option<RouterEvent<Self::NodeIdPublicKey, Self::InboundMessage, Self::TransportMessage>>
    {
        (**self).step_until(until)
    }
}

pub struct NoSerRouterScheduler<PT: PubKey, IM, OM> {
    all_peers: BTreeSet<NodeId<PT>>,
    events: VecDeque<(Duration, RouterEvent<PT, IM, OM>)>,
}

#[derive(Clone)]
pub struct NoSerRouterConfig<PT: PubKey, IM, OM> {
    pub all_peers: BTreeSet<NodeId<PT>>,
    _phantom: PhantomData<(IM, OM)>,
}

impl<PT: PubKey, IM, OM> NoSerRouterConfig<PT, IM, OM> {
    pub fn new(all_peers: BTreeSet<NodeId<PT>>) -> Self {
        Self {
            all_peers,
            _phantom: PhantomData,
        }
    }
}

impl<PT: PubKey, IM, OM> RouterSchedulerBuilder for NoSerRouterConfig<PT, IM, OM>
where
    OM: Clone,
    IM: From<OM>,
{
    type RouterScheduler = NoSerRouterScheduler<PT, IM, OM>;

    fn build(self) -> Self::RouterScheduler {
        Self::RouterScheduler {
            all_peers: self.all_peers,
            events: Default::default(),
        }
    }
}

impl<PT: PubKey, IM, OM> RouterScheduler for NoSerRouterScheduler<PT, IM, OM>
where
    OM: Clone,
    IM: From<OM>,
{
    type NodeIdPublicKey = PT;
    type TransportMessage = OM;
    type InboundMessage = IM;
    type OutboundMessage = OM;

    fn process_inbound(
        &mut self,
        time: Duration,
        from: NodeId<Self::NodeIdPublicKey>,
        message: Self::TransportMessage,
    ) {
        assert!(
            time >= self
                .events
                .back()
                .map(|(time, _)| *time)
                .unwrap_or(Duration::ZERO)
        );
        self.events
            .push_back((time, RouterEvent::Rx(from, message.into())))
    }

    fn send_outbound(
        &mut self,
        time: Duration,
        to: RouterTarget<Self::NodeIdPublicKey>,
        message: Self::OutboundMessage,
    ) {
        assert!(
            time >= self
                .events
                .back()
                .map(|(time, _)| *time)
                .unwrap_or(Duration::ZERO)
        );
        match to {
            RouterTarget::Broadcast => {
                self.events.extend(
                    self.all_peers
                        .iter()
                        .map(|to| (time, RouterEvent::Tx(*to, message.clone()))),
                );
            }
            RouterTarget::PointToPoint(to) => {
                self.events.push_back((time, RouterEvent::Tx(to, message)));
            }
        }
    }

    fn peek_tick(&self) -> Option<Duration> {
        self.events.front().map(|(tick, _)| *tick)
    }

    fn step_until(
        &mut self,
        until: Duration,
    ) -> Option<RouterEvent<Self::NodeIdPublicKey, Self::InboundMessage, Self::TransportMessage>>
    {
        if self.peek_tick().unwrap_or(Duration::MAX) <= until {
            let (_, event) = self.events.pop_front().expect("must exist");
            Some(event)
        } else {
            None
        }
    }
}
