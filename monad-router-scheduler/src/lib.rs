use std::{
    collections::{BTreeSet, VecDeque},
    time::Duration,
};

use monad_types::{NodeId, RouterTarget};

#[derive(Debug)]
pub enum RouterEvent<InboundMessage, TransportMessage> {
    Rx(NodeId, InboundMessage),
    Tx(NodeId, TransportMessage),
}

/// RouterScheduler describes HOW gossip messages get delivered
pub trait RouterScheduler {
    type Config;

    // Transport level message type (usually bytes)
    type TransportMessage;

    // Application level data
    type InboundMessage;
    type OutboundMessage;

    fn new(config: Self::Config) -> Self;

    fn process_inbound(&mut self, time: Duration, from: NodeId, message: Self::TransportMessage);
    fn send_outbound(&mut self, time: Duration, to: RouterTarget, message: Self::OutboundMessage);

    fn peek_tick(&self) -> Option<Duration>;
    fn step_until(
        &mut self,
        until: Duration,
    ) -> Option<RouterEvent<Self::InboundMessage, Self::TransportMessage>>;
}

pub struct NoSerRouterScheduler<IM, OM> {
    all_peers: BTreeSet<NodeId>,
    events: VecDeque<(Duration, RouterEvent<IM, OM>)>,
}

#[derive(Clone)]
pub struct NoSerRouterConfig {
    pub all_peers: BTreeSet<NodeId>,
}

impl<IM, OM> RouterScheduler for NoSerRouterScheduler<IM, OM>
where
    OM: Clone,
    IM: From<OM>,
{
    type Config = NoSerRouterConfig;
    type TransportMessage = OM;
    type InboundMessage = IM;
    type OutboundMessage = OM;

    fn new(config: NoSerRouterConfig) -> Self {
        Self {
            all_peers: config.all_peers,
            events: Default::default(),
        }
    }

    fn process_inbound(&mut self, time: Duration, from: NodeId, message: Self::TransportMessage) {
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

    fn send_outbound(&mut self, time: Duration, to: RouterTarget, message: Self::OutboundMessage) {
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
    ) -> Option<RouterEvent<Self::InboundMessage, Self::TransportMessage>> {
        if self.peek_tick().unwrap_or(Duration::MAX) <= until {
            let (_, event) = self.events.pop_front().expect("must exist");
            Some(event)
        } else {
            None
        }
    }
}
