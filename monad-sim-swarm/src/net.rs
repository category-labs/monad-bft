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

//! The network as a simulation process, generic over the node address `Addr`
//! and the wire message `Msg`. A node sends by scheduling a step on the [`Net`]
//! handle; the net samples its [`NetworkModel`] and schedules the surviving
//! deliveries against the recipients.

use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Range,
    rc::Rc,
    time::Duration,
};

use monad_sim::{Ctx, Time};
use rand::{distributions::Distribution, Rng, SeedableRng};
use rand_chacha::ChaChaRng;

/// A type-erased "deliver this message to one node at this time" operation.
///
/// Registered per address (see [`Net::register`] / [`crate::deliver_to`]), it
/// captures the recipient's *concrete* `Handle` and monomorphizes the
/// `ctx.schedule` call, so the network itself only ever names `Addr` and `Msg`.
/// This is what lets one [`Net`] carry heterogeneous node process types.
pub type Deliver<Addr, Msg> = Rc<dyn Fn(&mut Ctx, Time, Addr, Msg)>;

/// Per-message context handed to a [`NetworkModel`].
pub struct Link<Addr> {
    pub now: Time,
    pub from: Addr,
    pub to: Addr,
}

/// Decides the fate of a single message: the delays at which copies are
/// delivered to `link.to`. An empty result drops the message; more than one
/// duplicates it. Reordering needs no special handling — a smaller delay simply
/// arrives first.
pub trait NetworkModel<Addr, Msg> {
    fn deliveries(
        &mut self,
        link: &Link<Addr>,
        message: &Msg,
        rng: &mut ChaChaRng,
    ) -> Vec<Duration>;
}

type LatencyFn = Box<dyn FnMut(&mut ChaChaRng) -> Duration>;
type DropFn<Addr, Msg> = Box<dyn Fn(&Link<Addr>, &Msg) -> bool>;
type Partition<Addr> = (Range<Time>, Vec<BTreeSet<Addr>>);

/// A declarative network model: a base latency plus optional, independent
/// conditions. Anything left unconfigured behaves reliably.
pub struct Conditions<Addr, Msg> {
    latency: LatencyFn,
    loss: f64,
    partitions: Vec<Partition<Addr>>,
    drop_if: Option<DropFn<Addr, Msg>>,
}

impl<Addr: Ord, Msg> NetworkModel<Addr, Msg> for Conditions<Addr, Msg> {
    fn deliveries(
        &mut self,
        link: &Link<Addr>,
        message: &Msg,
        rng: &mut ChaChaRng,
    ) -> Vec<Duration> {
        if let Some(drop_if) = &self.drop_if {
            if drop_if(link, message) {
                return Vec::new();
            }
        }
        let split = self.partitions.iter().any(|(window, groups)| {
            window.contains(&link.now)
                && groups
                    .iter()
                    .any(|group| group.contains(&link.from) != group.contains(&link.to))
        });
        if split {
            return Vec::new();
        }
        if self.loss > 0.0 && rng.gen::<f64>() < self.loss {
            return Vec::new();
        }
        vec![(self.latency)(rng)]
    }
}

/// Declarative builder for a swarm's network behaviour. Start from
/// [`reliable`](Network::reliable) or [`with_latency`](Network::with_latency)
/// and layer conditions, or supply a [`custom`](Network::custom) model.
pub enum Network<Addr, Msg> {
    Conditions(Conditions<Addr, Msg>),
    Custom(Box<dyn NetworkModel<Addr, Msg>>),
}

impl<Addr, Msg> Default for Network<Addr, Msg> {
    fn default() -> Self {
        Network::reliable(Duration::from_millis(1))
    }
}

impl<Addr, Msg> Network<Addr, Msg> {
    /// Constant-latency, lossless network.
    pub fn reliable(latency: Duration) -> Self {
        Network::Conditions(Conditions {
            latency: Box::new(move |_| latency),
            loss: 0.0,
            partitions: Vec::new(),
            drop_if: None,
        })
    }

    /// Latency sampled per message from `distribution`.
    pub fn with_latency(distribution: impl Distribution<Duration> + 'static) -> Self {
        let mut network = Network::reliable(Duration::ZERO);
        network.conditions().latency = Box::new(move |rng| distribution.sample(rng));
        network
    }

    /// A fully custom model.
    pub fn custom(model: impl NetworkModel<Addr, Msg> + 'static) -> Self {
        Network::Custom(Box::new(model))
    }

    /// Drop each message independently with probability `probability`.
    pub fn loss(mut self, probability: f64) -> Self {
        self.conditions().loss = probability;
        self
    }

    /// While `window` is active, drop messages crossing between the given groups
    /// (a network split). Nodes absent from every group stay fully connected.
    pub fn partition(
        mut self,
        window: Range<Time>,
        groups: impl IntoIterator<Item = impl IntoIterator<Item = Addr>>,
    ) -> Self
    where
        Addr: Ord,
    {
        let groups = groups
            .into_iter()
            .map(|group| group.into_iter().collect())
            .collect();
        self.conditions().partitions.push((window, groups));
        self
    }

    /// Drop messages for which `predicate` holds.
    pub fn drop_if(mut self, predicate: impl Fn(&Link<Addr>, &Msg) -> bool + 'static) -> Self {
        self.conditions().drop_if = Some(Box::new(predicate));
        self
    }

    /// Panics if called on a [`custom`](Network::custom) model.
    fn conditions(&mut self) -> &mut Conditions<Addr, Msg> {
        match self {
            Network::Conditions(conditions) => conditions,
            Network::Custom(_) => panic!("cannot layer conditions onto a custom network model"),
        }
    }

    fn into_model(self) -> Box<dyn NetworkModel<Addr, Msg>>
    where
        Addr: Ord + 'static,
        Msg: 'static,
    {
        match self {
            Network::Conditions(conditions) => Box::new(conditions),
            Network::Custom(model) => model,
        }
    }
}

/// The network process: a routing table (type-erased per-node delivery) plus a
/// [`NetworkModel`] and an independent RNG (kept separate from the engine's
/// scheduling RNG so network randomness is reproducible on its own).
pub struct Net<Addr, Msg> {
    routes: BTreeMap<Addr, Deliver<Addr, Msg>>,
    model: Box<dyn NetworkModel<Addr, Msg>>,
    rng: ChaChaRng,
}

impl<Addr, Msg> Net<Addr, Msg>
where
    Addr: Ord + Copy + 'static,
    Msg: Clone + 'static,
{
    /// A fresh network with the given model, seeded independently of the engine.
    pub fn new(seed: u64, network: Network<Addr, Msg>) -> Self {
        Self {
            routes: BTreeMap::new(),
            model: network.into_model(),
            rng: ChaChaRng::seed_from_u64(seed),
        }
    }

    /// Register a node's delivery entry point (see [`crate::deliver_to`]).
    pub fn register(&mut self, addr: Addr, deliver: Deliver<Addr, Msg>) {
        self.routes.insert(addr, deliver);
    }

    /// Drop a node from the routing table (e.g. a restart/removal); messages to
    /// it are silently discarded thereafter.
    pub fn deregister(&mut self, addr: &Addr) {
        self.routes.remove(addr);
    }

    /// Replace the network model (mid-run reconfiguration).
    pub fn set_model(&mut self, network: Network<Addr, Msg>) {
        self.model = network.into_model();
    }

    /// Send one message `from` -> `to`: apply the model and schedule the
    /// surviving deliveries. A recipient not in the routing table drops it.
    pub fn send(&mut self, ctx: &mut Ctx, from: Addr, to: Addr, msg: Msg) {
        let link = Link {
            now: ctx.now(),
            from,
            to,
        };
        let delays = self.model.deliveries(&link, &msg, &mut self.rng);
        let Some(deliver) = self.routes.get(&to).cloned() else {
            return;
        };
        match delays.as_slice() {
            [] => {}
            [delay] => deliver(ctx, ctx.now() + *delay, from, msg),
            _ => {
                for delay in delays {
                    deliver(ctx, ctx.now() + delay, from, msg.clone());
                }
            }
        }
    }

    /// Send `msg` to every registered node (including `from` itself, if
    /// registered). The model is applied independently per recipient.
    pub fn broadcast(&mut self, ctx: &mut Ctx, from: Addr, msg: Msg) {
        let targets: Vec<Addr> = self.routes.keys().copied().collect();
        for to in targets {
            self.send(ctx, from, to, msg.clone());
        }
    }

    /// The set of currently-routable node addresses.
    pub fn addrs(&self) -> impl Iterator<Item = &Addr> {
        self.routes.keys()
    }
}
