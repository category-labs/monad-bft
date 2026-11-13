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

//! The network as per-node simulation processes, generic over the node address
//! `Addr` and the wire message `Msg`. Each node has its own [`Net`], its
//! network stack. A node sends by scheduling a step on its `Net`; the stack
//! samples the [`NetworkModel`] and schedules the surviving deliveries against
//! the recipients.
//!
//! All stacks share one read-only description of the network: the routing
//! table and the model. What changes as messages are sent (a sender's uplink
//! queue, its random generator) is per stack. Stacks therefore run in parallel
//! when their nodes' domains do, and the model's lower bound on link delays
//! ([`NetworkModel::min_delay`]) is the lookahead between them.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    ops::Range,
    sync::Arc,
    time::Duration,
};

use monad_sim::{CancelToken, Ctx, Handle, StepLabel, Time};
use rand::{distributions::Distribution, Rng, SeedableRng};
use rand_chacha::ChaChaRng;

/// A type-erased "deliver this message to one node at this time" operation.
///
/// Registered per address (see [`Wiring::register`] / [`crate::deliver_to`]),
/// it captures the recipient's *concrete* `Handle` and monomorphizes the
/// `ctx.schedule` call, so the network itself only ever names `Addr` and `Msg`.
/// This is what lets one network carry heterogeneous node process types.
pub type Deliver<Addr, Msg> = Arc<dyn Fn(&mut Ctx, Time, Addr, Msg) + Send + Sync>;

/// Per-message context handed to a [`SenderModel`].
pub struct Link<Addr> {
    pub now: Time,
    pub from: Addr,
    pub to: Addr,
}

/// A network model: a description shared by every node's stack. It creates the
/// part of the model that runs in each sender's stack, and declares a lower
/// bound on the delay of each link.
pub trait NetworkModel<Addr, Msg>: Send + Sync {
    /// The part of the model that runs in `from`'s stack. Everything that
    /// changes as messages are sent lives there; the model itself is immutable.
    fn sender(&self, from: &Addr) -> Box<dyn SenderModel<Addr, Msg>>;

    /// A lower bound on the delay of every message `from` -> `to`
    /// (`from != to`). A parallel swarm derives the lookahead between its
    /// domains from it. The default, 0, is always correct but keeps every node
    /// in one domain.
    fn min_delay(&self, _from: &Addr, _to: &Addr) -> Duration {
        Duration::ZERO
    }
}

/// Decides the fate of a single message from one sender: the delays at which
/// copies are delivered to `link.to`. An empty result drops the message; more
/// than one duplicates it. Reordering needs no special handling — a smaller
/// delay simply arrives first.
///
/// A sender may also hold messages back on its uplink, for example to send
/// them by priority: [`submit`](Self::submit) keeps a message, and the stack
/// asks for it again with [`release`](Self::release) from
/// [`wake_at`](Self::wake_at) on. A message's fate ([`deliveries`]) is
/// decided when it goes on the wire, with the link's `now` at that time. The
/// defaults hold nothing back.
///
/// [`deliveries`]: Self::deliveries
pub trait SenderModel<Addr, Msg>: Send {
    fn deliveries(
        &mut self,
        link: &Link<Addr>,
        message: &Msg,
        rng: &mut ChaChaRng,
    ) -> Vec<Duration>;

    /// The node sends `message` on `link`. `Some`: it goes on the wire now.
    /// `None`: the sender keeps it, to hand it back from `release`.
    fn submit(&mut self, _link: &Link<Addr>, message: Msg) -> Option<Msg> {
        Some(message)
    }

    /// From when a kept message may go on the wire; `None` while none is
    /// kept. A time already past means now.
    fn wake_at(&self) -> Option<Time> {
        None
    }

    /// The next kept message to put on the wire at `now`, with its
    /// receiver, if the uplink takes one then. Called until `None`.
    fn release(&mut self, _now: Time) -> Option<(Addr, Msg)> {
        None
    }
}

type LatencyFn<Addr> = Box<dyn Fn(&Link<Addr>, &mut ChaChaRng) -> Duration + Send + Sync>;
type MinLatencyFn<Addr> = Box<dyn Fn(&Addr, &Addr) -> Duration + Send + Sync>;
type DropFn<Addr, Msg> = Box<dyn Fn(&Link<Addr>, &Msg) -> bool + Send + Sync>;
type SizeFn<Msg> = Box<dyn Fn(&Msg) -> usize + Send + Sync>;
type ClassFn<Msg> = Box<dyn Fn(&Msg) -> u8 + Send + Sync>;
type Partition<Addr> = (Range<Time>, Vec<BTreeSet<Addr>>);

/// One-way base latencies between *regions*, plus a placement of addresses
/// into regions. A topology with one region per node is a full node-to-node
/// matrix; a geographic topology places many nodes per region.
///
/// Rows are senders and columns receivers, so asymmetric paths are allowed.
pub struct LatencyMatrix<Addr> {
    one_way: Vec<Vec<Duration>>,
    region: BTreeMap<Addr, usize>,
}

impl<Addr: Ord> LatencyMatrix<Addr> {
    /// Panics unless `one_way` is square.
    pub fn new(one_way: Vec<Vec<Duration>>) -> Self {
        let n = one_way.len();
        assert!(
            one_way.iter().all(|row| row.len() == n),
            "latency matrix must be square"
        );
        Self {
            one_way,
            region: BTreeMap::new(),
        }
    }

    /// Place `addr` in `region` (a row/column index). Panics if out of range.
    pub fn place(mut self, addr: Addr, region: usize) -> Self {
        assert!(region < self.one_way.len(), "region {region} out of range");
        self.region.insert(addr, region);
        self
    }

    /// The base one-way latency `from` -> `to`. Panics if either address was
    /// never placed: a missing placement is a configuration error, not a
    /// network condition.
    pub fn latency(&self, from: &Addr, to: &Addr) -> Duration {
        let row = self.region(from);
        let column = self.region(to);
        self.one_way[row][column]
    }

    fn region(&self, addr: &Addr) -> usize {
        *self
            .region
            .get(addr)
            .expect("address not placed in the latency matrix")
    }
}

/// A serial uplink per sender, configured once for the whole network. Messages
/// leave one after another in the order they are sent, each occupying the link
/// for its size divided by the rate; a message sent while the link is busy
/// waits its turn.
///
/// Sequencing is per message, which models a transport whose messages each
/// fit one datagram. Anything finer (frames of a larger message interleaving
/// with others) is out of scope.
struct Egress<Addr, Msg> {
    bits_per_sec: u64,
    rates: BTreeMap<Addr, u64>,
    size_of: SizeFn<Msg>,
    priority: Option<Priority<Msg>>,
}

/// Strict priority classes on the uplink (see [`Network::priority`]).
struct Priority<Msg> {
    class_of: ClassFn<Msg>,
    depth: usize,
}

/// A declarative network model: a base latency plus optional, independent
/// conditions. Anything left unconfigured behaves reliably.
pub struct Conditions<Addr, Msg> {
    latency: LatencyFn<Addr>,
    /// A lower bound on `latency` per link; every sample is checked against it.
    min_latency: MinLatencyFn<Addr>,
    loss: f64,
    partitions: Vec<Partition<Addr>>,
    drop_if: Option<DropFn<Addr, Msg>>,
    egress: Option<Egress<Addr, Msg>>,
}

/// [`Conditions`] shared by every stack.
struct SharedConditions<Addr, Msg>(Arc<Conditions<Addr, Msg>>);

impl<Addr, Msg> NetworkModel<Addr, Msg> for SharedConditions<Addr, Msg>
where
    Addr: Ord + Clone + Send + Sync + 'static,
    Msg: Send + 'static,
{
    fn sender(&self, from: &Addr) -> Box<dyn SenderModel<Addr, Msg>> {
        let rate = self.0.egress.as_ref().map(|egress| {
            egress
                .rates
                .get(from)
                .copied()
                .unwrap_or(egress.bits_per_sec)
        });
        Box::new(ConditionsSender {
            conditions: self.0.clone(),
            rate,
            busy_until: Time(0),
            ring: VecDeque::new(),
            waiting: BTreeMap::new(),
        })
    }

    // egress only adds delay, and loss, partitions and `drop_if` only remove
    // messages, so the latency bound is the link's bound
    fn min_delay(&self, from: &Addr, to: &Addr) -> Duration {
        (self.0.min_latency)(from, to)
    }
}

/// One sender's part of [`Conditions`]: its uplink.
struct ConditionsSender<Addr, Msg> {
    conditions: Arc<Conditions<Addr, Msg>>,
    /// The uplink rate, if egress is configured.
    rate: Option<u64>,
    busy_until: Time,
    /// With priority classes: when each message on the wire finishes
    /// leaving, in order (the transmit ring), and the messages waiting for
    /// room in it, per class.
    ring: VecDeque<Time>,
    waiting: BTreeMap<u8, VecDeque<(Addr, Msg)>>,
}

impl<Addr, Msg> ConditionsSender<Addr, Msg> {
    fn priority(&self) -> Option<&Priority<Msg>> {
        self.conditions.egress.as_ref()?.priority.as_ref()
    }

    /// Messages still leaving the uplink at `now`.
    fn on_the_wire(&mut self, now: Time) -> usize {
        while self.ring.front().is_some_and(|done| *done <= now) {
            self.ring.pop_front();
        }
        self.ring.len()
    }

    fn any_waiting(&self) -> bool {
        self.waiting.values().any(|queue| !queue.is_empty())
    }
}

/// The delay from `now` until `message` has fully left an uplink of `rate`
/// that is busy until `busy_until`, which it then extends.
fn transmit<Msg>(
    busy_until: &mut Time,
    rate: u64,
    size_of: &SizeFn<Msg>,
    now: Time,
    message: &Msg,
) -> Duration {
    let bits = size_of(message) as u128 * 8;
    let nanos = (bits * 1_000_000_000).div_ceil(u128::from(rate));
    let start = (*busy_until).max(now);
    let done = start + Duration::from_nanos(u64::try_from(nanos).expect("transmit time"));
    *busy_until = done;
    done.abs_diff(now)
}

impl<Addr, Msg> SenderModel<Addr, Msg> for ConditionsSender<Addr, Msg>
where
    Addr: Ord + Clone + Send + Sync,
    Msg: Send + 'static,
{
    fn deliveries(
        &mut self,
        link: &Link<Addr>,
        message: &Msg,
        rng: &mut ChaChaRng,
    ) -> Vec<Duration> {
        let conditions = &*self.conditions;
        // a message dropped in the network still used the sender's uplink;
        // a node's messages to itself never touch it
        let transmit = match (self.rate, &conditions.egress) {
            (Some(rate), Some(egress)) if link.from != link.to => {
                let transmit = transmit(
                    &mut self.busy_until,
                    rate,
                    &egress.size_of,
                    link.now,
                    message,
                );
                if egress.priority.is_some() {
                    self.ring.push_back(self.busy_until);
                }
                transmit
            }
            _ => Duration::ZERO,
        };
        if let Some(drop_if) = &conditions.drop_if {
            if drop_if(link, message) {
                return Vec::new();
            }
        }
        let split = conditions.partitions.iter().any(|(window, groups)| {
            window.contains(&link.now)
                && groups
                    .iter()
                    .any(|group| group.contains(&link.from) != group.contains(&link.to))
        });
        if split {
            return Vec::new();
        }
        if conditions.loss > 0.0 && rng.gen::<f64>() < conditions.loss {
            return Vec::new();
        }
        let latency = (conditions.latency)(link, rng);
        if link.from != link.to {
            let bound = (conditions.min_latency)(&link.from, &link.to);
            assert!(
                latency >= bound,
                "sampled latency {latency:?} below the declared minimum {bound:?}"
            );
        }
        vec![transmit + latency]
    }

    // Class 0 goes on the wire at once, behind what is on it already:
    // nothing can overtake it. Another class goes on only while fewer than
    // `depth` messages are on the wire and no message of its class or a more
    // urgent one waits; otherwise it waits for room.
    fn submit(&mut self, link: &Link<Addr>, message: Msg) -> Option<Msg> {
        let Some(priority) = self.priority() else {
            return Some(message);
        };
        let class = (priority.class_of)(&message);
        let depth = priority.depth;
        if class == 0 || link.from == link.to {
            return Some(message);
        }
        let queued = self
            .waiting
            .range(..=class)
            .any(|(_, queue)| !queue.is_empty());
        if !queued && self.on_the_wire(link.now) < depth {
            return Some(message);
        }
        let queue = self.waiting.entry(class).or_default();
        queue.push_back((link.to.clone(), message));
        None
    }

    // the ring has room once all but `depth − 1` of its messages have left
    fn wake_at(&self) -> Option<Time> {
        let depth = self.priority()?.depth;
        if !self.any_waiting() {
            return None;
        }
        let full = self.ring.len().checked_sub(depth);
        Some(full.map_or(Time(0), |index| self.ring[index]))
    }

    // the most urgent waiting message, oldest first, while the ring has room
    fn release(&mut self, now: Time) -> Option<(Addr, Msg)> {
        let depth = self.priority()?.depth;
        if self.on_the_wire(now) >= depth {
            return None;
        }
        self.waiting.values_mut().find_map(VecDeque::pop_front)
    }
}

/// Declarative builder for a swarm's network behaviour. Start from
/// [`reliable`](Network::reliable) or [`with_latency`](Network::with_latency)
/// and layer conditions, or supply a [`custom`](Network::custom) model.
pub enum Network<Addr, Msg> {
    Conditions(Conditions<Addr, Msg>),
    Custom(Arc<dyn NetworkModel<Addr, Msg>>),
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
            latency: Box::new(move |_, _| latency),
            min_latency: Box::new(move |_, _| latency),
            loss: 0.0,
            partitions: Vec::new(),
            drop_if: None,
            egress: None,
        })
    }

    /// Latency sampled per message from `distribution`. Its lower bound is
    /// unknown (0) unless declared with [`min_latency`](Network::min_latency).
    pub fn with_latency(distribution: impl Distribution<Duration> + Send + Sync + 'static) -> Self {
        Network::with_link_latency(move |_, rng| distribution.sample(rng))
    }

    /// Latency chosen per message from the link (sender, receiver, send time)
    /// and the sender's RNG. Its lower bound is unknown (0) unless declared with
    /// [`min_latency`](Network::min_latency).
    pub fn with_link_latency(
        latency: impl Fn(&Link<Addr>, &mut ChaChaRng) -> Duration + Send + Sync + 'static,
    ) -> Self {
        let mut network = Network::reliable(Duration::ZERO);
        network.conditions().latency = Box::new(latency);
        network
    }

    /// Base latency from `matrix` for each sender/receiver pair, plus
    /// `jitter` sampled per message. The base latency is the link's lower
    /// bound.
    pub fn with_matrix(
        matrix: LatencyMatrix<Addr>,
        jitter: impl Distribution<Duration> + Send + Sync + 'static,
    ) -> Self
    where
        Addr: Ord + Send + Sync + 'static,
    {
        let matrix = Arc::new(matrix);
        let bound = matrix.clone();
        let mut network = Network::with_link_latency(move |link, rng| {
            matrix.latency(&link.from, &link.to) + jitter.sample(rng)
        });
        network.conditions().min_latency = Box::new(move |from, to| bound.latency(from, to));
        network
    }

    /// Declare a lower bound on every link's latency, for a sampled latency
    /// whose bound the network cannot know. Every sample is checked against it.
    /// A parallel swarm uses it as the lookahead between nodes.
    pub fn min_latency(mut self, bound: Duration) -> Self {
        self.conditions().min_latency = Box::new(move |_, _| bound);
        self
    }

    /// Declare a lower bound per link (sender, receiver), for a link latency
    /// whose bound differs between links, such as a latency matrix plus a
    /// time-varying delay. Every sample is checked against it. A parallel
    /// swarm uses it as the lookahead between nodes.
    pub fn link_min_latency(
        mut self,
        bound: impl Fn(&Addr, &Addr) -> Duration + Send + Sync + 'static,
    ) -> Self {
        self.conditions().min_latency = Box::new(bound);
        self
    }

    /// Give every sender a serial uplink of `bits_per_sec` (see
    /// [`egress_rate`](Network::egress_rate) for per-sender rates). A message
    /// occupies it for `size_of(message)` bytes and waits for the messages
    /// the same sender sent before it; its delivery delay is that queueing
    /// and transmission time plus the link latency.
    pub fn egress(
        mut self,
        bits_per_sec: u64,
        size_of: impl Fn(&Msg) -> usize + Send + Sync + 'static,
    ) -> Self {
        assert!(bits_per_sec > 0, "egress rate must be positive");
        self.conditions().egress = Some(Egress {
            bits_per_sec,
            rates: BTreeMap::new(),
            size_of: Box::new(size_of),
            priority: None,
        });
        self
    }

    /// Strict priority classes on every sender's uplink, as a transmit ring
    /// of `depth` messages fed by one queue per class. `class_of` gives a
    /// message's class, 0 the most urgent.
    ///
    /// - Class 0 goes on the wire at once, behind the messages already on
    ///   it, in the order sent.
    /// - Any other class goes on only while fewer than `depth` messages are
    ///   on the wire, after every waiting message of a more urgent class and
    ///   the older ones of its own; until then it waits.
    ///
    /// So a class-0 message waits behind at most `depth` messages of other
    /// classes. A waiting message's fate (loss, latency, ...) is decided
    /// when it goes on the wire. Without `priority`, the uplink is one queue
    /// in the order sent. Panics unless [`egress`](Network::egress) is
    /// configured, or if `depth` is 0.
    pub fn priority(
        mut self,
        class_of: impl Fn(&Msg) -> u8 + Send + Sync + 'static,
        depth: usize,
    ) -> Self {
        assert!(depth > 0, "the transmit ring holds at least one message");
        self.conditions()
            .egress
            .as_mut()
            .expect("configure egress before priority classes")
            .priority = Some(Priority {
            class_of: Box::new(class_of),
            depth,
        });
        self
    }

    /// Override the uplink rate of one sender. Panics unless
    /// [`egress`](Network::egress) is configured.
    pub fn egress_rate(mut self, addr: Addr, bits_per_sec: u64) -> Self
    where
        Addr: Ord,
    {
        assert!(bits_per_sec > 0, "egress rate must be positive");
        self.conditions()
            .egress
            .as_mut()
            .expect("configure egress before per-sender rates")
            .rates
            .insert(addr, bits_per_sec);
        self
    }

    /// A fully custom model.
    pub fn custom(model: impl NetworkModel<Addr, Msg> + 'static) -> Self {
        Network::Custom(Arc::new(model))
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
    pub fn drop_if(
        mut self,
        predicate: impl Fn(&Link<Addr>, &Msg) -> bool + Send + Sync + 'static,
    ) -> Self {
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

    fn into_model(self) -> Arc<dyn NetworkModel<Addr, Msg>>
    where
        Addr: Ord + Clone + Send + Sync + 'static,
        Msg: Send + 'static,
    {
        match self {
            Network::Conditions(conditions) => Arc::new(SharedConditions(Arc::new(conditions))),
            Network::Custom(model) => model,
        }
    }
}

/// What every stack shares: the routing table and the model.
struct Shared<Addr, Msg> {
    routes: BTreeMap<Addr, Deliver<Addr, Msg>>,
    model: Arc<dyn NetworkModel<Addr, Msg>>,
}

/// A node's network stack: a process in the node's domain that sends the
/// node's messages. It shares the routing table and the model with every other
/// stack, and owns the sender's part of the model and its own random generator,
/// seeded from the network seed and a stream per address.
pub struct Net<Addr, Msg> {
    addr: Addr,
    shared: Arc<Shared<Addr, Msg>>,
    sender: Box<dyn SenderModel<Addr, Msg>>,
    rng: ChaChaRng,
    /// This stack's own handle, for its wake-up steps
    me: Option<Handle<Self>>,
    /// The armed wake-up step, while the sender holds messages back
    wake: Option<(Time, CancelToken)>,
}

impl<Addr, Msg> Net<Addr, Msg>
where
    Addr: Ord + Copy + Send + Sync + 'static,
    Msg: Clone + Send + 'static,
{
    /// The address this stack sends from.
    pub fn addr(&self) -> Addr {
        self.addr
    }

    /// The stack's own handle, which it needs to wake itself when its sender
    /// holds messages back (priority classes). [`crate::Swarm`] sets it; a
    /// stack wired by hand sets it after spawning.
    pub fn set_handle(&mut self, me: Handle<Self>) {
        self.me = Some(me);
    }

    /// Send one message to `to`: apply the model and schedule the surviving
    /// deliveries. A recipient not in the routing table drops it. The sender
    /// may hold the message back on its uplink; it then goes on the wire,
    /// and its fate is decided, when the uplink takes it.
    pub fn send(&mut self, ctx: &mut Ctx, to: Addr, msg: Msg) {
        if !self.shared.routes.contains_key(&to) {
            return;
        }
        let link = Link {
            now: ctx.now(),
            from: self.addr,
            to,
        };
        if let Some(msg) = self.sender.submit(&link, msg) {
            self.wire(ctx, to, msg);
        }
        self.arm(ctx);
    }

    // put a message on the wire now: apply the model, schedule the deliveries
    fn wire(&mut self, ctx: &mut Ctx, to: Addr, msg: Msg) {
        let Some(deliver) = self.shared.routes.get(&to).cloned() else {
            return;
        };
        let link = Link {
            now: ctx.now(),
            from: self.addr,
            to,
        };
        let delays = self.sender.deliveries(&link, &msg, &mut self.rng);
        match delays.as_slice() {
            [] => {}
            [delay] => deliver(ctx, ctx.now() + *delay, self.addr, msg),
            _ => {
                for delay in delays {
                    deliver(ctx, ctx.now() + delay, self.addr, msg.clone());
                }
            }
        }
    }

    /// Send `msg` to every registered node (including this one, if registered).
    /// The model is applied independently per recipient.
    pub fn broadcast(&mut self, ctx: &mut Ctx, msg: Msg) {
        let shared = self.shared.clone();
        for &to in shared.routes.keys() {
            self.send(ctx, to, msg.clone());
        }
    }

    // one wake-up step, at the sender's wake_at, while it holds messages back
    fn arm(&mut self, ctx: &mut Ctx) {
        let at = self.sender.wake_at().map(|at| at.max(ctx.now()));
        if self.wake.as_ref().map(|(armed, _)| *armed) == at {
            return;
        }
        if let Some((_, token)) = self.wake.take() {
            token.cancel();
        }
        if let Some(at) = at {
            let me = self
                .me
                .expect("a stack whose sender holds messages back needs Net::set_handle");
            let label = StepLabel::source("egress");
            let token = ctx.schedule(me, at, label, |net: &mut Self, ctx| net.wake(ctx));
            self.wake = Some((at, token));
        }
    }

    fn wake(&mut self, ctx: &mut Ctx) {
        self.wake = None;
        while let Some((to, msg)) = self.sender.release(ctx.now()) {
            self.wire(ctx, to, msg);
        }
        self.arm(ctx);
    }

    /// The set of currently-routable node addresses.
    pub fn addrs(&self) -> impl Iterator<Item = &Addr> {
        self.shared.routes.keys()
    }
}

/// The network under construction or reconfiguration: the model, the routing
/// table, and a random stream per address. Register every node's delivery, then
/// spawn each node's stack from [`stack`](Self::stack); after a change
/// (registration, a new model), [`refresh`](Self::refresh) each live stack.
///
/// [`crate::Swarm`] does all of this; use `Wiring` directly to build a swarm by
/// hand (e.g. with heterogeneous node types).
pub struct Wiring<Addr, Msg> {
    seed: u64,
    shared: Arc<Shared<Addr, Msg>>,
    /// Each address's RNG stream, in order of first registration.
    streams: BTreeMap<Addr, u64>,
}

impl<Addr, Msg> Wiring<Addr, Msg>
where
    Addr: Ord + Copy + Send + Sync + 'static,
    Msg: Clone + Send + 'static,
{
    /// A network with the given model; stack generators derive from `seed`.
    pub fn new(seed: u64, network: Network<Addr, Msg>) -> Self {
        Self {
            seed,
            shared: Arc::new(Shared {
                routes: BTreeMap::new(),
                model: network.into_model(),
            }),
            streams: BTreeMap::new(),
        }
    }

    /// Register a node's delivery entry point (see [`crate::deliver_to`]).
    pub fn register(&mut self, addr: Addr, deliver: Deliver<Addr, Msg>) {
        self.stream(addr);
        let mut routes = self.shared.routes.clone();
        routes.insert(addr, deliver);
        self.set_routes(routes);
    }

    /// Drop a node from the routing table (e.g. a restart/removal); messages to
    /// it are silently discarded thereafter.
    pub fn deregister(&mut self, addr: &Addr) {
        let mut routes = self.shared.routes.clone();
        routes.remove(addr);
        self.set_routes(routes);
    }

    /// Replace the network model (mid-run reconfiguration). Refreshed stacks
    /// start the new model's sender parts fresh.
    pub fn set_network(&mut self, network: Network<Addr, Msg>) {
        self.shared = Arc::new(Shared {
            routes: self.shared.routes.clone(),
            model: network.into_model(),
        });
    }

    /// The stack for `addr`, sharing the current routes and model.
    pub fn stack(&mut self, addr: Addr) -> Net<Addr, Msg> {
        let mut rng = ChaChaRng::seed_from_u64(self.seed);
        rng.set_stream(self.stream(addr));
        Net {
            addr,
            sender: self.shared.model.sender(&addr),
            shared: self.shared.clone(),
            rng,
            me: None,
            wake: None,
        }
    }

    /// Bring a live stack up to date: the current routes, and with a new model
    /// a fresh sender part. Messages the old sender part held back on its
    /// uplink are dropped with it.
    pub fn refresh(&self, net: &mut Net<Addr, Msg>) {
        if !Arc::ptr_eq(&net.shared.model, &self.shared.model) {
            net.sender = self.shared.model.sender(&net.addr);
        }
        net.shared = self.shared.clone();
    }

    /// The model's lower bound on the delay `from` -> `to`.
    pub fn min_delay(&self, from: &Addr, to: &Addr) -> Duration {
        self.shared.model.min_delay(from, to)
    }

    fn stream(&mut self, addr: Addr) -> u64 {
        let next = self.streams.len() as u64;
        *self.streams.entry(addr).or_insert(next)
    }

    fn set_routes(&mut self, routes: BTreeMap<Addr, Deliver<Addr, Msg>>) {
        self.shared = Arc::new(Shared {
            routes,
            model: self.shared.model.clone(),
        });
    }
}
