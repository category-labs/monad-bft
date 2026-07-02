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

//! Generic simulation infrastructure that sits *between* the domain-agnostic
//! engine ([`monad_sim`], "Layer A") and a protocol-specific adapter ("Layer
//! B"). It carries no consensus or Monad knowledge — everything here is
//! parameterized only by a node address type `Addr` and a wire message type
//! `Msg`.
//!
//! Three pieces, all reusable across protocols:
//!
//! - [`Net`] — the network as a `monad_sim` *process*: a routing table plus a
//!   swappable [`NetworkModel`] (latency / loss / partition / duplication),
//!   built declaratively with [`Network`]. Crucially, `Net` addresses nodes
//!   through a **type-erased** delivery boundary ([`Deliver`] / [`deliver_to`]),
//!   so a single network can carry *heterogeneous* node process types as long as
//!   they share `Msg` — the seam for testing different protocol versions (or a
//!   Byzantine node) in one swarm.
//! - [`Swarm`] — a minimal builder/harness: spawn a set of [`SimClient`] nodes
//!   plus a `Net`, wire them together, and forward run-control to the engine.
//!   Deliberately homogeneous for now (one node type `N`); the `Net` boundary is
//!   already erased, so a future heterogeneous swarm reuses it unchanged.
//! - [`Timers`] — the re-arming timer set (one armed timeout per key) that every
//!   node needs for pacemaker-style timeouts.
//!
//! Protocol-specific inspection (finalized blocks, current round, agreement
//! assertions) deliberately does *not* live here; it belongs to each protocol's
//! adapter, layered over [`Swarm::with_node`].

mod net;
mod swarm;
mod timers;

pub use net::{Conditions, Deliver, Link, Net, Network, NetworkModel};
pub use swarm::{deliver_to, SimClient, Swarm};
pub use timers::Timers;
