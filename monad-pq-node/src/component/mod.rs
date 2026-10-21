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

//! The pq components: the cadence over Quantum Chorus and the pq DA. The
//! rest (proposing, repeater, mempool) are monad-mcp-node's own.

mod cadence;
mod da;
mod spawn;

pub use monad_mcp_node::component::{
    Component, Dispatch, Link, Mempool, MempoolSource, ProposalSource, Proposing, ProposingInput,
    ProposingOutput, RandomSource, Recipients, Repeater, RepeaterConfig, RepeaterInput,
    RepeaterOutput, SharedMempool,
};

pub use self::{
    cadence::{Conductor, PqCadence, PqCadenceInput, PqCadenceOutput, PqCadenceWireMsg},
    da::{PqDA, PqDAInput},
    spawn::spawn,
};
