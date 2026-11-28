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

// Legacy engine (per-component event queues). Retired once the simulation
// framework below has been reviewed.
pub mod mock;
pub mod mock_swarm;
pub mod node;
#[cfg(feature = "raptorcast")]
pub mod raptorcast;
pub mod swarm;
pub mod swarm_relation;
pub mod terminator;
pub mod transformer;
mod utils;
pub mod verifier;

// Simulation framework: a Monad-node harness over the generic `monad-sim`
// discrete-event engine. See `sim` and the crate README / design doc.
pub mod sim;
pub mod sim_verify;
