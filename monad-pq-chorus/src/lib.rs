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

//! Quantum Chorus: certificate-free slot consensus with the Coda fallback,
//! running inside the unchanged `monad-mcp-chorus` Cadence runtime.

pub mod coda;
pub mod config;
pub mod da;
pub mod finalization;
pub mod message;
pub mod quorum;
mod rlp;
pub mod sig;
pub mod slot;
pub mod types;

pub use config::{QuantumConfig, QuantumContext};
pub use finalization::{PqEvent, PqHalt, PqPath, PqProof};
pub use message::PqMessage;
pub use slot::{PqTimer, QuantumChorus, evidence::Evidence};
