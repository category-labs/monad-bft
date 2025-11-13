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

pub(crate) mod metrics;
pub(crate) mod protocol;
pub(crate) mod session;

mod api;
mod context;
mod cookie;
mod error;
mod filter;
mod state;

pub use api::API;
pub use context::{Context, StdContext, TestContext};
pub use error::{Error, ProtocolErrorContext, Result};
pub use monad_secp::PubKey as PublicKey;
pub use protocol::{crypto, handshake, messages};
pub use session::{Config, DEFAULT_RETRY_ATTEMPTS, RETRY_ALWAYS};
