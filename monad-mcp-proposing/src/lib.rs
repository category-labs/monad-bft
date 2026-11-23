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

//! Proposal creation: when a node seals and submits its proposals. The
//! exports are under the env variants at crate::{stub,prod}::*, by the
//! module mechanism of monad-mcp-cadence (see its env/mod.rs).

#![allow(clippy::duplicate_mod)]

#[cfg(feature = "enable_stub")]
// override to top-level path
#[path = ""]
pub mod stub {
    mod env {
        pub(crate) use monad_mcp_cadence::stub as cadence;
        pub(crate) use monad_mcp_common::stub as common;
        pub(crate) use monad_mcp_epoch::stub as epoch;
    }

    #[path = "top_level.rs"]
    mod top_level;
    pub use top_level::*;
}

// override to top-level path
#[path = ""]
pub mod prod {
    mod env {
        pub(crate) use monad_mcp_cadence::prod as cadence;
        pub(crate) use monad_mcp_common::prod as common;
        pub(crate) use monad_mcp_epoch::prod as epoch;
    }

    #[path = "top_level.rs"]
    mod top_level;
    pub use top_level::*;
}
