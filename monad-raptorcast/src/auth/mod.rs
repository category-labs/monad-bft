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

pub mod metrics;
pub mod protocol;
pub mod socket;

pub use metrics::{
    GAUGE_RAPTORCAST_AUTH_AUTHENTICATED_UDP_BYTES_READ,
    GAUGE_RAPTORCAST_AUTH_AUTHENTICATED_UDP_BYTES_WRITTEN,
    GAUGE_RAPTORCAST_AUTH_NON_AUTHENTICATED_UDP_BYTES_READ,
    GAUGE_RAPTORCAST_AUTH_NON_AUTHENTICATED_UDP_BYTES_WRITTEN,
};
pub use protocol::{AuthenticationProtocol, NoopAuthProtocol, NoopHeader, WireAuthProtocol};
pub use socket::{AuthRecvMsg, AuthenticatedSocketHandle, DualSocketHandle};
