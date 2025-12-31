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

pub const GAUGE_RAPTORCAST_AUTH_AUTHENTICATED_UDP_BYTES_WRITTEN: &str =
    "monad.raptorcast.auth.authenticated_udp_bytes_written";
pub const GAUGE_RAPTORCAST_AUTH_NON_AUTHENTICATED_UDP_BYTES_WRITTEN: &str =
    "monad.raptorcast.auth.non_authenticated_udp_bytes_written";
pub const GAUGE_RAPTORCAST_AUTH_AUTHENTICATED_UDP_BYTES_READ: &str =
    "monad.raptorcast.auth.authenticated_udp_bytes_read";
pub const GAUGE_RAPTORCAST_AUTH_NON_AUTHENTICATED_UDP_BYTES_READ: &str =
    "monad.raptorcast.auth.non_authenticated_udp_bytes_read";

pub struct UdpMetrics;

monad_wireauth::impl_metric_names!(UdpMetrics, "udp");

pub const GAUGE_RAPTORCAST_AUTH_WIREAUTH_TCP_BYTES_WRITTEN: &str =
    "monad.raptorcast.auth.wireauth_tcp_bytes_written";
pub const GAUGE_RAPTORCAST_AUTH_SIGAUTH_TCP_BYTES_WRITTEN: &str =
    "monad.raptorcast.auth.sigauth_tcp_bytes_written";
pub const GAUGE_RAPTORCAST_AUTH_WIREAUTH_TCP_BYTES_READ: &str =
    "monad.raptorcast.auth.wireauth_tcp_bytes_read";
pub const GAUGE_RAPTORCAST_AUTH_SIGAUTH_TCP_BYTES_READ: &str =
    "monad.raptorcast.auth.sigauth_tcp_bytes_read";

pub struct TcpMetrics;

monad_wireauth::impl_metric_names!(TcpMetrics, "tcp");
