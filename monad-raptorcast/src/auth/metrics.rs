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

monad_executor::metric_consts! {
    pub GAUGE_RAPTORCAST_AUTH_AUTHENTICATED_UDP_BYTES_WRITTEN {
        name: "monad.raptorcast.auth.authenticated_udp_bytes_written",
        help: "Bytes written via authenticated UDP",
    }
    pub GAUGE_RAPTORCAST_AUTH_NON_AUTHENTICATED_UDP_BYTES_WRITTEN {
        name: "monad.raptorcast.auth.non_authenticated_udp_bytes_written",
        help: "Bytes written via non-authenticated UDP",
    }
    pub GAUGE_RAPTORCAST_AUTH_AUTHENTICATED_UDP_BYTES_READ {
        name: "monad.raptorcast.auth.authenticated_udp_bytes_read",
        help: "Bytes read via authenticated UDP",
    }
    pub GAUGE_RAPTORCAST_AUTH_NON_AUTHENTICATED_UDP_BYTES_READ {
        name: "monad.raptorcast.auth.non_authenticated_udp_bytes_read",
        help: "Bytes read via non-authenticated UDP",
    }
}

monad_wireauth::define_metric_names!(UDP_METRICS, "udp");
monad_wireauth::define_metric_names!(DIRECT_UDP_METRICS, "direct_udp");
