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

use std::net::{TcpListener, UdpSocket};

/// Find a free UDP port by binding to an ephemeral port and returning it.
pub fn find_udp_free_port() -> u16 {
    let socket = UdpSocket::bind("127.0.0.1:0").expect("failed to bind");
    socket.local_addr().expect("failed to get addr").port()
}

/// Find a free TCP port by binding to an ephemeral port and returning it.
#[allow(dead_code)]
pub fn find_tcp_free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("failed to bind");
    listener.local_addr().expect("failed to get addr").port()
}
