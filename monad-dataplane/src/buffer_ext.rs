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

use std::{
    io,
    os::fd::{AsRawFd, FromRawFd, IntoRawFd},
};

pub(crate) trait SocketBufferExt {
    fn set_recv_buffer_size(&self, size: usize) -> io::Result<()>;
    fn recv_buffer_size(&self) -> io::Result<usize>;

    fn set_send_buffer_size(&self, size: usize) -> io::Result<()>;
    fn send_buffer_size(&self) -> io::Result<usize>;
}

impl<T: AsRawFd> SocketBufferExt for T {
    fn set_recv_buffer_size(&self, size: usize) -> io::Result<()> {
        let sock = unsafe { socket2::Socket::from_raw_fd(self.as_raw_fd()) };
        let rst = sock.set_recv_buffer_size(size);
        _ = sock.into_raw_fd(); // defuse drop
        rst
    }

    fn recv_buffer_size(&self) -> io::Result<usize> {
        let sock = unsafe { socket2::Socket::from_raw_fd(self.as_raw_fd()) };
        let rst = sock.recv_buffer_size();
        _ = sock.into_raw_fd(); // defuse drop
        rst
    }

    fn set_send_buffer_size(&self, size: usize) -> io::Result<()> {
        let sock = unsafe { socket2::Socket::from_raw_fd(self.as_raw_fd()) };
        let rst = sock.set_send_buffer_size(size);
        _ = sock.into_raw_fd(); // defuse drop
        rst
    }

    fn send_buffer_size(&self) -> io::Result<usize> {
        let sock = unsafe { socket2::Socket::from_raw_fd(self.as_raw_fd()) };
        let rst = sock.send_buffer_size();
        _ = sock.into_raw_fd(); // defuse drop
        rst
    }
}
