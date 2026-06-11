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

//! Byte-stream transport between the node (host) and the signer (CVM).
//!
//! Production uses AF_VSOCK (Linux only); a unix socket lets the whole stack
//! run on a laptop with no confidential VM for development and tests.

use std::io::{self, Read, Write};

/// A bidirectional, blocking byte stream. Any `Read + Write + Send` type
/// qualifies (e.g. `VsockStream`, `UnixStream`).
pub trait Transport: Read + Write + Send {}
impl<T: Read + Write + Send> Transport for T {}

/// How to reach the signer. Cloneable so the client can reconnect / grow its
/// connection pool on demand.
#[derive(Debug, Clone)]
pub enum TransportConfig {
    /// AF_VSOCK to a confidential VM (Linux only).
    Vsock { cid: u32, port: u32 },
    /// Unix domain socket (development / tests; works on any unix host).
    Unix { path: std::path::PathBuf },
}

impl TransportConfig {
    /// Open a fresh connection.
    pub fn connect(&self) -> io::Result<Box<dyn Transport>> {
        match self {
            TransportConfig::Vsock { cid, port } => connect_vsock(*cid, *port),
            TransportConfig::Unix { path } => {
                #[cfg(unix)]
                {
                    let s = std::os::unix::net::UnixStream::connect(path)?;
                    Ok(Box::new(s))
                }
                #[cfg(not(unix))]
                {
                    let _ = path;
                    Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "unix sockets unavailable on this platform",
                    ))
                }
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn connect_vsock(cid: u32, port: u32) -> io::Result<Box<dyn Transport>> {
    let s = vsock::VsockStream::connect_with_cid_port(cid, port)?;
    Ok(Box::new(s))
}

#[cfg(not(target_os = "linux"))]
fn connect_vsock(_cid: u32, _port: u32) -> io::Result<Box<dyn Transport>> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "AF_VSOCK is only available on Linux; use TransportConfig::Unix for local development",
    ))
}
