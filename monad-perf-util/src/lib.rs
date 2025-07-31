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
    env::VarError,
    fs::File,
    io::{Read, Write},
    os::fd::FromRawFd,
    str::FromStr,
};

pub struct PerfController {
    control: File,
    control_ack: File,
}

impl PerfController {
    pub fn from_env() -> Result<Self, VarError> {
        let ctl_fd = std::env::var("PERF_CTL_FD")?;
        let ctl_fd_ack = std::env::var("PERF_CTL_FD_ACK")?;

        let control = unsafe { File::from_raw_fd(std::ffi::c_int::from_str(&ctl_fd).unwrap()) };
        let control_ack =
            unsafe { File::from_raw_fd(std::ffi::c_int::from_str(&ctl_fd_ack).unwrap()) };

        Ok(Self {
            control,
            control_ack,
        })
    }

    /// Enables sampling by writing to the control file descriptor and blocks waiting for ack.
    pub fn enable(&mut self) {
        self.control.write_all(b"enable\n").unwrap();
        let mut buf = [0; 5];
        self.control_ack.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, &[b'a', b'c', b'k', b'\n', 0u8]);
    }

    /// Disables sampling by writing to the control file descriptor and blocks waiting for ack.
    pub fn disable(&mut self) {
        self.control.write_all(b"disable\n").unwrap();
        let mut buf = [0; 5];
        self.control_ack.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, &[b'a', b'c', b'k', b'\n', 0u8]);
    }
}
