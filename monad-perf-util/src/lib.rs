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
