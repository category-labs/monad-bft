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
