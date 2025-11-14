use std::os::unix::io::RawFd;

use io_uring::{cqueue::Entry, IoUring};

use super::reader::{ReaderError, SocketConfig};
use crate::RecvUdpMsg;

pub(super) trait ReaderBackend {
    fn new(thread_id: usize, num_sockets: usize, batch_size: usize) -> Result<Self, ReaderError>
    where
        Self: Sized;

    fn ring_size(&self) -> u32;

    fn submit_initial_ops(
        &mut self,
        ring: &mut IoUring,
        socket_fds: &[RawFd],
    ) -> Result<(), ReaderError>;

    fn handle_completion<F>(
        &mut self,
        thread_id: usize,
        ring: &mut IoUring,
        configs: &[SocketConfig],
        socket_fds: &[RawFd],
        cqe: &Entry,
        on_message: &F,
    ) -> Result<(), ReaderError>
    where
        F: Fn(usize, RecvUdpMsg) -> Result<(), ()>;
}
