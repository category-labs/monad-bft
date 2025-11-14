use std::{os::unix::io::RawFd, thread};

use io_uring::{cqueue::Entry, opcode, types, IoUring};
use tracing::{error, info, trace, warn};

use super::{
    backend::ReaderBackend,
    bufring::BufferRing,
    reader::{ReaderError, SocketConfig},
};
use crate::RecvUdpMsg;

pub fn spawn_with_ringbuf<F>(
    socket_configs: Vec<SocketConfig>,
    on_message: F,
    num_threads: usize,
    buf_count_per_socket: usize,
    buffer_size: Option<usize>,
) -> Vec<RawFd>
where
    F: Fn(usize, RecvUdpMsg) -> Result<(), ()> + Send + Sync + Clone + 'static,
{
    let (first_fds, allocations) =
        super::shared::prepare_sockets_and_allocate(socket_configs, num_threads, buffer_size);

    for allocation in allocations {
        let thread_id = allocation.thread_id;
        let num_sockets = allocation.socket_count();
        let on_message = on_message.clone();

        thread::Builder::new()
            .name(format!("dp-uring-ring-{}", thread_id))
            .spawn(move || {
                match super::shared::run_reader_loop::<RingbufBackend, F>(
                    thread_id,
                    allocation.sockets,
                    allocation.configs,
                    on_message,
                    buf_count_per_socket,
                ) {
                    Ok(()) => {
                        info!(thread_id, "ringbuf reader exited cleanly");
                    }
                    Err(e) => {
                        error!(thread_id, error = %e, "ringbuf reader exited with error");
                    }
                }
            })
            .expect("failed to spawn uring ringbuf reader thread");

        info!(
            thread_id,
            num_sockets, buf_count_per_socket, "spawned io_uring ringbuf reader thread"
        );
    }

    first_fds
}

struct RingbufBackend {
    bufring: BufferRing,
    ring_size: u32,
}

impl ReaderBackend for RingbufBackend {
    fn new(thread_id: usize, num_sockets: usize, batch_size: usize) -> Result<Self, ReaderError> {
        use io_uring::register::Probe;

        let total_buf_count = num_sockets * batch_size;
        let bufring = BufferRing::new(total_buf_count, thread_id)?;

        let ring_size = 4096;

        let ring = IoUring::new(ring_size).map_err(ReaderError::CreateRing)?;
        let mut probe = Probe::new();

        ring.submitter().register_probe(&mut probe).map_err(|e| {
            ReaderError::CreateRing(std::io::Error::other(format!(
                "failed to register probe: {}",
                e
            )))
        })?;

        if !probe.is_supported(opcode::RecvMulti::CODE) {
            return Err(ReaderError::CreateRing(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "RecvMulti not supported on this kernel (requires 6.0+)",
            )));
        }

        Ok(Self { bufring, ring_size })
    }

    fn ring_size(&self) -> u32 {
        self.ring_size
    }

    fn submit_initial_ops(
        &mut self,
        ring: &mut IoUring,
        socket_fds: &[RawFd],
    ) -> Result<(), ReaderError> {
        self.bufring.register(ring)?;

        for (socket_idx, &fd) in socket_fds.iter().enumerate() {
            let recv_e = opcode::RecvMulti::new(types::Fd(fd), self.bufring.buf_group_id)
                .build()
                .user_data(socket_idx as u64);

            unsafe {
                ring.submission().push(&recv_e).map_err(|_| {
                    ReaderError::CreateRing(std::io::Error::other("failed to push RecvMulti"))
                })?;
            }
        }
        Ok(())
    }

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
        F: Fn(usize, RecvUdpMsg) -> Result<(), ()>,
    {
        let result = cqe.result();
        let socket_idx = cqe.user_data() as usize;

        if result > 0 {
            let bid = io_uring::cqueue::buffer_select(cqe.flags()).unwrap();
            let data_len = result as usize;

            let payload = self.bufring.copy_data_from_buffer(bid, data_len);

            let msg = RecvUdpMsg {
                src_addr: "0.0.0.0:0".parse().unwrap(),
                payload: payload.freeze(),
                stride: data_len.max(1).try_into().unwrap(),
            };

            let socket_id = configs[socket_idx].socket_id;
            trace!(thread_id, socket_id, len = data_len, "received udp packet");

            on_message(socket_id, msg)?;

            self.bufring.return_buffer(bid);
        } else if result < 0 && result != -libc::ENOBUFS {
            warn!(
                thread_id,
                socket_id = configs.get(socket_idx).map(|c| c.socket_id),
                error = %std::io::Error::from_raw_os_error(-result),
                "recv error"
            );
        }

        if !io_uring::cqueue::more(cqe.flags()) {
            let recv_e = opcode::RecvMulti::new(
                types::Fd(socket_fds[socket_idx]),
                self.bufring.buf_group_id,
            )
            .build()
            .user_data(socket_idx as u64);

            unsafe {
                if ring.submission().push(&recv_e).is_err() {
                    warn!(thread_id, "failed to resubmit RecvMulti");
                }
            }
        }

        Ok(())
    }
}
