use std::{
    net::{SocketAddr, UdpSocket},
    os::unix::io::{AsRawFd, RawFd},
};

use tracing::info;

use super::reader::SocketConfig;

pub(super) fn create_reuseport_socket(addr: SocketAddr) -> UdpSocket {
    let socket = socket2::Socket::new(
        if addr.is_ipv4() {
            socket2::Domain::IPV4
        } else {
            socket2::Domain::IPV6
        },
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )
    .expect("failed to create socket");

    socket
        .set_reuse_port(true)
        .expect("failed to set SO_REUSEPORT");

    socket.bind(&addr.into()).expect("failed to bind socket");

    let std_socket: UdpSocket = socket.into();
    let local_addr = std_socket.local_addr().expect("failed to get local addr");
    info!(
        local_addr = %local_addr,
        fd = std_socket.as_raw_fd(),
        "created reuseport socket"
    );
    std_socket
}

pub(super) struct ThreadAllocation {
    pub(super) thread_id: usize,
    pub(super) sockets: Vec<UdpSocket>,
    pub(super) configs: Vec<SocketConfig>,
}

impl ThreadAllocation {
    pub(super) fn socket_count(&self) -> usize {
        debug_assert_eq!(self.sockets.len(), self.configs.len());
        self.sockets.len()
    }
}

pub(super) fn partition_sockets_for_threads(
    all_sockets: Vec<UdpSocket>,
    replicated_configs: Vec<SocketConfig>,
    num_threads: usize,
    sockets_per_thread: usize,
) -> Vec<ThreadAllocation> {
    assert_eq!(
        all_sockets.len(),
        num_threads * sockets_per_thread,
        "socket count mismatch"
    );
    assert_eq!(
        replicated_configs.len(),
        num_threads * sockets_per_thread,
        "config count mismatch"
    );

    let mut all_sockets = all_sockets;
    let mut replicated_configs = replicated_configs;
    let mut allocations = Vec::with_capacity(num_threads);

    for thread_id in 0..num_threads {
        let sockets = all_sockets.drain(..sockets_per_thread).collect();
        let configs = replicated_configs.drain(..sockets_per_thread).collect();

        allocations.push(ThreadAllocation {
            thread_id,
            sockets,
            configs,
        });
    }

    allocations
}

pub(super) fn prepare_sockets_and_allocate(
    socket_configs: Vec<SocketConfig>,
    num_threads: usize,
    buffer_size: Option<usize>,
) -> (Vec<RawFd>, Vec<ThreadAllocation>) {
    let base_socket_count = socket_configs.len();
    let replicated_configs: Vec<SocketConfig> = (0..num_threads)
        .flat_map(|_| socket_configs.iter().cloned())
        .collect();

    let mut all_sockets = Vec::new();
    for config in &replicated_configs {
        let socket = create_reuseport_socket(config.socket_addr);
        if let Some(size) = buffer_size {
            super::sockopt::set_recv_buffer_size(&socket, size);
        }
        super::sockopt::set_mtu_discovery(&socket);
        all_sockets.push(socket);
    }

    let first_fds: Vec<RawFd> = all_sockets[0..base_socket_count]
        .iter()
        .map(|s| s.as_raw_fd())
        .collect();

    let allocations = partition_sockets_for_threads(
        all_sockets,
        replicated_configs,
        num_threads,
        base_socket_count,
    );

    (first_fds, allocations)
}

pub(super) fn run_reader_loop<B, F>(
    thread_id: usize,
    sockets: Vec<UdpSocket>,
    configs: Vec<SocketConfig>,
    on_message: F,
    batch_size: usize,
) -> Result<(), super::reader::ReaderError>
where
    B: super::backend::ReaderBackend,
    F: Fn(usize, crate::RecvUdpMsg) -> Result<(), ()>,
{
    use io_uring::IoUring;
    use tracing::trace;

    let num_sockets = sockets.len();
    let socket_fds: Vec<RawFd> = sockets.iter().map(|s| s.as_raw_fd()).collect();

    let mut backend = B::new(thread_id, num_sockets, batch_size)?;
    let mut ring =
        IoUring::new(backend.ring_size()).map_err(super::reader::ReaderError::CreateRing)?;

    backend.submit_initial_ops(&mut ring, &socket_fds)?;

    ring.submit().map_err(super::reader::ReaderError::Submit)?;

    trace!(thread_id, "entering main receive loop");

    loop {
        if ring.completion().is_empty() {
            ring.submit_and_wait(1)
                .map_err(super::reader::ReaderError::SubmitAndWait)?;
        } else {
            ring.submit().map_err(super::reader::ReaderError::Submit)?;
        }

        let cqes: Vec<_> = ring.completion().collect();
        trace!(thread_id, cqe_count = cqes.len(), "received completions");

        for cqe in cqes {
            backend.handle_completion(
                thread_id,
                &mut ring,
                &configs,
                &socket_fds,
                &cqe,
                &on_message,
            )?;
        }
    }
}
