use std::{mem, net::SocketAddr, os::unix::io::RawFd, thread};

use bytes::BytesMut;
use io_uring::{opcode, types, IoUring};
use thiserror::Error;
use tracing::{error, info, trace, warn};

use super::backend::ReaderBackend;
use crate::RecvUdpMsg;

#[derive(Debug, Error)]
pub(super) enum ReaderError {
    #[error("failed to create io_uring: {0}")]
    CreateRing(#[source] std::io::Error),
    #[error("submission queue full")]
    SubmissionQueueFull,
    #[error("failed to submit: {0}")]
    Submit(#[source] std::io::Error),
    #[error("failed to submit and wait: {0}")]
    SubmitAndWait(#[source] std::io::Error),
    #[error("message handler closed")]
    MessageHandlerClosed,
}

impl From<()> for ReaderError {
    fn from(_: ()) -> Self {
        ReaderError::MessageHandlerClosed
    }
}

#[repr(C)]
struct RecvBuffer {
    buffer: [u8; 1500],
    addr: libc::sockaddr_storage,
    iov: libc::iovec,
    msg: libc::msghdr,
}

impl RecvBuffer {
    fn new() -> Self {
        Self {
            buffer: [0u8; 1500],
            addr: unsafe { mem::zeroed() },
            iov: libc::iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: 0,
            },
            msg: unsafe { mem::zeroed() },
        }
    }

    fn reset(&mut self) {
        self.iov.iov_base = self.buffer.as_mut_ptr() as *mut _;
        self.iov.iov_len = self.buffer.len();

        self.msg.msg_name = &mut self.addr as *mut _ as *mut _;
        self.msg.msg_namelen = mem::size_of::<libc::sockaddr_storage>() as u32;
        self.msg.msg_iov = &mut self.iov as *mut _;
        self.msg.msg_iovlen = 1;
    }

    fn src_addr(&self) -> SocketAddr {
        unsafe {
            let addr = &self.addr as *const libc::sockaddr_storage as *const libc::sockaddr;
            let sa_family = (*addr).sa_family;
            match sa_family as i32 {
                libc::AF_INET => {
                    let addr_in = addr as *const libc::sockaddr_in;
                    let ip = u32::from_be((*addr_in).sin_addr.s_addr);
                    let port = u16::from_be((*addr_in).sin_port);
                    SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::from(ip)), port)
                }
                libc::AF_INET6 => {
                    let addr_in6 = addr as *const libc::sockaddr_in6;
                    let ip = (*addr_in6).sin6_addr.s6_addr;
                    let port = u16::from_be((*addr_in6).sin6_port);
                    SocketAddr::new(std::net::IpAddr::V6(std::net::Ipv6Addr::from(ip)), port)
                }
                _ => panic!(
                    "unsupported address family: {} (AF_INET={}, AF_INET6={})",
                    sa_family,
                    libc::AF_INET,
                    libc::AF_INET6
                ),
            }
        }
    }

    fn msg_ptr(&mut self) -> *mut libc::msghdr {
        &mut self.msg as *mut _
    }

    fn copy_data(&self, len: usize) -> BytesMut {
        let mut payload = BytesMut::with_capacity(len);
        unsafe {
            payload.set_len(len);
            std::ptr::copy_nonoverlapping(self.buffer.as_ptr(), payload.as_mut_ptr(), len);
        }
        payload
    }
}

struct RecvOp {
    socket_idx: usize,
    buffer_idx: usize,
}

impl RecvOp {
    fn new(socket_idx: usize, buffer_idx: usize) -> Self {
        Self {
            socket_idx,
            buffer_idx,
        }
    }

    fn submit(
        &self,
        ring: &mut IoUring,
        fd: RawFd,
        msg_ptr: *mut libc::msghdr,
    ) -> Result<(), ReaderError> {
        let recv_e = opcode::RecvMsg::new(types::Fd(fd), msg_ptr)
            .build()
            .user_data(UserData::encode(self.socket_idx, self.buffer_idx));

        unsafe {
            ring.submission()
                .push(&recv_e)
                .map_err(|_| ReaderError::SubmissionQueueFull)
        }
    }
}

#[derive(Clone)]
pub struct SocketConfig {
    pub socket_id: usize,
    pub socket_addr: SocketAddr,
    #[allow(dead_code)]
    pub label: String,
}

struct UserData {
    socket_idx: usize,
    buffer_idx: usize,
}

impl UserData {
    fn encode(socket_idx: usize, buffer_idx: usize) -> u64 {
        ((socket_idx as u64) << 32) | (buffer_idx as u64)
    }

    fn decode(user_data: u64) -> Self {
        Self {
            socket_idx: (user_data >> 32) as usize,
            buffer_idx: (user_data & 0xFFFFFFFF) as usize,
        }
    }
}

struct RecvMsgBackend {
    buffers: Vec<Vec<RecvBuffer>>,
}

impl ReaderBackend for RecvMsgBackend {
    fn new(_thread_id: usize, num_sockets: usize, batch_size: usize) -> Result<Self, ReaderError> {
        let mut buffers: Vec<Vec<RecvBuffer>> = (0..num_sockets)
            .map(|_| (0..batch_size).map(|_| RecvBuffer::new()).collect())
            .collect();

        for socket_buffers in &mut buffers {
            for buffer in socket_buffers {
                buffer.reset();
            }
        }

        Ok(Self { buffers })
    }

    fn ring_size(&self) -> u32 {
        (self.buffers.len() * self.buffers[0].len()) as u32
    }

    fn submit_initial_ops(
        &mut self,
        ring: &mut IoUring,
        socket_fds: &[RawFd],
    ) -> Result<(), ReaderError> {
        for (socket_idx, buffers) in self.buffers.iter_mut().enumerate() {
            let fd = socket_fds[socket_idx];
            for (buffer_idx, buffer) in buffers.iter_mut().enumerate() {
                let op = RecvOp::new(socket_idx, buffer_idx);
                op.submit(ring, fd, buffer.msg_ptr())?;
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
        cqe: &io_uring::cqueue::Entry,
        on_message: &F,
    ) -> Result<(), ReaderError>
    where
        F: Fn(usize, RecvUdpMsg) -> Result<(), ()>,
    {
        let result = cqe.result();
        let user_data = UserData::decode(cqe.user_data());

        if result > 0 {
            let socket_idx = user_data.socket_idx;
            let buffer_idx = user_data.buffer_idx;

            let buffer = &mut self.buffers[socket_idx][buffer_idx];
            let src_addr = buffer.src_addr();
            let payload = buffer.copy_data(result as usize);

            let msg = RecvUdpMsg {
                src_addr,
                payload: payload.freeze(),
                stride: result.max(1).try_into().unwrap(),
            };

            let socket_id = configs[socket_idx].socket_id;
            trace!(thread_id, socket_id, src_addr = %src_addr, len = result, "received udp packet");

            on_message(socket_id, msg)?;

            buffer.reset();
            let op = RecvOp::new(socket_idx, buffer_idx);
            op.submit(ring, socket_fds[socket_idx], buffer.msg_ptr())?;
        } else if result < 0 {
            warn!(
                socket_id = configs.get(user_data.socket_idx).map(|c| c.socket_id),
                error = %std::io::Error::from_raw_os_error(-result),
                "recv error"
            );
        }

        Ok(())
    }
}

pub fn spawn<F>(
    socket_configs: Vec<SocketConfig>,
    on_message: F,
    num_threads: usize,
    batch_size_per_socket: usize,
    buffer_size: Option<usize>,
    use_ringbuf: bool,
) -> Vec<RawFd>
where
    F: Fn(usize, RecvUdpMsg) -> Result<(), ()> + Send + Sync + Clone + 'static,
{
    assert!(
        num_threads > 0 && num_threads <= 16,
        "num_threads must be between 1 and 16"
    );
    assert!(!socket_configs.is_empty(), "socket_configs cannot be empty");

    if use_ringbuf {
        super::reader_ringbuf::spawn_with_ringbuf(
            socket_configs,
            on_message,
            num_threads,
            batch_size_per_socket,
            buffer_size,
        )
    } else {
        spawn_with_recvmsg(
            socket_configs,
            on_message,
            num_threads,
            batch_size_per_socket,
            buffer_size,
        )
    }
}

fn spawn_with_recvmsg<F>(
    socket_configs: Vec<SocketConfig>,
    on_message: F,
    num_threads: usize,
    batch_size_per_socket: usize,
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
            .name(format!("dp-uring-{}", thread_id))
            .spawn(move || {
                match super::shared::run_reader_loop::<RecvMsgBackend, F>(
                    thread_id,
                    allocation.sockets,
                    allocation.configs,
                    on_message,
                    batch_size_per_socket,
                ) {
                    Ok(()) => {
                        info!(thread_id, "reader exited cleanly");
                    }
                    Err(e) => {
                        error!(thread_id, error = %e, "reader exited with error");
                    }
                }
            })
            .expect("failed to spawn uring reader thread");

        info!(
            thread_id,
            num_sockets, batch_size_per_socket, "spawned io_uring reader thread"
        );
    }

    first_fds
}
