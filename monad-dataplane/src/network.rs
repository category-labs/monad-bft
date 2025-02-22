use std::{
    io::IoSliceMut,
    mem::MaybeUninit,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    os::fd::AsRawFd,
};

use bytes::Bytes;
use tracing::warn;

// When running in docker with vpnkit, the maximum safe MTU is 1480, as per:
// https://github.com/moby/vpnkit/tree/v0.5.0/src/hostnet/slirp.ml#L17-L18
pub const DEFAULT_MTU: u16 = 1480;

const MAX_IPV4_HDR: u16 = 20;
const MAX_UDP_HDR: u16 = 8;
pub const fn segment_size_for_mtu(mtu: u16) -> u16 {
    mtu - MAX_IPV4_HDR - MAX_UDP_HDR
}

pub const DEFAULT_SEGMENT_SIZE: u16 = segment_size_for_mtu(DEFAULT_MTU);

pub const MAX_UDP_PKT: usize = 65535;
const BUF_SIZE: usize = MAX_UDP_PKT;
const NUM_RX_MSGHDR: usize = 128;
const NUM_TX_MSGHDR: usize = 1024;
const CMSG_LEN: usize = 88;

// message length is limited by the max limit of the underlying protocol
//FIXME: This is expected size MAX_UDP_PKT - MAX_IPV4_HDR - MAX_UDP_HDR, but the actual measured
//number where the packet isn't being fragmented is 65493. investigate
pub const fn max_iovec_len(mtu: u16) -> u16 {
    let segment_size = segment_size_for_mtu(mtu);
    65493 / segment_size * segment_size
}

const LINUX_SENDMMSG_VLEN_MAX: usize = 1024;
const NUM_IOVECS: usize = 1024;

// num msgs in sendmmsg is limited to 1024 in the kernel
#[allow(clippy::assertions_on_constants)]
const _: () = assert!(NUM_TX_MSGHDR <= LINUX_SENDMMSG_VLEN_MAX);

#[derive(Copy, Clone)]
#[repr(align(8))]
pub struct AlignedCmsg(pub MaybeUninit<[u8; CMSG_LEN]>);

unsafe impl Send for NetworkSocket<'_> {}

pub struct TxSockets {
    sockets: lru::LruCache<i32, std::net::UdpSocket>,
    local_sock_addr: SocketAddr,
}

impl TxSockets {
    pub fn new(local_sock_addr: SocketAddr) -> Self {
        Self {
            sockets: lru::LruCache::new(std::num::NonZeroUsize::new(10).unwrap()),
            local_sock_addr,
        }
    }

    fn get_tx_socket(&mut self, mtu: i32) -> &std::net::UdpSocket {
        self.sockets.get_or_insert(mtu, || {
            let mut addr = self.local_sock_addr;
            addr.set_port(0);
            let socket = std::net::UdpSocket::bind(addr).unwrap();
            Self::set_tx_sock_opts(&socket, mtu);
            socket
        })
    }

    fn set_rx_sock_opts(socket: &std::net::UdpSocket, mtu: i32) {
        let r = unsafe {
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::SOL_UDP,
                libc::UDP_SEGMENT,
                &mtu as *const _ as _,
                std::mem::size_of_val(&mtu) as _,
            )
        };
        if r != 0 {
            panic!("set GSO failed with: {}", std::io::Error::last_os_error());
        }

        let r = unsafe {
            const GRO_EN: libc::c_int = 1;
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::SOL_UDP,
                libc::UDP_GRO,
                &GRO_EN as *const _ as _,
                std::mem::size_of_val(&GRO_EN) as _,
            )
        };
        if r != 0 {
            panic!("set GRO failed with: {}", std::io::Error::last_os_error());
        }

        let r = unsafe {
            const MTU_DISCOVER: libc::c_int = libc::IP_PMTUDISC_OMIT;
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::SOL_IP,
                libc::IP_MTU_DISCOVER,
                &MTU_DISCOVER as *const _ as _,
                std::mem::size_of_val(&MTU_DISCOVER) as _,
            )
        };
        if r != 0 {
            panic!(
                "set MTU discover failed with: {}",
                std::io::Error::last_os_error()
            );
        }
    }

    fn set_tx_sock_opts(socket: &std::net::UdpSocket, mtu: i32) {
        Self::set_rx_sock_opts(socket, mtu);
        let r = unsafe {
            const RCVBUF_SIZE: libc::c_int = 0;
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                &RCVBUF_SIZE as *const _ as _,
                std::mem::size_of_val(&RCVBUF_SIZE) as _,
            )
        };
        if r != 0 {
            warn!(
                "set tx socket recv buffer size to 0 failed with: {}",
                std::io::Error::last_os_error()
            );
        }
    }
}

pub struct NetworkSocket<'a> {
    pub default_socket: std::net::UdpSocket,
    pub tx_sockets: TxSockets,

    pub recv_ctrl: RecvCtrl<'a>,
    pub send_ctrl: SendCtrl,

    /// 1_000 = 1 Gbps, 10_000 = 10 Gbps
    pub up_bandwidth_mbps: u64,
    pub next_transmit: std::time::Instant,
    pub local_mtu: u16,
}

pub struct RecvCtrl<'a> {
    pub msgs: [libc::mmsghdr; NUM_RX_MSGHDR],
    pub name: [MaybeUninit<libc::sockaddr_storage>; NUM_RX_MSGHDR],
    pub cmsg: [AlignedCmsg; NUM_RX_MSGHDR],
    pub timeout: libc::timespec,
    pub buf_refs: Vec<IoSliceMut<'a>>,
}

pub struct SendCtrl {
    pub msgs: [libc::mmsghdr; NUM_TX_MSGHDR],
    pub name: [MaybeUninit<socket2::SockAddr>; NUM_TX_MSGHDR],
    pub bufs: Box<[[u8; BUF_SIZE]; NUM_TX_MSGHDR]>,
    pub stride: [u16; NUM_TX_MSGHDR],

    pub iovecs: [libc::iovec; NUM_IOVECS],
}

#[derive(Debug)]
pub enum BatchSendResult {
    BatchReady,
    Remaining(usize),
}

#[derive(Debug)]
pub struct RecvmmsgResult {
    // Total buffer size received in this msghdr
    pub len: usize,
    // Sender of the message
    pub src_addr: SocketAddr,
    // GRO segment size of messages in the buffer.
    // ie: if len=103 and stride=10, there are 10 messages of size 10 in the buffer and 1 of size 3
    // at the end
    pub stride: u16,
}

static mut BUF_PTR: *mut [[u8; BUF_SIZE]; NUM_RX_MSGHDR] = std::ptr::null_mut();

impl NetworkSocket<'_> {
    /// 1_000 = 1 Gbps, 10_000 = 10 Gbps
    pub fn new(sock_addr: &SocketAddr, up_bandwidth_mbps: u64, mtu: u16) -> Self {
        let default_socket = std::net::UdpSocket::bind(sock_addr).unwrap();
        TxSockets::set_rx_sock_opts(&default_socket, segment_size_for_mtu(mtu) as i32);

        let local_sock_addr = default_socket.local_addr().unwrap();

        unsafe {
            BUF_PTR = Box::into_raw(Box::new([[0; BUF_SIZE]; NUM_RX_MSGHDR]));
        }
        let buf_refs = unsafe {
            (*BUF_PTR)
                .iter_mut()
                .map(|c| IoSliceMut::new(c.as_mut_slice()))
                .collect::<Vec<_>>()
        };

        let recv_msgs = unsafe { std::mem::zeroed::<[libc::mmsghdr; NUM_RX_MSGHDR]>() };
        let recv_name = [MaybeUninit::<libc::sockaddr_storage>::uninit(); NUM_RX_MSGHDR];
        let recv_cmsg = [AlignedCmsg(MaybeUninit::<[u8; CMSG_LEN]>::uninit()); NUM_RX_MSGHDR];
        let recv_timeout = libc::timespec {
            tv_sec: 1,
            tv_nsec: 0,
        };

        let send_msgs = unsafe { std::mem::zeroed::<[libc::mmsghdr; NUM_TX_MSGHDR]>() };
        let send_name: [MaybeUninit<socket2::SockAddr>; NUM_TX_MSGHDR] =
            unsafe { MaybeUninit::uninit().assume_init() };
        let send_bufs = Box::new([[0; BUF_SIZE]; NUM_TX_MSGHDR]);
        let stride = [0; NUM_TX_MSGHDR];
        let iovecs = unsafe { std::mem::zeroed::<[libc::iovec; NUM_IOVECS]>() };

        Self {
            default_socket,
            tx_sockets: TxSockets::new(local_sock_addr),
            recv_ctrl: RecvCtrl {
                msgs: recv_msgs,
                buf_refs,
                name: recv_name,
                cmsg: recv_cmsg,
                timeout: recv_timeout,
            },
            send_ctrl: SendCtrl {
                msgs: send_msgs,
                name: send_name,
                bufs: send_bufs,
                stride,
                iovecs,
            },
            up_bandwidth_mbps,
            next_transmit: std::time::Instant::now(),
            local_mtu: mtu,
        }
    }

    pub fn recv(&mut self, buf: &mut [u8]) -> Option<(usize, SocketAddr)> {
        let (len, from) = match self.default_socket.recv_from(buf) {
            Ok(rx) => rx,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::WouldBlock {
                    return None;
                } else {
                    panic!("socket recv failed");
                }
            }
        };

        Some((len, from))
    }

    pub fn recvmmsg(&mut self) -> Option<Vec<RecvmmsgResult>> {
        for i in 0..NUM_RX_MSGHDR {
            NetworkSocket::init_recv_msghdr(
                &mut self.recv_ctrl.buf_refs[i],
                &mut self.recv_ctrl.name[i],
                &mut self.recv_ctrl.cmsg[i],
                &mut self.recv_ctrl.msgs[i].msg_hdr,
            );
        }

        let r = unsafe {
            super::retry_eintr(|| {
                libc::recvmmsg(
                    self.default_socket.as_raw_fd(),
                    &mut self.recv_ctrl.msgs[0],
                    NUM_RX_MSGHDR as _,
                    libc::MSG_DONTWAIT,
                    &mut self.recv_ctrl.timeout as *mut libc::timespec,
                )
            })
        };

        if r == -1 {
            let e = std::io::Error::last_os_error();
            if e.kind() == std::io::ErrorKind::WouldBlock {
                return None;
            }

            panic!("recvmmsg error {}", e);
        }

        let mut retval = Vec::with_capacity(NUM_RX_MSGHDR);
        for i in 0..r as usize {
            let msglen = self.recv_ctrl.msgs[i].msg_len as usize;
            let mut stride: u16 = 0;
            let mut first = unsafe { libc::CMSG_FIRSTHDR(&self.recv_ctrl.msgs[i].msg_hdr) };

            while !first.is_null() {
                unsafe {
                    if (*first).cmsg_level == libc::SOL_UDP && (*first).cmsg_type == libc::UDP_GRO {
                        std::ptr::copy_nonoverlapping::<u16>(
                            libc::CMSG_DATA(first) as _,
                            &mut stride,
                            1,
                        );
                    }
                }

                first = unsafe { libc::CMSG_NXTHDR(&self.recv_ctrl.msgs[i].msg_hdr, first) };
            }

            retval.push(RecvmmsgResult {
                len: msglen,
                src_addr: NetworkSocket::get_addr(self.recv_ctrl.name[i]),
                stride,
            });
        }

        Some(retval)
    }

    fn init_recv_msghdr(
        buf: &mut IoSliceMut,
        name: &mut MaybeUninit<libc::sockaddr_storage>,
        cmsg: &mut AlignedCmsg,
        hdr: &mut libc::msghdr,
    ) {
        hdr.msg_name = name.as_mut_ptr() as _;
        hdr.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as _;
        hdr.msg_iov = buf as *mut IoSliceMut as *mut libc::iovec;
        hdr.msg_iovlen = 1;
        hdr.msg_control = cmsg.0.as_mut_ptr() as _;
        hdr.msg_controllen = CMSG_LEN as _;
        hdr.msg_flags = 0;
    }

    fn get_addr(name: MaybeUninit<libc::sockaddr_storage>) -> SocketAddr {
        let name = unsafe { name.assume_init() };
        match libc::c_int::from(name.ss_family) {
            libc::AF_INET => {
                // Safety: if the ss_family field is AF_INET then storage must be a sockaddr_in.
                let addr: &libc::sockaddr_in =
                    unsafe { &*(&name as *const _ as *const libc::sockaddr_in) };
                SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::from(addr.sin_addr.s_addr.to_ne_bytes()),
                    u16::from_be(addr.sin_port),
                ))
            }
            libc::AF_INET6 => {
                // Safety: if the ss_family field is AF_INET6 then storage must be a sockaddr_in6.
                let addr: &libc::sockaddr_in6 =
                    unsafe { &*(&name as *const _ as *const libc::sockaddr_in6) };
                SocketAddr::V6(SocketAddrV6::new(
                    Ipv6Addr::from(addr.sin6_addr.s6_addr),
                    u16::from_be(addr.sin6_port),
                    addr.sin6_flowinfo,
                    addr.sin6_scope_id,
                ))
            }
            _ => unreachable!(),
        }
    }

    // send the same message in one continguous data buffer to multiple destinations
    //
    // TODO use more descriptive enum for return value rather than Option
    pub fn broadcast_buffer(
        &mut self,
        to: Vec<SocketAddr>,
        data: Bytes,
        stride: u16,
    ) -> Option<()> {
        if to.is_empty() {
            return None;
        }

        let max_iovec_len: usize = max_iovec_len(stride).into();

        let num_chunks = data.len().div_ceil(max_iovec_len);
        assert!(num_chunks * to.len() <= NUM_TX_MSGHDR);

        for (i, k) in (0..data.len()).step_by(max_iovec_len).enumerate() {
            let mut n = k + max_iovec_len;
            if n > data.len() {
                n = data.len();
            }

            self.send_ctrl.iovecs[i].iov_base = data[k..n].as_ptr() as *const _ as *mut _;
            self.send_ctrl.iovecs[i].iov_len = n - k;
        }

        let mut sendmmsg_len: u32 = 0;
        for (dst, i) in to.iter().zip((0..).step_by(num_chunks)) {
            for j in 0..num_chunks {
                sendmmsg_len += 1;
                let k = i + j;
                assert!(k < NUM_TX_MSGHDR);

                self.send_ctrl.name[k].write((*dst).into());
                self.send_ctrl.msgs[k].msg_hdr.msg_iov = &mut self.send_ctrl.iovecs[j];
                self.send_ctrl.msgs[k].msg_hdr.msg_iovlen = 1;

                self.send_ctrl.msgs[k].msg_hdr.msg_control = std::ptr::null_mut();
                self.send_ctrl.msgs[k].msg_hdr.msg_controllen = 0;

                self.send_ctrl.msgs[k].msg_hdr.msg_name =
                    self.send_ctrl.name[k].as_ptr() as *const _ as *mut _;
                self.send_ctrl.msgs[k].msg_hdr.msg_namelen =
                    unsafe { self.send_ctrl.name[k].assume_init_ref().len() };
                self.send_ctrl.msgs[k].msg_len = self.send_ctrl.iovecs[j].iov_len as u32;
                self.send_ctrl.stride[k] = stride;
            }
        }

        self.sendmmsg(sendmmsg_len)
    }

    pub fn unicast_buffer(&mut self, msg: Vec<(SocketAddr, Bytes)>) {
        if msg.is_empty() {
            return;
        }

        let _msg_clone = msg.clone(); // used just to keep reference counts alive until sendmmsg

        let mut i = 0;
        for (to, mut payload) in msg {
            while !payload.is_empty() {
                assert!(i < NUM_TX_MSGHDR);
                let chunk =
                    payload.split_to(usize::from(max_iovec_len(self.local_mtu)).min(payload.len()));

                self.send_ctrl.name[i].write(to.into());

                self.send_ctrl.iovecs[i].iov_base = (*chunk).as_ptr() as *const _ as *mut _;
                self.send_ctrl.iovecs[i].iov_len = chunk.len();
                self.send_ctrl.msgs[i].msg_hdr.msg_iov = &mut self.send_ctrl.iovecs[i];
                self.send_ctrl.msgs[i].msg_hdr.msg_iovlen = 1;

                self.send_ctrl.msgs[i].msg_hdr.msg_control = std::ptr::null_mut();
                self.send_ctrl.msgs[i].msg_hdr.msg_controllen = 0;

                self.send_ctrl.msgs[i].msg_hdr.msg_name =
                    self.send_ctrl.name[i].as_ptr() as *const _ as *mut _;
                self.send_ctrl.msgs[i].msg_hdr.msg_namelen =
                    unsafe { self.send_ctrl.name[i].assume_init_ref().len() };
                self.send_ctrl.stride[i] = segment_size_for_mtu(self.local_mtu);

                i += 1;
                if i == NUM_TX_MSGHDR {
                    self.sendmmsg(i.try_into().expect("msg len shouldn't exceed u32 capacity"));
                    i = 0;
                }
            }
        }

        if i != 0 {
            self.sendmmsg(i.try_into().expect("msg len shouldn't exceed u32 capacity"));
        }
    }

    fn sendmmsg(&mut self, num_msgs: u32) -> Option<()> {
        assert!(num_msgs as usize <= LINUX_SENDMMSG_VLEN_MAX);

        unsafe {
            for i in 0..num_msgs as usize {
                let now = std::time::Instant::now();
                if self.next_transmit > now {
                    std::thread::sleep(self.next_transmit - now);
                } else {
                    let late = now - self.next_transmit;

                    if late > std::time::Duration::from_millis(100) {
                        self.next_transmit = now;
                    }
                }

                let mtu = self.send_ctrl.stride[i] as i32;
                let socket = self.tx_sockets.get_tx_socket(mtu);

                // TODO instead of 1 sendmmsg per msg, we should create 1 sendmmsg per 65k of data
                let r = super::retry_eintr(|| {
                    libc::sendmmsg(
                        socket.as_raw_fd(),
                        (&mut self.send_ctrl.msgs[i]) as *mut _,
                        1,
                        0,
                    )
                });

                if r == -1 {
                    let e = std::io::Error::last_os_error();

                    // TODO: EINVAL return is likely due to MTU/GSO issues -- should getsockopt
                    // IP_MTU and include the returned value in the log message.
                    if e.kind() == std::io::ErrorKind::InvalidInput {
                        warn!("sendmmsg error {}", e);
                    } else {
                        panic!("sendmmsg error {}", e);
                    }
                }

                let sleep = std::time::Duration::from_nanos(
                    (self.send_ctrl.msgs[i].msg_len as u64) * 8 * 1000 / self.up_bandwidth_mbps,
                );
                self.next_transmit += sleep;
            }
        }
        // // TODO try sending the stuff that wasn't sent
        // if r != num_msgs as i32 {
        //     debug!("only sent {} out of {} msgs", r, num_msgs);
        // }

        Some(())
    }

    pub fn send(&self, to: SocketAddr, buf: &[u8], len: usize) -> std::io::Result<usize> {
        self.default_socket.send_to(&buf[..len], to)
    }

    pub fn get_local_addr(&self) -> SocketAddr {
        self.tx_sockets.local_sock_addr
    }
}
