use std::{
    io::IoSliceMut,
    mem::MaybeUninit,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    os::fd::AsRawFd,
};

use bytes::Bytes;
use log::debug;

pub const MAX_UDP_PKT: usize = 65535;
const MAX_IPV4_HDR: usize = 20;
const MAX_UDP_HDR: usize = 8;
const MONAD_GSO_SIZE: usize = 1490;

const BUF_SIZE: usize = MAX_UDP_PKT;
const NUM_RX_MSGHDR: usize = 128;
const NUM_TX_MSGHDR: usize = 128;
const CMSG_LEN: usize = 88;

// message length is limited by the max limit of the underlying protocol
const MAX_IOVEC_LEN: usize = MAX_UDP_PKT - MAX_IPV4_HDR - MAX_UDP_HDR;

const NUM_IOVECS: usize = 1024;

// 64 is linux kernel limit on max number of segments
#[allow(clippy::assertions_on_constants)]
const _: () = assert!((MAX_IOVEC_LEN / MONAD_GSO_SIZE) <= 64);

// num msgs in sendmmsg is limited to 1024 in the kernel
#[allow(clippy::assertions_on_constants)]
const _: () = assert!(NUM_TX_MSGHDR <= 1024);

#[derive(Copy, Clone)]
#[repr(align(8))]
pub struct AlignedCmsg(pub MaybeUninit<[u8; CMSG_LEN]>);

unsafe impl<'a> Send for NetworkSocket<'a> {}

pub struct NetworkSocket<'a> {
    pub socket: std::net::UdpSocket,
    pub local_sock_addr: SocketAddr,

    pub recv_ctrl: RecvCtrl<'a>,
    pub send_ctrl: SendCtrl,
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

impl<'a> NetworkSocket<'a> {
    pub fn new(sock_addr: &str) -> Self {
        let socket = std::net::UdpSocket::bind(sock_addr).unwrap();
        let r = unsafe {
            const GSO_SIZE: libc::c_int = MONAD_GSO_SIZE as i32;
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::SOL_UDP,
                libc::UDP_SEGMENT,
                &GSO_SIZE as *const _ as _,
                std::mem::size_of_val(&GSO_SIZE) as _,
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

        let local_sock_addr = socket.local_addr().unwrap();

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
        let iovecs = unsafe { std::mem::zeroed::<[libc::iovec; NUM_IOVECS]>() };

        Self {
            socket,
            local_sock_addr,
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
                iovecs,
            },
        }
    }

    pub fn recv(&mut self, buf: &mut [u8]) -> Option<(usize, SocketAddr)> {
        let (len, from) = match self.socket.recv_from(buf) {
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
            libc::recvmmsg(
                self.socket.as_raw_fd(),
                &mut self.recv_ctrl.msgs[0],
                NUM_RX_MSGHDR as _,
                libc::MSG_DONTWAIT,
                &mut self.recv_ctrl.timeout as *mut libc::timespec,
            )
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
                        debug!("cmsg_data = {}", stride);

                        assert!(stride as usize <= MONAD_GSO_SIZE);
                    }
                }

                first = unsafe { libc::CMSG_NXTHDR(&self.recv_ctrl.msgs[i].msg_hdr, first) };
            }

            retval.push(RecvmmsgResult {
                len: msglen,
                src_addr: NetworkSocket::get_addr(self.recv_ctrl.name[i]),
                stride: 0,
            });
            debug!("from: {}", NetworkSocket::get_addr(self.recv_ctrl.name[i]));
            debug!("received: {}", msglen);
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

    // send the same message to multiple destinations
    // one continguous data buffer
    // data buffer cannot be larger than max udp size
    // each msg_hdr is to one dest addr
    // TODO use more descriptive enum for return value rather than Option
    // TODO: should be able to split data greater than MAX_IOVEC_LEN into multiple messages,
    // as long as there are max 1024 messages
    pub fn broadcast_buffer(&mut self, to: Vec<SocketAddr>, data: Bytes) -> Option<()> {
        if to.is_empty() {
            return None;
        }

        assert!(to.len() < NUM_TX_MSGHDR);

        let n = data.len();
        assert!(n <= MAX_IOVEC_LEN);

        self.send_ctrl.iovecs[0].iov_base = data.as_ptr() as *const _ as *mut _;
        self.send_ctrl.iovecs[0].iov_len = n;

        for (i, dst) in to.iter().enumerate() {
            self.send_ctrl.name[i].write((*dst).into());
            self.send_ctrl.msgs[i].msg_hdr.msg_iov = self.send_ctrl.iovecs.as_mut_ptr();
            self.send_ctrl.msgs[i].msg_hdr.msg_iovlen = 1;

            self.send_ctrl.msgs[i].msg_hdr.msg_control = std::ptr::null_mut();
            self.send_ctrl.msgs[i].msg_hdr.msg_controllen = 0;

            self.send_ctrl.msgs[i].msg_hdr.msg_name =
                self.send_ctrl.name[i].as_ptr() as *const _ as *mut _;
            self.send_ctrl.msgs[i].msg_hdr.msg_namelen =
                unsafe { self.send_ctrl.name[i].assume_init_ref().len() };
        }

        self.sendmmsg(to.len() as u32)
    }

    pub fn unicast_buffer(&mut self, msg: Vec<(SocketAddr, Bytes)>) -> Option<()> {
        if msg.is_empty() {
            return None;
        }

        assert!(msg.len() < NUM_TX_MSGHDR);

        for (i, (to, payload)) in msg.iter().enumerate() {
            self.send_ctrl.name[i].write((*to).into());

            self.send_ctrl.iovecs[i].iov_base = (*payload).as_ptr() as *const _ as *mut _;
            self.send_ctrl.iovecs[i].iov_len = payload.len();
            self.send_ctrl.msgs[i].msg_hdr.msg_iov = &mut self.send_ctrl.iovecs[i];
            self.send_ctrl.msgs[i].msg_hdr.msg_iovlen = 1;

            self.send_ctrl.msgs[i].msg_hdr.msg_control = std::ptr::null_mut();
            self.send_ctrl.msgs[i].msg_hdr.msg_controllen = 0;

            self.send_ctrl.msgs[i].msg_hdr.msg_name =
                self.send_ctrl.name[i].as_ptr() as *const _ as *mut _;
            self.send_ctrl.msgs[i].msg_hdr.msg_namelen =
                unsafe { self.send_ctrl.name[i].assume_init_ref().len() };
        }

        self.sendmmsg(0)
    }

    fn sendmmsg(&mut self, num_msgs: u32) -> Option<()> {
        let r = unsafe {
            libc::sendmmsg(
                self.socket.as_raw_fd(),
                self.send_ctrl.msgs.as_mut_ptr(),
                num_msgs,
                0,
            )
        };

        if r == -1 {
            let e = std::io::Error::last_os_error();
            panic!("sendmmsg error {}", e);
        }

        // TODO try sending the stuff that wasn't sent
        if r != num_msgs as i32 {
            panic!("only sent {} out of {} msgs", r, num_msgs);
        }

        Some(())
    }

    pub fn send(&self, to: SocketAddr, buf: &[u8], len: usize) -> std::io::Result<usize> {
        self.socket.send_to(&buf[..len], to)
    }

    pub fn get_local_addr(&self) -> SocketAddr {
        self.local_sock_addr
    }
}
