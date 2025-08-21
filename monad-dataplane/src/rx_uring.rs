use std::{
    mem,
    net::SocketAddr,
    os::fd::AsRawFd,
    ptr,
    sync::{
        atomic::{AtomicU16, Ordering},
        Arc,
    },
};

use bytes::Bytes;
use io_uring::{
    opcode, squeue,
    types::{BufRingEntry, Fd, RecvMsgOut},
    IoUring,
};
use tokio::sync::mpsc;
use tracing::{error, trace, warn};

use super::RecvUdpMsg;

const ETHERNET_MTU: u16 = 1500;
const IPV4_HDR_SIZE: u16 = 20;
const UDP_HDR_SIZE: u16 = 8;
const ETHERNET_SEGMENT_SIZE: u16 = ETHERNET_MTU - IPV4_HDR_SIZE - UDP_HDR_SIZE;

struct AnonymousMmap {
    addr: ptr::NonNull<libc::c_void>,
    len: usize,
}

impl AnonymousMmap {
    fn new(len: usize) -> Result<Self, std::io::Error> {
        let addr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANONYMOUS | libc::MAP_SHARED | libc::MAP_POPULATE,
                -1,
                0,
            )
        };

        if addr == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }

        Ok(Self {
            addr: ptr::NonNull::new(addr).unwrap(),
            len,
        })
    }
}

impl Drop for AnonymousMmap {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.addr.as_ptr(), self.len);
        }
    }
}

struct InnerBufRing {
    bgid: u16,
    ring_entries: u16,
    buf_len: usize,
    ring_mem: AnonymousMmap,
    buffers: Vec<Vec<u8>>,
}

impl InnerBufRing {
    fn new(bgid: u16, ring_entries: u16, buf_cnt: u16, buf_len: usize) -> Result<Self, std::io::Error> {
        if !ring_entries.is_power_of_two() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "ring_entries must be power of 2",
            ));
        }
        if ring_entries > 32768 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "ring_entries too large (max 32768)",
            ));
        }

        let mem_size = std::mem::size_of::<BufRingEntry>() * ring_entries as usize;
        let page_size = 4096;
        let mem_size = (mem_size + page_size - 1) & !(page_size - 1);

        let ring_mem = AnonymousMmap::new(mem_size)?;

        let mut buffers = Vec::with_capacity(buf_cnt as usize);
        for _ in 0..buf_cnt {
            buffers.push(vec![0u8; buf_len]);
        }

        Ok(Self {
            bgid,
            ring_entries,
            buf_len,
            ring_mem,
            buffers,
        })
    }

    fn ring_base(&self) -> *mut BufRingEntry {
        self.ring_mem.addr.as_ptr() as *mut BufRingEntry
    }

    fn tail(&self) -> &AtomicU16 {
        unsafe { &*(BufRingEntry::tail(self.ring_base()) as *const AtomicU16) }
    }

    fn register(&self, ring: &IoUring) -> Result<(), std::io::Error> {
        let ring_base = self.ring_base();
        let mut tail_index = 0u16;

        for (i, buf) in self.buffers.iter().enumerate() {
            let entry = unsafe { &mut *ring_base.add((tail_index % self.ring_entries) as usize) };
            entry.set_addr(buf.as_ptr() as u64);
            entry.set_len(buf.len() as u32);
            entry.set_bid(i as u16);
            tail_index = tail_index.wrapping_add(1);
        }

        self.tail().store(tail_index, Ordering::Release);

        unsafe {
            ring.submitter().register_buf_ring(
                self.ring_mem.addr.as_ptr() as u64,
                self.ring_entries,
                self.bgid,
            )?;
        }

        Ok(())
    }

    fn add_buffer(&self, bid: u16) {
        let tail = self.tail().load(Ordering::Acquire);
        let ring_base = self.ring_base();

        let entry = unsafe { &mut *ring_base.add((tail % self.ring_entries) as usize) };
        let buf = &self.buffers[bid as usize];

        entry.set_addr(buf.as_ptr() as u64);
        entry.set_len(buf.len() as u32);
        entry.set_bid(bid);

        self.tail().store(tail.wrapping_add(1), Ordering::Release);
    }

    fn get_buffer(&self, bid: u16) -> Option<&[u8]> {
        self.buffers.get(bid as usize).map(|v| v.as_slice())
    }
}

struct BufRing {
    inner: Arc<InnerBufRing>,
}

impl BufRing {
    fn new(bgid: u16, ring_entries: u16, buf_cnt: u16, buf_len: usize) -> Result<Self, std::io::Error> {
        let inner = InnerBufRing::new(bgid, ring_entries, buf_cnt, buf_len)?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    fn register(&self, ring: &IoUring) -> Result<(), std::io::Error> {
        self.inner.register(ring)
    }

    fn add_buffer(&self, bid: u16) {
        self.inner.add_buffer(bid)
    }

    fn get_buffer(&self, bid: u16) -> Option<&[u8]> {
        self.inner.get_buffer(bid)
    }
}

pub fn spawn_uring_reader(
    socket: std::net::UdpSocket,
    udp_ingress_tx: mpsc::Sender<RecvUdpMsg>,
) {
    std::thread::Builder::new()
        .name("udp-uring-rx".into())
        .spawn(move || {
            if let Err(e) = run_uring_reader(socket, udp_ingress_tx) {
                error!(error = ?e, "io-uring reader failed");
            }
        })
        .expect("failed to spawn io-uring reader thread");
}

pub fn spawn_uring_reader_direct(
    socket: std::net::UdpSocket,
    udp_direct_ingress_tx: mpsc::Sender<RecvUdpMsg>,
) {
    std::thread::Builder::new()
        .name("udp-uring-rx-direct".into())
        .spawn(move || {
            if let Err(e) = run_uring_reader(socket, udp_direct_ingress_tx) {
                error!(error = ?e, "io-uring direct reader failed");
            }
        })
        .expect("failed to spawn io-uring direct reader thread");
}

fn run_uring_reader(
    socket: std::net::UdpSocket,
    udp_ingress_tx: mpsc::Sender<RecvUdpMsg>,
) -> Result<(), std::io::Error> {
    const RING_ENTRIES: u16 = 256;
    const BUFFER_COUNT: u16 = 1024;
    const BGID: u16 = 0;

    let fd = Fd(socket.as_raw_fd());
    // Keep the socket alive for the duration of this function
    let _socket = socket;

    let mut ring = IoUring::builder()
        .setup_single_issuer()
        .setup_coop_taskrun()
        .build(RING_ENTRIES as u32)?;

    let buf_ring = BufRing::new(
        BGID,
        RING_ENTRIES,
        BUFFER_COUNT,
        ETHERNET_SEGMENT_SIZE as usize + 300,
    )?;
    buf_ring.register(&ring)?;

    let mut msghdr: libc::msghdr = unsafe { mem::zeroed() };
    msghdr.msg_namelen = mem::size_of::<libc::sockaddr_storage>() as u32;

    let recvmsg_e = opcode::RecvMsgMulti::new(fd, &msghdr as *const _, BGID)
        .build()
        .flags(squeue::Flags::ASYNC | squeue::Flags::BUFFER_SELECT)
        .user_data(0x1000);

    unsafe {
        ring.submission().push(&recvmsg_e).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to push recvmsg: {:?}", e))
        })?;
    }
    ring.submit()?;

    let mut need_resubmit = false;

    loop {
        ring.submit_and_wait(1)?;

        {
            let cq = ring.completion();
            for cqe in cq {
                let res = cqe.result();
                let flags = cqe.flags();

                if cqe.user_data() == 0x1000 {
                    if res < 0 {
                        if res == -libc::ENOBUFS {
                            warn!(msg = "no buffers available");
                            need_resubmit = true;
                        } else if res == -libc::ECANCELED {
                            warn!(msg = "multishot canceled");
                            need_resubmit = true;
                        } else {
                            error!(msg = "recvmsg error", error = res);
                            need_resubmit = true;
                        }
                        continue;
                    }

                    if let Some(buf_id) = io_uring::cqueue::buffer_select(flags) {
                        if res as usize > 0 {
                            if let Some(buf) = buf_ring.get_buffer(buf_id) {
                                if let Ok(msg_out) = RecvMsgOut::parse(&buf[..res as usize], &msghdr) {
                                    let payload = msg_out.payload_data();
                                    let name_data = msg_out.name_data();
                                    let truncated = msg_out.is_payload_truncated();
                                    if name_data.is_empty() {
                                        buf_ring.add_buffer(buf_id);
                                        continue;
                                    }
                                    
                                    let src_addr = unsafe {
                                        let addr_ptr = name_data.as_ptr() as *const libc::sockaddr;
                                        let sockaddr = &*addr_ptr;
                                        match sockaddr.sa_family as i32 {
                                            libc::AF_INET => {
                                                let sockaddr_in = &*(addr_ptr as *const libc::sockaddr_in);
                                                let ip = std::net::Ipv4Addr::from(sockaddr_in.sin_addr.s_addr.to_be());
                                                let port = u16::from_be(sockaddr_in.sin_port);
                                                SocketAddr::from((ip, port))
                                            }
                                            libc::AF_INET6 => {
                                                let sockaddr_in6 = &*(addr_ptr as *const libc::sockaddr_in6);
                                                let ip = std::net::Ipv6Addr::from(sockaddr_in6.sin6_addr.s6_addr);
                                                let port = u16::from_be(sockaddr_in6.sin6_port);
                                                SocketAddr::from((ip, port))
                                            }
                                            _ => continue,
                                        }
                                    };

                                    trace!(
                                        src_addr = ?src_addr,
                                        payload_len = payload.len(),
                                        buf_len = buf.len(),
                                        truncated,
                                        res = res,
                                        "received udp packet via io-uring"
                                    );

                                    let msg = RecvUdpMsg {
                                        src_addr,
                                        payload: Bytes::copy_from_slice(payload),
                                        stride: payload.len().max(1).try_into().unwrap(),
                                    };

                                    if let Err(_) = udp_ingress_tx.blocking_send(msg) {
                                        warn!(?src_addr, "error queueing up received UDP message");
                                        return Ok(());
                                    }
                                }
                            }

                            buf_ring.add_buffer(buf_id);
                        }
                    }

                    let more = io_uring::cqueue::more(flags);
                    if !more {
                        need_resubmit = true;
                    }
                }
            }
        }

        if need_resubmit {
            unsafe {
                ring.submission().push(&recvmsg_e).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to push recvmsg: {:?}", e))
                })?;
            }
            need_resubmit = false;
            ring.submit()?;
        }
    }
}