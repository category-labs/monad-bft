use std::{mem, net::UdpSocket, os::unix::io::AsRawFd};

pub fn set_recv_buffer_size(socket: &UdpSocket, requested_size: usize) {
    let optval = requested_size as libc::c_int;
    let ret = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &optval as *const _ as *const libc::c_void,
            mem::size_of_val(&optval) as libc::socklen_t,
        )
    };

    if ret != 0 {
        panic!(
            "set_recv_buffer_size to {requested_size} failed with: {}",
            std::io::Error::last_os_error()
        );
    }

    let mut actual_size: libc::c_int = 0;
    let mut len = mem::size_of_val(&actual_size) as libc::socklen_t;
    let ret = unsafe {
        libc::getsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &mut actual_size as *mut _ as *mut libc::c_void,
            &mut len,
        )
    };

    if ret != 0 {
        panic!(
            "get recv buffer size failed with: {}",
            std::io::Error::last_os_error()
        );
    }

    if (actual_size as usize) < requested_size {
        panic!("unable to set udp receive buffer size to {requested_size}. Got {actual_size} instead. Set net.core.rmem_max to at least {requested_size}");
    }
}

pub fn set_mtu_discovery(socket: &UdpSocket) {
    const MTU_DISCOVER: libc::c_int = libc::IP_PMTUDISC_OMIT;
    let raw_fd = socket.as_raw_fd();

    if unsafe {
        libc::setsockopt(
            raw_fd,
            libc::SOL_IP,
            libc::IP_MTU_DISCOVER,
            &MTU_DISCOVER as *const _ as _,
            std::mem::size_of_val(&MTU_DISCOVER) as _,
        )
    } != 0
    {
        panic!(
            "set IP_MTU_DISCOVER failed with: {}",
            std::io::Error::last_os_error()
        );
    }
}
