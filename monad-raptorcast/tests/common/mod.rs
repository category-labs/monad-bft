use std::net::{TcpListener, UdpSocket};

/// Find a free UDP port by binding to an ephemeral port and returning it.
pub fn find_udp_free_port() -> u16 {
    let socket = UdpSocket::bind("127.0.0.1:0").expect("failed to bind");
    socket.local_addr().expect("failed to get addr").port()
}

/// Find a free TCP port by binding to an ephemeral port and returning it.
#[allow(dead_code)]
pub fn find_tcp_free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("failed to bind");
    listener.local_addr().expect("failed to get addr").port()
}
