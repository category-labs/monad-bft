// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::net::SocketAddrV4;

#[derive(Debug, PartialEq)]
pub enum IpCheckError {
    UnspecifiedIp,
    SpecialIP,
    PrivateIp,
    LoopbackIp,
    LinkLocalIp,
}

pub fn validate_socket_ipv4_address(
    socket_address: &SocketAddrV4,
    self_address: &SocketAddrV4,
) -> Result<(), IpCheckError> {
    let self_ip = self_address.ip();
    let peer_ip = socket_address.ip();

    // unspecified address of 0.0.0.0
    if peer_ip.is_unspecified() {
        return Err(IpCheckError::UnspecifiedIp);
    }

    // special use network range includes broadcast, multicast and documentation addresses
    // multicast address of 224.0.0.0/4
    if peer_ip.is_multicast() {
        return Err(IpCheckError::SpecialIP);
    }
    // broadcast address of 255.255.255.255
    if peer_ip.is_broadcast() {
        return Err(IpCheckError::SpecialIP);
    }
    // documentation address of 192.0.2.0/24, 198.51.100.0/24, 203.0.113.0/24
    if peer_ip.is_documentation() {
        return Err(IpCheckError::SpecialIP);
    }

    // loopback address of 127.0.0.0/8
    if peer_ip.is_loopback() && !self_ip.is_loopback() {
        return Err(IpCheckError::LoopbackIp);
    }

    // private address of 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16
    if peer_ip.is_private() && !self_ip.is_private() {
        return Err(IpCheckError::PrivateIp);
    }

    // link-local address of 169.254.0.0/16
    if peer_ip.is_link_local() && !self_ip.is_link_local() {
        return Err(IpCheckError::LinkLocalIp);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use rstest::*;

    use super::*;

    #[rstest]
    #[case("45.22.13.14", "0.0.0.0", Err(IpCheckError::UnspecifiedIp))] // unspecified ip
    #[case("45.22.13.14", "224.0.0.1", Err(IpCheckError::SpecialIP))] // multicast ip
    #[case("45.22.13.14", "255.255.255.255", Err(IpCheckError::SpecialIP))] // broadcast ip
    #[case("45.22.13.14", "198.51.100.1", Err(IpCheckError::SpecialIP))] // documentation ip
    #[case("45.22.13.14", "127.0.0.1", Err(IpCheckError::LoopbackIp))] // loopback ip
    #[case("127.0.0.2", "127.0.0.1", Ok(()))] // loopback ip with loopback self ip
    #[case("45.22.13.14", "10.0.0.1", Err(IpCheckError::PrivateIp))] // private ip
    #[case("10.0.0.2", "10.0.0.1", Ok(()))] // private ip with private self ip
    #[case("45.22.13.14", "169.254.1.1", Err(IpCheckError::LinkLocalIp))] // link-local ip
    #[case("169.254.1.2", "169.254.1.1", Ok(()))] // link-local ip with link-local self ip
    #[case("45.22.13.14", "45.22.13.15", Ok(()))] // public ip
    fn test_validate_socket_ipv4_address(
        #[case] self_address: &str,
        #[case] peer_address: &str,
        #[case] expect: Result<(), IpCheckError>,
    ) {
        let self_address = SocketAddrV4::new(self_address.parse().unwrap(), 8080);
        let peer_address = SocketAddrV4::new(peer_address.parse().unwrap(), 8080);
        assert_eq!(
            validate_socket_ipv4_address(&peer_address, &self_address),
            expect
        );
    }
}
