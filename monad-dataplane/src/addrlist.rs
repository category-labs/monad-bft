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

use std::{
    collections::HashMap,
    fmt::Display,
    net::IpAddr,
    sync::{Arc, Mutex},
};

use monoio::time::Instant;

#[derive(Debug, PartialEq)]
pub(crate) enum Status {
    Banned,
    Trusted,
    Unknown,
}

impl Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Status::Banned => write!(f, "banned"),
            Status::Trusted => write!(f, "trusted"),
            Status::Unknown => write!(f, "unknown"),
        }
    }
}

#[derive(Debug, Clone)]
struct AddrEntry {
    trusted: bool,
    banned_at: Option<Instant>,
}

impl AddrEntry {
    fn new_trusted() -> Self {
        Self {
            trusted: true,
            banned_at: None,
        }
    }

    fn new_banned_at(timestamp: Instant) -> Self {
        Self {
            trusted: false,
            banned_at: Some(timestamp),
        }
    }

    fn ban_at(&mut self, timestamp: Instant) {
        self.banned_at = Some(timestamp);
    }

    fn unban(&mut self) {
        self.banned_at = None;
    }

    fn set_trusted(&mut self, trusted: bool) {
        self.trusted = trusted;
    }

    fn is_banned(&self) -> bool {
        self.banned_at.is_some()
    }

    fn is_trusted(&self) -> bool {
        self.trusted
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Addrlist {
    entries: Arc<Mutex<HashMap<IpAddr, AddrEntry>>>,
}

impl Addrlist {
    pub(crate) fn new() -> Self {
        Self {
            entries: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) fn new_with_trusted(trusted: impl Iterator<Item = IpAddr>) -> Self {
        let addrlist = Self::new();
        for ip in trusted {
            addrlist.add_trusted(ip);
        }
        addrlist
    }

    pub(crate) fn add_trusted(&self, addr: IpAddr) {
        let mut entries = self.entries.lock().unwrap();
        entries
            .entry(addr)
            .and_modify(|e| e.set_trusted(true))
            .or_insert_with(AddrEntry::new_trusted);
    }

    pub(crate) fn remove_trusted(&self, addr: IpAddr) {
        let mut entries = self.entries.lock().unwrap();
        if let Some(entry) = entries.get_mut(&addr) {
            entry.set_trusted(false);
            if !entry.is_banned() {
                entries.remove(&addr);
            }
        }
    }

    pub(crate) fn ban(&self, addr: IpAddr, timestamp: Instant) {
        let mut entries = self.entries.lock().unwrap();
        entries
            .entry(addr)
            .and_modify(|e| e.ban_at(timestamp))
            .or_insert_with(|| AddrEntry::new_banned_at(timestamp));
    }

    pub(crate) fn unban(&self, addr: &IpAddr) {
        let mut entries = self.entries.lock().unwrap();
        if let Some(entry) = entries.get_mut(addr) {
            entry.unban();
            if !entry.is_trusted() {
                entries.remove(addr);
            }
        }
    }

    pub(crate) fn banned_at(&self, addr: &IpAddr) -> Option<Instant> {
        self.entries
            .lock()
            .unwrap()
            .get(addr)
            .and_then(|entry| entry.banned_at)
    }

    pub(crate) fn status(&self, addr: &IpAddr) -> Status {
        match self.entries.lock().unwrap().get(addr) {
            Some(entry) if entry.is_banned() => Status::Banned,
            Some(entry) if entry.is_trusted() => Status::Trusted,
            _ => Status::Unknown,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use rstest::*;

    use super::*;

    #[fixture]
    fn addrlist() -> Addrlist {
        Addrlist::new()
    }

    #[fixture]
    fn ipv4_addr() -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1))
    }

    #[rstest]
    #[case::ipv4_banned(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)))]
    fn test_add_and_remove_banned(addrlist: Addrlist, #[case] addr: IpAddr) {
        assert!(matches!(addrlist.status(&addr), Status::Unknown));

        addrlist.ban(addr, Instant::now());
        assert!(matches!(addrlist.status(&addr), Status::Banned));

        addrlist.unban(&addr);
        assert!(matches!(addrlist.status(&addr), Status::Unknown));
    }

    #[rstest]
    #[case::ipv4_trusted(IpAddr::V4(Ipv4Addr::new(172, 16, 0, 1)))]
    fn test_add_and_remove_trusted(addrlist: Addrlist, #[case] addr: IpAddr) {
        assert!(matches!(addrlist.status(&addr), Status::Unknown));

        addrlist.add_trusted(addr);
        assert!(matches!(addrlist.status(&addr), Status::Trusted));

        addrlist.remove_trusted(addr);
        assert!(matches!(addrlist.status(&addr), Status::Unknown));
    }

    #[rstest]
    #[case::ipv4_precedence(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 1)))]
    fn test_banned_precedence_over_trusted(addrlist: Addrlist, #[case] addr: IpAddr) {
        addrlist.add_trusted(addr);
        addrlist.ban(addr, Instant::now());

        assert!(matches!(addrlist.status(&addr), Status::Banned));

        addrlist.unban(&addr);
        assert!(matches!(addrlist.status(&addr), Status::Trusted));
    }

    #[rstest]
    #[case::unknown_ipv4(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 1)))]
    fn test_status_unknown(addrlist: Addrlist, #[case] addr: IpAddr) {
        assert!(matches!(addrlist.status(&addr), Status::Unknown));
    }

    #[rstest]
    fn test_multiple_operations(addrlist: Addrlist) {
        let addr1 = IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1));
        let addr2 = IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8));
        let addr3 = IpAddr::V4(Ipv4Addr::new(9, 9, 9, 9));

        let ban_time = Instant::now();
        addrlist.ban(addr1, ban_time);
        addrlist.add_trusted(addr2);

        assert!(matches!(addrlist.status(&addr1), Status::Banned));
        assert!(matches!(addrlist.status(&addr2), Status::Trusted));
        assert!(matches!(addrlist.status(&addr3), Status::Unknown));

        addrlist.ban(addr2, ban_time);
        assert!(matches!(addrlist.status(&addr2), Status::Banned));
    }

    #[rstest]
    fn test_remove_nonexistent_addresses(addrlist: Addrlist, ipv4_addr: IpAddr) {
        addrlist.unban(&ipv4_addr);
        addrlist.remove_trusted(ipv4_addr);

        assert!(matches!(addrlist.status(&ipv4_addr), Status::Unknown));
    }

    #[rstest]
    fn test_banned_at(addrlist: Addrlist, ipv4_addr: IpAddr) {
        assert!(addrlist.banned_at(&ipv4_addr).is_none());

        let ban_timestamp = Instant::now();
        addrlist.ban(ipv4_addr, ban_timestamp);
        let banned_time = addrlist.banned_at(&ipv4_addr);
        assert!(banned_time.is_some());
        assert_eq!(banned_time.unwrap(), ban_timestamp);

        addrlist.unban(&ipv4_addr);
        assert!(addrlist.banned_at(&ipv4_addr).is_none());
    }
}
