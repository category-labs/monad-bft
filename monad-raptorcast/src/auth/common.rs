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
    future::Future,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};

use bytes::{Bytes, BytesMut};
use tokio::time::Sleep;
use tracing::{trace, warn};
use zerocopy::IntoBytes;

use super::protocol::AuthenticationProtocol;

pub struct AuthenticatedTimerFuture<'a, AP: AuthenticationProtocol> {
    auth_protocol: &'a mut AP,
    auth_timer: &'a mut Option<(Pin<Box<Sleep>>, Instant)>,
}

impl<'a, AP: AuthenticationProtocol> AuthenticatedTimerFuture<'a, AP> {
    pub fn new(
        auth_protocol: &'a mut AP,
        auth_timer: &'a mut Option<(Pin<Box<Sleep>>, Instant)>,
    ) -> Self {
        Self {
            auth_protocol,
            auth_timer,
        }
    }
}

impl<AP: AuthenticationProtocol> Future for AuthenticatedTimerFuture<'_, AP> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        trace!("polling wireauth timer");

        loop {
            let Some(deadline) = self.auth_protocol.next_deadline() else {
                return Poll::Pending;
            };

            let now = Instant::now();
            if deadline <= now {
                if let Some(d) = now.checked_duration_since(deadline) {
                    if d > std::time::Duration::from_millis(100) {
                        warn!(delta_ms = d.as_millis(), "slow polling wireauth timer");
                    }
                }

                self.auth_protocol.tick();
                return Poll::Ready(());
            }

            let should_update_timer = self
                .auth_timer
                .as_ref()
                .is_none_or(|(_, stored_deadline)| deadline < *stored_deadline);
            if should_update_timer {
                *self.auth_timer = Some((
                    Box::pin(tokio::time::sleep_until(deadline.into())),
                    deadline,
                ));
            }

            let Some((sleep, _)) = self.auth_timer.as_mut() else {
                return Poll::Pending;
            };

            match sleep.as_mut().poll(cx) {
                Poll::Ready(()) => *self.auth_timer = None,
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

pub fn encrypt_packet<AP: AuthenticationProtocol>(
    auth_protocol: &mut AP,
    addr: &SocketAddr,
    plaintext: Bytes,
) -> Option<(SocketAddr, Bytes)> {
    let header_size = AP::HEADER_SIZE as usize;
    let mut packet = BytesMut::with_capacity(header_size + plaintext.len());
    packet.resize(header_size, 0);
    packet.extend_from_slice(&plaintext);

    match auth_protocol.encrypt_by_socket(addr, &mut packet[header_size..]) {
        Ok(header) => {
            let header_bytes = header.as_bytes();
            packet[..header_size].copy_from_slice(header_bytes);
            Some((*addr, packet.freeze()))
        }
        Err(e) => {
            warn!(addr=?addr, error=?e, "failed to encrypt message");
            None
        }
    }
}
