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

use std::{collections::VecDeque, net::IpAddr, pin::Pin, sync::Arc, time::Duration};

use monoio::{
    select,
    time::{sleep, Instant, Sleep},
};
use tokio::sync::mpsc;
use tracing::{debug, info, trace};

use crate::addrlist::Addrlist;

pub(crate) async fn task(
    addrlist: Arc<Addrlist>,
    mut banned_connections: mpsc::UnboundedReceiver<(IpAddr, Instant)>,
    ban_duration: Duration,
) {
    let mut queue: VecDeque<(IpAddr, Instant)> = VecDeque::new();
    let mut ticker: Option<Pin<Box<Sleep>>> = None;
    loop {
        select! {
           _ = async {
                match &mut ticker {
                    Some(t) => t.await,
                    None => std::future::pending().await,
                }
            } => {
                while let Some((addr, timestamp)) = queue.front() {
                    // we track two timestamps
                    // 1. when ip address was last banned
                    // 2. and when it was inserted into the queue for expiry
                    // we separate them so that ban can be re-newed before original ban has expired
                    if timestamp.elapsed() >= ban_duration {
                        addrlist.banned_at(addr).is_some_and(|banned_at| banned_at.elapsed() >= ban_duration)
                            .then(|| {
                                debug!(%addr, "unban");
                                addrlist.unban(addr)
                            });
                        queue.pop_front();
                    } else {
                        trace!("renew ticker");
                        ticker = Some(Box::pin(sleep(ban_duration - timestamp.elapsed())));
                        break;
                    }
                }
                queue.is_empty().then(|| ticker = None);
            },
            banned = banned_connections.recv() => {
                match banned {
                    Some((addr, timestamp)) => {
                        debug!(%addr, ?timestamp, "ban");
                        queue.push_back((addr, timestamp));
                        if ticker.is_none() {
                            ticker = Some(Box::pin(sleep(ban_duration)));
                        }
                    },
                    None => {
                        info!("task for ban expiry is stopped");
                        break;
                    },
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{net::IpAddr, pin::pin, sync::Arc, task::Poll, time::Duration};

    use futures::poll;
    use monoio::time::{sleep, Instant};
    use rstest::*;
    use tokio::sync::mpsc;

    use crate::{
        addrlist::{Addrlist, Status},
        ban_expiry,
    };

    // NOTE(dshulyak)
    // monoio doesn't support advancing time manually hence we have to use sleep in the tests below.
    // also the futures are manually polled, so expect tests to fail if logic in select! above was changed

    #[fixture]
    fn addrlist() -> Arc<Addrlist> {
        Arc::new(Addrlist::new())
    }

    #[fixture]
    fn ban_duration() -> Duration {
        Duration::from_millis(100)
    }

    #[fixture]
    fn expiry_channel() -> (
        mpsc::UnboundedSender<(IpAddr, Instant)>,
        mpsc::UnboundedReceiver<(IpAddr, Instant)>,
    ) {
        mpsc::unbounded_channel()
    }

    #[fixture]
    fn addr() -> IpAddr {
        "127.0.0.1".parse().unwrap()
    }

    #[rstest]
    #[monoio::test(timer_enabled = true)]
    async fn test_no_work(
        addrlist: Arc<Addrlist>,
        #[from(expiry_channel)] (_tx, rx): (
            mpsc::UnboundedSender<(IpAddr, Instant)>,
            mpsc::UnboundedReceiver<(IpAddr, Instant)>,
        ),
        ban_duration: Duration,
    ) {
        let mut ban_expiry_future = pin!(ban_expiry::task(addrlist.clone(), rx, ban_duration));
        assert_eq!(poll!(&mut ban_expiry_future), Poll::Pending);
    }

    #[rstest]
    #[monoio::test(timer_enabled = true)]
    async fn test_single_expiry(
        addrlist: Arc<Addrlist>,
        #[from(expiry_channel)] (tx, rx): (
            mpsc::UnboundedSender<(IpAddr, Instant)>,
            mpsc::UnboundedReceiver<(IpAddr, Instant)>,
        ),
        ban_duration: Duration,
        addr: IpAddr,
    ) {
        let now = Instant::now();
        addrlist.ban(&addr, now);
        tx.send((addr, now)).unwrap();

        let mut ban_expiry_future = pin!(ban_expiry::task(addrlist.clone(), rx, ban_duration));
        assert_eq!(poll!(&mut ban_expiry_future), Poll::Pending);

        assert_eq!(addrlist.status(&addr), Status::Banned);

        sleep(ban_duration + Duration::from_millis(1)).await;
        assert_eq!(poll!(&mut ban_expiry_future), Poll::Pending);
        assert_eq!(addrlist.status(&addr), Status::Unknown);
    }

    #[rstest]
    #[monoio::test(timer_enabled = true)]
    async fn test_renewed_expiry(
        addrlist: Arc<Addrlist>,
        #[from(expiry_channel)] (tx, rx): (
            mpsc::UnboundedSender<(IpAddr, Instant)>,
            mpsc::UnboundedReceiver<(IpAddr, Instant)>,
        ),
        ban_duration: Duration,
        addr: IpAddr,
    ) {
        let now = Instant::now();
        addrlist.ban(&addr, now);
        tx.send((addr, now)).unwrap();

        let mut ban_expiry_future = pin!(ban_expiry::task(addrlist.clone(), rx, ban_duration));
        assert_eq!(poll!(&mut ban_expiry_future), Poll::Pending);
        assert_eq!(addrlist.status(&addr), Status::Banned);

        sleep(ban_duration / 2).await;

        let now = Instant::now();
        addrlist.ban(&addr, now);
        tx.send((addr, now)).unwrap();
        assert_eq!(poll!(&mut ban_expiry_future), Poll::Pending);

        sleep(ban_duration / 2 + Duration::from_millis(1)).await;
        assert_eq!(poll!(&mut ban_expiry_future), Poll::Pending);
        assert_eq!(addrlist.status(&addr), Status::Banned);

        sleep(ban_duration / 2 + Duration::from_millis(1)).await;
        assert_eq!(poll!(&mut ban_expiry_future), Poll::Pending);
        assert_eq!(addrlist.status(&addr), Status::Unknown);
    }
}
