use core::time;
use std::mem;

use log::debug;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct RawFd(pub i32);

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct EpollToken(pub u64);

#[repr(C)]
#[derive(Copy, Clone)]
pub struct EpollEvent {
    event: libc::epoll_event,
}

impl EpollEvent {
    pub fn new() -> Self {
        Self {
            event: libc::epoll_event { events: 0, u64: 0 },
        }
    }

    pub fn token(&self) -> EpollToken {
        EpollToken(self.event.u64)
    }

    pub fn events_flags(&self) -> u32 {
        self.event.events
    }
}

impl Default for EpollEvent {
    fn default() -> Self {
        Self::new()
    }
}

pub struct EpollFd<const N: usize> {
    fd: i32,
    pub events: [EpollEvent; N],
}

impl<const N: usize> EpollFd<N> {
    pub fn new() -> std::io::Result<Self> {
        let fd = unsafe { libc::epoll_create1(0) };
        if fd == -1 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(Self {
                fd,
                events: [EpollEvent::new(); N],
            })
        }
    }

    pub fn register(&self, token: EpollToken, fd: RawFd) -> std::io::Result<()> {
        let mut ev = libc::epoll_event {
            events: libc::EPOLLIN as u32,
            u64: token.0,
        };

        let ret = unsafe { libc::epoll_ctl(self.fd, libc::EPOLL_CTL_ADD, fd.0, &mut ev) };
        if ret == -1 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    // don't use timeouts here, expect users to register timerfd if timeouts are desired
    pub fn wait(&mut self) -> std::io::Result<Vec<EpollEvent>> {
        let nfds = unsafe {
            libc::epoll_wait(
                self.fd,
                self.events.as_mut_ptr() as *mut libc::epoll_event,
                self.events.len() as i32,
                -1,
            )
        };

        if nfds == -1 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(self.events[0..nfds as usize].to_vec())
        }
    }
}

#[derive(Copy, Clone)]
pub struct EventFd {
    fd: i32,
}

impl EventFd {
    pub fn new() -> std::io::Result<Self> {
        let efd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK) };
        if efd == -1 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(Self { fd: efd })
        }
    }

    // u64 can be written into eventfd to pass to the receiver
    pub fn trigger_event(&self, n: u64) -> std::io::Result<()> {
        let data = &n as *const u64;
        let s = unsafe { libc::write(self.fd, data as *const libc::c_void, mem::size_of::<u64>()) };
        if s == -1 {
            return Err(std::io::Error::last_os_error());
        }
        if s != mem::size_of::<u64>() as isize {
            panic!("write into eventfd should not fail");
        }

        Ok(())
    }

    // returns the u64 from the writer and an isize for the return value of the read syscall
    pub fn handle_event(&self) -> (u64, isize) {
        let mut n: u64 = 0;
        let data = &mut n as *mut u64;
        let s = unsafe { libc::read(self.fd, data as *mut libc::c_void, mem::size_of::<u64>()) };
        debug!("got tx event");

        (n, s)
    }

    pub fn get_fd(&self) -> RawFd {
        RawFd(self.fd)
    }
}

#[derive(Copy, Clone)]
pub struct TimerFd {
    fd: i32,
}

impl TimerFd {
    pub fn new() -> std::io::Result<Self> {
        let tfd = unsafe { libc::timerfd_create(libc::CLOCK_MONOTONIC, libc::TFD_NONBLOCK) };
        if tfd == -1 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(Self { fd: tfd })
        }
    }

    pub fn arm_oneshot_timer(&self, time: time::Duration) -> std::io::Result<()> {
        let interval = libc::itimerspec {
            it_interval: libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            },
            it_value: libc::timespec {
                tv_sec: time.as_secs() as i64,
                tv_nsec: time.subsec_nanos() as i64,
            },
        };

        let interval_data = &interval as *const libc::itimerspec;

        let s = unsafe { libc::timerfd_settime(self.fd, 0, interval_data, std::ptr::null_mut()) };

        Ok(())
    }

    pub fn handle_event(&self) -> isize {
        let mut n: u64 = 0;
        let data = &mut n as *mut u64;
        let s = unsafe { libc::read(self.fd, data as *mut libc::c_void, mem::size_of::<u64>()) };
        debug!("handled timer");
        s
    }

    pub fn get_fd(&self) -> RawFd {
        RawFd(self.fd)
    }
}
