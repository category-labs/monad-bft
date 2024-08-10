pub mod epoll;
pub mod event_loop;
pub mod network;

// Helper logic to automatically restart system calls when EINTR is returned.
// Inspired by cvt() / cvt_r() in sys::std::unix.
trait IsMinusOne {
    fn is_minus_one(&self) -> bool;
}

macro_rules! impl_is_minus_one {
    ($($t:ident)*) => ($(impl IsMinusOne for $t {
        fn is_minus_one(&self) -> bool {
            *self == -1
        }
    })*)
}

impl_is_minus_one! { i8 i16 i32 i64 isize }

pub fn retry_eintr<T, F>(mut f: F) -> T
where
    T: IsMinusOne,
    F: FnMut() -> T,
{
    loop {
        let ret = f();

        if !ret.is_minus_one()
            || std::io::Error::last_os_error().kind() != std::io::ErrorKind::Interrupted
        {
            return ret;
        }
    }
}
