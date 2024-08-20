pub mod error;
pub mod protocol;
pub(crate) mod util;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

pub use bytes;
pub use error::Error;
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TimestampSec(u64);
impl_codec!(
    struct TimestampSec(u64)
);
impl TimestampSec {
    pub fn now() -> Self {
        Self(crate::util::timestamp_sec())
    }
}

pub const DEFAULT_TCP_PORT: u16 = 9559;
pub const DEFAULT_TCP_ADDR: IpAddr = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
pub const DEFAULT_TCP_SOCKET_ADDR: SocketAddr = SocketAddr::new(DEFAULT_TCP_ADDR, DEFAULT_TCP_PORT);
