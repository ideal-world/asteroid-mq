pub mod error;
pub mod protocol;
pub(crate) mod util;
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
