pub mod error;
pub mod event_handler;
pub mod protocol;
pub(crate) mod util;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

pub use bytes;
pub use error::Error;
pub use openraft;
use serde::{Deserialize, Serialize};
pub type Result<T> = std::result::Result<T, Error>;
pub mod prelude {
    pub use crate::error::Error;
    pub use crate::event_handler::{Event, EventAttribute, EventCodec, HandleEventLoop, Handler};
    pub use crate::protocol::endpoint::{EndpointAddr, LocalEndpoint, LocalEndpointRef};
    pub use crate::protocol::interest::{Interest, Subject};
    pub use crate::protocol::message::*;
    pub use crate::protocol::node::{Node, NodeConfig, NodeId};
    pub use crate::protocol::topic::{
        durable_message::{
            Durable, DurableError, DurableService, DurableMessage, MessageDurableConfig,
        },
        Topic, TopicCode,
    };
    pub use crate::protocol::node::raft::state_machine::topic::config::*;
    pub use crate::util::MaybeBase64Bytes;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TimestampSec(u64);

impl TimestampSec {
    pub fn now() -> Self {
        Self(crate::util::timestamp_sec())
    }
}

pub const DEFAULT_TCP_PORT: u16 = 9559;
pub const DEFAULT_TCP_ADDR: IpAddr = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
pub const DEFAULT_TCP_SOCKET_ADDR: SocketAddr = SocketAddr::new(DEFAULT_TCP_ADDR, DEFAULT_TCP_PORT);
