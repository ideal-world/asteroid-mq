pub mod error;
pub mod protocol;
pub(crate) mod util;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

pub use bytes;
pub use error::Error;
pub use openraft;
pub type Result<T> = std::result::Result<T, Error>;
pub mod prelude {
    pub use crate::error::Error;
    pub use crate::model::*;
    pub use crate::protocol::interest::{Interest, Subject};
    pub use crate::protocol::message::*;
    pub use crate::protocol::node::durable_message::{
        Durable, DurableError, DurableMessage, DurableMessageQuery, DurableService,
        MessageDurableConfig,
    };
    pub use crate::protocol::node::raft::state_machine::topic::config::*;
    pub use crate::protocol::node::{Node, NodeConfig, NodeId};
    pub use crate::util::MaybeBase64Bytes;
}
pub use asteroid_mq_model as model;
pub const DEFAULT_TCP_PORT: u16 = 9559;
pub const DEFAULT_TCP_ADDR: IpAddr = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
pub const DEFAULT_TCP_SOCKET_ADDR: SocketAddr = SocketAddr::new(DEFAULT_TCP_ADDR, DEFAULT_TCP_PORT);
