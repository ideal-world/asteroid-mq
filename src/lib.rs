use bytes::Bytes;
use connection::ConnectionRef;
use event::{Event, EventHandler};
use futures_util::StreamExt;
use std::{str::FromStr, time::Duration};
use tokio::io::AsyncWriteExt;
use tracing::Level;

pub mod codec;
pub mod connection;
pub mod error;
pub mod event;
pub mod protocol;
pub(crate) mod util;

pub struct Endpoint {
    pub connection: ConnectionRef,
    pub address: EndpointAddr,
}

impl Endpoint {
    pub async fn send() {
        unimplemented!()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EndpointAddr {
    pub bytes: [u8; 16],
}
