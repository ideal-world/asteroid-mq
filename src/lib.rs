use connection::ConnectionRef;
use event::{Event, EventHandler};

pub mod codec;
pub mod connection;
pub mod error;
pub mod protocol;
pub mod event;
pub struct Endpoint {
    pub connection: ConnectionRef,
    pub address: EndpointAddr,
}



#[derive(Debug, Clone, Copy)]
pub struct EndpointAddr {
    pub bytes: [u8; 16],
}
