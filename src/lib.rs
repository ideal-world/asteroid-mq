use bytes::Bytes;
use connection::ConnectionRef;
use event::{Event, EventHandler};
use futures_util::StreamExt;
use zeromq::{Socket, SocketRecv, SocketSend, ZmqMessage};
use std::{str::FromStr, time::Duration};
use tokio::io::AsyncWriteExt;
use tracing::Level;

pub mod codec;
pub mod connection;
pub mod error;
pub mod event;
pub mod protocol;
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

#[tokio::test]
async fn test_zmq() {
    use zeromq::Endpoint;
    let ep = "tcp://localhost:4499";
    use zeromq::RouterSocket;
    use zeromq::DealerSocket;
    let mut socket = RouterSocket::with_options(Default::default());
    let _ = socket.connect(ep).await;
    let x = socket.recv().await;
}
