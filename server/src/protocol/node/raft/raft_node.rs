use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use crate::DEFAULT_TCP_SOCKET_ADDR;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Copy)]
pub struct TcpNode {
    pub addr: SocketAddr,
}

impl TcpNode {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }
}

impl Default for TcpNode {
    fn default() -> Self {
        Self::new(DEFAULT_TCP_SOCKET_ADDR)
    }
}
