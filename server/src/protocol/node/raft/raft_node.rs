use serde::{Deserialize, Serialize};

use crate::DEFAULT_TCP_SOCKET_ADDR;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TcpNode {
    pub addr: String,
}

impl std::fmt::Debug for TcpNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("TcpNode").field(&self.addr).finish()
    }
}

impl TcpNode {
    pub fn new(addr: String) -> Self {
        Self { addr }
    }
}

impl Default for TcpNode {
    fn default() -> Self {
        Self::new(DEFAULT_TCP_SOCKET_ADDR.to_string())
    }
}
