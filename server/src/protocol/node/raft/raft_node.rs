
use serde::{Deserialize, Serialize};

use crate::DEFAULT_TCP_SOCKET_ADDR;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TcpNode {
    pub addr: String,
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
