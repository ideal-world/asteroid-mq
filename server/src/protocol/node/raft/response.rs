use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftResponse {
    pub result: Result<(), ()>,
}
