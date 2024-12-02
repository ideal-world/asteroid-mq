use std::collections::HashSet;

use codec::CodecKind;
use packet::Auth;
pub mod auth;
pub mod codec;
pub mod connection;
pub mod packet;
pub mod middleware;

use super::NodeId;
pub use asteroid_mq_model::{
    EdgeEndpointOffline, EdgeEndpointOnline, EdgeError, EdgeErrorKind, EdgeMessage,
    EdgeMessageHeader, EdgePayload, EdgePush, EdgeRequest, EdgeRequestEnum, EdgeResponse,
    EdgeResponseEnum, EdgeResult,
};

#[derive(Debug, Clone)]
pub struct EdgeConfig {
    pub supported_codec_kinds: HashSet<CodecKind>,
    pub peer_id: NodeId,
    pub peer_auth: Auth,
}
