use std::future::Future;

use bytes::Bytes;
use openraft::raft::VoteRequest;

use crate::prelude::NodeId;

pub struct BidirectionalServiceConnection {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TypeCode(pub u16);

pub enum RaftRequestBody {
    Vote(VoteRequest<NodeId>),
}
pub struct RaftRequest {
    id: u64,
    code: TypeCode,
    data: Bytes,
}

pub struct RaftResponse {
    request_id: u64,
    data: Bytes,
}

pub struct RaftConnectionError {}
pub trait RaftConnection {
    fn request(
        &self,
        req: RaftRequestBody,
    ) -> impl Future<Output = Result<RaftResponse, RaftConnectionError>>;
}
