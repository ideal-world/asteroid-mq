use bytes::Bytes;

use crate::impl_codec;

use super::NodeId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum RaftRole {
    Leader = 0,
    Follower = 1,
    Candidate = 2,
}

impl RaftRole {
    pub fn is_leader(self) -> bool {
        matches!(self, Self::Leader)
    }
}

impl_codec! {
    enum RaftRole {
        Leader = 0,
        Follower = 1,
        Candidate = 2,
    }
}

#[derive(Debug, Clone)]
pub struct RaftState {
    pub term: RaftTerm,
    pub role: RaftRole,
    pub leader: Option<NodeId>,
}

impl_codec!(
    struct RaftState {
        term: RaftTerm,
        role: RaftRole,
        leader: Option<NodeId>,
    }
);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct RaftTerm(u64);
impl RaftTerm {
    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}
impl_codec!(
    struct RaftTerm(u64)
);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RaftLogIndex(u64);

impl_codec!(
    struct RaftLogIndex(u64)
);

pub struct LogEntry {
    pub term: RaftTerm,
    pub index: RaftLogIndex,
    pub data: Bytes,
}

pub struct LeaderRequestVote {
    pub term: RaftTerm,
}

pub struct Vote {}
