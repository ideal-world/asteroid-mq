use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use bytes::Bytes;

use crate::impl_codec;

use super::{
    event::{N2nEventKind, N2nPacket},
    Node, NodeId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum RaftRoleKind {
    Leader = 0,
    Follower = 1,
    Candidate = 2,
}

impl RaftRoleKind {
    pub fn is_leader(self) -> bool {
        matches!(self, Self::Leader)
    }
    pub fn is_follower(self) -> bool {
        matches!(self, Self::Follower)
    }
}

impl_codec! {
    enum RaftRoleKind {
        Leader = 0,
        Follower = 1,
        Candidate = 2,
    }
}

#[derive(Debug, Clone)]
pub struct RaftInfo {
    pub term: RaftTerm,
    pub index: RaftLogIndex,
    pub role: RaftRoleKind,
}

impl_codec!(
    struct RaftInfo {
        term: RaftTerm,
        index: RaftLogIndex,
        role: RaftRoleKind,
    }
);

#[derive(Debug)]
pub struct LeaderState {
    hb_task: tokio::task::JoinHandle<()>,
    pub term: RaftTerm,
    pub logs: HashMap<u64, UncommittedLog>,
}

impl LeaderState {
    pub fn new(tx: flume::Sender<N2nPacket>, interval: Duration, term: RaftTerm) -> Self {
        let hb_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval);
            loop {
                let result = tx
                    .send_async(N2nPacket::raft_heartbeat(Heartbeat { term }))
                    .await;
                if result.is_err() {
                    break;
                }
                interval.tick().await;
            }
        });
        Self {
            hb_task,
            term,
            logs: Default::default(),
        }
    }
}

impl Drop for LeaderState {
    fn drop(&mut self) {
        self.hb_task.abort();
    }
}

#[derive(Debug)]
pub struct FollowerState {
    pub latest_heartbeat: Option<Instant>,
    pub leader: Option<NodeId>,
    pub hb_timeout_task: tokio::task::JoinHandle<()>,
    pub timeout_reporter: flume::Sender<()>,
    pub logs: HashMap<u64, LogReplicate>,
}

impl Drop for FollowerState {
    fn drop(&mut self) {
        self.hb_timeout_task.abort();
    }
}
impl FollowerState {
    pub fn new(timeout: Duration, timeout_reporter: flume::Sender<()>) -> Self {
        let task = {
            let timeout_reporter = timeout_reporter.clone();
            tokio::spawn(async move {
                tokio::time::sleep(timeout).await;
                timeout_reporter.send(());
            })
        };
        Self {
            latest_heartbeat: Default::default(),
            leader: None,
            hb_timeout_task: task,
            timeout_reporter,
            logs: Default::default(),
        }
    }
    pub fn record_hb(&mut self, instant: Instant, timeout: Duration) {
        self.latest_heartbeat = Some(instant);
        let reporter = self.timeout_reporter.clone();
        self.hb_timeout_task.abort();
        tokio::spawn(async move {
            tokio::time::sleep(timeout).await;
            reporter.send(());
        });
    }
    pub fn from_leader(
        leader: NodeId,
        timeout: Duration,
        timeout_reporter: flume::Sender<()>,
    ) -> Self {
        let mut this = Self::new(timeout, timeout_reporter);
        this.leader = Some(leader);
        this
    }
}
#[derive(Debug, Default)]
pub struct CandidateState {
    pub votes: u64,
    pub launch_vote_delay: Option<Duration>,
}

impl CandidateState {
    pub fn new() -> Self {
        Self {
            votes: 0,
            launch_vote_delay: None,
        }
    }
    pub fn from_delay(delay: Duration) -> Self {
        Self {
            votes: 0,
            launch_vote_delay: Some(delay),
        }
    }
}
#[derive(Debug)]
pub enum RaftRole {
    Leader(LeaderState),
    Follower(FollowerState),
    Candidate(CandidateState),
}

impl RaftRole {
    pub fn is_leader(&self) -> bool {
        matches!(self, RaftRole::Leader(_))
    }
    pub fn is_follower(&self) -> bool {
        matches!(self, RaftRole::Leader(_))
    }
}

#[derive(Debug)]
pub struct RaftState {
    pub term: RaftTerm,
    pub index: RaftLogIndex,
    pub role: RaftRole,
    pub voted_for: Option<(NodeId, RaftTerm)>,
}
impl RaftState {
    pub fn get_info(&self) -> RaftInfo {
        RaftInfo {
            term: self.term,
            index: self.index,
            role: match self.role {
                RaftRole::Leader(_) => RaftRoleKind::Leader,
                RaftRole::Follower(_) => RaftRoleKind::Follower,
                RaftRole::Candidate(_) => RaftRoleKind::Candidate,
            },
        }
    }
}
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct RaftLogIndex(u64);

impl RaftLogIndex {
    pub fn inc(&mut self) -> Self {
        self.0 = self.0.wrapping_add(1);
        self.clone()
    }
}

impl_codec!(
    struct RaftLogIndex(u64)
);
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub kind: N2nEventKind,
    pub payload: Bytes,
}

impl_codec!(
    struct LogEntry {
        kind: N2nEventKind,
        payload: Bytes,
    }
);

pub struct LeaderRequestVote {
    pub term: RaftTerm,
}

pub struct Vote {
    pub term: RaftTerm,
}
impl_codec!(
    struct Vote {
        term: RaftTerm,
    }
);

pub struct RequestVote {
    pub term: RaftTerm,
    pub index: RaftLogIndex,
}

impl_codec!(
    struct RequestVote {
        term: RaftTerm,
        index: RaftLogIndex,
    }
);

pub struct Heartbeat {
    pub term: RaftTerm,
}
impl_codec!(
    struct Heartbeat {
        term: RaftTerm,
    }
);

#[derive(Debug, Clone)]
pub struct LogAppend {
    pub id: u64,
    pub term: RaftTerm,
    pub entry: LogEntry,
}

impl LogAppend {
    pub fn replicate(&self) -> LogReplicate {
        LogReplicate {
            id: self.id,
            term: self.term,
            entry: self.entry.clone(),
        }
    }
    pub fn uncommitted(&self, cluster_size: u64) -> UncommittedLog {
        UncommittedLog {
            term: self.term,
            id: self.id,
            entry: self.entry.clone(),
            ack_expect: cluster_size,
            ack_count: 0,
        }
    }
}

impl_codec!(
    struct LogAppend {
        id: u64,
        term: RaftTerm,
        entry: LogEntry,
    }
);
#[derive(Debug)]
pub struct LogReplicate {
    pub id: u64,
    pub term: RaftTerm,
    pub entry: LogEntry,
}

impl_codec!(
    struct LogReplicate {
        id: u64,
        term: RaftTerm,
        entry: LogEntry,
    }
);

pub struct LogAck {
    pub id: u64,
}

impl_codec!(
    struct LogAck {
        id: u64,
    }
);

pub struct LogCommit {
    pub id: u64,
    pub index: RaftLogIndex,
}

impl_codec!(
    struct LogCommit {
        id: u64,
        index: RaftLogIndex,
    }
);

#[derive(Debug, Clone)]
pub struct UncommittedLog {
    pub term: RaftTerm,
    pub id: u64,
    pub entry: LogEntry,
    pub ack_count: u64,
    pub ack_expect: u64,
}
