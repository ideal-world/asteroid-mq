use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
const ELECTION_TIMEOUT: Duration = Duration::from_secs(1);
use bytes::Bytes;

use crate::{impl_codec, protocol::{codec::CodecType, endpoint::Message}};

use super::{
    event::{N2nEventKind, N2nPacket},
    Node, NodeId, NodeRef, NodeSnapshot,
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
    pub ack_map: HashMap<RaftLogIndex, u64>,
}

impl LeaderState {
    pub fn new(node_ref: NodeRef, interval: Duration, term: RaftTerm) -> Self {
        let hb_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval);
            loop {
                let Some(node) = node_ref.upgrade() else {
                    break;
                };
                node.cluster_wise_broadcast_packet(N2nPacket::raft_heartbeat(Heartbeat { term }));
                interval.tick().await;
            }
        });
        Self {
            hb_task,
            term,
            ack_map: Default::default(),
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
        }
    }
    pub fn record_hb(&mut self, instant: Instant, timeout: Duration) {
        self.latest_heartbeat = Some(instant);
        let reporter = self.timeout_reporter.clone();
        self.hb_timeout_task.abort();
        self.hb_timeout_task = tokio::spawn(async move {
            tokio::time::sleep(timeout).await;
            let _ = reporter.send_async(()).await;
        });
    }
    pub fn from_new_leader(
        leader: NodeId,
        timeout: Duration,
        timeout_reporter: flume::Sender<()>,
    ) -> Self {
        let mut this = Self::new(timeout, timeout_reporter);
        this.leader = Some(leader);
        this
    }
}
#[derive(Debug)]
pub struct CandidateState {
    pub votes: u64,
    pub vote_timeout_task: tokio::task::JoinHandle<()>,
}

impl CandidateState {
    pub fn new(timeout: Duration, timeout_reporter: flume::Sender<()>) -> Self {
        let task = tokio::spawn(async move {
            tokio::time::sleep(timeout).await;
            let _ = timeout_reporter.send(());
        });
        Self {
            votes: 0,
            vote_timeout_task: task,
        }
    }
}

impl Drop for CandidateState {
    fn drop(&mut self) {
        self.vote_timeout_task.abort();
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
        matches!(self, RaftRole::Follower(_))
    }
    pub fn is_candidate(&self) -> bool {
        matches!(self, RaftRole::Candidate(_))
    }
    pub fn is_ready(&self) -> bool {
        match self {
            RaftRole::Leader(_) => true,
            RaftRole::Follower(state) => state.leader.is_some(),
            RaftRole::Candidate(_) => false,
        }
    }
}
#[derive(Debug, Clone)]
pub struct IndexedLog {
    pub index: RaftLogIndex,
    pub entry: LogEntry,
}
impl PartialEq for IndexedLog {
    fn eq(&self, other: &Self) -> bool {
        self.index.0 == other.index.0
    }
}
impl Eq for IndexedLog {}
impl Ord for IndexedLog {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.index.0.cmp(&other.index.0)
    }
}
impl PartialOrd for IndexedLog {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
#[derive(Debug, Default, Clone)]
pub struct PendingLogQueue {
    heap: BinaryHeap<Reverse<IndexedLog>>,
    resolved: HashSet<RaftLogIndex>,
}

impl PendingLogQueue {
    pub fn resolve(&mut self, index: RaftLogIndex) {
        self.resolved.insert(index);
    }

    pub fn blocking_pop_log(&mut self) -> Option<IndexedLog> {
        let front = self.front_index()?;
        if self.resolved.contains(&front) {
            self.pop_log()
        } else {
            None
        }
    }
    pub fn pop_log(&mut self) -> Option<IndexedLog> {
        self.heap.pop().map(|i| i.0)
    }
    pub fn push_log(&mut self, log: IndexedLog) {
        self.heap.push(Reverse(log));
    }
    pub fn front_index(&self) -> Option<RaftLogIndex> {
        self.heap.peek().map(|i| i.0.index)
    }
}

#[derive(Debug)]
pub struct RaftState {
    pub term: RaftTerm,
    pub index: RaftLogIndex,
    pub timeout_reporter: flume::Sender<()>,
    pub node_ref: NodeRef,
    pub role: RaftRole,
    pub pending_logs: PendingLogQueue,
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
    pub fn term_timeout(&mut self) -> Option<Vote> {
        match &mut self.role {
            RaftRole::Leader(_) => None,
            RaftRole::Follower(_) | RaftRole::Candidate(_) => {
                let node = self.node_ref.upgrade()?;
                self.term = self.term.next();
                if node.cluster_size() == 1 {
                    self.set_role(RaftRole::Leader(LeaderState::new(
                        self.node_ref.clone(),
                        ELECTION_TIMEOUT,
                        self.term,
                    )));
                    return None;
                }
                self.set_role(RaftRole::Candidate(CandidateState::new(
                    ELECTION_TIMEOUT,
                    self.timeout_reporter.clone(),
                )));
                Some(Vote { term: self.term })
            }
        }
    }
    pub fn set_role(&mut self, role: RaftRole) {
        tracing::debug!("set role: {:?}", role);
        self.role = role;
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
        *self
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


impl LogEntry {
    pub fn delegate_message(message: Message) -> Self {
        Self {
            kind: N2nEventKind::DelegateMessage,
            payload: message.encode_to_bytes(),
        }
    }
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
    pub term: RaftTerm,
    pub entry: LogEntry,
}

impl LogAppend {
    pub fn replicate(&self, index: RaftLogIndex) -> LogReplicate {
        LogReplicate {
            index,
            term: self.term,
            entry: self.entry.clone(),
        }
    }
}

impl_codec!(
    struct LogAppend {
        term: RaftTerm,
        entry: LogEntry,
    }
);
#[derive(Debug)]
pub struct LogReplicate {
    pub index: RaftLogIndex,
    pub term: RaftTerm,
    pub entry: LogEntry,
}

impl_codec!(
    struct LogReplicate {
        index: RaftLogIndex,
        term: RaftTerm,
        entry: LogEntry,
    }
);

pub struct LogAck {
    pub term: RaftTerm,
    pub index: RaftLogIndex,
}

impl_codec!(
    struct LogAck {
        term: RaftTerm,
        index: RaftLogIndex,
    }
);

pub struct LogCommit {
    pub term: RaftTerm,
    pub index: RaftLogIndex,
}

impl_codec!(
    struct LogCommit {
        term: RaftTerm,
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

pub struct RaftSnapshot {
    pub term: RaftTerm,
    pub index: RaftLogIndex,
    pub data: NodeSnapshot,
}

impl_codec!(
    struct RaftSnapshot {
        term: RaftTerm,
        index: RaftLogIndex,
        data: NodeSnapshot,
    }
);
