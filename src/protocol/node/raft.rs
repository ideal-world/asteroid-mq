use std::{
    borrow::Cow,
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, HashSet},
    time::{Duration, Instant},
};
const ELECTION_TIMEOUT: Duration = Duration::from_secs(1);
use bytes::Bytes;

use crate::{
    impl_codec,
    protocol::{
        codec::CodecType,
        endpoint::{DelegateMessage, EndpointInterest, EndpointOffline, EndpointOnline, SetState},
        topic::durable_message::{LoadTopic, UnloadTopic},
    },
};

use super::{
    event::{EventKind, N2nPacket},
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
                let _ = timeout_reporter.send(());
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
    pub trace_id: RaftTraceId,
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
        tracing::trace!(?index, "log resolved");
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
    pub commit_hooks: HashMap<RaftTraceId, flume::Sender<Result<RaftLogIndex, RaftCommitError>>>,
    pub voted_for: Option<(NodeId, RaftTerm)>,
}
pub struct RaftCommitError {
    kind: RaftCommitErrorKind,
    reason: Option<Cow<'static, str>>,
    context: Cow<'static, str>,
}

impl RaftCommitError {
    pub fn process_failed(context: impl Into<Cow<'static, str>>) -> Self {
        Self {
            kind: RaftCommitErrorKind::ProcessFailed,
            reason: None,
            context: context.into(),
        }
    }
    pub fn with_reason(mut self, reason: impl Into<Cow<'static, str>>) -> Self {
        self.reason = Some(reason.into());
        self
    }
}
impl std::fmt::Debug for RaftCommitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftCommitError")
            .field("kind", &self.kind)
            .field("context", &self.context)
            .finish()
    }
}

#[derive(Debug)]
pub enum RaftCommitErrorKind {
    MissingReporter = 0,
    ProcessFailed = 1,
}

pin_project_lite::pin_project! {
    pub struct CommitHandle {
        #[pin]
        recv: flume::r#async::RecvFut<'static, Result<RaftLogIndex, RaftCommitError>>
    }
}

impl std::future::Future for CommitHandle {
    type Output = Result<RaftLogIndex, RaftCommitError>;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.project();
        this.recv.poll(cx).map(|result| {
            result.map_err(|_| RaftCommitError {
                kind: RaftCommitErrorKind::MissingReporter,
                context: "commit hook receiver dropped".into(),
                reason: None,
            })?
        })
    }
}

impl RaftState {
    pub fn add_commit_handle(&mut self, trace_id: RaftTraceId) -> CommitHandle {
        let (tx, rx) = flume::bounded(1);
        let _ = self.commit_hooks.insert(trace_id, tx);
        CommitHandle {
            recv: rx.into_recv_async(),
        }
    }
    pub fn resolve_commit_hook(&mut self, trace_id: RaftTraceId, index: RaftLogIndex) {
        if let Some(tx) = self.commit_hooks.remove(&trace_id) {
            let _ = tx.send(Ok(index));
        }
    }
    pub fn commit(&mut self, indexed: IndexedLog, node: &Node) {
        node.apply_log(indexed.entry);
        self.resolve_commit_hook(indexed.trace_id, indexed.index);
    }

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
    pub fn term_timeout(&mut self) -> Option<RequestVote> {
        match &mut self.role {
            RaftRole::Leader(_) => None,
            RaftRole::Follower(_) | RaftRole::Candidate(_) => {
                let node = self.node_ref.upgrade()?;
                let cluster_size = node.cluster_size();
                if cluster_size == 1 {
                    self.set_role(RaftRole::Leader(LeaderState::new(
                        self.node_ref.clone(),
                        ELECTION_TIMEOUT,
                        self.term,
                    )));
                    None
                } else if cluster_size == 0 {
                    self.set_role(RaftRole::Follower(FollowerState::new(
                        crate::util::random_duration_ms(Node::RAFT_RANDOM_DURATION_RANGE),
                        self.timeout_reporter.clone(),
                    )));
                    None
                } else {
                    self.set_role(RaftRole::Candidate(CandidateState::new(
                        ELECTION_TIMEOUT,
                        self.timeout_reporter.clone(),
                    )));
                    self.term = self.term.next();
                    Some(RequestVote {
                        term: self.term,
                        index: self.index,
                    })
                }
            }
        }
    }
    pub fn set_role(&mut self, role: RaftRole) {
        tracing::info!("set role: {:?}", role);
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
    pub fn next(&self) -> Self {
        Self(self.0.wrapping_add(1))
    }
}

impl_codec!(
    struct RaftLogIndex(u64)
);
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub kind: EventKind,
    pub payload: Bytes,
}

impl LogEntry {
    pub fn create_append(self, term: RaftTerm) -> LogAppend {
        LogAppend::new(self, term)
    }
    pub fn create_indexed(self, index: RaftLogIndex) -> IndexedLog {
        IndexedLog {
            index,
            entry: self,
            trace_id: RaftTraceId::new_snowflake(),
        }
    }
    pub fn create_replicate(self, index: RaftLogIndex, term: RaftTerm) -> LogReplicate {
        LogReplicate {
            index,
            term,
            entry: self,
            trace_id: RaftTraceId::new_snowflake(),
        }
    }
    pub fn load_topic(load: LoadTopic) -> Self {
        Self {
            kind: EventKind::LoadTopic,
            payload: load.encode_to_bytes(),
        }
    }
    pub fn unload_topic(unload: UnloadTopic) -> Self {
        Self {
            kind: EventKind::UnloadTopic,
            payload: unload.encode_to_bytes(),
        }
    }
    pub fn delegate_message(message: DelegateMessage) -> Self {
        Self {
            kind: EventKind::DelegateMessage,
            payload: message.encode_to_bytes(),
        }
    }
    pub fn ep_online(ep: EndpointOnline) -> Self {
        Self {
            kind: EventKind::EpOnline,
            payload: ep.encode_to_bytes(),
        }
    }
    pub fn ep_offline(ep: EndpointOffline) -> Self {
        Self {
            kind: EventKind::EpOffline,
            payload: ep.encode_to_bytes(),
        }
    }
    pub fn ep_interest(ep: EndpointInterest) -> Self {
        Self {
            kind: EventKind::EpInterest,
            payload: ep.encode_to_bytes(),
        }
    }
    pub fn set_state(set_state: SetState) -> Self {
        Self {
            kind: EventKind::SetState,
            payload: set_state.encode_to_bytes(),
        }
    }
}
impl_codec!(
    struct LogEntry {
        kind: EventKind,
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
#[derive(Debug, Clone)]
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
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct RaftTraceId {
    pub bytes: [u8; 16],
}
impl_codec!(
    struct RaftTraceId {
        bytes: [u8; 16],
    }
);
impl std::fmt::Debug for RaftTraceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("RaftTraceId")
            .field(&crate::util::hex(&self.bytes))
            .finish()
    }
}

impl RaftTraceId {
    pub fn new_snowflake() -> Self {
        thread_local! {
            static COUNTER: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
        }
        let timestamp = crate::util::timestamp_sec();
        let counter = COUNTER.with(|c| {
            let v = c.get();
            c.set(v.wrapping_add(1));
            v
        });
        let eid = crate::util::executor_digest() as u32;
        let mut bytes = [0; 16];
        bytes[0..8].copy_from_slice(&timestamp.to_be_bytes());
        bytes[8..12].copy_from_slice(&counter.to_be_bytes());
        bytes[12..16].copy_from_slice(&eid.to_be_bytes());
        Self { bytes }
    }
}
#[derive(Debug, Clone)]
pub struct LogAppend {
    pub trace_id: RaftTraceId,
    pub term: RaftTerm,
    pub entry: LogEntry,
}

impl LogAppend {
    pub fn indexed(self, index: RaftLogIndex) -> IndexedLog {
        IndexedLog {
            index,
            trace_id: self.trace_id,
            entry: self.entry,
        }
    }
    pub fn new(entry: LogEntry, term: RaftTerm) -> Self {
        Self {
            trace_id: RaftTraceId::new_snowflake(),
            term,
            entry,
        }
    }
    pub fn replicate(&self, index: RaftLogIndex) -> LogReplicate {
        LogReplicate {
            index,
            term: self.term,
            entry: self.entry.clone(),
            trace_id: self.trace_id,
        }
    }
}

impl_codec!(
    struct LogAppend {
        trace_id: RaftTraceId,
        term: RaftTerm,
        entry: LogEntry,
    }
);
#[derive(Debug, Clone)]
pub struct LogReplicate {
    pub trace_id: RaftTraceId,
    pub index: RaftLogIndex,
    pub term: RaftTerm,
    pub entry: LogEntry,
}

impl LogReplicate {
    pub fn indexed(self) -> IndexedLog {
        IndexedLog {
            index: self.index,
            trace_id: self.trace_id,
            entry: self.entry,
        }
    }
    pub fn ack(&self) -> LogAck {
        LogAck {
            trace_id: self.trace_id,
            term: self.term,
            index: self.index,
        }
    }
}

impl_codec!(
    struct LogReplicate {
        trace_id: RaftTraceId,
        index: RaftLogIndex,
        term: RaftTerm,
        entry: LogEntry,
    }
);

pub struct LogAck {
    pub term: RaftTerm,
    pub trace_id: RaftTraceId,
    pub index: RaftLogIndex,
}

impl_codec!(
    struct LogAck {
        term: RaftTerm,
        trace_id: RaftTraceId,
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
