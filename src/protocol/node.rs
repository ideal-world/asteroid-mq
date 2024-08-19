pub mod connection;
pub mod event;
pub mod raft;

use std::{
    collections::HashMap,
    ops::Deref,
    sync::{self, atomic::AtomicUsize, Arc, RwLock},
};

use super::{
    codec::CodecType,
    endpoint::{EndpointInterest, SetState},
    topic::{TopicCode, TopicInner, TopicSnapshot},
};
use connection::{
    ConnectionConfig, N2NConnection, N2NConnectionError, N2NConnectionInstance, N2NConnectionRef,
};
use crossbeam::sync::ShardedLock;
use event::{EventKind, N2NAuth, N2NUnreachableEvent, N2nEvent, N2nPacket, NodeKind, NodeTrace};
use raft::{
    CommitHandle, FollowerState, LogEntry, RaftCommitError, RaftLogIndex, RaftRole, RaftState,
};
use tracing::instrument;

use crate::{
    impl_codec,
    protocol::{
        endpoint::EndpointOnline,
        topic::durable_message::{LoadTopic, UnloadTopic},
    },
};

use super::endpoint::{CastMessage, DelegateMessage, EndpointOffline, EndpointSync, MessageAck};

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId {
    pub bytes: [u8; 16],
}

impl std::fmt::Debug for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("NodeId")
            .field(&crate::util::dashed(&[
                crate::util::hex(&self.bytes[0..8]),
                crate::util::hex(&self.bytes[8..12]),
                crate::util::hex(&self.bytes[12..16]),
            ]))
            .finish()
    }
}
impl NodeId {
    pub fn snowflake() -> NodeId {
        static INSTANCE_ID: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        let dg = crate::util::executor_digest();
        let mut bytes = [0; 16];
        bytes[0..8].copy_from_slice(&dg.to_be_bytes());
        bytes[8..12].copy_from_slice(&(crate::util::timestamp_sec() as u32).to_be_bytes());
        bytes[12..16].copy_from_slice(
            &INSTANCE_ID
                .fetch_add(1, sync::atomic::Ordering::SeqCst)
                .to_be_bytes(),
        );
        NodeId { bytes }
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self::snowflake()
    }
}
#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub id: NodeId,
    pub kind: NodeKind,
}

impl NodeInfo {
    pub fn new(id: NodeId, kind: NodeKind) -> Self {
        Self { id, kind }
    }
    pub fn new_cluster_by_id(id: NodeId) -> Self {
        Self::new(id, NodeKind::Cluster)
    }
    pub fn new_cluster() -> Self {
        Self::new(NodeId::snowflake(), NodeKind::Cluster)
    }
}

impl_codec!(
    struct NodeInfo {
        id: NodeId,
        kind: NodeKind,
    }
);
impl Default for NodeInfo {
    fn default() -> Self {
        Self {
            id: NodeId::snowflake(),
            kind: NodeKind::Cluster,
        }
    }
}

#[derive(Debug)]
pub struct NodeInner {
    info: NodeInfo,
    cluster_size: AtomicUsize,
    raft_state: Option<RwLock<RaftState>>,
    pub(crate) topics: ShardedLock<HashMap<TopicCode, Arc<TopicInner>>>,
    n2n_routing_table: ShardedLock<HashMap<NodeId, N2nRoutingInfo>>,
    connections: ShardedLock<HashMap<NodeId, Arc<N2NConnectionInstance>>>,
    auth: RwLock<N2NAuth>,
}

pub struct NodeSnapshot {
    topics: HashMap<TopicCode, TopicSnapshot>,
    routing: HashMap<NodeId, N2nRoutingInfo>,
}

impl_codec!(
    struct NodeSnapshot {
        topics: HashMap<TopicCode, TopicSnapshot>,
        routing: HashMap<NodeId, N2nRoutingInfo>,
    }
);
#[derive(Debug, Clone)]
pub struct NodeRef {
    inner: std::sync::Weak<NodeInner>,
}

impl NodeRef {
    pub(crate) fn upgrade(&self) -> Option<Node> {
        self.inner.upgrade().map(|inner| Node { inner })
    }
}

#[derive(Debug, Clone)]
pub struct Node {
    pub(crate) inner: Arc<NodeInner>,
}

impl Deref for Node {
    type Target = NodeInner;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Default for Node {
    fn default() -> Self {
        let timeout = crate::util::random_duration_ms(200..700);
        let (timeout_reporter, timeout_receiver) = flume::bounded(1);

        let this = Self {
            inner: Arc::new_cyclic(|this| NodeInner {
                cluster_size: AtomicUsize::new(0),
                info: NodeInfo::default(),
                topics: Default::default(),
                n2n_routing_table: ShardedLock::new(HashMap::new()),
                connections: ShardedLock::new(HashMap::new()),
                auth: Default::default(),
                raft_state: Some(RwLock::new(RaftState {
                    term: Default::default(),
                    timeout_reporter: timeout_reporter.clone(),
                    role: RaftRole::Follower(FollowerState::new(timeout, timeout_reporter)),
                    index: Default::default(),
                    voted_for: None,
                    node_ref: NodeRef {
                        inner: this.clone(),
                    },
                    pending_logs: Default::default(),
                    commit_hooks: Default::default(),
                })),
            }),
        };
        let node_ref = this.node_ref();
        tokio::spawn(async move {
            while let Ok(()) = timeout_receiver.recv_async().await {
                let Some(node) = node_ref.upgrade() else {
                    break;
                };
                let req = node.raft_state_unwrap().write().unwrap().term_timeout();
                tracing::debug!(?req, "raft term timeout");
                if let Some(req) = req {
                    node.cluster_wise_broadcast_packet(N2nPacket::raft_request_vote(req));
                }
            }
        });
        this
    }
}

impl Node {
    pub fn new(cluster_info: NodeInfo) -> Self {
        let timeout = crate::util::random_duration_ms(200..700);
        let (timeout_reporter, timeout_receiver) = flume::bounded(1);
        let this = Self {
            inner: Arc::new_cyclic(|this| NodeInner {
                cluster_size: AtomicUsize::new(0),
                info: cluster_info,
                topics: Default::default(),
                n2n_routing_table: ShardedLock::new(HashMap::new()),
                connections: ShardedLock::new(HashMap::new()),
                auth: Default::default(),
                raft_state: Some(RwLock::new(RaftState {
                    term: Default::default(),
                    timeout_reporter: timeout_reporter.clone(),
                    role: RaftRole::Follower(FollowerState::new(timeout, timeout_reporter)),
                    index: Default::default(),
                    voted_for: None,
                    node_ref: NodeRef {
                        inner: this.clone(),
                    },
                    pending_logs: Default::default(),
                    commit_hooks: Default::default(),
                })),
            }),
        };
        let node_ref = this.node_ref();
        tokio::spawn(async move {
            while let Ok(()) = timeout_receiver.recv_async().await {
                let Some(node) = node_ref.upgrade() else {
                    break;
                };
                let req = node.raft_state_unwrap().write().unwrap().term_timeout();
                tracing::debug!(?req, "raft term timeout");
                if let Some(req) = req {
                    node.cluster_wise_broadcast_packet(N2nPacket::raft_request_vote(req));
                }
            }
        });
        this
    }
    pub(crate) fn snapshot(&self) -> NodeSnapshot {
        let topics = self.topics.read().unwrap();
        let routing = self.n2n_routing_table.read().unwrap();
        NodeSnapshot {
            topics: topics
                .iter()
                .map(|(code, topic)| (code.clone(), topic.snapshot()))
                .collect(),
            routing: routing.clone(),
        }
    }
    pub(crate) fn apply_snapshot(&self, snapshot: NodeSnapshot) {
        let mut routing = self.n2n_routing_table.write().unwrap();
        for (code, topic_snapshot) in snapshot.topics {
            let topic = self.get_or_init_topic(code);
            topic.apply_snapshot(topic_snapshot);
        }
        for (node, info) in snapshot.routing {
            routing.insert(node, info);
        }
    }
    pub(crate) fn raft_state_unwrap(&self) -> &RwLock<RaftState> {
        self.raft_state.as_ref().unwrap()
    }
    pub fn cluster_size(&self) -> u64 {
        self.cluster_size.load(sync::atomic::Ordering::SeqCst) as u64
    }
    pub fn set_cluster_size(&self, size: u64) {
        self.cluster_size
            .store(size as usize, sync::atomic::Ordering::SeqCst);
    }
    pub fn node_ref(&self) -> NodeRef {
        NodeRef {
            inner: Arc::downgrade(&self.inner),
        }
    }
    pub fn is_edge(&self) -> bool {
        matches!(self.info.kind, NodeKind::Edge)
    }
    pub fn is_cluster(&self) -> bool {
        matches!(self.info.kind, NodeKind::Cluster)
    }
    pub(crate) fn new_trace(&self) -> NodeTrace {
        NodeTrace {
            source: self.id(),
            hops: Vec::new(),
        }
    }
    pub(crate) fn new_cast_message(&self, to: NodeId, message: CastMessage) -> N2nEvent {
        N2nEvent {
            to,
            trace: self.new_trace(),
            payload: message.encode_to_bytes(),
            kind: EventKind::CastMessage,
        }
    }
    pub(crate) fn new_ack(&self, to: NodeId, message: MessageAck) -> N2nEvent {
        N2nEvent {
            to,
            trace: self.new_trace(),
            payload: message.encode_to_bytes(),
            kind: EventKind::Ack,
        }
    }
    pub(crate) fn get_ep_sync(&self) -> EndpointSync {
        let entries = self
            .topics
            .read()
            .unwrap()
            .iter()
            .map(|(code, t)| (code.clone(), t.get_ep_sync()))
            .collect::<Vec<_>>();
        EndpointSync { entries }
    }
    pub async fn create_connection<C: N2NConnection>(
        &self,
        conn: C,
    ) -> Result<NodeId, N2NConnectionError> {
        let config = ConnectionConfig {
            attached_node: self.node_ref(),
            auth: self.auth.read().unwrap().clone(),
        };
        let conn_inst = N2NConnectionInstance::init(config, conn).await?;
        let peer = conn_inst.peer_info.id;
        {
            let mut wg = self.connections.write().unwrap();
            wg.insert(peer, Arc::new(conn_inst));
        }
        Ok(peer)
    }
    pub fn id(&self) -> NodeId {
        self.info.id
    }
    #[inline(always)]
    pub fn is(&self, id: NodeId) -> bool {
        self.id() == id
    }
    fn commit_log_cluster_wise(&self, log: LogEntry) -> Result<CommitHandle, LogEntry> {
        let mut raft_state = self.raft_state_unwrap().write().unwrap();
        if !raft_state.role.is_ready() {
            return Err(log);
        }
        if raft_state.role.is_leader() {
            let index = raft_state.index.inc();
            let term = raft_state.term;
            let log_replicate = log.create_replicate(index, term);
            let indexed_log = log_replicate.clone().indexed();
            raft_state.pending_logs.push_log(indexed_log.clone());
            let RaftRole::Leader(ref mut ls) = raft_state.role else {
                unreachable!("should be leader");
            };
            ls.ack_map.insert(index, 0);
            let commit_hook = raft_state.add_commit_handle(log_replicate.trace_id);
            {
                drop(raft_state);
            }
            tracing::debug!(?log_replicate, "log replicate");
            self.cluster_wise_broadcast_packet(N2nPacket::log_replicate(log_replicate));
            Ok(commit_hook)
        } else if raft_state.role.is_follower() {
            let log_append = log.create_append(raft_state.term);
            let trace_id = log_append.trace_id;
            let log_append = N2nPacket::log_append(log_append);
            let RaftRole::Follower(ref fs) = raft_state.role else {
                unreachable!("should be follower");
            };
            let leader = fs.leader.expect("should have leader");
            let commit_handle = raft_state.add_commit_handle(trace_id);
            {
                drop(raft_state);
            }
            tracing::debug!(?log_append, "log append");
            self.send_packet(log_append, leader)
                .expect("should send log append");
            Ok(commit_handle)
        } else {
            unreachable!("should be ready");
        }
    }
    pub(crate) async fn commit_log(&self, mut log: LogEntry) -> Result<RaftLogIndex, RaftCommitError> {
        tracing::debug!(?log, "commit log");
        if self.cluster_size() == 1 {
            let index = {
                let mut raft_state = self.raft_state_unwrap().write().unwrap();
                raft_state.index.inc()
            };
            self.apply_log(log);
            Ok(index)
        } else {
            loop {
                match self.commit_log_cluster_wise(log) {
                    Ok(hook) => {
                        return hook.await;
                    }
                    Err(prev_log) => {
                        tokio::task::yield_now().await;
                        log = prev_log;
                    }
                }
            }
        }
    }
    pub(crate) fn apply_log(&self, log: LogEntry) {
        match log.kind {
            event::EventKind::DelegateMessage => {
                let Ok((evt, _)) = DelegateMessage::decode(log.payload).inspect_err(|e| {
                    tracing::error!("failed to decode delegate message: {e:?}");
                }) else {
                    return;
                };
                tracing::info!(message=?evt.message, "hold message");
                self.get_or_init_topic(evt.topic)
                    .hold_new_message(evt.message);
            }
            event::EventKind::LoadTopic => {
                let Ok((mut evt, _)) = LoadTopic::decode(log.payload) else {
                    return;
                };
                tracing::debug!(config = ?evt.config, "load topic");
                let topic = self.apply_load_topic(evt.config);
                evt.queue.sort_by_key(|m| m.time);
                topic.apply_snapshot(TopicSnapshot {
                    ep_routing_table: Default::default(),
                    ep_interest_map: Default::default(),
                    ep_latest_active: Default::default(),
                    queue: evt.queue,
                });
            }
            event::EventKind::UnloadTopic => {
                let Ok((evt, _)) = UnloadTopic::decode(log.payload) else {
                    return;
                };
                tracing::debug!(code=?evt.code, "unload topic");
                self.remove_topic(&evt.code);
            }
            event::EventKind::EpOnline => {
                let Ok((evt, _)) = EndpointOnline::decode(log.payload) else {
                    return;
                };
                tracing::info!("ep {ep:?} online", ep = evt.endpoint);
                let topic = self.get_or_init_topic(evt.topic_code.clone());
                topic.ep_online(evt);
            }
            event::EventKind::EpOffline => {
                let Ok((evt, _)) = EndpointOffline::decode(log.payload) else {
                    return;
                };
                self.get_or_init_topic(evt.topic_code.clone())
                    .ep_offline(&evt.endpoint);
            }
            event::EventKind::EpInterest => {
                let Ok(evt) = EndpointInterest::decode_from_bytes(log.payload) else {
                    return;
                };
                self.get_or_init_topic(evt.topic_code.clone())
                    .update_ep_interest(&evt.endpoint, evt.interests);
            }
            event::EventKind::EpSync => {
                let Ok((evt, _)) = EndpointSync::decode(log.payload) else {
                    return;
                };
                self.handle_ep_sync(evt);
            }
            event::EventKind::SetState => {
                let Ok((evt, _)) = SetState::decode(log.payload) else {
                    return;
                };
                tracing::debug!(topic=?evt.topic, update=?evt.update, "set message state");
                let topic = self.get_or_init_topic(evt.topic);
                topic.update_and_flush(evt.update)
            }
            _ => {
                tracing::warn!(?log, "unhandled log");
            }
        }
    }
    #[instrument(skip_all, fields(node = ?self.id()))]
    pub(crate) async fn handle_message(&self, message_evt: N2nEvent) {
        tracing::trace!(?message_evt, "node recv new message");
        self.record_routing_info(&message_evt.trace);
        if message_evt.to == self.id() {
            self.handle_message_as_destination(message_evt).await;
        } else {
            self.forward_message(message_evt);
        }
    }
    #[instrument(skip_all, fields(node = ?self.id()))]
    pub(crate) async fn handle_message_as_destination(&self, _message_evt: N2nEvent) {
        // TODO: implement this for edge nodes
    }
    pub(crate) fn known_nodes(&self) -> Vec<NodeId> {
        self.connections.read().unwrap().keys().cloned().collect()
    }
    pub(crate) fn known_peer_cluster(&self) -> Vec<NodeId> {
        self.connections
            .read()
            .unwrap()
            .iter()
            .filter(|(id, instance)| {
                instance.peer_info.kind == NodeKind::Cluster && (**id != self.id())
            })
            .map(|(id, _)| *id)
            .collect()
    }
    pub(crate) fn get_connection(&self, to: NodeId) -> Option<N2NConnectionRef> {
        let connections = self.connections.read().unwrap();
        let conn = connections.get(&to)?;
        if conn.is_alive() {
            Some(conn.get_connection_ref())
        } else {
            None
        }
    }

    pub(crate) fn remove_connection(&self, to: NodeId) {
        self.connections.write().unwrap().remove(&to);
    }
    pub(crate) fn cluster_wise_broadcast_packet(&self, packet: N2nPacket) {
        let known_peer_cluster = self.known_peer_cluster();
        tracing::trace!(header = ?packet.header, peers=?known_peer_cluster, "cluster wise broadcast");
        for peer in known_peer_cluster {
            let _ = self.send_packet(packet.clone(), peer);
        }
    }
    #[instrument(skip(self, packet), fields(node=?self.id(), ?to))]
    pub(crate) fn send_packet(&self, packet: N2nPacket, to: NodeId) -> Result<(), N2nPacket> {
        let Some(conn) = self.get_connection(to) else {
            tracing::error!("failed to send packet no connection to {to:?}");
            return Err(packet);
        };
        tracing::trace!(header = ?packet.header,"having connection, prepared to send packet");
        if let Err(e) = conn.outbound.send(packet) {
            tracing::error!("failed to send packet: connection closed");
            return Err(e.0);
        }
        Ok(())
    }
    pub(crate) fn report_unreachable(&self, raw: N2nEvent) {
        let source = raw.trace.source();
        let prev_node = raw.trace.prev_node();
        let unreachable_target = raw.to;
        let unreachable_event = N2NUnreachableEvent {
            to: source,
            trace: NodeTrace {
                source: self.id(),
                hops: Vec::new(),
            },
            unreachable_target,
        };
        let packet = N2nPacket::unreachable(unreachable_event);
        if let Err(packet) = self.send_packet(packet, prev_node) {
            tracing::warn!(
                id = ?packet.id(),
                "trying to report unreachable but previous node lost connection"
            );
        }
    }

    pub(crate) fn record_routing_info(&self, trace: &NodeTrace) {
        let prev_node = trace.prev_node();
        let rg = self.n2n_routing_table.read().unwrap();
        let mut update = Vec::new();
        for (hop, node) in trace.trace_back().enumerate().skip(1) {
            if let Some(routing) = rg.get(node) {
                if routing.hops > hop as u32 {
                    update.push((
                        *node,
                        N2nRoutingInfo {
                            next_jump: prev_node,
                            hops: hop as u32,
                        },
                    ));
                }
            } else {
                update.push((
                    *node,
                    N2nRoutingInfo {
                        next_jump: prev_node,
                        hops: hop as u32,
                    },
                ));
            }
        }
        if !update.is_empty() {
            let mut wg = self.n2n_routing_table.write().unwrap();
            for (node, info) in update {
                wg.insert(node, info);
            }
        }
    }

    pub(crate) fn forward_message(&self, mut message_evt: N2nEvent) {
        let Some(next_jump) = self.get_next_jump(message_evt.to) else {
            self.report_unreachable(message_evt);
            return;
        };
        message_evt.trace.hops.push(self.id());
        if let Err(packet) = self.send_packet(N2nPacket::event(message_evt), next_jump) {
            let raw_event = N2nEvent::decode(packet.payload).expect("should be valid").0;
            self.report_unreachable(raw_event);
        }
    }

    pub(crate) fn handle_unreachable(&self, unreachable_evt: N2NUnreachableEvent) {
        let source = unreachable_evt.to;
        let prev_node = unreachable_evt.trace.prev_node();
        let unreachable_target = unreachable_evt.unreachable_target;
        let mut routing_table = self.n2n_routing_table.write().unwrap();
        if let Some(routing) = routing_table.get(&unreachable_target) {
            if routing.next_jump == source {
                routing_table.remove(&unreachable_target);
            }
        }
        if let Some(routing) = routing_table.get(&source) {
            if routing.next_jump == prev_node {
                routing_table.remove(&source);
            }
        }
    }
    pub(crate) fn handle_ep_sync(&self, sync_evt: EndpointSync) {
        tracing::debug!(?sync_evt, "handle ep sync event");
        for (code, infos) in sync_evt.entries {
            let topic = self.get_or_init_topic(code);
            topic.load_ep_sync(infos);
        }
    }
    pub(crate) fn get_next_jump(&self, to: NodeId) -> Option<NodeId> {
        let routing_table = self.n2n_routing_table.read().unwrap();
        routing_table.get(&to).map(|info| info.next_jump)
    }
}

#[derive(Debug, Clone)]
pub struct N2nRoutingInfo {
    next_jump: NodeId,
    hops: u32,
}

impl_codec!(
    struct N2nRoutingInfo {
        next_jump: NodeId,
        hops: u32,
    }
);

pub struct Connection {
    pub attached_node: sync::Weak<NodeInner>,
    pub peer_info: NodeInfo,
}
