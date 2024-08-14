pub mod connection;
pub mod event;
pub mod raft;

// pub mod hold_message;
// pub mod wait_ack;
use std::{
    collections::HashMap,
    ops::Deref,
    sync::{self, atomic::AtomicUsize, Arc, RwLock},
    time::{Duration, Instant},
};

use super::{
    codec::CodecType,
    topic::{Topic, TopicCode, TopicInner, TopicSnapshot},
};
use connection::{
    ConnectionConfig, N2NConnection, N2NConnectionError, N2NConnectionErrorKind,
    N2NConnectionInstance, N2NConnectionRef,
};
use crossbeam::{epoch::Atomic, sync::ShardedLock};
use event::{N2NAuth, N2NUnreachableEvent, N2nEvent, N2nEventKind, N2nPacket, NodeKind, NodeTrace};
use raft::{
    CandidateState, FollowerState, IndexedLog, LogEntry, RaftInfo, RaftLogIndex, RaftRole,
    RaftRoleKind, RaftState, RaftTerm, Vote,
};
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::{
    impl_codec,
    protocol::{
        endpoint::{EndpointAddr, EndpointOnline},
        interest::InterestMap,
    },
    TimestampSec,
};

use super::endpoint::{
    CastMessage, DelegateMessage, EndpointOffline, EndpointSync, EpInfo, LocalEndpointRef,
    MessageAck, MessageId,
};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
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
    pub(crate) fn snowflake() -> NodeId {
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
        let timeout = crate::util::random_duration_ms(100..500);
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
                })),
            }),
        };
        let node_ref = this.node_ref();
        tokio::spawn(async move {
            while let Ok(()) = timeout_receiver.recv_async().await {
                let Some(node) = node_ref.upgrade() else {
                    break;
                };
                let vote = node.raft_state_unwrap().write().unwrap().term_timeout();
                if let Some(vote) = vote {
                    node.cluster_wise_broadcast_packet(N2nPacket::raft_vote(vote));
                }
            }
        });
        this
    }
}

impl Node {
    pub fn snapshot(&self) -> NodeSnapshot {
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
    pub fn apply_snapshot(&self, snapshot: NodeSnapshot) {
        let mut routing = self.n2n_routing_table.write().unwrap();
        for (code, topic_snapshot) in snapshot.topics {
            let topic = self.get_or_init_topic(code);
            topic.apply_snapshot(topic_snapshot);
        }
        for (node, info) in snapshot.routing {
            routing.insert(node, info);
        }
    }
    pub fn raft_state_unwrap(&self) -> &RwLock<RaftState> {
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
            kind: N2nEventKind::CastMessage,
        }
    }
    pub(crate) fn new_ack(&self, to: NodeId, message: MessageAck) -> N2nEvent {
        N2nEvent {
            to,
            trace: self.new_trace(),
            payload: message.encode_to_bytes(),
            kind: N2nEventKind::Ack,
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
    ) -> Result<(), N2NConnectionError> {
        let config = ConnectionConfig {
            attached_node: self.node_ref(),
            auth: self.auth.read().unwrap().clone(),
        };
        let conn_inst = N2NConnectionInstance::init(config, conn).await?;
        let peer = conn_inst.peer_info.id;
        {
            let mut wg = self.connections.write().unwrap();
            wg.insert(peer, Arc::new(conn_inst));
            self.n2n_routing_table.write().unwrap().insert(
                peer,
                N2nRoutingInfo {
                    next_jump: peer,
                    hops: 0,
                },
            );
        }
        let sync_info = self.get_ep_sync();
        self.send_packet(
            N2nPacket::event(N2nEvent {
                to: peer,
                trace: self.new_trace(),
                kind: N2nEventKind::EpSync,
                payload: sync_info.encode_to_bytes(),
            }),
            peer,
        )
        .map_err(|_| {
            N2NConnectionError::new(N2NConnectionErrorKind::Closed, "fail to send sync error")
        })?;
        Ok(())
    }
    pub fn id(&self) -> NodeId {
        self.info.id
    }
    #[inline(always)]
    pub fn is(&self, id: NodeId) -> bool {
        self.id() == id
    }
    pub async fn commit_log(&self, log: LogEntry) {
        loop {
            let is_candidate = self.raft_state_unwrap().read().unwrap().role.is_ready();
            if is_candidate {
                tokio::task::yield_now().await;
            } else {
                break;
            }
        }
        let mut raft_state = self.raft_state_unwrap().write().unwrap();
        if raft_state.role.is_leader() {
            let index = raft_state.index.inc();
            if self.cluster_size() == 1 {
                self.apply_log(log);
            } else {
                let term = raft_state.term;
                let indexed_log = IndexedLog {
                    index,
                    entry: log.clone(),
                };
                raft_state.pending_logs.push_log(indexed_log);
                let log_replicate = N2nPacket::log_replicate(raft::LogReplicate {
                    index,
                    term,
                    entry: log,
                });
                self.cluster_wise_broadcast_packet(log_replicate);
            }
        } else if raft_state.role.is_follower() {
            let log_append = N2nPacket::log_append(log);
            let RaftRole::Follower(ref fs) = raft_state.role else {
                unreachable!("should be follower");
            };
            let leader = fs.leader.expect("should have leader");
            self.send_packet(log_append, leader)
                .expect("should send log append");
        }
    }
    pub fn apply_log(&self, log: LogEntry) {
        match log.kind {
            event::N2nEventKind::DelegateMessage => {
                let Ok((evt, _)) = DelegateMessage::decode(log.payload) else {
                    return;
                };
                self.get_or_init_topic(evt.message.topic().clone())
                    .hold_new_message(evt.message);
            }
            event::N2nEventKind::CastMessage => {
                let Ok((evt, _)) = CastMessage::decode(log.payload).inspect_err(|e| {
                    tracing::error!(error = ?e, "failed to decode cast message");
                }) else {
                    return;
                };
                tracing::debug!(
                    "cast message {id:?} to {eps:?}",
                    id = evt.message.id(),
                    eps = evt.target_eps
                );
                for ep in &evt.target_eps {
                    let result = self
                        .get_or_init_topic(evt.message.topic().clone())
                        .push_message_to_local_ep(ep, evt.message.clone());
                    if result.is_err() {
                        // TODO: handle error
                    }
                }
            }
            event::N2nEventKind::Ack => {
                let Ok((evt, _)) = MessageAck::decode(log.payload) else {
                    return;
                };
                self.get_or_init_topic(evt.topic_code.clone())
                    .local_ack(evt);
            }
            event::N2nEventKind::AckReport => todo!(),
            event::N2nEventKind::EpOnline => {
                let Ok((evt, _)) = EndpointOnline::decode(log.payload) else {
                    return;
                };
                tracing::debug!("ep {ep:?} online", ep = evt.endpoint);
                let topic = self.get_or_init_topic(evt.topic_code.clone());
                topic.ep_online(evt);
            }
            event::N2nEventKind::EpOffline => {
                let Ok((evt, _)) = EndpointOffline::decode(log.payload) else {
                    return;
                };
                self.get_or_init_topic(evt.topic_code.clone())
                    .ep_offline(&evt.endpoint);
            }
            event::N2nEventKind::EpSync => {
                let Ok((evt, _)) = EndpointSync::decode(log.payload) else {
                    return;
                };
                self.handle_ep_sync(evt);
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
    pub(crate) async fn handle_message_as_destination(&self, message_evt: N2nEvent) {
        match message_evt.kind {
            event::N2nEventKind::DelegateMessage => {
                let Ok((evt, _)) = DelegateMessage::decode(message_evt.payload) else {
                    return;
                };
                let handle = self
                    .get_or_init_topic(evt.message.topic().clone())
                    .hold_new_message(evt.message);
                // TODO: handle delegate message
            }
            event::N2nEventKind::CastMessage => {
                let Ok((evt, _)) = CastMessage::decode(message_evt.payload).inspect_err(|e| {
                    tracing::error!(error = ?e, "failed to decode cast message");
                }) else {
                    return;
                };
                tracing::debug!(
                    "cast message {id:?} to {eps:?}",
                    id = evt.message.id(),
                    eps = evt.target_eps
                );
                for ep in &evt.target_eps {
                    let result = self
                        .get_or_init_topic(evt.message.topic().clone())
                        .push_message_to_local_ep(ep, evt.message.clone());
                    if result.is_err() {
                        // TODO: handle error
                    }
                }
            }
            event::N2nEventKind::Ack => {
                let Ok((evt, _)) = MessageAck::decode(message_evt.payload) else {
                    return;
                };
                self.get_or_init_topic(evt.topic_code.clone())
                    .local_ack(evt)
            }
            event::N2nEventKind::AckReport => todo!(),
            event::N2nEventKind::EpOnline => {
                let Ok((evt, _)) = EndpointOnline::decode(message_evt.payload) else {
                    return;
                };
                tracing::debug!("ep {ep:?} online", ep = evt.endpoint);
                if evt.host == message_evt.trace.source() {
                    let topic = self.get_or_init_topic(evt.topic_code.clone());
                    topic.ep_online(evt);
                }
            }
            event::N2nEventKind::EpOffline => {
                let Ok((evt, _)) = EndpointOffline::decode(message_evt.payload) else {
                    return;
                };
                let ep = evt.endpoint;
                if evt.host == message_evt.trace.source() {
                    let topic = self.get_or_init_topic(evt.topic_code.clone());
                    topic.ep_offline(&ep);
                }
            }
            event::N2nEventKind::EpSync => {
                let Ok((evt, _)) = EndpointSync::decode(message_evt.payload) else {
                    return;
                };
                self.handle_ep_sync(evt);
            }
        }
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
        for peer in self.known_peer_cluster() {
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
