pub mod connection;
pub mod edge;
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
    topic::{TopicCode, TopicData, TopicSnapshot},
};
use bytes::Bytes;
use connection::{
    ConnectionConfig, EdgeConnectionInstance, EdgeConnectionRef, NodeConnection,
    NodeConnectionError,
};
use crossbeam::sync::ShardedLock;
use edge::{EdgeError, EdgeErrorKind, EdgeMessage, EdgePayload};
use event::{EventKind, N2NUnreachableEvent, N2nEvent, N2nPacket, NodeAuth, NodeKind, NodeTrace};
use futures_util::TryFutureExt;
use openraft::{BasicNode, Raft};
use raft::{
    log_storage::LogStorage,
    network_factory::{RaftNodeInfo, TcpNetworkService},
    state_machine::StateMachineStore,
    MaybeLoadingRaft, TypeConfig,
};
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::{
    impl_codec,
    protocol::{
        endpoint::EndpointOnline,
        topic::durable_message::{LoadTopic, UnloadTopic},
    },
};

use super::endpoint::{CastMessage, DelegateMessage, EndpointOffline, EndpointSync, MessageAck};

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeId {
    pub bytes: [u8; 16],
}
impl From<u64> for NodeId {
    fn from(id: u64) -> Self {
        let mut bytes = [0; 16];
        bytes[1..9].copy_from_slice(&id.to_be_bytes());
        NodeId { bytes }
    }
}

impl std::fmt::Debug for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("NodeId")
            .field(&crate::util::dashed(&[
                crate::util::hex(&self.bytes[0..1]),
                crate::util::hex(&self.bytes[1..9]),
                crate::util::hex(&self.bytes[9..10]),
                crate::util::hex(&self.bytes[10..16]),
            ]))
            .finish()
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}-{}-{}-{}",
            crate::util::hex(&self.bytes[0..1]),
            crate::util::hex(&self.bytes[1..9]),
            crate::util::hex(&self.bytes[9..10]),
            crate::util::hex(&self.bytes[10..16]),
        )
    }
}

impl NodeId {
    pub const KIND_SNOWFLAKE: u8 = 0x00;
    pub const KIND_SHA256: u8 = 0x01;
    pub fn sha256(bytes: &[u8]) -> Self {
        let dg = <sha2::Sha256 as sha2::Digest>::digest(bytes);
        let mut bytes = [0; 16];
        bytes[0] = Self::KIND_SHA256;
        bytes[1..16].copy_from_slice(&dg.as_slice()[0..15]);
        NodeId { bytes }
    }
    pub fn snowflake() -> NodeId {
        static INSTANCE_ID: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);
        let dg = crate::util::executor_digest();
        let mut bytes = [0; 16];
        bytes[0] = Self::KIND_SNOWFLAKE;
        bytes[1..9].copy_from_slice(&dg.to_be_bytes());
        bytes[9..10].copy_from_slice(
            &INSTANCE_ID
                .fetch_add(1, sync::atomic::Ordering::SeqCst)
                .to_be_bytes(),
        );
        bytes[10..16].copy_from_slice(&(crate::util::timestamp_sec()).to_be_bytes()[2..8]);
        NodeId { bytes }
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self { bytes: [0; 16] }
    }
}
#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub id: NodeId,
}

#[derive(Debug, Clone, Default)]
pub struct NodeData {
    pub(crate) topics: HashMap<TopicCode, TopicData>,
    routing: HashMap<NodeId, N2nRoutingInfo>,
}

impl NodeData {
    pub(crate) fn snapshot(&self) -> NodeSnapshot {
        let topics = self.topics.iter().map(|(code, topic)| {
            let snapshot = topic.snapshot();
            (code.clone(), snapshot)
        }).collect();
        let routing = self.routing.clone();
        NodeSnapshot { topics, routing }
    }
    pub(crate) fn apply_delegate_message(
        &mut self,
        DelegateMessage { topic, message }: DelegateMessage,
    ) {
        if let Some(topic) = self.topics.get_mut(&topic) {
            topic.hold_new_message(message);
        } else {
            todo!()
        }
    }
    pub(crate) fn apply_snapshot(&mut self, snapshot: NodeSnapshot) {
        self.topics = snapshot.topics.into_iter().map(|(key, value)| {
            (key, TopicData::from_snapshot(value))
        }).collect();
        self.routing = snapshot.routing
    }
    pub(crate) fn apply_load_topic(&mut self, LoadTopic { config, mut queue }: LoadTopic) {
        queue.sort_by_key(|m| m.time);
        let topic = TopicData::new(config);
        self.topics
            .insert(topic.config.code.clone(), topic);
        let topic = self.topics.get_mut(&config.code).expect("just inserted");
        topic.apply_snapshot(TopicSnapshot {
            
            ep_routing_table: Default::default(),
            ep_interest_map: Default::default(),
            ep_latest_active: Default::default(),
            queue,
        });
    }
    pub(crate) fn apply_set_state(&mut self, SetState { topic, update }: SetState) {
        let topic = self.get_topic(topic);
        topic.update_and_flush(update);
    }
    pub(crate) fn apply_unload_topic(&self, UnloadTopic { code }: UnloadTopic) {
        self.remove_topic(&code);
    }
    pub(crate) fn apply_ep_online(
        &mut self,
        EndpointOnline {
            topic_code,
            endpoint,
            interests,
            host,
        }: EndpointOnline,
    ) {
        self.get_topic(topic_code)
            .ep_online(endpoint, interests, host);
    }
    pub(crate) fn apply_ep_offline(
        &mut self,
        EndpointOffline {
            topic_code,
            endpoint,
            host: _,
        }: EndpointOffline,
    ) {
        self.get_topic(topic_code).ep_offline(&endpoint);
    }
    pub(crate) fn apply_ep_interest(
        &mut self,
        EndpointInterest {
            topic_code,
            endpoint,
            interests,
        }: EndpointInterest,
    ) {
        self.get_topic(topic_code)
            .update_ep_interest(&endpoint, interests);
    }
}

#[derive(Debug, Clone)]
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
    inner: std::sync::Weak<NodeData>,
}

impl NodeRef {
    pub(crate) fn upgrade(&self) -> Option<Node> {
        self.inner.upgrade().map(|inner| Node { inner })
    }
}

#[derive(Debug, Clone)]
pub struct Node {
    pub(crate) inner: Arc<NodeData>,
}

impl Deref for Node {
    type Target = NodeData;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Default for Node {
    fn default() -> Self {
        Self::init(NodeConfig::default())
    }
}

impl Node {
    const RAFT_RANDOM_DURATION_RANGE: std::ops::Range<u64> = 200..1200;
    pub async fn init(config: NodeConfig) -> Self {
        let id = config.id;
        let timeout = crate::util::random_duration_ms(Self::RAFT_RANDOM_DURATION_RANGE);

        let this = Self {
            inner: Arc::new_cyclic(|this| NodeData {
                config,
                topics: Default::default(),
                routing: ShardedLock::new(HashMap::new()),
                edge_connections: ShardedLock::new(HashMap::new()),
                auth: Default::default(),
            }),
        };
        let node_ref = this.node_ref();

        this
    }
    pub(crate) fn snapshot(&self) -> NodeSnapshot {
        let topics = self.topics;
        let routing = self.routing;
        NodeSnapshot {
            topics: topics
                .iter()
                .map(|(code, topic)| (code.clone(), topic.snapshot()))
                .collect(),
            routing: routing.clone(),
        }
    }
    pub(crate) fn apply_snapshot(&self, snapshot: NodeSnapshot) {
        let mut routing = self.routing.write().unwrap();
        for (code, topic_snapshot) in snapshot.topics {
            let topic = self.get_topic(code);
            topic.apply_snapshot(topic_snapshot);
        }
        for (node, info) in snapshot.routing {
            routing.insert(node, info);
        }
    }
    pub fn node_ref(&self) -> NodeRef {
        NodeRef {
            inner: Arc::downgrade(&self.inner),
        }
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

    pub async fn create_edge_connection<C: NodeConnection>(
        &self,
        conn: C,
    ) -> Result<NodeId, NodeConnectionError> {
        let config = ConnectionConfig {
            attached_node: self.node_ref(),
            auth: self.auth.read().unwrap().clone(),
        };
        let conn_inst = EdgeConnectionInstance::init(config, conn).await?;
        let peer = conn_inst.peer_info.id;
        {
            let mut wg = self.edge_connections.write().unwrap();
            wg.insert(peer, Arc::new(conn_inst));
        }
        Ok(peer)
    }
    pub fn id(&self) -> NodeId {
        self.config.id
    }
    #[inline(always)]
    pub fn is(&self, id: NodeId) -> bool {
        self.id() == id
    }

    #[instrument(skip_all, fields(node = ?self.id()))]
    pub(crate) async fn handle_message_as_destination(&self, _message_evt: N2nEvent) {
        // TODO: implement this for edge nodes
    }
    pub fn get_edge_connection(&self, to: NodeId) -> Option<EdgeConnectionRef> {
        let connections = self.edge_connections.read().unwrap();
        let conn = connections.get(&to)?;
        if conn.is_alive() {
            Some(conn.get_connection_ref())
        } else {
            None
        }
    }
    pub fn remove_edge_connection(&self, to: NodeId) {
        self.edge_connections.write().unwrap().remove(&to);
    }

    pub(crate) fn record_routing_info(&self, trace: &NodeTrace) {
        let prev_node = trace.prev_node();
        let rg = self.routing.read().unwrap();
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
            let mut wg = self.routing.write().unwrap();
            for (node, info) in update {
                wg.insert(node, info);
            }
        }
    }

    pub(crate) fn handle_unreachable(&self, unreachable_evt: N2NUnreachableEvent) {
        let source = unreachable_evt.to;
        let prev_node = unreachable_evt.trace.prev_node();
        let unreachable_target = unreachable_evt.unreachable_target;
        let mut routing_table = self.routing.write().unwrap();
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
            let topic = self.get_topic(code);
            topic.load_ep_sync(infos);
        }
    }
    pub(crate) fn get_next_jump(&self, to: NodeId) -> Option<NodeId> {
        let routing_table = self.routing.read().unwrap();
        routing_table.get(&to).map(|info| info.next_jump)
    }

    pub(crate) async fn handle_edge_request(
        &self,
        from: NodeId,
        edge_request: edge::EdgeRequest,
    ) -> Result<Bytes, edge::EdgeError> {
        match edge_request.kind {
            edge::EdgeRequestKind::SendMessage => {
                let message = EdgeMessage::decode_from_bytes(edge_request.data).map_err(|e| {
                    EdgeError::with_message(
                        "decode EdgeMessage",
                        e.to_string(),
                        EdgeErrorKind::Decode,
                    )
                })?;
                let (message, topic_code) = message.into_message();
                let Some(topic) = self.get_topic(&topic_code) else {
                    return Err(EdgeError::new(
                        format!("topic ${topic_code} not found"),
                        EdgeErrorKind::TopicNotFound,
                    ));
                };
                let handle = topic
                    .send_message(message)
                    .map_err(|e| {
                        EdgeError::with_message(
                            "send message",
                            e.to_string(),
                            EdgeErrorKind::Internal,
                        )
                    })
                    .await?;
                let response = handle.await;
                Ok(response.encode_to_bytes())
            }
            edge::EdgeRequestKind::CreateEndpoint => Err(EdgeError::new(
                "create endpoint not supported",
                EdgeErrorKind::Internal,
            )),
            edge::EdgeRequestKind::DeleteEndpoint => Err(EdgeError::new(
                "create endpoint not supported",
                EdgeErrorKind::Internal,
            )),
            edge::EdgeRequestKind::Ack => Err(EdgeError::new(
                "create endpoint not supported",
                EdgeErrorKind::Internal,
            )),
        }
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
    pub attached_node: sync::Weak<NodeData>,
    pub peer_info: NodeConfig,
}
