pub mod connection;
pub mod edge;
pub mod event;
pub mod raft;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    net::SocketAddr,
    ops::Deref,
    sync::{self, atomic::AtomicUsize, Arc, RwLock},
};

use super::{
    codec::CodecType,
    endpoint::{EndpointInterest, SetState},
    topic::{Topic, TopicCode, TopicInner},
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
use openraft::{docs::components::state_machine, BasicNode, ChangeMembers, Raft};
use raft::{
    cluster::ClusterProvider,
    log_storage::LogStorage,
    network_factory::{RaftNodeInfo, TcpNetworkService},
    proposal::Proposal,
    state_machine::{topic::config::TopicConfig, StateMachineStore},
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
        bytes[0] = Self::KIND_INDEXED;
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
    pub const KIND_INDEXED: u8 = 0x02;
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
    pub addr: SocketAddr,
    pub raft: openraft::Config,
}

#[derive(Debug)]
pub struct NodeInner {
    raft: MaybeLoadingRaft,
    network: TcpNetworkService,
    config: NodeConfig,
    edge_connections: RwLock<HashMap<NodeId, Arc<EdgeConnectionInstance>>>,
    topics: RwLock<HashMap<TopicCode, Topic>>,
}

#[derive(Debug, Clone, Default)]
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

impl Node {
    pub async fn raft(&self) -> Raft<TypeConfig> {
        self.raft.get().await
    }
    pub async fn init_raft<C: ClusterProvider>(
        &self,
        mut cluster_provider: C,
    ) -> Result<(), crate::Error> {
        if self.raft_opt().is_some() {
            return Ok(());
        }
        let node_ref = self.node_ref();
        let id = self.id();
        let maybe_loading_raft = self.raft.clone();
        let tcp_service = self.network.clone();
        let state_machine_store = StateMachineStore::new(node_ref);
        let raft_config = self
            .config
            .raft
            .clone()
            .validate()
            .map_err(crate::Error::contextual_custom("validate raft config"))?;
        tcp_service.run();
        let raft = Raft::<TypeConfig>::new(
            id,
            Arc::new(raft_config),
            tcp_service.clone(),
            LogStorage::default(),
            Arc::new(state_machine_store),
        )
        .await
        .map_err(crate::Error::contextual_custom("create raft node"))?;
        let members = cluster_provider
            .pristine_nodes()
            .await?
            .into_iter()
            .map(|(id, addr)| {
                let node = BasicNode::new(addr);
                (id, node)
            })
            .collect::<BTreeMap<_, _>>();
        raft.initialize(members.clone())
            .await
            .map_err(crate::Error::contextual_custom("init raft node"))?;
        maybe_loading_raft.set(raft.clone());
        {
            let mut prev_members = members.keys().cloned().collect::<HashSet<_>>();
            tokio::spawn(async move {
                loop {
                    let members = cluster_provider.next_update().await;
                    let members = match members {
                        Ok(members) => members,
                        Err(e) => {
                            tracing::error!(?e, "cluster provider error");
                            continue;
                        }
                    };
                    let members = members
                        .into_iter()
                        .map(|(id, addr)| {
                            let node = BasicNode::new(addr);
                            (id, node)
                        })
                        .collect::<BTreeMap<_, _>>();
                    let now_ids = members.keys().cloned().collect::<HashSet<_>>();
                    let added_ids = now_ids
                        .difference(&prev_members)
                        .cloned()
                        .collect::<BTreeSet<_>>();
                    let deleted_ids = prev_members
                        .difference(&now_ids)
                        .cloned()
                        .collect::<BTreeSet<_>>();
                    let mut added_members = BTreeMap::new();
                    for node_id in added_ids {
                        let node = members.get(&node_id).unwrap().clone();
                        let result = raft.add_learner(node_id, node.clone(), true).await;
                        if let Err(e) = result {
                            tracing::error!(?e, "add learner error");
                        }
                        added_members.insert(node_id, node);
                    }
                    if !deleted_ids.is_empty() {
                        let remove = ChangeMembers::RemoveNodes(deleted_ids);
                        let _ = raft
                            .change_membership(remove, false)
                            .await
                            .inspect_err(|e| {
                                tracing::error!(?e, "remove nodes error");
                            });
                    }
                    if !added_members.is_empty() {
                        let add = ChangeMembers::AddNodes(added_members);
                        let _ = raft.change_membership(add, false).await.inspect_err(|e| {
                            tracing::error!(?e, "add voters error");
                        });
                    }
                    prev_members = now_ids.clone();
                }
            });
        }
        Ok(())
    }
    pub(crate) async fn proposal(&self, proposal: Proposal) -> Result<(), crate::Error> {
        let raft = self.raft().await;
        let Some(leader) = raft.current_leader().await else {
            return Err(crate::Error::new(
                "no leader",
                crate::error::ErrorKind::Offline,
            ));
        };
        let this = self.id();
        if this == leader {
            raft.client_write(proposal)
                .await
                .map_err(crate::Error::contextual_custom("client write"))?;
            Ok(())
        } else {
            let Some(connection) = self.network.connections.read().await.get(&leader).cloned()
            else {
                return Err(crate::Error::new(
                    "no connection to leader",
                    crate::error::ErrorKind::Offline,
                ));
            };
            connection.proposal(proposal).await?;
            Ok(())
        }
    }

    pub fn raft_opt(&self) -> Option<Raft<TypeConfig>> {
        self.raft.get_opt()
    }
    pub fn new(config: NodeConfig) -> Self {
        let raft = MaybeLoadingRaft::new();
        let network = raft.net_work_service(config.id, BasicNode::new(config.addr));
        let inner = NodeInner {
            edge_connections: RwLock::new(HashMap::new()),
            topics: RwLock::new(HashMap::new()),
            config,
            raft,
            network,
        };
        Self {
            inner: Arc::new(inner),
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

    pub async fn create_edge_connection<C: NodeConnection>(
        &self,
        conn: C,
    ) -> Result<NodeId, NodeConnectionError> {
        let config = ConnectionConfig {
            attached_node: self.node_ref(),
            auth: NodeAuth {},
        };
        let conn_inst = EdgeConnectionInstance::init(config, conn).await?;
        let peer = conn_inst.peer_id;
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

    pub fn get_topic(&self, code: &TopicCode) -> Option<Topic> {
        let topics = self.topics.read().unwrap();
        topics.get(code).cloned()
    }
    pub async fn create_new_topic<C: Into<TopicConfig>>(&self, config: C) -> crate::Result<Topic> {
        let config: TopicConfig = config.into();
        tracing::info!(?config, "create new topic");
        self.proposal(Proposal::LoadTopic(LoadTopic {
            config: config.clone(),
            queue: Default::default(),
        }))
        .await?;
        let topics = self.topics.read().unwrap();
        let topic = topics
            .get(&config.code)
            .cloned()
            .ok_or_else(|| crate::Error::unknown("topic proposal committed but still not found"))?;
        Ok(topic)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub peer_info: NodeConfig,
}
