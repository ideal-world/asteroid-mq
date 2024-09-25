pub mod edge;
pub mod raft;
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    hash::Hash,
    net::SocketAddr,
    ops::Deref,
    sync::{self, Arc, RwLock},
};

use super::{
    endpoint::EndpointAddr,
    topic::{durable_message::DurableCommand, Topic, TopicCode},
};
use edge::{
    auth::EdgeAuthService,
    codec::CodecRegistry,
    connection::{
        ConnectionConfig, EdgeConnectionInstance, EdgeConnectionRef, NodeConnection,
        NodeConnectionError,
    },
    EdgeConfig, EdgeResult,
};
use edge::{
    packet::{Auth, EdgePacket},
    EdgeError, EdgeErrorKind,
};
use futures_util::TryFutureExt;
use openraft::{BasicNode, ChangeMembers, Raft};
use raft::{
    cluster::ClusterProvider,
    log_storage::LogStorage,
    network_factory::TcpNetworkService,
    proposal::{EndpointOffline, EndpointOnline, LoadTopic, Proposal},
    state_machine::{topic::config::TopicConfig, StateMachineStore},
    MaybeLoadingRaft, TypeConfig,
};
use serde::{Deserialize, Serialize};
use typeshare::typeshare;

use crate::{
    prelude::{DurableMessage, DurableService},
    DEFAULT_TCP_SOCKET_ADDR,
};

#[derive(Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[typeshare(serialized_as = "string")]
pub struct NodeId {
    pub bytes: [u8; 16],
}

impl Serialize for NodeId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_base64())
        } else {
            <[u8; 16]>::serialize(&self.bytes, serializer)
        }
    }
}

impl<'de> Deserialize<'de> for NodeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;
        if deserializer.is_human_readable() {
            let s = <&'de str>::deserialize(deserializer)?;
            NodeId::from_base64(s).map_err(D::Error::custom)
        } else {
            let bytes = <[u8; 16]>::deserialize(deserializer)?;
            Ok(NodeId { bytes })
        }
    }
}

impl From<u64> for NodeId {
    fn from(id: u64) -> Self {
        Self::new_indexed(id)
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
    pub const KIND_INDEXED: u8 = 0x00;
    pub const KIND_SHA256: u8 = 0x01;
    pub const KIND_SNOWFLAKE: u8 = 0x02;
    pub const fn new_indexed(id: u64) -> Self {
        let mut bytes = [0; 16];
        bytes[0] = Self::KIND_INDEXED;
        let index_part = id.to_be_bytes();
        bytes[1] = index_part[0];
        bytes[2] = index_part[1];
        bytes[3] = index_part[2];
        bytes[4] = index_part[3];
        bytes[5] = index_part[4];
        bytes[6] = index_part[5];
        bytes[7] = index_part[6];
        bytes[8] = index_part[7];
        NodeId { bytes }
    }
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
    pub fn to_base64(&self) -> String {
        use base64::Engine;
        base64::engine::general_purpose::URL_SAFE.encode(self.bytes)
    }
    pub fn from_base64(s: &str) -> Result<Self, base64::DecodeError> {
        use base64::Engine;
        let id = base64::engine::general_purpose::URL_SAFE.decode(s)?;
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&id);
        Ok(Self { bytes })
    }
}

#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub id: NodeId,
    pub addr: SocketAddr,
    pub raft: openraft::Config,
    pub durable: Option<DurableService>,
    pub edge_auth: Option<EdgeAuthService>,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            id: NodeId::default(),
            addr: DEFAULT_TCP_SOCKET_ADDR,
            raft: openraft::Config::default(),
            durable: None,
            edge_auth: None,
        }
    }
}

#[derive(Debug)]
pub struct NodeInner {
    raft: MaybeLoadingRaft,
    network: TcpNetworkService,
    config: NodeConfig,
    edge_connections: RwLock<HashMap<NodeId, Arc<EdgeConnectionInstance>>>,
    edge_routing: RwLock<HashMap<EndpointAddr, (NodeId, TopicCode)>>,
    codec_registry: Arc<CodecRegistry>,
    topics: RwLock<HashMap<TopicCode, Topic>>,
    durable_commands_queue: std::sync::RwLock<VecDeque<DurableCommand>>,
    pub(crate) durable_syncs: tokio::sync::Mutex<HashMap<TopicCode, Arc<tokio::sync::Mutex<()>>>>,
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

#[derive(Clone)]
pub struct Node {
    pub(crate) inner: Arc<NodeInner>,
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("id", &self.id())
            .field("config", &self.config())
            .finish()
    }
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
    pub fn config(&self) -> &NodeConfig {
        &self.config
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
                    let now_members = members.keys().cloned().collect::<HashSet<_>>();
                    let is_modified = now_members != prev_members;
                    if is_modified {
                        let added_nodes = now_members.difference(&prev_members).cloned().collect();
                        let replace_all_nodes = ChangeMembers::ReplaceAllNodes(members);
                        tracing::info!(nodes=?replace_all_nodes, "change membership");
                        let raft = maybe_loading_raft.get().await;
                        let _ = raft
                            .change_membership(replace_all_nodes, false)
                            .await
                            .inspect_err(|e| {
                                tracing::error!(?e, "change membership error");
                            });
                        let _ = raft
                            .change_membership(ChangeMembers::AddVoterIds(added_nodes), false)
                            .await
                            .inspect_err(|e| {
                                tracing::error!(?e, "add voters error");
                            });
                        prev_members = now_members.clone();
                    }
                }
            });
        }
        Ok(())
    }
    pub(crate) async fn proposal(&self, proposal: Proposal) -> Result<(), crate::Error> {
        let raft = self.raft().await;
        let metric = raft
            .wait(None)
            .metrics(
                |rm| rm.current_leader.is_some(),
                "wait for leader to be elected",
            )
            .await
            .map_err(crate::Error::contextual_custom(
                "wait for leader when proposal",
            ))?;
        let leader = metric.current_leader.expect("leader should be elected");
        let this = self.id();
        let client_write_result = if this == leader {
            raft.client_write(proposal)
                .await
                .map_err(crate::Error::contextual_custom("client write"))?
        } else {
            let Some(connection) = self.network.connections.read().await.get(&leader).cloned()
            else {
                return Err(crate::Error::new(
                    "no connection to leader",
                    crate::error::ErrorKind::Offline,
                ));
            };
            connection.proposal(proposal).await?
        };
        let id = client_write_result.log_id();
        raft.wait(None)
            .applied_index_at_least(Some(id.index), "proposal resolved")
            .await
            .map_err(crate::Error::contextual_custom("wait for proposal"))?;
        Ok(())
    }

    pub fn raft_opt(&self) -> Option<Raft<TypeConfig>> {
        self.raft.get_opt()
    }
    pub fn new(config: NodeConfig) -> Self {
        let raft = MaybeLoadingRaft::new();
        let network = raft.net_work_service(config.id, BasicNode::new(config.addr));
        let inner = NodeInner {
            edge_connections: RwLock::new(HashMap::new()),
            edge_routing: RwLock::new(HashMap::new()),
            topics: RwLock::new(HashMap::new()),
            config,
            raft,
            codec_registry: Arc::new(CodecRegistry::new_preloaded()),
            network,
            durable_commands_queue: Default::default(),
            durable_syncs: Default::default(),
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

    pub async fn create_edge_connection<C: NodeConnection>(
        &self,
        conn: C,
        edge_config: EdgeConfig,
    ) -> Result<NodeId, NodeConnectionError> {
        let preferred = self
            .codec_registry
            .pick_preferred_codec(&edge_config.supported_codec_kinds)
            .ok_or_else(|| {
                NodeConnectionError::new(
                    edge::connection::NodeConnectionErrorKind::Protocol,
                    "no codec available",
                )
            })?;
        let config = ConnectionConfig {
            attached_node: self.node_ref(),
            auth: Auth {},
            codec_registry: self.codec_registry.clone(),
            preferred_codec: preferred,
            peer_id: edge_config.peer_id,
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
    pub(crate) fn get_edge_routing(&self, addr: &EndpointAddr) -> Option<NodeId> {
        let routing = self.edge_routing.read().unwrap();
        Some(routing.get(addr)?.0)
    }
    pub(crate) fn set_edge_routing(&self, addr: EndpointAddr, edge: NodeId, topic: TopicCode) {
        self.edge_routing
            .write()
            .unwrap()
            .insert(addr, (edge, topic));
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
        let Some(_connection) = self.edge_connections.write().unwrap().remove(&to) else {
            return;
        };
        let mut routing = self.edge_routing.write().unwrap();
        let mut offline_eps = Vec::new();
        for (addr, (node_id, topic)) in routing.iter() {
            if node_id == &to {
                offline_eps.push((*addr, topic.clone()));
            }
        }
        for (ref addr, _) in &offline_eps {
            routing.remove(addr);
        }
        drop(routing);
        let host = self.id();
        for (addr, topic_code) in offline_eps {
            let offline = EndpointOffline {
                endpoint: addr,
                host,
                topic_code,
            };
            let node = self.clone();
            tokio::spawn(async move {
                let _ = node.proposal(Proposal::EpOffline(offline)).await;
            });
        }
    }
    pub fn check_ep_auth(&self, ep: &EndpointAddr, peer: &NodeId) -> Result<(), EdgeError> {
        let routing = self.edge_routing.read().unwrap();
        if let Some((owner, _topic)) = routing.get(ep) {
            if owner == peer {
                Ok(())
            } else {
                Err(EdgeError::new(
                    "endpoint not owned by sender",
                    EdgeErrorKind::Unauthorized,
                ))
            }
        } else {
            Err(EdgeError::new(
                "endpoint not found",
                EdgeErrorKind::EndpointNotFound,
            ))
        }
    }
    pub(crate) async fn handle_edge_request(
        &self,
        from: NodeId,
        edge_request_kind: edge::EdgeRequestEnum,
    ) -> Result<edge::EdgeResponseEnum, edge::EdgeError> {
        // auth check
        if let Some(auth) = self.config.edge_auth.as_ref() {
            if let Err(e) = auth.check(from, &edge_request_kind).await {
                return Err(EdgeError::with_message(
                    "auth check failed",
                    e.to_string(),
                    EdgeErrorKind::Unauthorized,
                ));
            }
        }
        match edge_request_kind {
            edge::EdgeRequestEnum::SendMessage(edge_message) => {
                let (message, topic_code) = edge_message.into_message();
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
                Ok(edge::EdgeResponseEnum::SendMessage(EdgeResult::from_std(
                    response,
                )))
            }
            edge::EdgeRequestEnum::EndpointOnline(online) => {
                let topic_code = online.topic_code.clone();
                let topic = self.get_topic(&topic_code).ok_or_else(|| {
                    EdgeError::new(
                        format!("topic ${topic_code} not found"),
                        EdgeErrorKind::TopicNotFound,
                    )
                })?;
                let node = topic.node();
                let endpoint = EndpointAddr::new_snowflake();
                node.proposal(Proposal::EpOnline(EndpointOnline {
                    topic_code: topic_code.clone(),
                    interests: online.interests,
                    endpoint,
                    host: node.id(),
                }))
                .await
                .map_err(|e| {
                    EdgeError::with_message(
                        "endpoint online",
                        e.to_string(),
                        EdgeErrorKind::Internal,
                    )
                })?;
                self.set_edge_routing(endpoint, from, topic_code);
                Ok(edge::EdgeResponseEnum::EndpointOnline(endpoint))
            }
            edge::EdgeRequestEnum::EndpointOffline(offline) => {
                let topic_code = offline.topic_code.clone();
                let topic = self.get_topic(&topic_code).ok_or_else(|| {
                    EdgeError::new(
                        format!("topic ${topic_code} not found"),
                        EdgeErrorKind::TopicNotFound,
                    )
                })?;
                let node = topic.node();
                self.check_ep_auth(&offline.endpoint, &from)?;
                node.proposal(Proposal::EpOffline(EndpointOffline {
                    topic_code: topic_code.clone(),
                    endpoint: offline.endpoint,
                    host: from,
                }))
                .await
                .map_err(|e| {
                    EdgeError::with_message(
                        "endpoint offline",
                        e.to_string(),
                        EdgeErrorKind::Internal,
                    )
                })?;
                self.edge_routing.write().unwrap().remove(&offline.endpoint);
                Ok(edge::EdgeResponseEnum::EndpointOffline)
            }
            edge::EdgeRequestEnum::EndpointInterest(interest) => {
                let topic_code = interest.topic_code.clone();

                let topic = self.get_topic(&topic_code).ok_or_else(|| {
                    EdgeError::new(
                        format!("topic ${topic_code} not found"),
                        EdgeErrorKind::TopicNotFound,
                    )
                })?;
                let node = topic.node();
                self.check_ep_auth(&interest.endpoint, &from)?;
                node.proposal(Proposal::EpInterest(interest.clone()))
                    .await
                    .map_err(|e| {
                        EdgeError::with_message(
                            "endpoint interest",
                            e.to_string(),
                            EdgeErrorKind::Internal,
                        )
                    })?;
                Ok(edge::EdgeResponseEnum::EndpointInterest)
            }
            edge::EdgeRequestEnum::SetState(set_state) => {
                let topic_code = set_state.topic.clone();
                let topic = self.get_topic(&topic_code).ok_or_else(|| {
                    EdgeError::new(
                        format!("topic ${topic_code} not found"),
                        EdgeErrorKind::TopicNotFound,
                    )
                })?;
                let node = topic.node();
                for ep in set_state.update.status.keys() {
                    self.check_ep_auth(ep, &from)?;
                }
                node.proposal(Proposal::SetState(set_state.clone()))
                    .await
                    .map_err(|e| {
                        EdgeError::with_message("set state", e.to_string(), EdgeErrorKind::Internal)
                    })?;
                Ok(edge::EdgeResponseEnum::SetState)
            }
        }
    }

    pub fn get_topic(&self, code: &TopicCode) -> Option<Topic> {
        let topics = self.topics.read().unwrap();
        topics.get(code).cloned()
    }
    pub async fn is_leader(&self) -> bool {
        let raft = self.raft().await;
        raft.ensure_linearizable().await.is_ok()
    }
    pub async fn load_topic<C: Into<TopicConfig>>(
        &self,
        config: C,
        queue: Vec<DurableMessage>,
    ) -> Result<Topic, crate::Error> {
        let config: TopicConfig = config.into();
        let config_code = config.code.clone();
        // check if topic already exists
        {
            let topics = self.topics.read().unwrap();
            if topics.contains_key(&config_code) {
                return Err(crate::Error::new(
                    "topic already exists",
                    crate::error::ErrorKind::TopicAlreadyExists,
                ));
            }
        }
        tracing::info!(?config, "load_topic");
        self.proposal(Proposal::LoadTopic(LoadTopic { config, queue }))
            .await?;
        let topics = self.topics.read().unwrap();
        let topic = topics
            .get(&config_code)
            .cloned()
            .ok_or_else(|| crate::Error::unknown("topic proposal committed but still not found"))?;
        Ok(topic)
    }
    pub async fn create_new_topic<C: Into<TopicConfig>>(&self, config: C) -> crate::Result<Topic> {
        self.load_topic(config, Vec::new()).await
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct N2nRoutingInfo {
    next_jump: NodeId,
    hops: u32,
}

pub struct Connection {
    pub attached_node: sync::Weak<NodeInner>,
    pub peer_info: NodeConfig,
}
