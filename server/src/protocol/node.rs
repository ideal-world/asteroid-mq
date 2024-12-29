pub mod edge;
pub mod raft;
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    net::SocketAddr,
    ops::Deref,
    sync::{self, Arc, RwLock},
    time::Duration,
};

use super::{
    endpoint::EndpointAddr,
    topic::{
        durable_message::{DurableCommand, DurableMessageQuery},
        Topic, TopicCode,
    },
};
pub use asteroid_mq_model::NodeId;
use edge::{
    auth::EdgeAuthService,
    codec::CodecRegistry,
    connection::{
        ConnectionConfig, EdgeConnectionInstance, EdgeConnectionRef, NodeConnection,
        NodeConnectionError,
    },
    middleware::EdgeConnectionHandlerObject,
    EdgeConfig, EdgeResult,
};
use edge::{
    packet::{Auth, EdgePacket},
    EdgeError, EdgeErrorKind,
};
use futures_util::TryFutureExt;
use openraft::Raft;
use raft::{
    cluster::{ClusterProvider, ClusterService},
    log_storage::LogStorage,
    network_factory::TcpNetworkService,
    proposal::{EndpointOffline, EndpointOnline, LoadTopic, Proposal},
    raft_node::TcpNode,
    state_machine::{topic::config::TopicConfig, StateMachineStore},
    MaybeLoadingRaft, TypeConfig,
};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::{
    prelude::{DurableMessage, DurableService},
    DEFAULT_TCP_SOCKET_ADDR,
};

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
    pub edge_handler: tokio::sync::RwLock<EdgeConnectionHandlerObject>,
    codec_registry: Arc<CodecRegistry>,
    topics: RwLock<HashMap<TopicCode, Topic>>,
    durable_commands_queue: std::sync::RwLock<VecDeque<DurableCommand>>,
    /// ensure operations on the same topic are linearizable
    pub(crate) durable_syncs: tokio::sync::Mutex<HashMap<TopicCode, Arc<tokio::sync::Mutex<()>>>>,
    pub(crate) scheduler: tsuki_scheduler::AsyncSchedulerClient<tsuki_scheduler::runtime::Tokio>,
    ct: CancellationToken,
}

impl NodeInner {
    pub async fn shutdown(&self) {
        if let Some(raft) = self.raft.get_opt() {
            let result = raft.shutdown().await;
            if let Err(e) = result {
                tracing::error!(?e, "raft shutdown error");
            }
        }
        self.ct.cancel();
    }
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
    /// Get the inner openraft structure
    pub async fn raft(&self) -> Raft<TypeConfig> {
        self.raft.get().await
    }
    /// Get node config
    pub fn config(&self) -> &NodeConfig {
        &self.config
    }
    /// Create a new node
    pub fn new(config: NodeConfig) -> Self {
        let ct = CancellationToken::new();
        let raft = MaybeLoadingRaft::new();
        let network = raft.net_work_service(config.id, TcpNode::new(config.addr), ct.child_token());
        let scheduler_runner = tsuki_scheduler::AsyncSchedulerRunner::tokio()
            .with_execute_duration(Duration::from_secs(1));
        let scheduler_client = scheduler_runner.client();
        let scheduler_running =
            scheduler_runner.run_with_shutdown_signal(Box::pin(ct.child_token().cancelled_owned()));
        tokio::spawn(scheduler_running);
        let inner = NodeInner {
            edge_connections: RwLock::new(HashMap::new()),
            edge_routing: RwLock::new(HashMap::new()),
            topics: RwLock::new(HashMap::new()),
            edge_handler: tokio::sync::RwLock::new(EdgeConnectionHandlerObject::basic()),
            config,
            raft,
            codec_registry: Arc::new(CodecRegistry::new_preloaded()),
            network,
            durable_commands_queue: Default::default(),
            durable_syncs: Default::default(),
            scheduler: scheduler_client,
            ct,
        };
        Self {
            inner: Arc::new(inner),
        }
    }
    /// Start running, this will start tcp connection service and node discovery service
    pub async fn start<C: ClusterProvider>(
        &self,
        mut cluster_provider: C,
    ) -> Result<(), crate::Error> {
        if self.raft_opt().is_some() {
            return Ok(());
        }
        let cluster_service_ct = self.ct.child_token();
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
        tcp_service.run_service();
        // ensure connection layer
        tokio::spawn(async move {});
        let raft = Raft::<TypeConfig>::new(
            id,
            Arc::new(raft_config),
            tcp_service.clone(),
            LogStorage::default(),
            Arc::new(state_machine_store),
        )
        .await
        .map_err(crate::Error::contextual_custom("create raft node"))?;
        // waiting for members contain self
        let pristine_nodes = cluster_provider.pristine_nodes().await?;
        tracing::info!(?pristine_nodes);
        if pristine_nodes.contains_key(&id) {
            raft.initialize(
                pristine_nodes
                    .into_iter()
                    .map(|(k, n)| (k, TcpNode::new(n)))
                    .collect::<BTreeMap<_, _>>(),
            )
            .await
            .map_err(crate::Error::contextual_custom("init raft node"))?;
        } else {
            return Err(crate::Error::unknown(
                format!("{id} not in cluster: {pristine_nodes:?}")
            ));
        }
        maybe_loading_raft.set(raft.clone());
        let cluster_service =
            ClusterService::new(cluster_provider, tcp_service, cluster_service_ct);
        cluster_service.spawn();
        tracing::info!("node started");
        Ok(())
    }

    /// load existed topic from durable service
    #[tracing::instrument(skip_all)]
    pub async fn load_from_durable_service(&self) -> Result<(), crate::Error> {
        const PAGE_SIZE: u32 = 100;
        let Some(durable) = self.config.durable.as_ref().cloned() else {
            return Ok(());
        };
        let topics = durable
            .topic_list()
            .await
            .map_err(crate::Error::contextual("load topic list"))?;
        let mut task_set = tokio::task::JoinSet::new();
        for topic in topics {
            let node = self.clone();
            let durable = durable.clone();
            let task = async move {
                let mut query = DurableMessageQuery {
                    limit: PAGE_SIZE,
                    offset: 0,
                };
                let mut queue = Vec::new();
                let code = topic.code.clone();
                loop {
                    let page = durable
                        .batch_retrieve(code.clone(), query)
                        .await
                        .map_err(crate::Error::contextual_custom("batch retrieve"))?;
                    let page_len = page.len();
                    queue.extend(page);
                    if page_len < PAGE_SIZE as usize {
                        break;
                    } else {
                        query = query.next_page()
                    }
                }
                let result = node.load_topic(topic, queue).await;
                if let Err(e) = result {
                    match e.kind {
                        crate::error::ErrorKind::TopicAlreadyExists
                        | crate::error::ErrorKind::NotLeader => {
                            // do nothing
                        }
                        _ => {
                            return Err(e);
                        }
                    }
                    tracing::error!(?e, "load topic error");
                }
                tracing::info!(topic = %code, "load topic done");
                crate::Result::Ok(())
            };
            task_set.spawn(task);
        }
        while let Some(task) = task_set.join_next().await {
            task.map_err(|e| crate::Error::custom("runtime join error", e))??;
        }
        Ok(())
    }
    /// Propose a raft proposal
    ///
    /// The final behavior is determined by the role of raft node.
    pub(crate) async fn propose(&self, proposal: Proposal) -> Result<(), crate::Error> {
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
            let Some(connection) = self.network.get_connection(leader).await else {
                return Err(crate::Error::new(
                    "no connection to leader",
                    crate::error::ErrorKind::Offline,
                ));
            };
            connection.propose(proposal).await?
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

    /// Get a weak reference of this node
    pub fn node_ref(&self) -> NodeRef {
        NodeRef {
            inner: Arc::downgrade(&self.inner),
        }
    }

    /// Create a connection to a edge node.
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
            auth: edge_config.peer_auth,
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
    pub(crate) fn remove_edge_routing(&self, addr: &EndpointAddr) {
        self.edge_routing.write().unwrap().remove(addr);
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
                let _ = node.propose(Proposal::EpOffline(offline)).await;
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
                self.set_edge_routing(endpoint, from, topic_code.clone());
                let result = node
                    .propose(Proposal::EpOnline(EndpointOnline {
                        topic_code,
                        interests: online.interests,
                        endpoint,
                        host: node.id(),
                    }))
                    .await;
                if let Err(e) = result {
                    // rollback edge routing change
                    self.remove_edge_routing(&endpoint);
                    return Err(EdgeError::with_message(
                        "endpoint online",
                        e.to_string(),
                        EdgeErrorKind::Internal,
                    ));
                }
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
                node.propose(Proposal::EpOffline(EndpointOffline {
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
                self.remove_edge_routing(&offline.endpoint);
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
                node.propose(Proposal::EpInterest(interest.clone()))
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
                node.propose(Proposal::SetState(set_state.clone()))
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
        self.propose(Proposal::LoadTopic(LoadTopic { config, queue }))
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

impl Drop for NodeInner {
    fn drop(&mut self) {}
}
