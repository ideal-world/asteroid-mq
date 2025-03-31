pub mod durable_commit_service;
pub mod durable_message;
pub mod edge;
pub mod raft;
use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    ops::Deref,
    sync::{self, Arc, RwLock},
    time::Duration,
};

use durable_message::DurableMessageQuery;

pub use asteroid_mq_model::NodeId;
use asteroid_mq_model::{
    codec::BINCODE_CONFIG, connection::EdgeNodeConnection, EndpointAddr, Message, MessageId,
    TopicCode,
};
use durable_commit_service::DurableCommitService;
use edge::{
    auth::EdgeAuthService,
    connection::{
        ConnectionConfig, EdgeConnectionError, EdgeConnectionInstance, EdgeConnectionRef,
    },
    middleware::EdgeConnectionHandlerObject,
    EdgeConfig, EdgeResult,
};
use edge::{packet::Auth, EdgeError, EdgeErrorKind};
use futures_util::TryFutureExt;
use openraft::Raft;
use raft::{
    cluster::{ClusterProvider, ClusterService},
    log_storage::LogStorage,
    network_factory::TcpNetworkService,
    proposal::{DelegateMessage, EndpointOffline, EndpointOnline, LoadTopic, Proposal},
    raft_node::TcpNode,
    state_machine::{
        node::NodeData,
        topic::{
            config::TopicConfig,
            wait_ack::{AckSender, WaitAckHandle},
        },
        StateMachineStore,
    },
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
    edge_routing: tokio::sync::RwLock<HashMap<EndpointAddr, (NodeId, TopicCode)>>,
    pub edge_handler: tokio::sync::RwLock<EdgeConnectionHandlerObject>,

    // May we delete this and access the node api from edge sdk with tokio channel socket connection?
    // #[deprecated]
    // topics: RwLock<HashMap<TopicCode, Topic>>,
    /// ack responser pool
    pub(crate) ack_waiting_pool: Arc<tokio::sync::RwLock<HashMap<MessageId, AckSender>>>,

    /// ensure operations on the same topic are linearized
    pub(crate) durable_commit_service: Option<DurableCommitService>,
    /// time scheduler
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
        let network = raft.net_work_service(
            config.id,
            TcpNode::new(config.addr.to_string()),
            ct.child_token(),
        );
        let scheduler_runner = tsuki_scheduler::AsyncSchedulerRunner::tokio()
            .with_execute_duration(Duration::from_secs(1));
        let scheduler_client = scheduler_runner.client();
        let scheduler_running =
            scheduler_runner.run_with_shutdown_signal(Box::pin(ct.child_token().cancelled_owned()));
        tokio::spawn(scheduler_running);
        let inner = NodeInner {
            edge_connections: RwLock::new(HashMap::new()),
            edge_routing: tokio::sync::RwLock::new(HashMap::new()),
            // topics: RwLock::new(HashMap::new()),
            edge_handler: tokio::sync::RwLock::new(EdgeConnectionHandlerObject::basic()),
            durable_commit_service: config
                .durable
                .as_ref()
                .map(|ds| DurableCommitService::new(raft.clone(), ds.clone(), ct.child_token())),
            config,
            raft,
            network,
            scheduler: scheduler_client,
            ack_waiting_pool: Default::default(),
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
        // start up durable commit service
        if let Some(durable_commit_service) = &self.durable_commit_service {
            durable_commit_service.clone().spawn()
        }
        let raft = Raft::<TypeConfig>::new(
            id,
            Arc::new(raft_config.clone()),
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
            loop {
                let mut peer_nodes = pristine_nodes.clone();
                peer_nodes.remove(&id);
                let mut ensured_nodes = BTreeMap::new();
                for (peer_id, peer_addr) in peer_nodes.iter() {
                    if ensured_nodes.contains_key(peer_id) {
                        continue;
                    }
                    let ensure_result = tcp_service
                        .ensure_connection(*peer_id, peer_addr.clone())
                        .await;
                    if let Err(e) = ensure_result {
                        tracing::warn!("failed to ensure connection to {}: {}", peer_id, e);
                    } else {
                        ensured_nodes.insert(*peer_id, peer_addr.clone());
                        tracing::trace!("connection to {} ensured", peer_id);
                    }
                }
                if ensured_nodes.len() == peer_nodes.len() {
                    break;
                } else {
                    tokio::time::sleep(Duration::from_millis(raft_config.heartbeat_interval)).await;
                }
            }
            maybe_loading_raft.set(raft.clone());
            raft.initialize(
                pristine_nodes
                    .into_iter()
                    .map(|(k, n)| (k, TcpNode::new(n)))
                    .collect::<BTreeMap<_, _>>(),
            )
            .await
            .map_err(crate::Error::contextual_custom("init raft node"))?;

            // wait for election
            raft.wait(None)
                .metrics(
                    |rm| rm.current_leader.is_some(),
                    "wait for leader to be elected",
                )
                .await
                .map_err(crate::Error::contextual_custom("wait for leader"))?;
        } else {
            return Err(crate::Error::unknown(format!(
                "{id} not in cluster: {pristine_nodes:?}"
            )));
        }
        let cluster_service =
            ClusterService::new(cluster_provider, tcp_service, cluster_service_ct);
        cluster_service.spawn();
        tracing::info!("node started");
        Ok(())
    }

    /// load existed topic from durable service
    #[tracing::instrument(skip_all)]
    pub async fn load_from_durable_service(&self) -> Result<(), crate::Error> {
        const PAGE_SIZE: u32 = 1000;
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
                        tracing::info!(size = ?queue.len(), "message chunk loading finished");
                        break;
                    } else {
                        tracing::info!(size = ?queue.len(), "message chunk loaded");
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
        let is_leader = this == leader;
        tracing::debug!(?proposal, is_leader, "trigger proposal");
        let client_write_result = if is_leader {
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
    pub async fn create_edge_connection<C: EdgeNodeConnection>(
        &self,
        conn: C,
        edge_config: EdgeConfig,
    ) -> Result<NodeId, EdgeConnectionError> {
        let config = ConnectionConfig {
            attached_node: self.node_ref(),
            auth: edge_config.peer_auth.into(),
            peer_id: edge_config.peer_id,
        };
        let conn_inst = EdgeConnectionInstance::init(config, conn).await?;
        let peer = conn_inst.peer_id;
        {
            let mut wg = self.edge_connections.write().unwrap();
            wg.insert(peer, Arc::new(conn_inst));
            tracing::info!(?peer, "new edge connection established");
        }
        Ok(peer)
    }
    #[inline]
    pub fn id(&self) -> NodeId {
        self.config.id
    }
    #[inline(always)]
    pub fn is(&self, id: NodeId) -> bool {
        self.id() == id
    }

    pub(crate) fn blocking_get_edge_routing(&self, addr: &EndpointAddr) -> Option<NodeId> {
        // we can do this for we never hold write guard across an await point
        loop {
            let routing = self.edge_routing.try_read();
            match routing {
                Ok(routing) => return Some(routing.get(addr)?.0),
                Err(_e) => {
                    continue;
                }
            }
        }
    }
    // pub(crate) async fn get_edge_routing(&self, addr: &EndpointAddr) -> Option<NodeId> {
    //     let routing = self.edge_routing.read().await;
    //     Some(routing.get(addr)?.0)
    // }
    pub(crate) async fn set_edge_routing(
        &self,
        addr: EndpointAddr,
        edge: NodeId,
        topic: TopicCode,
    ) {
        self.edge_routing.write().await.insert(addr, (edge, topic));
    }
    /// Read in sync, write in async
    pub(crate) async fn remove_edge_routing(&self, addr: &EndpointAddr) {
        tracing::error!(?addr, "remove_edge_routing start");
        self.edge_routing.write().await.remove(addr);
        tracing::error!(?addr, "remove_edge_routing finished");
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
    pub async fn remove_edge_connection(&self, to: NodeId) {
        let Some(_connection) = self.edge_connections.write().unwrap().remove(&to) else {
            return;
        };
        let mut routing = self.edge_routing.write().await;
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
    pub async fn check_ep_auth(&self, ep: &EndpointAddr, peer: &NodeId) -> Result<(), EdgeError> {
        let routing = self.edge_routing.read().await;
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

                let handle = self
                    .send_message(topic_code, message)
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
                let Some(node) = self.node_ref().upgrade() else {
                    return Err(EdgeError::with_message(
                        "set state",
                        "node dropped",
                        EdgeErrorKind::Internal,
                    ));
                };
                let endpoint = EndpointAddr::new_snowflake();
                self.set_edge_routing(endpoint, from, topic_code.clone())
                    .await;
                tracing::info!(?online, ?endpoint, "edge endpoint online");
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
                    self.remove_edge_routing(&endpoint).await;
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

                let Some(node) = self.node_ref().upgrade() else {
                    return Err(EdgeError::with_message(
                        "set state",
                        "node dropped",
                        EdgeErrorKind::Internal,
                    ));
                };
                self.check_ep_auth(&offline.endpoint, &from).await?;
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
                tracing::error!("proposed");
                self.remove_edge_routing(&offline.endpoint).await;
                tracing::error!("finished");
                Ok(edge::EdgeResponseEnum::EndpointOffline)
            }
            edge::EdgeRequestEnum::EndpointInterest(interest) => {
                let Some(node) = self.node_ref().upgrade() else {
                    return Err(EdgeError::with_message(
                        "set state",
                        "node dropped",
                        EdgeErrorKind::Internal,
                    ));
                };
                self.check_ep_auth(&interest.endpoint, &from).await?;
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
                let Some(node) = self.node_ref().upgrade() else {
                    return Err(EdgeError::with_message(
                        "set state",
                        "node dropped",
                        EdgeErrorKind::Internal,
                    ));
                };
                for ep in set_state.update.status.keys() {
                    self.check_ep_auth(ep, &from).await?;
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

    pub async fn is_leader(&self) -> bool {
        let raft = self.raft().await;
        raft.ensure_linearizable().await.is_ok()
    }
    pub async fn wait_for_leader(&self) -> Result<(), crate::Error> {
        let raft = self.raft().await;
        tracing::info!("wait for leader to be elected");
        raft.wait(None)
            .metrics(|rm| rm.current_leader.is_some(), "wait for leader")
            .await
            .map_err(crate::Error::contextual_custom("wait for leader"))?;
        Ok(())
    }
    pub async fn load_topic<C: Into<TopicConfig>>(
        &self,
        config: C,
        queue: Vec<DurableMessage>,
    ) -> Result<(), crate::Error> {
        let config: TopicConfig = config.into();
        tracing::info!(?config, "load_topic");
        self.propose(Proposal::LoadTopic(LoadTopic { config, queue }))
            .await?;
        Ok(())
    }
    pub async fn create_new_topic<C: Into<TopicConfig>>(&self, config: C) -> crate::Result<()> {
        self.load_topic(config, Vec::new()).await
    }
    /// Create the wait handle of a specific message
    pub(crate) async fn remove_wait_ack(&self, id: MessageId) {
        self.ack_waiting_pool.write().await.remove(&id);
    }

    /// Create the wait handle of a specific message
    pub async fn set_wait_ack(&self, id: MessageId) -> WaitAckHandle {
        let (sender, handle) = WaitAckHandle::new(id);
        self.ack_waiting_pool.write().await.insert(id, sender);
        handle
    }

    /// Send a message out, and get a awaitable handle.
    pub async fn send_message(
        &self,
        topic: TopicCode,
        message: Message,
    ) -> Result<WaitAckHandle, crate::Error> {
        let id = message.id();
        let source = self.id();
        let handle = self.set_wait_ack(id).await;
        let proposal_result = self
            .propose(Proposal::DelegateMessage(DelegateMessage {
                topic,
                message,
                source,
            }))
            .await;
        if proposal_result.is_err() {
            self.remove_wait_ack(id).await;
            tracing::warn!(?proposal_result, "proposal error");
            proposal_result?;
        }
        Ok(handle)
    }
    pub async fn snapshot_data(&self) -> Result<NodeData, crate::Error> {
        let raft = self.raft().await;
        let mut snapshot = raft
            .get_snapshot()
            .await
            .map_err(crate::Error::contextual_custom("get snapshot"))?
            .ok_or(crate::Error::custom(
                "get snapshot",
                "raft instance missing snapshot",
            ))?;
        let data = bincode::serde::decode_from_std_read::<NodeData, _, _>(
            &mut snapshot.snapshot,
            BINCODE_CONFIG,
        )
        .map_err(crate::Error::contextual_custom("get snapshot"))?;
        Ok(data)
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
