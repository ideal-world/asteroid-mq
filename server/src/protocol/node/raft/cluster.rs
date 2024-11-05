use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    future::Future,
    net::SocketAddr,
};

use crate::prelude::NodeId;
#[cfg(feature = "cluster-k8s")]
pub(crate) mod k8s;
#[cfg(feature = "cluster-k8s")]
pub use k8s::K8sClusterProvider;
pub(crate) mod r#static;
use openraft::ChangeMembers;
pub use r#static::StaticClusterProvider;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

use super::{network_factory::TcpNetworkService, raft_node::TcpNode};
pub trait ClusterProvider: Send + 'static {
    fn pristine_nodes(
        &mut self,
    ) -> impl Future<Output = crate::Result<BTreeMap<NodeId, SocketAddr>>> + Send {
        self.next_update()
    }
    fn next_update(
        &mut self,
    ) -> impl Future<Output = crate::Result<BTreeMap<NodeId, SocketAddr>>> + Send;
    fn name(&self) -> Cow<'static, str> {
        std::any::type_name::<Self>().into()
    }
}

pub struct DynClusterProvider {
    inner: Box<dyn sealed::ClusterProviderObjectTrait + Send>,
    name: Cow<'static, str>,
}

impl std::fmt::Debug for DynClusterProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynClusterProvider")
            .field("name", &self.name)
            .finish()
    }
}

impl DynClusterProvider {
    pub fn new<T>(inner: T) -> Self
    where
        T: ClusterProvider,
    {
        let provider_name = inner.name();
        Self {
            inner: Box::new(inner),
            name: provider_name,
        }
    }
    pub fn with_name(self, name: impl Into<Cow<'static, str>>) -> Self {
        Self {
            name: name.into(),
            ..self
        }
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub async fn pristine_nodes(&mut self) -> crate::Result<BTreeMap<NodeId, SocketAddr>> {
        self.inner.pristine_nodes().await
    }
    pub async fn next_update(&mut self) -> crate::Result<BTreeMap<NodeId, SocketAddr>> {
        self.inner.next_update().await
    }
}

mod sealed {
    use super::ClusterProvider;
    use crate::prelude::NodeId;
    use std::{collections::BTreeMap, future::Future, net::SocketAddr, pin::Pin};
    type DynUpdate<'a> =
        dyn Future<Output = crate::Result<BTreeMap<NodeId, SocketAddr>>> + Send + 'a;
    pub trait ClusterProviderObjectTrait {
        fn pristine_nodes(&mut self) -> Pin<Box<DynUpdate<'_>>>;
        fn next_update(&mut self) -> Pin<Box<DynUpdate<'_>>>;
    }

    impl<T> ClusterProviderObjectTrait for T
    where
        T: ClusterProvider,
    {
        fn pristine_nodes(&mut self) -> Pin<Box<DynUpdate<'_>>> {
            Box::pin(self.pristine_nodes())
        }
        fn next_update(&mut self) -> Pin<Box<DynUpdate<'_>>> {
            Box::pin(self.next_update())
        }
    }
}

pub struct ClusterService {
    provider: DynClusterProvider,
    tcp_network_service: TcpNetworkService,
    ct: CancellationToken,
}

impl ClusterService {
    pub fn new(
        provider: impl ClusterProvider,
        tcp_network_service: TcpNetworkService,
        ct: CancellationToken,
    ) -> Self {
        Self {
            provider: DynClusterProvider::new(provider),
            tcp_network_service,
            ct,
        }
    }
    #[instrument(name="cluster_service", skip(self), fields(cluster_provider_name=%self.provider.name(), local_id=%self.tcp_network_service.info.id))]
    pub async fn run(self) -> Result<(), crate::Error> {
        tracing::info!("cluster service started");
        let Self {
            mut provider,
            tcp_network_service,
            ct,
        } = self;
        let local_id = tcp_network_service.info.id;
        // 3. listen cluster update
        loop {
            let nodes = tokio::select! {
                _ = ct.cancelled() => break,
                nodes = provider.next_update() => {
                    nodes?
                }
            };

            // ensure connections to all nodes
            let mut ensured_nodes = BTreeMap::new();
            for (peer_id, peer_addr) in nodes.clone() {
                if local_id == peer_id {
                    ensured_nodes.insert(peer_id, TcpNode::new(peer_addr));
                } else {
                    tracing::trace!("ensuring connection to {}", peer_id);
                    let ensure_result = tcp_network_service
                        .ensure_connection(peer_id, peer_addr)
                        .await;
                    if let Err(e) = ensure_result {
                        tracing::warn!("failed to ensure connection to {}: {}", peer_id, e);
                    } else {
                        tracing::trace!("connection to {} ensured", peer_id);
                        ensured_nodes.insert(peer_id, TcpNode::new(peer_addr));
                    }
                }
            }
            // raft update members
            let raft = tcp_network_service.raft.get().await;
            let Ok(current_members) = raft.with_raft_state(|r| r.membership_state.clone()).await
            else {
                continue;
            };
            let current_nodes = current_members
                .committed()
                .nodes()
                .map(|(k, v)| (*k, *v))
                .collect::<BTreeMap<_, _>>();
            let to_remove = current_nodes
                .keys()
                .filter(|k| !ensured_nodes.contains_key(k))
                .cloned()
                .collect::<BTreeSet<_>>();
            let to_add = ensured_nodes
                .iter()
                .filter_map(|(k, v)| {
                    if !current_nodes.contains_key(k) {
                        Some((*k, *v))
                    } else {
                        None
                    }
                })
                .collect::<BTreeMap<_, _>>();
            tracing::debug!(ensured = ?ensured_nodes, remove = ?to_remove, add = ?to_add, "updating raft members");
            let leader_node = raft.current_leader().await;
            let Some(leader_node) = leader_node else {
                tracing::warn!("no leader node found");
                let elect_result = raft.trigger().elect().await;
                if let Err(e) = elect_result {
                    tracing::error!("failed to trigger election: {}", e);
                }
                continue;
            };
            if to_remove.contains(&leader_node) {
                let elect_result = raft.trigger().elect().await;
                if let Err(e) = elect_result {
                    tracing::error!("failed to trigger election: {}", e);
                }
            }
            if local_id == leader_node {
                if !to_add.is_empty() {
                    let raft = raft.clone();
                    tokio::spawn(async move {
                        let add_nodes_result = raft
                            .change_membership(ChangeMembers::AddNodes(to_add.clone()), false)
                            .await;
                        if let Err(e) = add_nodes_result {
                            tracing::warn!("failed to add nodes: {}", e);
                        }
                        let add_voters_result = raft
                            .change_membership(ChangeMembers::AddVoters(to_add.clone()), false)
                            .await;
                        if let Err(e) = add_voters_result {
                            tracing::warn!("failed to add voters: {}", e);
                        }
                    });
                }
                if !to_remove.is_empty() {
                    let raft = raft.clone();
                    tokio::spawn(async move {
                        let remove_nodes_result = raft
                            .change_membership(ChangeMembers::RemoveNodes(to_remove.clone()), false)
                            .await;
                        if let Err(e) = remove_nodes_result {
                            tracing::warn!("failed to remove nodes: {}", e);
                        }
                        let remove_voters_result = raft
                            .change_membership(
                                ChangeMembers::RemoveVoters(to_remove.clone()),
                                false,
                            )
                            .await;
                        if let Err(e) = remove_voters_result {
                            tracing::warn!("failed to remove voters: {}", e);
                        }
                    });
                }
            }
            tracing::trace!("waiting for next update")
        }
        Ok(())
    }
    pub fn spawn(self) {
        tokio::spawn(self.run());
    }
}
