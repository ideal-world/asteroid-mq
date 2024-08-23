#[cfg(feature = "cluster-k8s")]
pub mod k8s;
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    net::SocketAddr,
    sync::Arc,
};

use tokio::sync::RwLock;
use tracing::warn;

use crate::protocol::node::connection;

use super::node::{Node, NodeId};
#[derive(Debug, Clone)]
pub struct TcpClusterInfo {
    pub size: u64,
    pub nodes: HashMap<NodeId, SocketAddr>,
}

pub struct TcpClusterConnection {}

pub trait TcpClusterProvider: Send + 'static {
    fn next_update(&mut self) -> impl Future<Output = TcpClusterInfo> + Send;
}

impl Node {
    pub async fn create_cluster(
        &self,
        mut provider: impl TcpClusterProvider,
        listen_on: SocketAddr,
    ) -> std::io::Result<()> {
        let this = self.clone();
        let listener = tokio::net::TcpListener::bind(listen_on).await?;
        let connection_state: Arc<RwLock<HashMap<NodeId, SocketAddr>>> = Default::default();
        {
            let node = this.clone();
            let connection_state = connection_state.clone();
            tokio::spawn(async move {
                loop {
                    let info = provider.next_update().await;
                    node.set_cluster_size(info.size);
                    let mut lock = connection_state.write().await;
                    let mut deleted = HashSet::new();
                    let mut added = HashMap::new();
                    for (peer_node_id, addr) in &info.nodes {
                        if !lock.contains_key(peer_node_id) && *peer_node_id != node.id() {
                            added.insert(*peer_node_id, *addr);
                        }
                    }
                    for (key, _peer_addr) in lock.iter() {
                        if !info.nodes.contains_key(key) && *key != node.id() {
                            deleted.insert(*key);
                        }
                    }
                    if !(added.is_empty() || deleted.is_empty()) {
                        tracing::info!(?added, ?deleted, "cluster update");
                    }
                    for delete in deleted {
                        if let Some(_peer_addr) = lock.remove(&delete) {
                            node.remove_connection(delete)
                        }
                    }
                    for (peer_id, peer_addr) in added {
                        let node = node.clone();
                        let connection_state = connection_state.clone();
                        tokio::spawn(async move {
                            let stream = tokio::net::TcpStream::connect(peer_addr).await;
                            if let Ok(stream) = stream {
                                let conn =
                                    crate::protocol::node::connection::tokio_tcp::TokioTcp::new(
                                        stream,
                                    );
                                match node.create_connection(conn).await {
                                    Ok(peer_id) => {
                                        connection_state.write().await.insert(peer_id, peer_addr);
                                        tracing::info!(
                                            ?peer_id,
                                            ?peer_addr,
                                            "connected to new node"
                                        );
                                    }
                                    Err(e) => {
                                        if let connection::N2NConnectionErrorKind::Existed = e.kind
                                        {
                                            tracing::debug!(
                                                ?peer_id,
                                                ?peer_addr,
                                                "connection existed"
                                            );
                                        } else {
                                            warn!(?e, "failed to create connection");
                                        }
                                    }
                                }
                            }
                        });
                    }
                }
            })
        };
        let _h_accept = {
            let node = this.clone();
            let connection_state = connection_state.clone();
            tokio::spawn(async move {
                loop {
                    if let Ok((stream, peer)) = listener.accept().await.inspect_err(|e| {
                        warn!(?e, "accept error");
                    }) {
                        let connection_state = connection_state.clone();
                        let node = node.clone();
                        tokio::spawn(async move {
                            tracing::debug!(?peer, "accept new tcp connection");
                            let conn =
                                crate::protocol::node::connection::tokio_tcp::TokioTcp::new(stream);
                            let mut lock = connection_state.write().await;
                            match node.create_connection(conn).await {
                                Ok(peer_id) => {
                                    lock.insert(peer_id, peer);
                                    tracing::info!(?peer, "new connection");
                                }
                                Err(e) => {
                                    if let connection::N2NConnectionErrorKind::Existed = e.kind {
                                        tracing::debug!(?peer, "connection existed");
                                    } else {
                                        warn!(?e, "failed to create connection");
                                    }
                                }
                            }
                        });
                    }
                }
            })
        };
        Ok(())
    }
}
