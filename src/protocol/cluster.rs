use std::{
    collections::{HashMap, HashSet},
    future::Future,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use dashmap::DashMap;
use tracing::warn;

use super::node::{Node, NodeId};
#[derive(Debug)]
pub struct TcpClusterInfo {
    pub size: u64,
    pub nodes: Vec<SocketAddr>,
}

pub struct TcpClusterConnection {}

pub trait TcpClusterProvider: Send + 'static {
    fn next_update(&mut self) -> impl Future<Output = TcpClusterInfo> + Send;
}

impl Node {
    pub fn create_cluster(
        &self,
        mut provider: impl TcpClusterProvider,
        local_bind: SocketAddr,
    ) -> std::io::Result<tokio::task::JoinHandle<()>> {
        let this = self.clone();
        let listener = std::net::TcpListener::bind(local_bind)?;
        let listener = tokio::net::TcpListener::from_std(listener)?;

        let task = tokio::spawn(async move {
            let connection_state: Arc<DashMap<SocketAddr, NodeId>> = Arc::new(DashMap::new());
            enum Event {
                Accept((tokio::net::TcpStream, SocketAddr)),
                Update(TcpClusterInfo),
            }
            let (event_sender, event_receiver) = flume::unbounded();

            {
                let event_sender = event_sender.clone();
                tokio::spawn(async move {
                    loop {
                        let update = provider.next_update().await;
                        let result = event_sender.send(Event::Update(update));
                        if result.is_err() {
                            return;
                        }
                    }
                })
            };
            tokio::spawn(async move {
                loop {
                    if let Ok((stream, peer)) = listener.accept().await.inspect_err(|e| {
                        warn!(?e, "accept error");
                    }) {
                        let result = event_sender.send(Event::Accept((stream, peer)));
                        if result.is_err() {
                            return;
                        }
                    }
                }
            });
            loop {
                let event = event_receiver.recv_async().await;
                let Ok(event) = event else { break };
                match event {
                    Event::Accept((stream, peer)) => {
                        let node = this.clone();
                        let conn =
                            crate::protocol::node::connection::tokio_tcp::TokioTcp::new(stream);
                        let connection_state = connection_state.clone();

                        tokio::spawn(async move {
                            let peer_id = node.create_connection(conn).await;
                            if let Ok(peer_id) = peer_id {
                                connection_state.insert(peer, peer_id);
                            }
                        });
                    }
                    Event::Update(info) => {
                        this.set_cluster_size(info.size);
                        let mut deleted = HashSet::new();
                        let mut added = HashSet::new();
                        for update in info.nodes {
                            if connection_state.contains_key(&update) {
                                deleted.insert(update);
                            } else {
                                added.insert(update);
                            }
                        }
                        tracing::info!(?added, ?deleted, "cluster update");

                        for delete in deleted {
                            if let Some((_, id)) = connection_state.remove(&delete) {
                                this.remove_connection(id)
                            }
                        }
                        for add in added {
                            let node = this.clone();
                            let connection_state = connection_state.clone();
                            tokio::spawn(async move {
                                let stream = tokio::net::TcpStream::connect(add).await;
                                tracing::debug!(?add, "connecting to new node");
                                if let Ok(stream) = stream {
                                    let conn =
                                        crate::protocol::node::connection::tokio_tcp::TokioTcp::new(
                                            stream,
                                        );
                                    if let Ok(peer_id) = node.create_connection(conn).await {
                                        connection_state.insert(add, peer_id);
                                    }
                                }
                            });
                        }
                    }
                }
            }
        });
        Ok(task)
    }
}
