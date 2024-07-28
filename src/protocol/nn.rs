pub mod codec;
pub mod connection;
pub mod event;

use std::{
    borrow::Cow,
    collections::HashMap,
    mem::size_of,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    str::FromStr,
    sync::{self, Arc},
};

use bytes::{Buf, Bytes, BytesMut};
use codec::NNCodecType;
use connection::{
    tokio_tcp::TokioTcp, ConnectionConfig, N2NConnection, N2NConnectionError,
    N2NConnectionInstance, N2NConnectionRef,
};
use crossbeam::{epoch::Pointable, sync::ShardedLock};
use event::{N2NAuth, N2NEventPacket, N2NMessageEvent, N2NUnreachableEvent, NodeKind, NodeTrace};
use serde::{Deserialize, Serialize};

use crate::endpoint::{Endpoint, EndpointAddr};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeId {
    pub bytes: [u8; 16],
}

impl std::fmt::Debug for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("NodeId")
            .field(&crate::util::hex(&self.bytes))
            .finish()
    }
}
impl NodeId {
    pub fn new() -> NodeId {
        static INSTANCE_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dg = crate::util::executor_digest();
        let mut bytes = [0; 16];
        bytes[0..8].copy_from_slice(&dg.to_be_bytes());
        bytes[8..16].copy_from_slice(
            &INSTANCE_ID
                .fetch_add(1, sync::atomic::Ordering::SeqCst)
                .to_be_bytes(),
        );
        NodeId { bytes }
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self::new()
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: NodeId,
    pub kind: NodeKind,
}

impl Default for NodeInfo {
    fn default() -> Self {
        Self {
            id: NodeId::new(),
            kind: NodeKind::Cluster,
        }
    }
}

#[derive(Debug, Default)]
pub struct Node {
    info: NodeInfo,
    pub(crate) endpoints: ShardedLock<HashMap<EndpointAddr, Arc<Endpoint>>>,
    // ne layer
    pub(crate) ep_routing_table: ShardedLock<HashMap<EndpointAddr, NodeId>>,
    // nn layer
    n2n_routing_table: ShardedLock<HashMap<NodeId, N2nRoutingInfo>>,
    connections: ShardedLock<HashMap<NodeId, Arc<N2NConnectionInstance>>>,
    auth: N2NAuth,
}

impl Node {
    pub async fn create_connection<C: N2NConnection>(
        self: &Arc<Self>,
        conn: C,
    ) -> Result<(), N2NConnectionError> {
        let config = ConnectionConfig {
            attached_node: Arc::downgrade(self),
            auth: self.auth.clone(),
        };
        let conn_inst = N2NConnectionInstance::init(config, conn).await?;
        self.connections
            .write()
            .unwrap()
            .insert(conn_inst.peer_info.id, Arc::new(conn_inst));
        Ok(())
    }
    pub fn id(&self) -> NodeId {
        self.info.id
    }
    pub async fn handle_message(&self, message_evt: N2NMessageEvent) {
        self.record_routing_info(&message_evt.trace);
        if message_evt.to == self.id() {
            self.handle_message_as_desitination(message_evt).await;
        } else {
            self.forward_message(message_evt);
        }
    }
    pub async fn handle_message_as_desitination(&self, message_evt: N2NMessageEvent) {
        tracing::trace!(message=?message_evt, "message received");
    }
    pub fn get_connection(&self, to: NodeId) -> Option<N2NConnectionRef> {
        let connections = self.connections.read().unwrap();
        let conn = connections.get(&to)?;
        if conn.is_alive() {
            Some(conn.get_connection_ref())
        } else {
            None
        }
    }

    pub fn remove_connection(&self, to: NodeId) {
        self.connections.write().unwrap().remove(&to);
    }
    pub fn send_packet(&self, packet: N2NEventPacket, to: NodeId) -> Result<(), N2NEventPacket> {
        let Some(conn) = self.get_connection(to) else {
            return Err(packet);
        };
        if let Err(e) = conn.outbound.send(packet) {
            tracing::error!(error=?e, "failed to send packet");
            return Err(e.0);
        }
        Ok(())
    }
    pub fn report_unreachable(&self, raw: N2NMessageEvent) {
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
        let packet = N2NEventPacket::unreachable(unreachable_event);
        if let Err(packet) = self.send_packet(packet, prev_node) {
            tracing::warn!(
                id = ?packet.id(),
                "trying to report unreachable but previous node lost connection"
            );
        }
    }

    pub fn record_routing_info(&self, trace: &NodeTrace) {
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

    pub fn forward_message(&self, mut message_evt: N2NMessageEvent) {
        let Some(next_jump) = self.get_next_jump(message_evt.to) else {
            self.report_unreachable(message_evt);
            return;
        };
        message_evt.trace.hops.push(self.id());
        if let Err(packet) = self.send_packet(N2NEventPacket::message(message_evt), next_jump) {
            let raw_event = N2NMessageEvent::decode(packet.payload)
                .expect("should be valid")
                .0;
            self.report_unreachable(raw_event);
        }
    }

    pub fn handle_unreachable(&self, unreachable_evt: N2NUnreachableEvent) {
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
    pub fn get_next_jump(&self, to: NodeId) -> Option<NodeId> {
        let routing_table = self.n2n_routing_table.read().unwrap();
        routing_table.get(&to).map(|info| info.next_jump)
    }
}
#[derive(Debug)]
pub struct N2nRoutingInfo {
    next_jump: NodeId,
    hops: u32,
}

pub struct Connection {
    pub attached_node: sync::Weak<Node>,
    pub peer_info: NodeInfo,
}

#[tokio::test]
async fn test_nodes() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .init();

    let node_server = <Arc<Node>>::default();
    let server_id = node_server.id();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:10080")
        .await
        .unwrap();
    tokio::spawn(async move {
        while let Ok((stream, peer)) = listener.accept().await {
            tracing::info!(peer=?peer, "new connection");
            let node = Arc::clone(&node_server);
            tokio::spawn(async move {
                let conn = TokioTcp::new(stream);
                node.create_connection(conn).await.unwrap();
            });
        }
    });

    let node_client = <Arc<Node>>::default();
    let client_id = node_client.id();
    let stream_client = tokio::net::TcpSocket::new_v4()
        .unwrap()
        .connect(SocketAddr::from_str("127.0.0.1:10080").unwrap())
        .await
        .unwrap();

    node_client
        .create_connection(TokioTcp::new(stream_client))
        .await
        .unwrap();

    let server_conn = node_client.get_connection(server_id).unwrap();
    server_conn
        .outbound
        .send(N2NEventPacket::message(N2NMessageEvent {
            to: server_id,
            trace: NodeTrace {
                source: client_id,
                hops: vec![],
            },
            payload: Bytes::from_static(b"hello"),
        }))
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
}
