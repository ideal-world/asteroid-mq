pub mod connection;
pub mod event;
pub mod hold_message;
pub mod wait_ack;
use std::{
    collections::HashMap,
    ops::Deref,
    sync::{self, Arc},
};

use super::codec::CodecType;
use connection::{
    ConnectionConfig, N2NConnection, N2NConnectionError, N2NConnectionErrorKind,
    N2NConnectionInstance, N2NConnectionRef,
};
use crossbeam::sync::ShardedLock;
use event::{N2NAuth, N2NUnreachableEvent, N2nEvent, N2nEventKind, N2nPacket, NodeKind, NodeTrace};
use hold_message::HoldMessage;
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
    pub fn new() -> NodeId {
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
        Self::new()
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
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
            id: NodeId::new(),
            kind: NodeKind::Cluster,
        }
    }
}

#[derive(Debug)]
pub struct NodeInner {
    info: NodeInfo,
    pub(crate) local_endpoints: ShardedLock<HashMap<EndpointAddr, LocalEndpointRef>>,
    // ne layer
    pub(crate) ep_routing_table: ShardedLock<HashMap<EndpointAddr, NodeId>>,
    pub(crate) ep_interest_map: ShardedLock<InterestMap<EndpointAddr>>,
    pub(crate) ep_latest_active: ShardedLock<HashMap<EndpointAddr, TimestampSec>>,
    pub(crate) hold_messages: ShardedLock<HashMap<MessageId, HoldMessage>>,
    // nn layer
    n2n_routing_table: ShardedLock<HashMap<NodeId, N2nRoutingInfo>>,
    connections: ShardedLock<HashMap<NodeId, Arc<N2NConnectionInstance>>>,
    auth: N2NAuth,
}

#[derive(Debug, Clone)]
pub struct NodeRef {
    inner: std::sync::Weak<NodeInner>,
}

impl NodeRef {
    pub fn upgrade(&self) -> Option<Node> {
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
        Self {
            inner: Arc::new(NodeInner {
                info: NodeInfo::default(),
                local_endpoints: ShardedLock::new(HashMap::new()),
                ep_routing_table: ShardedLock::new(HashMap::new()),
                ep_interest_map: ShardedLock::new(InterestMap::new()),
                ep_latest_active: ShardedLock::new(HashMap::new()),
                hold_messages: Default::default(),
                n2n_routing_table: ShardedLock::new(HashMap::new()),
                connections: ShardedLock::new(HashMap::new()),
                auth: N2NAuth::default(),
            }),
        }
    }
}

impl Node {
    pub fn node_ref(&self) -> NodeRef {
        NodeRef {
            inner: Arc::downgrade(&self.inner),
        }
    }
    pub fn is_edge(&self) -> bool {
        matches!(self.info.kind, NodeKind::Edge)
    }
    pub fn new_trace(&self) -> NodeTrace {
        NodeTrace {
            source: self.id(),
            hops: Vec::new(),
        }
    }
    pub fn new_cast_message(&self, to: NodeId, message: CastMessage) -> N2nEvent {
        N2nEvent {
            to,
            trace: self.new_trace(),
            payload: message.encode_to_bytes(),
            kind: N2nEventKind::CastMessage,
        }
    }
    pub fn new_ack(&self, to: NodeId, message: MessageAck) -> N2nEvent {
        N2nEvent {
            to,
            trace: self.new_trace(),
            payload: message.encode_to_bytes(),
            kind: N2nEventKind::Ack,
        }
    }
    pub fn get_ep_sync(&self) -> EndpointSync {
        let ep_interest_map = self.ep_interest_map.read().unwrap();
        let ep_latest_active = self.ep_latest_active.read().unwrap();
        let mut eps = Vec::new();
        for (ep, host) in self.ep_routing_table.read().unwrap().iter() {
            if let Some(latest_active) = ep_latest_active.get(ep) {
                eps.push(EpInfo {
                    addr: *ep,
                    host: *host,
                    interests: ep_interest_map
                        .interest_of(ep)
                        .map(|s| s.iter().cloned().collect())
                        .unwrap_or_default(),
                    latest_active: *latest_active,
                });
            }
        }
        EndpointSync { eps }
    }
    pub async fn create_connection<C: N2NConnection>(
        &self,
        conn: C,
    ) -> Result<(), N2NConnectionError> {
        let config = ConnectionConfig {
            attached_node: self.node_ref(),
            auth: self.auth.clone(),
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
    #[instrument(skip_all, fields(node = ?self.id()))]
    pub async fn handle_message(&self, message_evt: N2nEvent) {
        tracing::trace!(?message_evt, "node recv new message");
        self.record_routing_info(&message_evt.trace);
        if message_evt.to == self.id() {
            self.handle_message_as_destination(message_evt).await;
        } else {
            self.forward_message(message_evt);
        }
    }
    #[instrument(skip_all, fields(node = ?self.id()))]
    pub async fn handle_message_as_destination(&self, message_evt: N2nEvent) {
        match message_evt.kind {
            event::N2nEventKind::HoldMessage => {
                let Ok((evt, _)) = DelegateMessage::decode(message_evt.payload) else {
                    return;
                };
                self.hold_new_message(evt.message).await;
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
                    self.push_message_to_local_ep(ep, evt.message.clone());
                }
            }
            event::N2nEventKind::Ack => {
                let Ok((evt, _)) = MessageAck::decode(message_evt.payload) else {
                    return;
                };
                self.local_ack(evt)
            }
            event::N2nEventKind::AckReport => todo!(),
            event::N2nEventKind::EpOnline => {
                let Ok((evt, _)) = EndpointOnline::decode(message_evt.payload) else {
                    return;
                };
                tracing::debug!("ep {ep:?} online", ep = evt.endpoint);
                let ep = evt.endpoint;
                if evt.host == message_evt.trace.source() {
                    let mut routing_wg = self.ep_routing_table.write().unwrap();
                    let mut interest_wg = self.ep_interest_map.write().unwrap();
                    let mut active_wg = self.ep_latest_active.write().unwrap();
                    active_wg.insert(ep, TimestampSec::now());
                    routing_wg.insert(ep, message_evt.trace.source());
                    for interest in &evt.interests {
                        interest_wg.insert(interest.clone(), ep);
                    }
                }
            }
            event::N2nEventKind::EpOffline => {
                let Ok((evt, _)) = EndpointOffline::decode(message_evt.payload) else {
                    return;
                };
                let ep = evt.endpoint;
                if evt.host == message_evt.trace.source() {
                    let mut routing_wg = self.ep_routing_table.write().unwrap();
                    let mut interest_wg = self.ep_interest_map.write().unwrap();
                    let mut active_wg = self.ep_latest_active.write().unwrap();
                    active_wg.remove(&ep);
                    routing_wg.remove(&ep);
                    interest_wg.delete(&ep);
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
    pub fn known_nodes(&self) -> Vec<NodeId> {
        self.connections.read().unwrap().keys().cloned().collect()
    }
    pub fn known_peer_cluster(&self) -> Vec<NodeId> {
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
    #[instrument(skip(self, packet), fields(node=?self.id(), ?to))]
    pub fn send_packet(&self, packet: N2nPacket, to: NodeId) -> Result<(), N2nPacket> {
        let Some(conn) = self.get_connection(to) else {
            tracing::error!("failed to send packet no connection to {to:?}");
            return Err(packet);
        };
        tracing::trace!(header = ?packet.header,"having connection, prepared to send packet");
        if let Err(e) = conn.outbound.send(packet) {
            tracing::error!(error=?e, "failed to send packet");
            return Err(e.0);
        }
        Ok(())
    }
    pub fn report_unreachable(&self, raw: N2nEvent) {
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

    pub fn forward_message(&self, mut message_evt: N2nEvent) {
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
    pub fn handle_ep_sync(&self, sync_evt: EndpointSync) {
        tracing::debug!(?sync_evt, "handle ep sync event");
        let mut active_wg = self.ep_latest_active.write().unwrap();
        let mut routing_wg = self.ep_routing_table.write().unwrap();
        let mut interest_wg = self.ep_interest_map.write().unwrap();
        for ep in sync_evt.eps {
            if let Some(existed_record) = active_wg.get(&ep.addr) {
                if *existed_record > ep.latest_active {
                    continue;
                }
            }
            active_wg.insert(ep.addr, ep.latest_active);
            routing_wg.insert(ep.addr, ep.host);
            for interest in ep.interests {
                interest_wg.insert(interest, ep.addr);
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
    pub attached_node: sync::Weak<NodeInner>,
    pub peer_info: NodeInfo,
}

#[cfg(test)]
mod test {
    use std::{net::SocketAddr, str::FromStr, time::Duration};

    use bytes::Bytes;
    use tracing::info;

    use crate::protocol::{
        endpoint::{Message, MessageAckExpectKind, MessageHeader, MessageId, MessageTargetKind},
        interest::{Interest, Subject},
        node::{connection::tokio_tcp::TokioTcp, Node},
    };

    #[tokio::test]
    async fn test_nodes() {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .init();

        let node_server = Node::default();
        let server_id = node_server.id();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:10080")
            .await
            .unwrap();
        tokio::spawn(async move {
            let node_server_user = node_server.create_endpoint(vec![Interest::new("events/**")]);
            {
                let node_server = node_server.clone();
                tokio::spawn(async move {
                    while let Ok((stream, peer)) = listener.accept().await {
                        tracing::info!(peer=?peer, "new connection");
                        let node = node_server.clone();
                        tokio::spawn(async move {
                            let conn = TokioTcp::new(stream);
                            node.create_connection(conn).await.unwrap();
                        });
                    }
                });
            }

            loop {
                let message = node_server_user.next_message().await;
                tracing::info!(?message, "recv message in server node");
                node_server_user.ack_processed(&message);
            }
        });

        let node_client = Node::default();
        let client_id = node_client.id();
        let stream_client = tokio::net::TcpSocket::new_v4()
            .unwrap()
            .connect(SocketAddr::from_str("127.0.0.1:10080").unwrap())
            .await
            .unwrap();

        let node_client_sender = node_client.create_endpoint(vec![Interest::new("events/**")]);
        node_client
            .create_connection(TokioTcp::new(stream_client))
            .await
            .unwrap();

        let server_conn = node_client.get_connection(server_id).unwrap();

        tokio::spawn(async move {
            loop {
                let message = node_client_sender.next_message().await;
                tracing::info!(?message, "recv message");
                node_client_sender.ack_processed(&message);
            }
        });
        let ep = node_client.create_endpoint(None);
        tokio::time::sleep(Duration::from_secs(1)).await;
        let ack_handle = ep
            .send_message(Message {
                header: MessageHeader {
                    message_id: MessageId::new_snowflake(),
                    holder_node: node_client.id(),
                    ack_kind: Some(MessageAckExpectKind::Processed),
                    target_kind: MessageTargetKind::Online,
                    subjects: vec![Subject::new("events/hello-world")].into(),
                },
                payload: Bytes::from_static(b"Hello every one!"),
            })
            .await
            .unwrap();
        ack_handle.await.unwrap();
        tracing::info!("recv ack")
    }
}
