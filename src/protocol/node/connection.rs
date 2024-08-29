use std::{borrow::Cow, error::Error, ops::Deref, sync::Arc, time::Instant};

use tracing::warn;

use crate::protocol::{
    codec::{CodecType, DecodeError},
    node::{
        edge::{EdgeRequest, EdgeResponse},
        event::{N2NPayloadKind, N2nAuth},
        raft::{
            FollowerState, Heartbeat, LeaderState, LogAck, LogAppend, LogCommit, LogReplicate,
            RaftRole, RaftSnapshot, RequestVote, Vote,
        },
    },
};

use super::{N2nPacket, NodeAuth, NodeInfo, NodeRef};

pub mod tokio_tcp;
pub mod tokio_ws;
#[derive(Debug)]
pub struct NodeConnectionError {
    pub kind: NodeConnectionErrorKind,
    pub context: Cow<'static, str>,
}

impl NodeConnectionError {
    pub fn new(kind: NodeConnectionErrorKind, context: impl Into<Cow<'static, str>>) -> Self {
        Self {
            kind,
            context: context.into(),
        }
    }
}
#[derive(Debug)]
pub enum NodeConnectionErrorKind {
    Send(NodeSendError),
    Decode(DecodeError),
    Io(std::io::Error),
    Underlying(Box<dyn Error + Send>),
    Closed,
    Timeout,
    Protocol,
    Existed,
}
#[derive(Debug)]

pub struct NodeSendError {
    pub raw_packet: N2nPacket,
    pub error: Box<dyn std::error::Error + Send + Sync>,
}
use futures_util::{FutureExt, Sink, SinkExt, Stream, StreamExt};
pub trait NodeConnection:
    Send
    + 'static
    + Stream<Item = Result<N2nPacket, NodeConnectionError>>
    + Sink<N2nPacket, Error = NodeConnectionError>
{
}
#[derive(Clone, Debug)]
pub struct ConnectionConfig {
    pub attached_node: NodeRef,
    pub auth: NodeAuth,
}
pub struct ClusterConnectionRef {
    inner: Arc<ClusterConnectionInstance>,
}

impl Deref for ClusterConnectionRef {
    type Target = ClusterConnectionInstance;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Debug)]
pub struct ClusterConnectionInstance {
    pub config: ConnectionConfig,
    pub outbound: flume::Sender<N2nPacket>,
    pub alive: Arc<std::sync::atomic::AtomicBool>,
    pub peer_info: NodeInfo,
    pub peer_auth: NodeAuth,
}

impl ClusterConnectionInstance {
    pub fn is_alive(&self) -> bool {
        self.alive.load(std::sync::atomic::Ordering::Relaxed)
    }
    pub fn get_connection_ref(self: &Arc<Self>) -> ClusterConnectionRef {
        ClusterConnectionRef {
            inner: Arc::clone(self),
        }
    }
    pub async fn init<C: NodeConnection>(
        config: ConnectionConfig,
        connection: C,
    ) -> Result<Self, NodeConnectionError> {
        tracing::debug!(?config, "init connection");
        let config_clone = config.clone();
        let (mut sink, mut stream) = connection.split();
        let Some(node) = config.attached_node.upgrade() else {
            return Err(NodeConnectionError::new(
                NodeConnectionErrorKind::Closed,
                "node was dropped",
            ));
        };
        let info = NodeInfo {
            id: node.info.id,
            kind: node.info.kind,
        };
        let my_auth = config.auth.clone();
        let evt = N2nPacket::auth(N2nAuth {
            info,
            auth: my_auth,
        });
        sink.send(evt).await?;
        tracing::trace!("sent auth");
        let auth = stream.next().await.unwrap_or(Err(NodeConnectionError::new(
            NodeConnectionErrorKind::Closed,
            "event stream reached unexpected end when send auth packet",
        )))?;
        if auth.header.kind != N2NPayloadKind::Auth {
            return Err(NodeConnectionError::new(
                NodeConnectionErrorKind::Protocol,
                "unexpected event, expect auth",
            ));
        }
        let auth_event = N2nAuth::decode(auth.payload)
            .map_err(|e| {
                NodeConnectionError::new(NodeConnectionErrorKind::Decode(e), "decode auth")
            })?
            .0;
        tracing::debug!(auth=?auth_event, "received auth");
        let peer_id = auth_event.info.id;

        if let Some(existed_connection) = node.get_cluster_connection(peer_id) {
            if existed_connection.is_alive() {
                return Err(NodeConnectionError::new(
                    NodeConnectionErrorKind::Existed,
                    "connection already exists",
                ));
            } else {
                node.remove_cluster_connection(peer_id);
            }
        }
        // if peer is cluster, and we are cluster:

        let (outbound_tx, outbound_rx) = flume::bounded::<N2nPacket>(1024);
        let task = if node.is_cluster() && auth_event.info.kind.is_cluster() {
            const HB_DURATION: std::time::Duration = std::time::Duration::from_secs(5);
            {
                let state = node.raft_state_unwrap().read().unwrap();
                if state.role.is_leader() {
                    let term = node.raft_state_unwrap().read().unwrap().term;
                    outbound_tx
                        .send(N2nPacket::raft_heartbeat(Heartbeat { term }))
                        .map_err(|_| {
                            NodeConnectionError::new(
                                NodeConnectionErrorKind::Closed,
                                "connection closed when send the first heartbeat",
                            )
                        })?;
                    outbound_tx
                        .send(N2nPacket::raft_snapshot(RaftSnapshot {
                            term: state.term,
                            index: state.index,
                            data: node.snapshot(),
                        }))
                        .map_err(|_| {
                            NodeConnectionError::new(
                                NodeConnectionErrorKind::Closed,
                                "connection closed when send the first heartbeat",
                            )
                        })?;
                }
            }

            enum PollEvent {
                PacketIn(N2nPacket),
                PacketOut(N2nPacket),
            }
            let internal_outbound_tx = outbound_tx.clone();
            let task = async move {
                loop {
                    let event = futures_util::select! {
                        next_pack = stream.next().fuse() => {
                            let Some(next_event) = next_pack else {
                                break;
                            };
                            let Ok(next_event) = next_event else {
                                return Err(NodeConnectionError::new(
                                    NodeConnectionErrorKind::Io(std::io::Error::new(
                                        std::io::ErrorKind::InvalidData,
                                        "failed to read next event",
                                    )),
                                    "read next event",
                                ));
                            };
                            PollEvent::PacketIn(next_event)
                        }
                        packet = outbound_rx.recv_async().fuse() => {
                            let Ok(packet) = packet else {
                                break;
                            };
                            PollEvent::PacketOut(packet)
                        }
                    };
                    match event {
                        PollEvent::PacketIn(packet) => {
                            tracing::trace!(?packet, "received packet");
                            match packet.kind() {
                                N2NPayloadKind::Auth => {
                                    // ignore
                                }
                                N2NPayloadKind::Event => {}
                                N2NPayloadKind::Heartbeat => {
                                    let heartbeat = Heartbeat::decode_from_bytes(packet.payload)
                                        .map_err(|e| {
                                            NodeConnectionError::new(
                                                NodeConnectionErrorKind::Decode(e),
                                                "decode heartbeat",
                                            )
                                        })?;
                                    let mut self_state = node.raft_state_unwrap().write().unwrap();

                                    let self_term = self_state.term;
                                    match self_term.cmp(&heartbeat.term) {
                                        std::cmp::Ordering::Less => {
                                            // follow this leader
                                            self_state.term = heartbeat.term;
                                            let timeout_reporter =
                                                self_state.timeout_reporter.clone();
                                            self_state.set_role(RaftRole::Follower(
                                                FollowerState::from_new_leader(
                                                    peer_id,
                                                    HB_DURATION * 2,
                                                    timeout_reporter.clone(),
                                                ),
                                            ));
                                        }
                                        std::cmp::Ordering::Equal => {
                                            match &mut self_state.role {
                                                RaftRole::Leader(_) => {
                                                    // this should not happen
                                                }
                                                RaftRole::Follower(ref mut f) => {
                                                    f.record_hb(Instant::now(), HB_DURATION * 2)
                                                }
                                                RaftRole::Candidate(_) => {
                                                    let timeout_reporter =
                                                        self_state.timeout_reporter.clone();
                                                    self_state.set_role(RaftRole::Follower(
                                                        FollowerState::from_new_leader(
                                                            peer_id,
                                                            HB_DURATION * 2,
                                                            timeout_reporter.clone(),
                                                        ),
                                                    ))
                                                }
                                            }
                                        }
                                        std::cmp::Ordering::Greater => {
                                            // ignore
                                        }
                                    }
                                }
                                N2NPayloadKind::RequestVote => {
                                    let request_vote = RequestVote::decode_from_bytes(
                                        packet.payload,
                                    )
                                    .map_err(|e| {
                                        NodeConnectionError::new(
                                            NodeConnectionErrorKind::Decode(e),
                                            "decode request vote",
                                        )
                                    })?;
                                    tracing::trace!(?request_vote, "received request vote");
                                    let mut self_state = node.raft_state_unwrap().write().unwrap();
                                    let vote_this = match &mut self_state.role {
                                        RaftRole::Leader(_) => {
                                            request_vote.term > self_state.term
                                                && request_vote.index >= self_state.index
                                        }
                                        RaftRole::Follower(_) => !self_state
                                            .voted_for
                                            .is_some_and(|(_, term)| term >= request_vote.term),
                                        RaftRole::Candidate(_) => {
                                            request_vote.index >= self_state.index
                                                && request_vote.term > self_state.term
                                        }
                                    };
                                    if vote_this {
                                        let result =
                                            internal_outbound_tx.send(N2nPacket::raft_vote(Vote {
                                                term: request_vote.term,
                                            }));
                                        if result.is_ok() {
                                            self_state.voted_for =
                                                Some((peer_id, request_vote.term));
                                            let timeout_reporter =
                                                self_state.timeout_reporter.clone();
                                            self_state.set_role(RaftRole::Follower(
                                                FollowerState::from_new_leader(
                                                    peer_id,
                                                    HB_DURATION * 2,
                                                    timeout_reporter,
                                                ),
                                            ))
                                        }
                                    }
                                }
                                N2NPayloadKind::Vote => {
                                    let cluster_size = node.cluster_size();
                                    if cluster_size <= 1 {
                                        continue;
                                    }
                                    // 1->0 2->1 3->1 4->2 5->2 ... n->n mod 2
                                    let vote =
                                        Vote::decode_from_bytes(packet.payload).map_err(|e| {
                                            NodeConnectionError::new(
                                                NodeConnectionErrorKind::Decode(e),
                                                "decode vote",
                                            )
                                        })?;
                                    let mut self_state = node.raft_state_unwrap().write().unwrap();
                                    if self_state.term == vote.term {
                                        if let RaftRole::Candidate(candidate_state) =
                                            &mut self_state.role
                                        {
                                            candidate_state.votes += 1;
                                            if candidate_state.votes >= node.cluster_size() / 2 {
                                                let term = self_state.term;
                                                self_state.set_role(RaftRole::Leader(
                                                    LeaderState::new(
                                                        node.node_ref(),
                                                        HB_DURATION,
                                                        term,
                                                    ),
                                                ))
                                            }
                                        };
                                    }
                                }
                                N2NPayloadKind::LogAppend => {
                                    let log_append = LogAppend::decode_from_bytes(packet.payload)
                                        .map_err(|e| {
                                        NodeConnectionError::new(
                                            NodeConnectionErrorKind::Decode(e),
                                            "decode log append",
                                        )
                                    })?;
                                    let mut state = node.raft_state_unwrap().write().unwrap();
                                    let term = state.term;
                                    let new_index = if state.role.is_leader() {
                                        if term != log_append.term {
                                            continue;
                                        }
                                        let new_index = state.index.inc();
                                        let indexed_log = log_append.clone().indexed(new_index);
                                        state.pending_logs.push_log(indexed_log);
                                        new_index
                                    } else {
                                        continue;
                                    };
                                    if let RaftRole::Leader(ref mut ls) = state.role {
                                        ls.ack_map.insert(new_index, 0);
                                        node.cluster_wise_broadcast_packet(
                                            N2nPacket::log_replicate(
                                                log_append.replicate(new_index),
                                            ),
                                        );
                                    }
                                }
                                N2NPayloadKind::LogReplicate => {
                                    let log_replicate = LogReplicate::decode_from_bytes(
                                        packet.payload,
                                    )
                                    .map_err(|e| {
                                        NodeConnectionError::new(
                                            NodeConnectionErrorKind::Decode(e),
                                            "decode LogReplicate",
                                        )
                                    })?;
                                    let mut state = node.raft_state_unwrap().write().unwrap();
                                    if !state.role.is_follower() && state.term <= log_replicate.term
                                    {
                                        let new_role = FollowerState::from_new_leader(
                                            peer_id,
                                            HB_DURATION * 2,
                                            state.timeout_reporter.clone(),
                                        );
                                        state.set_role(RaftRole::Follower(new_role));
                                    }
                                    if state.role.is_follower() {
                                        state
                                            .pending_logs
                                            .push_log(log_replicate.clone().indexed());
                                        let log_ack = N2nPacket::log_ack(log_replicate.ack());
                                        let _ = node.send_packet(log_ack, peer_id);
                                    }
                                }
                                N2NPayloadKind::LogAck => {
                                    let log_ack = LogAck::decode_from_bytes(packet.payload)
                                        .map_err(|e| {
                                            NodeConnectionError::new(
                                                NodeConnectionErrorKind::Decode(e),
                                                "decode LogAck",
                                            )
                                        })?;
                                    let mut state = node.raft_state_unwrap().write().unwrap();
                                    let cluster_size = node.cluster_size();
                                    match &mut state.role {
                                        RaftRole::Leader(l) => {
                                            let is_yield = if let Some(count) =
                                                l.ack_map.get_mut(&log_ack.index)
                                            {
                                                *count += 1;
                                                *count >= cluster_size / 2
                                            } else {
                                                false
                                            };
                                            if is_yield {
                                                state.pending_logs.resolve(log_ack.index);
                                                while let Some(log) =
                                                    state.pending_logs.blocking_pop_log()
                                                {
                                                    state.commit(log, &node);
                                                    node.cluster_wise_broadcast_packet(
                                                        N2nPacket::log_commit(LogCommit {
                                                            term: log_ack.term,
                                                            index: log_ack.index,
                                                        }),
                                                    )
                                                }
                                            }
                                        }
                                        _ => {
                                            // ignore
                                        }
                                    }
                                }
                                N2NPayloadKind::LogCommit => {
                                    let log_commit = LogCommit::decode_from_bytes(packet.payload)
                                        .map_err(|e| {
                                        NodeConnectionError::new(
                                            NodeConnectionErrorKind::Decode(e),
                                            "decode LogCommit",
                                        )
                                    })?;
                                    let mut state = node.raft_state_unwrap().write().unwrap();
                                    if log_commit.index <= state.index {
                                        let packet = N2nPacket::request_snapshot();
                                        internal_outbound_tx.send(packet).map_err(|e| {
                                            NodeConnectionError::new(
                                                NodeConnectionErrorKind::Send(NodeSendError {
                                                    raw_packet: e.0,
                                                    error: "missing outbound sender".into(),
                                                }),
                                                "decode LogCommit",
                                            )
                                        })?;
                                        continue;
                                    }
                                    if state.role.is_follower() {
                                        state.pending_logs.resolve(log_commit.index);
                                        while let Some(log) = state.pending_logs.blocking_pop_log()
                                        {
                                            state.commit(log, &node);
                                        }
                                    }
                                }
                                N2NPayloadKind::Snapshot => {
                                    let snapshot = RaftSnapshot::decode_from_bytes(packet.payload)
                                        .map_err(|e| {
                                            NodeConnectionError::new(
                                                NodeConnectionErrorKind::Decode(e),
                                                "decode RaftSnapshot",
                                            )
                                        })?;
                                    let mut state = node.raft_state_unwrap().write().unwrap();
                                    match &mut state.role {
                                        RaftRole::Follower(_) => {
                                            if state.term == snapshot.term {
                                                state.index = snapshot.index;
                                                node.apply_snapshot(snapshot.data);
                                            }
                                        }
                                        _ => {
                                            // ignore
                                        }
                                    }
                                }
                                N2NPayloadKind::RequestSnapshot => {
                                    let raft_state = node.raft_state_unwrap().read().unwrap();
                                    let raft_snapshot = RaftSnapshot {
                                        term: raft_state.term,
                                        index: raft_state.index,
                                        data: node.snapshot(),
                                    };
                                    let packet = N2nPacket::raft_snapshot(raft_snapshot);
                                    internal_outbound_tx.send(packet).map_err(|e| {
                                        NodeConnectionError::new(
                                            NodeConnectionErrorKind::Send(NodeSendError {
                                                raw_packet: e.0,
                                                error: "missing outbound sender".into(),
                                            }),
                                            "decode LogCommit",
                                        )
                                    })?;
                                }
                                N2NPayloadKind::Unreachable => {
                                    // handle unreachable
                                }
                                N2NPayloadKind::Unknown => {
                                    warn!("received unknown packet from cluster node, ignore it");
                                }
                                _ => {
                                    // invalid event, ignore
                                }
                            }
                        }
                        PollEvent::PacketOut(packet) => {
                            sink.send(packet).await?;
                        }
                    }
                }

                Ok(())
            };
            task
        } else {
            unimplemented!("non-cluster connection");
        };

        let alive_flag = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let _handle = {
            let alive_flag = Arc::clone(&alive_flag);
            let attached_node = config.attached_node.clone();
            let peer_id = auth_event.info.id;
            tokio::spawn(async move {
                let result = task.await;
                let node = attached_node.upgrade();
                if let Err(e) = result {
                    if node.is_some() {
                        warn!(?e, "connection task failed");
                    }
                }
                alive_flag.store(false, std::sync::atomic::Ordering::Relaxed);
                if let Some(node) = node {
                    node.remove_cluster_connection(peer_id);
                }
            })
        };
        Ok(Self {
            config: config_clone,
            alive: alive_flag,
            outbound: outbound_tx,
            peer_info: auth_event.info,
            peer_auth: auth_event.auth,
        })
    }
}

#[derive(Debug)]
pub struct EdgeConnectionInstance {
    pub config: ConnectionConfig,
    pub outbound: flume::Sender<N2nPacket>,
    pub alive: Arc<std::sync::atomic::AtomicBool>,
    pub peer_info: NodeInfo,
    pub peer_auth: NodeAuth,
    pub finish_signal: flume::Receiver<()>,
}

pub struct EdgeConnectionRef {
    inner: Arc<EdgeConnectionInstance>,
}

impl Deref for EdgeConnectionRef {
    type Target = EdgeConnectionInstance;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl EdgeConnectionInstance {
    pub fn is_alive(&self) -> bool {
        self.alive.load(std::sync::atomic::Ordering::Relaxed)
    }
    pub fn get_connection_ref(self: &Arc<Self>) -> EdgeConnectionRef {
        EdgeConnectionRef {
            inner: Arc::clone(self),
        }
    }
    pub async fn init<C: NodeConnection>(
        config: ConnectionConfig,
        connection: C,
    ) -> Result<Self, NodeConnectionError> {
        tracing::debug!(?config, "init edge connection");
        let config_clone = config.clone();
        let (mut sink, mut stream) = connection.split();
        let Some(node) = config.attached_node.upgrade() else {
            return Err(NodeConnectionError::new(
                NodeConnectionErrorKind::Closed,
                "node was dropped",
            ));
        };
        let info = NodeInfo {
            id: node.info.id,
            kind: node.info.kind,
        };
        let my_auth = config.auth.clone();
        let evt = N2nPacket::auth(N2nAuth {
            info,
            auth: my_auth,
        });
        sink.send(evt).await?;
        tracing::trace!("sent auth");
        let auth = stream.next().await.unwrap_or(Err(NodeConnectionError::new(
            NodeConnectionErrorKind::Closed,
            "event stream reached unexpected end when send auth packet",
        )))?;
        if auth.header.kind != N2NPayloadKind::Auth {
            return Err(NodeConnectionError::new(
                NodeConnectionErrorKind::Protocol,
                "unexpected event, expect auth",
            ));
        }
        let auth_event = N2nAuth::decode(auth.payload)
            .map_err(|e| {
                NodeConnectionError::new(NodeConnectionErrorKind::Decode(e), "decode auth")
            })?
            .0;
        let peer_id = auth_event.info.id;
        tracing::debug!(auth=?auth_event, "received auth");

        if let Some(existed_connection) = node.get_edge_connection(peer_id) {
            if existed_connection.is_alive() {
                return Err(NodeConnectionError::new(
                    NodeConnectionErrorKind::Existed,
                    "connection already exists",
                ));
            } else {
                node.remove_cluster_connection(peer_id);
            }
        }
        // if peer is cluster, and we are cluster:

        let (outbound_tx, outbound_rx) = flume::bounded::<N2nPacket>(1024);
        let task = if node.is_cluster() && auth_event.info.kind.is_edge() {
            enum PollEvent {
                PacketIn(N2nPacket),
                PacketOut(N2nPacket),
            }
            let internal_outbound_tx = outbound_tx.clone();
            let task = async move {
                loop {
                    let event = futures_util::select! {
                        next_pack = stream.next().fuse() => {
                            let Some(next_event) = next_pack else {
                                break;
                            };
                            let Ok(next_event) = next_event else {
                                return Err(NodeConnectionError::new(
                                    NodeConnectionErrorKind::Io(std::io::Error::new(
                                        std::io::ErrorKind::InvalidData,
                                        "failed to read next event",
                                    )),
                                    "read next event",
                                ));
                            };
                            PollEvent::PacketIn(next_event)
                        }
                        packet = outbound_rx.recv_async().fuse() => {
                            let Ok(packet) = packet else {
                                break;
                            };
                            PollEvent::PacketOut(packet)
                        }
                    };
                    match event {
                        PollEvent::PacketIn(packet) => {
                            match packet.header.kind {
                                N2NPayloadKind::Heartbeat => {}
                                N2NPayloadKind::EdgeRequest => {
                                    let node = node.clone();
                                    let Ok(request) =
                                        EdgeRequest::decode_from_bytes(packet.payload)
                                    else {
                                        continue;
                                    };
                                    let outbound = internal_outbound_tx.clone();
                                    let id = request.id;
                                    tokio::spawn(async move {
                                        let resp = node.handle_edge_request(peer_id, request).await;
                                        let resp = EdgeResponse::from_result(id, resp);
                                        let packet = N2nPacket::edge_response(resp);
                                        outbound.send(packet).unwrap_or_else(|e| {
                                            warn!(?e, "failed to send edge response");
                                        });
                                    });
                                }
                                _ => {
                                    // invalid event, ignore
                                }
                            }
                        }
                        PollEvent::PacketOut(packet) => {
                            sink.send(packet).await?;
                        }
                    }
                }

                Ok(())
            };
            task
        } else {
            return Err(NodeConnectionError::new(
                NodeConnectionErrorKind::Protocol,
                "peer is not an edge node",
            ));
        };

        let alive_flag = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let (finish_notify, finish_signal) = flume::bounded(1);
        let _handle = {
            let alive_flag = Arc::clone(&alive_flag);
            let attached_node = config.attached_node.clone();
            let peer_id = auth_event.info.id;

            tokio::spawn(async move {
                let result = task.await;
                let node = attached_node.upgrade();
                if let Err(e) = result {
                    if node.is_some() {
                        warn!(?e, "connection task failed");
                    }
                }
                alive_flag.store(false, std::sync::atomic::Ordering::Relaxed);
                let _ = finish_notify.send(());
                if let Some(node) = node {
                    node.remove_edge_connection(peer_id);
                }
            })
        };
        Ok(Self {
            config: config_clone,
            alive: alive_flag,
            outbound: outbound_tx,
            peer_info: auth_event.info,
            peer_auth: auth_event.auth,
            finish_signal,
        })
    }
}
