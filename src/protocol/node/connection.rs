use std::{borrow::Cow, ops::Deref, sync::Arc, time::Instant};

use tracing::warn;

use crate::protocol::{
    codec::{CodecType, DecodeError},
    node::{
        event::{N2NPayloadKind, N2nAuth},
        raft::{
            FollowerState, Heartbeat, LeaderState, LogAck, LogAppend, LogCommit, LogReplicate,
            RaftRole, RaftSnapshot, RequestVote, Vote,
        },
    },
};

use super::{N2NAuth, N2nPacket, NodeInfo, NodeRef};

pub mod tokio_tcp;
#[derive(Debug)]
pub struct N2NConnectionError {
    pub kind: N2NConnectionErrorKind,
    pub context: Cow<'static, str>,
}

impl N2NConnectionError {
    pub fn new(kind: N2NConnectionErrorKind, context: impl Into<Cow<'static, str>>) -> Self {
        Self {
            kind,
            context: context.into(),
        }
    }
}
#[derive(Debug)]
pub enum N2NConnectionErrorKind {
    Send(N2NSendError),
    Decode(DecodeError),
    Io(std::io::Error),
    Closed,
    Timeout,
    Protocol,
}
#[derive(Debug)]

pub struct N2NSendError {
    pub raw_packet: N2nPacket,
    pub error: Box<dyn std::error::Error + Send + Sync>,
}
use futures_util::{FutureExt, Sink, SinkExt, Stream, StreamExt};
pub trait N2NConnection:
    Send
    + 'static
    + Stream<Item = Result<N2nPacket, N2NConnectionError>>
    + Sink<N2nPacket, Error = N2NConnectionError>
{
}
#[derive(Clone, Debug)]
pub struct ConnectionConfig {
    pub attached_node: NodeRef,
    pub auth: N2NAuth,
}
pub struct N2NConnectionRef {
    inner: Arc<N2NConnectionInstance>,
}

impl Deref for N2NConnectionRef {
    type Target = N2NConnectionInstance;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Debug)]
pub struct N2NConnectionInstance {
    pub config: ConnectionConfig,
    pub outbound: flume::Sender<N2nPacket>,
    pub alive: Arc<std::sync::atomic::AtomicBool>,
    pub peer_info: NodeInfo,
    pub peer_auth: N2NAuth,
}

impl N2NConnectionInstance {
    pub fn is_alive(&self) -> bool {
        self.alive.load(std::sync::atomic::Ordering::Relaxed)
    }
    pub fn get_connection_ref(self: &Arc<Self>) -> N2NConnectionRef {
        N2NConnectionRef {
            inner: Arc::clone(self),
        }
    }
    pub async fn init<C: N2NConnection>(
        config: ConnectionConfig,
        connection: C,
    ) -> Result<Self, N2NConnectionError> {
        let config_clone = config.clone();
        let (mut sink, mut stream) = connection.split();
        let Some(node) = config.attached_node.upgrade() else {
            return Err(N2NConnectionError::new(
                N2NConnectionErrorKind::Closed,
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
        let auth = stream.next().await.unwrap_or(Err(N2NConnectionError::new(
            N2NConnectionErrorKind::Closed,
            "event stream reached unexpected end when send auth packet",
        )))?;
        if auth.header.kind != N2NPayloadKind::Auth {
            return Err(N2NConnectionError::new(
                N2NConnectionErrorKind::Protocol,
                "unexpected event, expect auth",
            ));
        }
        let auth_event = N2nAuth::decode(auth.payload)
            .map_err(|e| N2NConnectionError::new(N2NConnectionErrorKind::Decode(e), "decode auth"))?
            .0;
        tracing::debug!(auth=?auth_event, "received auth event");
        let peer_id = auth_event.info.id;
        // if peer is cluster, and we are cluster:

        let (outbound_tx, outbound_rx) = flume::bounded::<N2nPacket>(1024);
        let task = if node.is_cluster() && auth_event.info.kind.is_cluster() {
            if auth_event.auth.cluster_id != node.auth.read().unwrap().cluster_id {
                return Err(N2NConnectionError::new(
                    N2NConnectionErrorKind::Protocol,
                    "cluster id mismatch",
                ));
            }
            const HB_DURATION: std::time::Duration = std::time::Duration::from_secs(1);
            {
                let state = node.raft_state_unwrap().read().unwrap();
                if state.role.is_leader() {
                    let term = node.raft_state_unwrap().read().unwrap().term;
                    outbound_tx
                        .send(N2nPacket::raft_heartbeat(Heartbeat { term }))
                        .map_err(|_| {
                            N2NConnectionError::new(
                                N2NConnectionErrorKind::Closed,
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
                            N2NConnectionError::new(
                                N2NConnectionErrorKind::Closed,
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
                                return Err(N2NConnectionError::new(
                                    N2NConnectionErrorKind::Io(std::io::Error::new(
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
                                            N2NConnectionError::new(
                                                N2NConnectionErrorKind::Decode(e),
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
                                        N2NConnectionError::new(
                                            N2NConnectionErrorKind::Decode(e),
                                            "decode request vote",
                                        )
                                    })?;
                                    let mut self_state = node.raft_state_unwrap().write().unwrap();
                                    let vote_this = match &mut self_state.role {
                                        RaftRole::Leader(_) => false,
                                        RaftRole::Follower(_) => !self_state
                                            .voted_for
                                            .is_some_and(|(_, term)| term >= request_vote.term),
                                        RaftRole::Candidate(_) => {
                                            request_vote.index >= self_state.index
                                                && request_vote.term >= self_state.term
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
                                        }
                                    }
                                }
                                N2NPayloadKind::Vote => {
                                    let vote =
                                        Vote::decode_from_bytes(packet.payload).map_err(|e| {
                                            N2NConnectionError::new(
                                                N2NConnectionErrorKind::Decode(e),
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
                                        N2NConnectionError::new(
                                            N2NConnectionErrorKind::Decode(e),
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
                                        N2NConnectionError::new(
                                            N2NConnectionErrorKind::Decode(e),
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
                                            N2NConnectionError::new(
                                                N2NConnectionErrorKind::Decode(e),
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
                                        N2NConnectionError::new(
                                            N2NConnectionErrorKind::Decode(e),
                                            "decode LogCommit",
                                        )
                                    })?;
                                    let mut state = node.raft_state_unwrap().write().unwrap();
                                    if log_commit.index <= state.index {
                                        let packet = N2nPacket::request_snapshot();
                                        internal_outbound_tx.send(packet).map_err(|e| {
                                            N2NConnectionError::new(
                                                N2NConnectionErrorKind::Send(N2NSendError {
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
                                            N2NConnectionError::new(
                                                N2NConnectionErrorKind::Decode(e),
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
                                N2NPayloadKind::Unreachable => {
                                    // handle unreachable
                                }
                                N2NPayloadKind::Unknown => {
                                    warn!("received unknown packet from cluster node, ignore it");
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
                                        N2NConnectionError::new(
                                            N2NConnectionErrorKind::Send(N2NSendError {
                                                raw_packet: e.0,
                                                error: "missing outbound sender".into(),
                                            }),
                                            "decode LogCommit",
                                        )
                                    })?;
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
                    node.remove_connection(peer_id);
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
