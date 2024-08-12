use std::{borrow::Cow, ops::Deref, sync::Arc};

use tokio::task::JoinHandle;
use tracing::warn;

use crate::protocol::{
    codec::{CodecType, DecodeError},
    node::{
        event::{N2NPayloadKind, N2nAuth},
        raft::{
            FollowerState, Heartbeat, LeaderState, LogAck, LogAppend, LogCommit, LogReplicate,
            RaftInfo, RaftRole, RequestVote, Vote,
        },
    },
};

use super::{N2NAuth, N2nEvent, N2nPacket, NodeInfo, NodeRef};

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
    pub raw_event: N2nPacket,
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
            if auth_event.auth.cluster_id != node.auth.cluster_id {
                return Err(N2NConnectionError::new(
                    N2NConnectionErrorKind::Protocol,
                    "cluster id mismatch",
                ));
            }
            const HB_DURATION: std::time::Duration = std::time::Duration::from_secs(1);
            enum PollEvent {
                PacketIn(N2nPacket),
                PacketOut(N2nPacket),
                HbTimeout,
            }
            let internal_outbound_tx = outbound_tx.clone();
            let (hb_timeout_report, hb_timeout_recv) = flume::bounded::<()>(1);
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
                        hb_timeout = hb_timeout_recv.recv_async().fuse() => {
                            PollEvent::HbTimeout
                        }
                    };
                    match event {
                        PollEvent::PacketIn(packet) => match packet.kind() {
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
                                if self_term < heartbeat.term {
                                    // follow this leader
                                    self_state.term = heartbeat.term;
                                    self_state.role =
                                        RaftRole::Follower(FollowerState::from_leader(
                                            peer_id,
                                            HB_DURATION * 2,
                                            hb_timeout_report.clone(),
                                        ));
                                }
                            }
                            N2NPayloadKind::RequestVote => {
                                let request_vote = RequestVote::decode_from_bytes(packet.payload)
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
                                        request_vote.index > self_state.index
                                            && request_vote.term >= self_state.term
                                    }
                                };
                                if vote_this {
                                    let result =
                                        internal_outbound_tx.send(N2nPacket::raft_vote(Vote {
                                            term: request_vote.term,
                                        }));
                                    if result.is_ok() {
                                        self_state.voted_for = Some((peer_id, request_vote.term));
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
                                        if candidate_state.votes > node.cluster_size() / 2 {
                                            self_state.role = RaftRole::Leader(LeaderState::new(
                                                internal_outbound_tx.clone(),
                                                HB_DURATION,
                                                self_state.term,
                                            ));
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
                                match &mut state.role {
                                    RaftRole::Leader(ls) => {
                                        if term != log_append.term {
                                            continue;
                                        }
                                        ls.logs.insert(
                                            log_append.id,
                                            log_append.uncommitted(node.cluster_size()),
                                        );

                                        for peer in node.known_peer_cluster() {
                                            if let Some(conn) = node.get_connection(peer) {
                                                let _result =
                                                    conn.outbound.send(N2nPacket::log_replicate(
                                                        log_append.replicate(),
                                                    ));
                                            }
                                        }
                                    }
                                    RaftRole::Follower(_) => {
                                        // ignore
                                    }
                                    RaftRole::Candidate(_) => {
                                        // ignore
                                    }
                                }
                            }
                            N2NPayloadKind::LogReplicate => {
                                let log_replicate = LogReplicate::decode_from_bytes(packet.payload)
                                    .map_err(|e| {
                                        N2NConnectionError::new(
                                            N2NConnectionErrorKind::Decode(e),
                                            "decode LogReplicate",
                                        )
                                    })?;
                                let mut state = node.raft_state_unwrap().write().unwrap();
                                match &mut state.role {
                                    RaftRole::Follower(f) => {
                                        f.logs.insert(log_replicate.id, log_replicate);
                                    }
                                    _ => {
                                        if state.term <= log_replicate.term {
                                            let mut new_role = FollowerState::from_leader(
                                                peer_id,
                                                HB_DURATION * 2,
                                                hb_timeout_report.clone(),
                                            );
                                            new_role.logs.insert(log_replicate.id, log_replicate);
                                            state.role = RaftRole::Follower(new_role)
                                        }
                                    }
                                }
                            }
                            N2NPayloadKind::LogAck => {
                                let log_ack =
                                    LogAck::decode_from_bytes(packet.payload).map_err(|e| {
                                        N2NConnectionError::new(
                                            N2NConnectionErrorKind::Decode(e),
                                            "decode LogAck",
                                        )
                                    })?;
                                let mut state = node.raft_state_unwrap().write().unwrap();
                                match &mut state.role {
                                    RaftRole::Leader(l) => {
                                        let is_yield =
                                            if let Some(log) = l.logs.get_mut(&log_ack.id) {
                                                log.ack_count += 1;
                                                log.ack_count > log.ack_expect / 2
                                            } else {
                                                false
                                            };
                                        if is_yield {
                                            // commit log
                                            l.logs.remove(&log_ack.id);
                                            for peer in node.known_peer_cluster() {
                                                if let Some(conn) = node.get_connection(peer) {
                                                    let index = state.index.inc();
                                                    let _result = conn.outbound.send(
                                                        N2nPacket::log_commit(LogCommit {
                                                            id: log_ack.id,
                                                            index,
                                                        }),
                                                    );
                                                }
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
                                match &mut state.role {
                                    RaftRole::Follower(f) => {
                                        if let Some(replicate) = f.logs.remove(&log_commit.id) {
                                            todo!("commit")
                                        }
                                    }
                                    _ => {
                                        //ignore
                                    }
                                }
                            }
                            N2NPayloadKind::Unreachable => {
                                // handle unreachable
                            }
                            N2NPayloadKind::Unknown => {
                                warn!("received unknown packet from cluster node, ignore it");
                            }
                        },
                        PollEvent::PacketOut(packet) => {
                            sink.send(packet).await?;
                        }
                        PollEvent::HbTimeout => {
                            // todo : become a candidate
                        },
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
                alive_flag.store(false, std::sync::atomic::Ordering::Relaxed);
                if let Some(node) = attached_node.upgrade() {
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
