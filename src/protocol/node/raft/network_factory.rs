use std::{
    collections::HashMap,
    ops::Deref,
    sync::{
        atomic::{self, AtomicBool, AtomicU64},
        Arc, OnceLock,
    },
};

use openraft::{
    error::Unreachable, raft::ClientWriteResponse, BasicNode, Raft, RaftNetworkFactory,
};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
use tracing::Instrument;

use crate::{
    prelude::NodeId,
    protocol::node::raft::{network::TcpNetwork, TypeConfig},
};

use super::{
    network::{Packet, Payload, Request, Response},
    proposal::Proposal,
    MaybeLoadingRaft,
};
#[derive(Clone, Debug)]
pub struct TcpNetworkService {
    pub info: RaftNodeInfo,
    pub raft: MaybeLoadingRaft,
    pub connections: RaftTcpConnectionMap,
    pub service_task: Arc<OnceLock<tokio::task::JoinHandle<()>>>,
}

impl TcpNetworkService {
    pub fn run(&self) {
        {
            let tcp_service = self.clone();
            let info = self.info.clone();
            let info_for_tracing = info.clone();
            let create_task = move || {
                tokio::spawn(
                    async move {
                        let inner_task = async move {
                            // let raft = tcp_service.raft.get().await;
                            let tcp_listener = TcpListener::bind(info.node.addr.clone()).await?;
                            loop {
                                let (stream, _) = tcp_listener.accept().await?;
                                if let Ok(connection) = RaftTcpConnection::from_tokio_tcp_stream(
                                    stream,
                                    tcp_service.clone(),
                                )
                                .await
                                {
                                    let peer_id = connection.peer_id();
                                    // let peer_node = connection.peer_node();
                                    if tcp_service.connections.read().await.contains_key(&peer_id) {
                                        tracing::warn!(?connection, "connection already exists");
                                        continue;
                                    } else {
                                        tracing::info!(?connection, "new connection");
                                        tcp_service
                                            .connections
                                            .write()
                                            .await
                                            .insert(peer_id, Arc::new(connection));
                                    }

                                    // let result = raft.add_learner(peer_id, peer_node, false).await;
                                    // match result {
                                    //     Ok(_) => {
                                    //         tracing::info!("add learner success");
                                    //     }
                                    //     Err(e) => {
                                    //         tracing::error!(?e, "add learner failed");
                                    //     }
                                    // }
                                }
                            }
                            #[allow(unreachable_code)]
                            std::io::Result::Ok(())
                        };
                        if let Err(e) = inner_task.await {
                            tracing::error!(?e, "tcp service error");
                        };
                    }
                    .instrument(tracing::span!(
                        tracing::Level::INFO,
                        "tcp_network_service",
                        info=?info_for_tracing
                    )),
                )
            };
            self.service_task.get_or_init(create_task);
        }
    }
}

#[derive(Clone, Default, Debug)]
pub struct RaftTcpConnectionMap {
    map: Arc<tokio::sync::RwLock<HashMap<NodeId, Arc<RaftTcpConnection>>>>,
}

impl Deref for RaftTcpConnectionMap {
    type Target = Arc<tokio::sync::RwLock<HashMap<NodeId, Arc<RaftTcpConnection>>>>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}
#[derive(Debug)]
pub struct RaftTcpConnection {
    peer: RaftNodeInfo,
    packet_tx: flume::Sender<Packet>,
    wait_poll: Arc<tokio::sync::Mutex<HashMap<u64, oneshot::Sender<Response>>>>,
    local_seq: Arc<AtomicU64>,
    alive: Arc<AtomicBool>,
    read_task: tokio::task::JoinHandle<()>,
    write_task: tokio::task::JoinHandle<()>,
}

impl Drop for RaftTcpConnection {
    fn drop(&mut self) {
        self.read_task.abort();
        self.write_task.abort();
        self.alive.store(false, atomic::Ordering::Relaxed);
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RaftNodeInfo {
    pub id: NodeId,
    pub node: BasicNode,
}
impl RaftTcpConnection {
    pub fn is_alive(&self) -> bool {
        self.alive.load(atomic::Ordering::Relaxed)
    }
    pub fn peer_id(&self) -> NodeId {
        self.peer.id
    }
    pub fn peer_node(&self) -> BasicNode {
        self.peer.node.clone()
    }
    fn next_seq(&self) -> u64 {
        self.local_seq.fetch_add(1, atomic::Ordering::Relaxed)
    }
    pub(crate) async fn proposal(
        &self,
        proposal: Proposal,
    ) -> crate::Result<ClientWriteResponse<TypeConfig>> {
        let req = Request::Proposal(proposal);
        let resp = self
            .send_request(req)
            .await
            .map_err(crate::Error::contextual_custom(
                "sending proposal to remote",
            ))?;
        let resp = resp.await.map_err(crate::Error::contextual_custom(
            "waiting for proposal response",
        ))?;
        let Response::Proposal(resp) = resp else {
            return Err(crate::Error::unknown("unexpected response"));
        };
        let resp = resp.map_err(crate::Error::contextual("remote proposal"))?;
        Ok(resp)
    }
    pub(super) async fn send_request(
        &self,
        req: Request,
    ) -> Result<oneshot::Receiver<Response>, Unreachable> {
        tracing::trace!(?req, "send request");
        let payload = Payload::Request(req);
        let seq_id = self.next_seq();
        let packet = Packet { seq_id, payload };
        let (sender, receiver) = tokio::sync::oneshot::channel();

        self.wait_poll.lock().await.insert(seq_id, sender);
        self.packet_tx
            .send(packet)
            .inspect_err(|_| {
                let pool = self.wait_poll.clone();
                tokio::spawn(async move {
                    pool.lock().await.remove(&seq_id);
                });
            })
            .map_err(|e| Unreachable::new(&e))?;
        Ok(receiver)
    }
    pub async fn from_tokio_tcp_stream(
        mut stream: TcpStream,
        service: TcpNetworkService,
    ) -> std::io::Result<Self> {
        let raft = service.raft.get().await;
        let info = service.info.clone();
        let packet = bincode::serialize(&info).map_err(|_| std::io::ErrorKind::InvalidData)?;
        stream.write_u32(packet.len() as u32).await?;
        stream.write_all(&packet).await?;
        let hello_size = stream.read_u32().await?;
        let mut hello_data = vec![0; hello_size as usize];
        stream.read_exact(&mut hello_data).await?;
        let peer: RaftNodeInfo =
            bincode::deserialize(&hello_data).map_err(|_| std::io::ErrorKind::InvalidData)?;

        let (mut read, mut write) = stream.into_split();
        let wait_poll = Arc::new(tokio::sync::Mutex::new(HashMap::<
            u64,
            oneshot::Sender<Response>,
        >::new()));
        let wait_poll_clone = wait_poll.clone();
        let (packet_tx, packet_rx) = flume::bounded::<Packet>(512);
        let write_task = tokio::spawn(
            async move {
                let task_inner = async move {
                    let write_loop = async {
                        while let Ok(packet) = packet_rx.recv_async().await {
                            let bytes = bincode::serialize(&packet.payload)
                                .expect("should be valid for bincode");
                            write.write_u64(packet.seq_id).await?;
                            write.write_u32(bytes.len() as u32).await?;
                            write.write_all(&bytes).await?;
                            write.flush().await?;
                            tracing::trace!(?packet, "flushed");
                        }
                        std::io::Result::<()>::Ok(())
                    };
                    match write_loop.await {
                        Ok(_) => Ok(()),
                        Err(e) => {
                            tracing::error!(?e);
                            Err(e)
                        }
                    }
                };
                let result = task_inner.await;
                if let Err(e) = result {
                    tracing::error!(?e, "write task error");
                }
            }
            .instrument(tracing::span!(
                tracing::Level::INFO,
                "write_loop",
                ?info,
                ?peer
            )),
        );
        let alive = Arc::new(AtomicBool::new(true));
        let read_task = {
            let packet_tx = packet_tx.clone();
            let alive = alive.clone();
            let inner_task = async move {
                let mut buffer = Vec::with_capacity(1024);
                loop {
                    let seq_id = read.read_u64().await?;
                    let len = read.read_u32().await? as usize;
                    if len > buffer.capacity() {
                        buffer.reserve(len - buffer.capacity());
                    }
                    unsafe {
                        buffer.set_len(len);
                    }
                    let data = &mut buffer[..len];
                    read.read_exact(data).await?;
                    let Ok(payload) = bincode::deserialize::<Payload>(data).inspect_err(|e| {
                        tracing::error!(?e);
                    }) else {
                        continue;
                    };
                    tracing::trace!(?seq_id, ?payload, "received");
                    match payload {
                        Payload::Request(req) => {
                            let raft = raft.clone();
                            let packet_tx = packet_tx.clone();
                            tokio::spawn(async move {
                                let resp = match req {
                                    Request::Vote(vote) => Response::Vote(raft.vote(vote).await),
                                    Request::AppendEntries(append) => {
                                        Response::AppendEntries(raft.append_entries(append).await)
                                    }
                                    Request::InstallSnapshot(install) => Response::InstallSnapshot(
                                        raft.install_snapshot(install).await,
                                    ),
                                    Request::Proposal(proposal) => {
                                        Response::Proposal(raft.client_write(proposal).await)
                                    }
                                };
                                let payload = Payload::Response(resp);
                                let _ = packet_tx.send_async(Packet { seq_id, payload }).await;
                            });
                        }
                        Payload::Response(resp) => {
                            let sender = wait_poll_clone.lock().await.remove(&seq_id);
                            if let Some(sender) = sender {
                                sender.send(resp).unwrap();
                            } else {
                                tracing::warn!(?seq_id, "responder not found");
                            }
                        }
                    }
                }
            };
            tokio::spawn(async move {
                let result: std::io::Result<()> = inner_task.await;
                if let Err(e) = result {
                    tracing::error!(?e, "read task error");
                }
                alive.store(false, atomic::Ordering::Relaxed);
            })
        };
        Ok(Self {
            packet_tx,
            wait_poll,
            peer,
            local_seq: Arc::new(AtomicU64::new(0)),
            alive,
            read_task,
            write_task,
        })
    }
}

impl TcpNetworkService {
    pub fn new(info: RaftNodeInfo, raft: MaybeLoadingRaft) -> Self {
        Self {
            info,
            raft,
            connections: RaftTcpConnectionMap::default(),
            service_task: Arc::new(OnceLock::new()),
        }
    }
    pub fn set_raft(&self, raft: Raft<TypeConfig>) {
        self.raft.set(raft);
    }
}

impl RaftNetworkFactory<TypeConfig> for TcpNetworkService {
    type Network = TcpNetwork;
    async fn new_client(
        &mut self,
        target: <TypeConfig as openraft::RaftTypeConfig>::NodeId,
        node: &<TypeConfig as openraft::RaftTypeConfig>::Node,
    ) -> Self::Network {
        TcpNetwork::new(
            RaftNodeInfo {
                id: target,
                node: node.clone(),
            },
            self.clone(),
        )
    }
}
#[cfg(test)]
#[test]
fn test_mem() {
    use crate::protocol::node::{LogStorage, StateMachineStore};

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    pub struct MemStore {}
    impl openraft::testing::StoreBuilder<TypeConfig, LogStorage, Arc<StateMachineStore>> for MemStore {
        async fn build(
            &self,
        ) -> Result<((), LogStorage, Arc<StateMachineStore>), openraft::StorageError<NodeId>>
        {
            Ok((
                (),
                LogStorage::default(),
                Arc::new(unsafe { StateMachineStore::new_uninitialized() }),
            ))
        }
    }
    openraft::testing::Suite::test_all(MemStore {}).unwrap();
}