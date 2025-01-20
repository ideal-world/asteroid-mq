use std::{
    collections::HashMap,
    ops::Deref,
    sync::{
        atomic::{self, AtomicBool, AtomicU64, AtomicUsize},
        Arc, OnceLock,
    },
};

use asteroid_mq_model::codec::BINCODE_CONFIG;
use openraft::{error::Unreachable, raft::ClientWriteResponse, Raft, RaftNetworkFactory};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
use tokio_util::sync::CancellationToken;
use tracing::{instrument, Instrument};

use crate::{
    prelude::NodeId,
    protocol::node::raft::{network::TcpNetwork, TypeConfig},
};

use super::{
    network::{Packet, Payload, Request, Response},
    proposal::Proposal,
    raft_node::TcpNode,
    MaybeLoadingRaft,
};
#[derive(Clone, Debug)]
pub struct TcpNetworkService {
    pub info: RaftNodeInfo,
    pub raft: MaybeLoadingRaft,
    pub service_api: Arc<OnceLock<tokio::sync::mpsc::UnboundedSender<TcpNetworkServiceRequest>>>,
    pub ct: CancellationToken,
}

// TODO: Expose the API rather than the service itself
// pub struct TcpNetworkServiceApi {
//     api: tokio::sync::mpsc::UnboundedSender<TcpNetworkServiceRequest>,
// }

/// 4KB for each connection, this should be enough
const BUFFER_CAPACITY: usize = 4096;
#[derive(Debug)]

pub struct GetConnection {
    peer_id: NodeId,
    responder: oneshot::Sender<Option<Arc<RaftTcpConnection>>>,
}
#[derive(Debug)]
pub struct EnsureConnection {
    peer_id: NodeId,
    peer_addr: String,
    responder: oneshot::Sender<Arc<RaftTcpConnection>>,
}
#[derive(Debug)]
pub enum TcpNetworkServiceRequest {
    GetConnection(GetConnection),
    EnsureConnection(EnsureConnection),
}

impl TcpNetworkService {
    pub async fn get_connection(&self, peer_id: NodeId) -> Option<Arc<RaftTcpConnection>> {
        let sender = self.service_api.get()?;
        let (responder, receiver) = oneshot::channel();
        let get_connection = GetConnection { peer_id, responder };
        let _ = sender
            .send(TcpNetworkServiceRequest::GetConnection(get_connection))
            .inspect_err(|_| {
                tracing::error!("service not running");
            });
        receiver.await.ok().flatten()
    }
    #[instrument(skip_all, fields(local=%self.info.id, peer=%peer_id))]
    pub async fn ensure_connection(
        &self,
        peer_id: NodeId,
        peer_addr: String,
    ) -> std::io::Result<Arc<RaftTcpConnection>> {
        let Some(sender) = self.service_api.get() else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "service not running",
            ));
        };
        let (responder, receiver) = oneshot::channel();
        let ensure_connection = EnsureConnection {
            peer_id,
            peer_addr,
            responder,
        };

        sender
            .send(TcpNetworkServiceRequest::EnsureConnection(
                ensure_connection,
            ))
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::NotConnected, "service not running")
            })?;
        let connection = receiver.await.map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "service not running")
        });
        tracing::trace!(?connection, "response received");
        connection
    }
    pub fn run_service(&self) {
        {
            let tcp_service = self.clone();
            let info = self.info.clone();
            let info_for_tracing = info.clone();
            let create_task = move || {
                let ct = tcp_service.ct.clone();
                let (ensure_connection_tx, mut ensure_connection_rx) =
                    tokio::sync::mpsc::unbounded_channel();
                tokio::spawn(
                    async move {
                        tracing::info!(?info, "tcp service started");
                        let this_id = info.id;
                        let inner_task = async move {
                            let tcp_listener = TcpListener::bind(info.node.addr).await?;
                            let mut connection_map: HashMap<NodeId, Arc<RaftTcpConnection>> = HashMap::new();
                            let mut ensure_waiting_queue:HashMap<NodeId, Vec<oneshot::Sender<Arc<RaftTcpConnection>>>>  = HashMap::new();
                            // let pending_connection: Arc<Mutex<HashMap::<NodeId, Arc<Notify>>>> = Default::default();
                            enum SelectEvent {
                                Accepted(TcpStream),
                                Request(TcpNetworkServiceRequest),
                            }
                            loop {
                                let event: SelectEvent = tokio::select! {
                                    _ = ct.cancelled() => {
                                        return Ok(());
                                    }
                                    accepted = tcp_listener.accept() => {
                                        let Ok((stream, _)) = accepted else {
                                            continue;
                                        };
                                        tracing::info!(local=%this_id, "tcp connection accepted");
                                        SelectEvent::Accepted(stream)
                                    }
                                    ensure_connection_req = ensure_connection_rx.recv() => {
                                        if let Some(ensure_connection_req) = ensure_connection_req {
                                            SelectEvent::Request(ensure_connection_req)
                                        } else {
                                            return Ok(());
                                        }
                                    }
                                };
                                match event {
                                    SelectEvent::Accepted(stream) => {
                                        if let Ok(connection) =
                                                RaftTcpConnection::from_tokio_tcp_stream(
                                                    stream,
                                                    tcp_service.clone(),
                                                )
                                                .await.inspect_err(|e| {
                                                    tracing::error!(%e, "tcp connection error");
                                                })
                                            {
                                                let peer_id = connection.peer_id();
                                                tracing::info!(local=%this_id, peer=%peer_id, "tcp connection established");
                                                if let Some(connection) = connection_map.get(&peer_id) {
                                                    if connection.is_alive() {
                                                        if let Some(waiting) = ensure_waiting_queue.remove(&peer_id) {
                                                            for responder in waiting {
                                                                let _ = responder.send(connection.clone());
                                                            }
                                                        }
                                                        tracing::trace!(local=%this_id, peer=%peer_id, "connection exists");
                                                        continue;
                                                    }
                                                }
                                                let connection = Arc::new(connection);
                                                connection_map.insert(peer_id, connection.clone());
                                                if let Some(waiting) = ensure_waiting_queue.remove(&peer_id) {
                                                    for responder in waiting {
                                                        let _ = responder.send(connection.clone());
                                                    }
                                                }
                                                tracing::info!(local=%this_id, peer=%peer_id, "connection stored");
                                            }
                                    }
                                    SelectEvent::Request(request) => {
                                        match request {
                                            TcpNetworkServiceRequest::GetConnection(get_connection) => {
                                                let GetConnection {
                                                    peer_id,
                                                    responder,
                                                } = get_connection;
                                                let connection = connection_map.get(&peer_id).cloned();
                                                let _ = responder.send(connection);
                                            },
                                            TcpNetworkServiceRequest::EnsureConnection(ensure_connection) => {
                                                static REQ_ID: AtomicUsize = AtomicUsize::new(0);
                                                let req_id = REQ_ID.fetch_add(1, atomic::Ordering::Relaxed);

                                                let EnsureConnection {
                                                    peer_id,
                                                    peer_addr,
                                                    responder,
                                                } = ensure_connection;
                                                if let Some(connection) = connection_map.get(&peer_id) {
                                                    if connection.is_alive() {
                                                        tracing::trace!(req_id, local=%this_id, peer=%peer_id, "connection exists");
                                                        let _ = responder.send(connection.clone());
                                                        continue;
                                                    }
                                                }
                                                // compare id 
                                                match peer_id.cmp(&info.id) {
                                                    std::cmp::Ordering::Less => {
                                                        // just wait for connection
                                                        tracing::info!(local=%this_id, peer=%peer_id, "waiting for connection({req_id})");
                                                        ensure_waiting_queue.entry(peer_id).or_default().push(responder);
                                                    },
                                                    std::cmp::Ordering::Equal => {
                                                        // self connection is not allowed
                                                        panic!("self connection is not allowed");
                                                    },
                                                    std::cmp::Ordering::Greater => {
                                                        ensure_waiting_queue.entry(peer_id).or_default().push(responder);
                                                        let create = async {
                                                            tracing::info!(req_id, local=%this_id, peer=%peer_id, %peer_addr, "tcp connecting");
                                                            let stream = TcpStream::connect(&peer_addr).await?;
                                                            tracing::info!(req_id, local=%this_id, peer=%peer_id, %peer_addr, "tcp connected");
                                                            let connection =
                                                                RaftTcpConnection::from_tokio_tcp_stream(
                                                                    stream,
                                                                    tcp_service.clone(),
                                                                )
                                                                .await?;
                                                            tracing::info!(req_id, local=%this_id, peer=%peer_id, %peer_addr, "connected established");
                                                            <Result<Arc<RaftTcpConnection>, std::io::Error>>::Ok(
                                                                Arc::new(connection),
                                                            )
                                                        };
                                                        let result = create.await
                                                        .inspect_err(|e| {
                                                            tracing::error!(req_id, local=%this_id, peer=%peer_id, %peer_addr, %e, "tcp connection error");
                                                        });

                                                        if let Ok(connection) = result {
                                                            connection_map.insert(peer_id, connection.clone());
                                                            if let Some(waiting) = ensure_waiting_queue.remove(&peer_id) {
                                                                for responder in waiting {
                                                                    let _ = responder.send(connection.clone());
                                                                }
                                                            }
                                                        } else {
                                                            // drop waiting handles
                                                            ensure_waiting_queue.remove(&peer_id);
                                                        }
                                                    },
                                                }
                                            },
                                        }
                                    }
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
                );
                ensure_connection_tx
            };
            self.service_api.get_or_init(create_task);
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
    packet_tx: tokio::sync::mpsc::Sender<Packet>,
    wait_poll: Arc<tokio::sync::Mutex<HashMap<u64, oneshot::Sender<Response>>>>,
    local_seq: Arc<AtomicU64>,
    alive: Arc<AtomicBool>,
    ct: CancellationToken,
}

impl Drop for RaftTcpConnection {
    fn drop(&mut self) {
        let peer = self.peer.id;
        tracing::info!(%peer, "connection dropped");
        self.ct.cancel();
        self.alive.store(false, atomic::Ordering::Relaxed);
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RaftNodeInfo {
    pub id: NodeId,
    pub node: TcpNode,
}
impl RaftTcpConnection {
    pub fn is_alive(&self) -> bool {
        self.alive.load(atomic::Ordering::Relaxed)
    }
    pub fn peer_id(&self) -> NodeId {
        self.peer.id
    }
    pub fn peer_node(&self) -> &TcpNode {
        &self.peer.node
    }
    fn next_seq(&self) -> u64 {
        self.local_seq.fetch_add(1, atomic::Ordering::Relaxed)
    }
    pub(crate) async fn propose(
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
            .await
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
        let connection_ct = service.ct.child_token();
        let info = service.info.clone();
        let pending_raft = service.raft.clone();
        let local_id = info.id;
        let packet = bincode::serde::encode_to_vec(&info, BINCODE_CONFIG).map_err(|_| std::io::ErrorKind::InvalidData)?;
        stream.write_u32(packet.len() as u32).await?;
        stream.write_all(&packet).await?;
        let hello_size = stream.read_u32().await?;
        let mut hello_data = vec![0; hello_size as usize];
        stream.read_exact(&mut hello_data).await?;
        let peer: RaftNodeInfo =
            bincode::serde::decode_from_slice(&hello_data, BINCODE_CONFIG).map_err(|_| std::io::ErrorKind::InvalidData)?.0;
        let peer_id = peer.id;
        tracing::info!(peer=%peer_id, local=%local_id, "hello received");
        let (mut read, mut write) = stream.into_split();
        let wait_pool = Arc::new(tokio::sync::Mutex::new(HashMap::<
            u64,
            oneshot::Sender<Response>,
        >::new()));
        let wait_poll_clone = wait_pool.clone();
        let (packet_tx, mut packet_rx) = tokio::sync::mpsc::channel::<Packet>(512);
        let write_task_ct = connection_ct.child_token();
        let _write_task = tokio::spawn(
            async move {
                let write_loop = async {
                    loop {
                        let packet = tokio::select! {
                            _ = write_task_ct.cancelled() => {
                                return std::io::Result::<()>::Ok(());
                            }
                            maybe_packet = packet_rx.recv() => {
                                match maybe_packet {
                                    None => {
                                        return std::io::Result::<()>::Ok(());
                                    }
                                    Some(packet) => packet,
                                }
                            }
                        };
                        let bytes = bincode::serde::encode_to_vec(&packet.payload, BINCODE_CONFIG)
                            .expect("should be valid for bincode");
                        write.write_u64(packet.seq_id).await?;
                        write.write_u32(bytes.len() as u32).await?;
                        write.write_all(&bytes).await?;
                        write.flush().await?;
                        tracing::trace!(?packet, "flushed");
                    }
                };
                match write_loop.await {
                    Ok(_) => {}
                    Err(e) => {
                        tracing::error!(%e, "write loop error");
                    }
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
        let read_task_ct = connection_ct.child_token();
        let _read_task = {
            let packet_tx = packet_tx.clone();
            let alive = alive.clone();
            let inner_task = async move {
                let mut buffer = Vec::with_capacity(BUFFER_CAPACITY);
                loop {
                    let seq_id = tokio::select! {
                        seq_id = read.read_u64() => {
                            seq_id
                        }
                        _ = read_task_ct.cancelled() => {
                            return Ok(())
                        }
                    };
                    let seq_id = seq_id?;
                    let len = read.read_u32().await? as usize;
                    if len > buffer.capacity() {
                        buffer.reserve(len - buffer.capacity());
                    }
                    buffer.resize(len, 0);
                    // unsafe {
                    //     buffer.set_len(len);
                    // }
                    let data = &mut buffer[..len];
                    read.read_exact(data).await?;
                    let Ok((payload, _)) = bincode::serde::decode_from_slice::<Payload, _>(data, BINCODE_CONFIG).inspect_err(|e| {
                        tracing::error!(?e);
                    }) else {
                        continue;
                    };
                    tracing::trace!(?seq_id, ?payload, "received");
                    match payload {
                        Payload::Request(req) => {
                            let pending_raft = pending_raft.clone();
                            let packet_tx = packet_tx.clone();
                            tokio::spawn(
                                async move {
                                    let raft = pending_raft.get().await;
                                    let resp = match req {
                                        Request::Vote(vote) => {
                                            Response::Vote(raft.vote(vote).await)
                                        }
                                        Request::AppendEntries(append) => Response::AppendEntries(
                                            raft.append_entries(append).await,
                                        ),
                                        Request::InstallSnapshot(install) => {
                                            Response::InstallSnapshot(
                                                raft.install_snapshot(install).await,
                                            )
                                        }
                                        Request::Proposal(proposal) => {
                                            Response::Proposal(raft.client_write(proposal).await)
                                        }
                                    };
                                    let payload = Payload::Response(resp);
                                    let _ = packet_tx.send(Packet { seq_id, payload }).await;
                                }
                                .instrument(tracing::span!(
                                    tracing::Level::INFO,
                                    "tcp_request_handler",
                                    local=%local_id,
                                    peer=%peer_id
                                )),
                            );
                        }
                        Payload::Response(resp) => {
                            let sender = wait_poll_clone.lock().await.remove(&seq_id);
                            if let Some(sender) = sender {
                                let _result = sender.send(resp);
                            } else {
                                tracing::warn!(?seq_id, "responder not found");
                            }
                        }
                    }
                }
            };
            tokio::spawn(
                async move {
                    let result: std::io::Result<()> = inner_task.await;
                    if let Err(e) = result {
                        tracing::error!(%e, "read task error");
                    }
                    alive.store(false, atomic::Ordering::Relaxed);
                }
                .instrument(tracing::span!(
                    tracing::Level::INFO,
                    "tcp_read_loop",
                    local=%local_id,
                    peer=%peer_id
                )),
            )
        };
        Ok(Self {
            packet_tx,
            wait_poll: wait_pool,
            peer,
            local_seq: Arc::new(AtomicU64::new(0)),
            alive,
            ct: connection_ct,
        })
    }
}

impl TcpNetworkService {
    pub fn new(info: RaftNodeInfo, raft: MaybeLoadingRaft, ct: CancellationToken) -> Self {
        Self {
            info,
            raft,
            service_api: Arc::new(OnceLock::new()),
            ct,
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
