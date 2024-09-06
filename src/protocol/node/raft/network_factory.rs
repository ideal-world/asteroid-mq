use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    ops::Deref,
    sync::{
        atomic::{self, AtomicBool, AtomicU64},
        Arc,
    },
};

use k8s_openapi::api::core::v1::Node;
use openraft::{error::Unreachable, BasicNode, Raft, RaftNetworkFactory};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{oneshot, OnceCell},
};

use crate::{
    prelude::NodeId,
    protocol::node::raft::{
        log_storage::LogStorage, network::TcpNetwork, state_machine::StateMachineStore, TypeConfig,
    },
};

use super::network::{Packet, Payload, Request, Response};
#[derive(Clone)]
pub struct TcpNetworkService {
    pub info: RaftNodeInfo,
    pub raft: Arc<OnceCell<Raft<TypeConfig>>>,
    pub connections: RaftTcpConnectionMap,
}

#[derive(Clone, Default)]
pub struct RaftTcpConnectionMap {
    map: Arc<tokio::sync::RwLock<HashMap<NodeId, Arc<RaftTcpConnection>>>>,
}

impl Deref for RaftTcpConnectionMap {
    type Target = Arc<tokio::sync::RwLock<HashMap<NodeId, Arc<RaftTcpConnection>>>>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

pub struct RaftTcpConnection {
    peer: RaftNodeInfo,
    this: RaftNodeInfo,
    packet_tx: flume::Sender<Packet>,
    wait_poll: Arc<tokio::sync::Mutex<HashMap<u64, oneshot::Sender<Response>>>>,
    local_seq: Arc<AtomicU64>,
    alive: Arc<AtomicBool>,
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
    pub(super) async fn send_request(
        &self,
        req: Request,
    ) -> Result<oneshot::Receiver<Response>, Unreachable> {
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
        let raft = loop {
            if let Some(raft) = service.raft.get() {
                break raft.clone();
            }
            tokio::task::yield_now().await;
        };
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
        let write_task = tokio::spawn(async move {
            while let Ok(packet) = packet_rx.recv_async().await {
                let bytes = bincode::serialize(&packet.payload).expect("should be valid for bincode");
                write.write_u64(packet.seq_id).await?;
                write.write_u32(bytes.len() as u32).await?;
                write.write_all(&bytes).await?;
            }
            std::io::Result::<()>::Ok(())
        });
        let alive = Arc::new(AtomicBool::new(true));
        let read_task = {
            let packet_tx = packet_tx.clone();
            let alive = alive.clone();
            let inner_task = async move {
                loop {
                    let seq_id = read.read_u64().await?;
                    let len = read.read_u32().await? as usize;
                    let mut data = vec![0; len];
                    read.read_exact(&mut data).await?;
                    tracing::warn!(?data);
                    let Ok(payload) = bincode::deserialize::<Payload>(&data).inspect_err(|e| {
                        tracing::error!(?e);
                    }) else {
                        continue;
                    };
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
                                };
                                tracing::warn!(?resp);
                                let payload = Payload::Response(resp);
                                let _ = packet_tx.send(Packet { seq_id, payload });
                            });
                        }
                        Payload::Response(resp) => {
                            let sender = wait_poll_clone.lock().await.remove(&seq_id);
                            if let Some(sender) = sender {
                                sender.send(resp).unwrap();
                            }
                        }
                    }
                }
            };
            tokio::spawn(async move {
                let result: std::io::Result<()> = inner_task.await;
                alive.store(false, atomic::Ordering::Relaxed);
            })
        };
        Ok(Self {
            packet_tx,
            wait_poll,
            peer,
            this: info,
            local_seq: Arc::new(AtomicU64::new(0)),
            alive,
        })
    }
}

impl TcpNetworkService {
    pub fn new(info: RaftNodeInfo) -> Self {
        Self {
            info,
            raft: Arc::new(OnceCell::new()),
            connections: RaftTcpConnectionMap::default(),
        }
    }
    pub fn set_raft(&self, raft: Raft<TypeConfig>) {
        let _ = self.raft.set(raft);
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
                Arc::new(StateMachineStore::default()),
            ))
        }
    }
    openraft::testing::Suite::test_all(MemStore {}).unwrap();
}
#[tokio::test]
async fn test_network_factory() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    let config = Arc::new(
        openraft::Config {
            cluster_name: "test".to_string(),
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );

    const CLUSTER_SIZE: usize = 2;
    const PORT_START: u16 = 12321;
    let nodes = (0..CLUSTER_SIZE).map(|idx| {
        let port = PORT_START + idx as u16;
        let socket_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
        let node_id = NodeId::snowflake();
        let node = BasicNode::new(socket_addr.to_string());
        (node_id, node)
    });
    let nodes = Arc::new(HashMap::<NodeId, BasicNode>::from_iter(nodes));
    let nodes_iter = nodes.clone();
    for (id, node) in nodes_iter.iter() {
        let config = config.clone();
        let nodes = nodes.clone();
        let node = node.clone();
        let id = *id;
        let service = tokio::spawn(async move {
            let tcp_service = TcpNetworkService::new(RaftNodeInfo {
                id,
                node: node.clone(),
            });
            let store = LogStorage::default();
            let sm = Arc::new(StateMachineStore::default());
            let raft = Raft::<TypeConfig>::new(id, config, tcp_service.clone(), store, sm)
                .await
                .unwrap();
            tcp_service.set_raft(raft.clone());
            let tcp_listener = TcpListener::bind(node.addr).await.unwrap();
            {
                let raft = raft.clone();
                tokio::spawn(async move {
                    loop {
                        let (stream, _) = tcp_listener.accept().await.unwrap();
                        if let Ok(connection) =
                            RaftTcpConnection::from_tokio_tcp_stream(stream, tcp_service.clone())
                                .await
                        {
                            let result = raft
                                .add_learner(connection.peer_id(), connection.peer_node(), false)
                                .await;
                            tracing::info!("add learner result: {:?}", result);
                        }
                    }
                });
            }
            let result = raft
                .initialize(
                    nodes
                        .iter()
                        .map(|(k, v)| (*k, v.clone()))
                        .collect::<BTreeMap<_, _>>(),
                )
                .await;
            tracing::info!("initialize result: {:?}", result);
        });
    }
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
}
