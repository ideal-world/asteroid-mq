use std::{
    cell::RefCell,
    collections::HashMap,
    io::Write,
    sync::{atomic::AtomicBool, Arc},
    task::Waker,
};

use openraft::{
    error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    BasicNode, Raft, RaftNetwork,
};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{tcp::OwnedWriteHalf, TcpStream},
    sync::{
        oneshot::{Receiver, Sender},
        Mutex, OnceCell, RwLock,
    },
};

use crate::prelude::NodeId;

use super::{
    network_factory::{RaftNodeInfo, RaftTcpConnection, RaftTcpConnectionMap, TcpNetworkService},
    TypeConfig,
};

pub struct TcpNetwork {
    peer: RaftNodeInfo,
    source: TcpNetworkService,
}

pub struct TcpNetworkConnected {
    packet_tx: flume::Sender<Packet>,
    write_task: tokio::task::JoinHandle<std::io::Result<()>>,
    read_task: tokio::task::JoinHandle<std::io::Result<()>>,
    wait_poll: Arc<Mutex<HashMap<u64, Sender<Response>>>>,
}

impl Drop for TcpNetworkConnected {
    fn drop(&mut self) {
        self.read_task.abort();
        self.write_task.abort();
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) enum Request {
    Vote(VoteRequest<NodeId>),
    AppendEntries(AppendEntriesRequest<TypeConfig>),
    InstallSnapshot(InstallSnapshotRequest<TypeConfig>),
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) enum Response {
    Vote(Result<VoteResponse<NodeId>, RaftError<NodeId>>),
    AppendEntries(Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>),
    InstallSnapshot(
        Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>>,
    ),
}
#[derive(Debug, Serialize, Deserialize)]
pub(super) enum Payload {
    Request(Request),
    Response(Response),
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct Packet {
    pub seq_id: u64,
    pub payload: Payload,
}

#[derive(Debug)]
pub struct ConnectionNotEstablished;
impl std::fmt::Display for ConnectionNotEstablished {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "connection not established")
    }
}
impl std::error::Error for ConnectionNotEstablished {}

impl TcpNetwork {
    pub fn new(peer: RaftNodeInfo, source: TcpNetworkService) -> Self {
        Self { peer, source }
    }
    async fn create_connection(&self) -> Result<(), Unreachable> {
        let stream = TcpStream::connect(self.peer.node.addr.clone())
            .await
            .map_err(|e| Unreachable::new(&e))?;
        let connections = self.source.connections.clone();
        let connection = RaftTcpConnection::from_tokio_tcp_stream(stream, self.source.clone())
            .await
            .map_err(|e| Unreachable::new(&e))?;
        let mut connections = connections.write().await;
        connections.insert(self.peer.id, Arc::new(connection));
        Ok(())
    }
    async fn send_request(&mut self, req: Request) -> Result<Receiver<Response>, Unreachable> {
        let connections = self.source.connections.read().await;
        let connection = connections.get(&self.peer.id);
        if connection.is_none() || connection.is_some_and(|c| !c.is_alive()) {
            drop(connections);
            self.create_connection().await?;
        }
        let connection = self
            .source
            .connections
            .read()
            .await
            .get(&self.peer.id)
            .ok_or_else(|| Unreachable::new(&ConnectionNotEstablished))?
            .clone();
        connection.send_request(req).await
    }
}

impl RaftNetwork<TypeConfig> for TcpNetwork {
    async fn vote(
        &mut self,
        rpc: VoteRequest<<TypeConfig as openraft::RaftTypeConfig>::NodeId>,
        option: openraft::network::RPCOption,
    ) -> Result<
        VoteResponse<<TypeConfig as openraft::RaftTypeConfig>::NodeId>,
        RPCError<
            <TypeConfig as openraft::RaftTypeConfig>::NodeId,
            <TypeConfig as openraft::RaftTypeConfig>::Node,
            RaftError<<TypeConfig as openraft::RaftTypeConfig>::NodeId>,
        >,
    > {
        let receiver = self.send_request(Request::Vote(rpc)).await?;
        let response = receiver.await.map_err(|e| {
            openraft::error::RPCError::Network(openraft::error::NetworkError::new(&e))
        })?;
        let Response::Vote(vote) = response else {
            unreachable!("wrong implementation: expect vote response")
        };
        vote.map_err(|e| RPCError::RemoteError(RemoteError::new(self.peer.id, e)))
    }
    async fn append_entries(
        &mut self,
        rpc: openraft::raft::AppendEntriesRequest<TypeConfig>,
        option: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::AppendEntriesResponse<<TypeConfig as openraft::RaftTypeConfig>::NodeId>,
        openraft::error::RPCError<
            <TypeConfig as openraft::RaftTypeConfig>::NodeId,
            <TypeConfig as openraft::RaftTypeConfig>::Node,
            openraft::error::RaftError<<TypeConfig as openraft::RaftTypeConfig>::NodeId>,
        >,
    > {
        let receiver = self.send_request(Request::AppendEntries(rpc)).await?;
        let response = receiver.await.map_err(|e| {
            openraft::error::RPCError::Network(openraft::error::NetworkError::new(&e))
        })?;
        let Response::AppendEntries(append_entries) = response else {
            unreachable!("wrong implementation: expect vote response")
        };
        append_entries.map_err(|e| RPCError::RemoteError(RemoteError::new(self.peer.id, e)))
    }
    async fn install_snapshot(
        &mut self,
        rpc: openraft::raft::InstallSnapshotRequest<TypeConfig>,
        option: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::InstallSnapshotResponse<<TypeConfig as openraft::RaftTypeConfig>::NodeId>,
        openraft::error::RPCError<
            <TypeConfig as openraft::RaftTypeConfig>::NodeId,
            <TypeConfig as openraft::RaftTypeConfig>::Node,
            openraft::error::RaftError<
                <TypeConfig as openraft::RaftTypeConfig>::NodeId,
                openraft::error::InstallSnapshotError,
            >,
        >,
    > {
        let receiver = self.send_request(Request::InstallSnapshot(rpc)).await?;
        let response = receiver.await.map_err(|e| {
            openraft::error::RPCError::Network(openraft::error::NetworkError::new(&e))
        })?;
        let Response::InstallSnapshot(install_snapshot) = response else {
            unreachable!("wrong implementation: expect vote response")
        };
        install_snapshot.map_err(|e| RPCError::RemoteError(RemoteError::new(self.peer.id, e)))
    }
}
