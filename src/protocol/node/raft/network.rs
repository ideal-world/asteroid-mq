use std::sync::Arc;

use openraft::{
    error::{
        ClientWriteError, InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable,
    },
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, ClientWriteResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    BasicNode, RaftNetwork,
};
use serde::{Deserialize, Serialize};
use tokio::{net::TcpStream, sync::oneshot::Receiver};

use crate::prelude::NodeId;

use super::{
    network_factory::{RaftNodeInfo, RaftTcpConnection, TcpNetworkService},
    proposal::Proposal,
    TypeConfig,
};

pub struct TcpNetwork {
    peer: RaftNodeInfo,
    source: TcpNetworkService,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) enum Request {
    Vote(VoteRequest<NodeId>),
    AppendEntries(AppendEntriesRequest<TypeConfig>),
    InstallSnapshot(InstallSnapshotRequest<TypeConfig>),
    Proposal(Proposal),
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) enum Response {
    Vote(Result<VoteResponse<NodeId>, RaftError<NodeId>>),
    AppendEntries(Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>),
    InstallSnapshot(
        Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>>,
    ),
    Proposal(
        Result<
            ClientWriteResponse<TypeConfig>,
            RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>,
        >,
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
    async fn create_connection(&self) -> Result<RaftTcpConnection, Unreachable> {
        let stream = TcpStream::connect(self.peer.node.addr.clone())
            .await
            .map_err(|e| Unreachable::new(&e))?;
        let connection = RaftTcpConnection::from_tokio_tcp_stream(stream, self.source.clone())
            .await
            .map_err(|e| Unreachable::new(&e))?;
        Ok(connection)
    }
    #[tracing::instrument(skip_all, fields(peer_id = ?self.peer.id))]
    async fn send_request(&mut self, req: Request) -> Result<Receiver<Response>, Unreachable> {
        let mut connections = self.source.connections.write().await;
        let connection = connections.get(&self.peer.id);
        if connection.is_none() || connection.is_some_and(|c| !c.is_alive()) {
            // the node with smaller id will create connection
            if self.peer.id < self.source.info.id {
                return Err(Unreachable::new(&ConnectionNotEstablished));
            }
            tracing::debug!(peer=?self.peer.id, this=?self.source.info.id, "connection not established, creating new connection");
            let connection = Arc::new(self.create_connection().await?);
            connections.insert(connection.peer_id(), connection.clone());
        }
        let connection = connections
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
        _option: openraft::network::RPCOption,
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
        _option: openraft::network::RPCOption,
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
        _option: openraft::network::RPCOption,
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
