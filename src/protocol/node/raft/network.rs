use std::{cell::RefCell, collections::HashMap, io::Write, sync::Arc, task::Waker};

use openraft::RaftNetwork;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{tcp::OwnedWriteHalf, TcpStream},
    sync::{
        oneshot::{Receiver, Sender},
        Mutex,
    },
};

use crate::{prelude::NodeId, protocol::node::connection::ClusterConnectionRef};

use super::TypeConfig;

pub struct TcpNetWorkInner {
    write: OwnedWriteHalf,
    read_task: tokio::task::JoinHandle<std::io::Result<()>>,
    seq_id: u64,
    wait_poll: Arc<Mutex<HashMap<u64, Sender<Vec<u8>>>>>,
}
#[derive(Debug, Serialize, Deserialize)]
#[repr(u8)]
pub enum PacketKind {
    Request = 0,
    Response = 1,
}

impl From<u8> for PacketKind {
    fn from(v: u8) -> Self {
        match v {
            0 => PacketKind::Request,
            _ => PacketKind::Response,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Packet {
    pub kind: PacketKind,
    pub seq_id: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Deserialize)]
pub struct RecvPacket {
    seq_id: u64,
    data: Vec<u8>,
}

impl TcpNetWorkInner {
    pub fn new(stream: TcpStream) -> Self {
        let (mut read, write) = stream.into_split();
        let wait_poll = Arc::new(Mutex::new(HashMap::<u64, Sender<Vec<u8>>>::new()));
        let wait_poll_clone = wait_poll.clone();
        let read_task = tokio::spawn(async move {
            loop {
                let kind = read.read_u8().await?.into();
                let seq_id = read.read_u64().await?;
                let len = read.read_u64().await? as usize;
                let mut data = vec![0; len];
                read.read_exact(&mut data).await?;
                match kind {
                    PacketKind::Request => {
                        todo!("handle request")
                    }
                    PacketKind::Response => {
                        let sender = wait_poll_clone.lock().await.remove(&seq_id);
                        if let Some(sender) = sender {
                            sender.send(data).unwrap();
                        }
                    }
                }
            }
            Ok(())
        });
        Self {
            write,
            read_task,
            seq_id: 0,
            wait_poll,
        }
    }
    pub async fn write_request<T: Serialize>(
        &mut self,
        req: &T,
    ) -> std::io::Result<Receiver<Vec<u8>>> {
        let bytes = bincode::serialize(req).unwrap();
        let seq_id = self.seq_id;
        self.seq_id = self.seq_id.wrapping_add(1);
        let packet = Packet {
            kind: PacketKind::Request,
            seq_id,
            data: bytes,
        };
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.wait_poll.lock().await.insert(seq_id, sender);
        self.write
            .write_all(&bincode::serialize(&packet).unwrap())
            .await
            .inspect_err(|_| {
                let pool = self.wait_poll.clone();
                tokio::spawn(async move {
                    pool.lock().await.remove(&seq_id);
                });
            })?;
        Ok(receiver)
    }
}

pub struct TcpNetwork {
    inner: Mutex<Arc<TcpNetWorkInner>>,
}

impl RaftNetwork<TypeConfig> for TcpNetWorkInner {
    async fn vote(
        &mut self,
        rpc: openraft::raft::VoteRequest<<TypeConfig as openraft::RaftTypeConfig>::NodeId>,
        option: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::VoteResponse<<TypeConfig as openraft::RaftTypeConfig>::NodeId>,
        openraft::error::RPCError<
            <TypeConfig as openraft::RaftTypeConfig>::NodeId,
            <TypeConfig as openraft::RaftTypeConfig>::Node,
            openraft::error::RaftError<<TypeConfig as openraft::RaftTypeConfig>::NodeId>,
        >,
    > {
        let receiver = self.write_request(&rpc).await.map_err(|e| {
            openraft::error::RPCError::Network(openraft::error::NetworkError::new(&e))
        })?;
        // self.connection.outbound.send_async(item);
        unimplemented!()
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
        unimplemented!()
    }
    async fn install_snapshot(
        &mut self,
        _rpc: openraft::raft::InstallSnapshotRequest<TypeConfig>,
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
        unimplemented!()
    }
}
