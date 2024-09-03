use std::{cell::RefCell, collections::HashMap, io::Write, sync::Arc, task::Waker};

use openraft::RaftNetwork;
use serde::{Deserialize, Serialize};
use tokio::{
    io::AsyncWriteExt,
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
    read_task: tokio::task::JoinHandle<()>,
    seq_id: u64,
    wait_poll: Arc<Mutex<HashMap<u64, Receiver<RecvPacket>>>>,
}
#[derive(Debug, Serialize, Deserialize)]
#[repr(u8)]
pub enum PacketKind {
    Request = 0,
    Response = 1,
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
        let (read, write) = stream.into_split();
        let wait_poll = Arc::new(Mutex::new(HashMap::new()));
        let wait_poll_clone = wait_poll.clone();
        let read_task = tokio::spawn(async move {
            let mut reader = tokio::io::BufReader::new(read);
            loop {
                let packet: Packet = bincode::deserialize_from(&mut reader).unwrap();
                let receiver = wait_poll_clone.lock().await.remove(&packet.seq_id).unwrap();
                receiver.send(RecvPacket {
                    seq_id: packet.seq_id,
                    data: packet.data,
                });
            }
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
    ) -> std::io::Result<Sender<RecvPacket>> {
        let bytes = bincode::serialize(req).unwrap();
        let seq_id = self.seq_id;
        self.seq_id = self.seq_id.wrapping_add(1);
        let packet = Packet {
            kind: PacketKind::Request,
            seq_id,
            data: bytes,
        };
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.wait_poll.lock().await.insert(seq_id, receiver);
        self.write
            .write_all(&bincode::serialize(&packet).unwrap())
            .await
            .inspect_err(|_| {
                let pool = self.wait_poll.clone();
                tokio::spawn(async move {
                    pool.lock().await.remove(&seq_id);
                });
            })?;
        Ok(sender)
    }
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
