use std::sync::{Arc, OnceLock};

pub mod cluster;
pub mod log_storage;
pub mod network;
pub mod network_factory;
pub mod proposal;
pub mod raft_node;
pub mod response;
pub mod state_machine;
use network_factory::{RaftNodeInfo, TcpNetworkService};
use openraft::{BasicNode, Raft};
use proposal::Proposal;
use response::RaftResponse;

use super::NodeId;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Default, Copy)]
pub struct TypeConfig {
    _private: (),
}

impl openraft::RaftTypeConfig for TypeConfig {
    type D = Proposal;
    type R = RaftResponse;
    type NodeId = NodeId;
    type Node = openraft::BasicNode;
    type Entry = openraft::Entry<TypeConfig>;
    type SnapshotData = std::io::Cursor<Vec<u8>>;
    type AsyncRuntime = openraft::TokioRuntime;
    type Responder = openraft::raft::responder::OneshotResponder<Self>;
}
#[derive(Clone)]
pub struct MaybeLoadingRaft {
    loading: Arc<OnceLock<Raft<TypeConfig>>>,
    signal: Arc<tokio::sync::Notify>,
}

impl std::fmt::Debug for MaybeLoadingRaft {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let loaded = self.loading.get().is_some();
        f.debug_struct("MaybeLoadingRaft")
            .field("loaded", &loaded)
            .finish()
    }
}

impl Default for MaybeLoadingRaft {
    fn default() -> Self {
        Self::new()
    }
}

impl MaybeLoadingRaft {
    pub fn new() -> Self {
        Self {
            loading: Default::default(),
            signal: tokio::sync::Notify::new().into(),
        }
    }
    pub fn set(&self, raft: Raft<TypeConfig>) {
        if self.loading.set(raft).is_ok() {
            self.signal.notify_waiters();
        }
    }
    pub async fn get(&self) -> Raft<TypeConfig> {
        loop {
            if let Some(raft) = self.loading.get() {
                return raft.clone();
            } else {
                self.signal.notified().await;
            }
        }
    }
    pub fn get_opt(&self) -> Option<Raft<TypeConfig>> {
        self.loading.get().cloned()
    }
    pub fn net_work_service(&self, id: NodeId, node: BasicNode) -> TcpNetworkService {
        TcpNetworkService::new(RaftNodeInfo { id, node }, self.clone())
    }
}
