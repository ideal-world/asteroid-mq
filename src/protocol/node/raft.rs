use std::{
    borrow::Cow,
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, HashSet},
    io::Cursor,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
pub mod connection;
pub mod log_storage;
pub mod network;
pub mod network_factory;
pub mod proposal;
pub mod raft_node;
pub mod state_machine;
use openraft::{storage::RaftLogStorage, Raft};
use proposal::Proposal;
use tokio::sync::OnceCell;

use crate::{
    impl_codec,
    protocol::{
        codec::CodecType,
        endpoint::{DelegateMessage, EndpointInterest, EndpointOffline, EndpointOnline, SetState},
        topic::durable_message::{LoadTopic, UnloadTopic},
    },
};

use super::{
    event::{EventKind, N2nPacket, RaftData, RaftResponse},
    Node, NodeId, NodeRef,
};
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
    loading: Arc<OnceCell<Raft<TypeConfig>>>,
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

impl MaybeLoadingRaft {
    pub fn new() -> Self {
        Self {
            loading: Arc::new(OnceCell::new()),
            signal: tokio::sync::Notify::new().into(),
        }
    }
    pub fn set(&self, raft: Raft<TypeConfig>) {
        self.loading.set(raft);
        self.signal.notify_waiters();
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
}
