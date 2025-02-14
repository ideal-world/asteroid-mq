pub mod node;
pub mod topic;

use std::{
    io::{self, Cursor},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use asteroid_mq_model::codec::BINCODE_CONFIG;
use node::NodeData;
use openraft::{
    storage::RaftStateMachine, EntryPayload, LogId, RaftSnapshotBuilder, RaftTypeConfig, Snapshot,
    SnapshotMeta, StorageError, StoredMembership,
};
use tokio::sync::RwLock;

use crate::{
    prelude::{NodeId, Topic},
    protocol::{
        node::{raft::proposal::ProposalContext, NodeRef},
        topic::TopicInner,
    },
};

use super::{raft_node::TcpNode, response::RaftResponse, TypeConfig};
#[derive(Debug)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<NodeId, TcpNode>,

    /// The data of the state machine at the time of this snapshot.
    pub data: NodeData,
}
#[derive(Debug, Clone, Default)]
pub struct StateMachineData<C: RaftTypeConfig> {
    pub last_applied_log: Option<LogId<C::NodeId>>,

    pub last_membership: StoredMembership<C::NodeId, C::Node>,

    pub node: NodeData,
}

/// Defines a state machine for the Raft cluster. This state machine represents a copy of the
/// data for this node. Additionally, it is responsible for storing the last snapshot of the data.
#[derive(Debug)]
pub struct StateMachineStore {
    /// The Raft state machine.
    pub state_machine: RwLock<StateMachineData<TypeConfig>>,

    /// Used in identifier for snapshot.
    ///
    /// Note that concurrently created snapshots and snapshots created on different nodes
    /// are not guaranteed to have sequential `snapshot_idx` values, but this does not matter for
    /// correctness.
    snapshot_idx: AtomicU64,

    /// The last received snapshot.
    current_snapshot: RwLock<Option<StoredSnapshot>>,
    node_ref: NodeRef,
}

impl StateMachineStore {
    pub fn new(node_ref: NodeRef) -> Self {
        Self {
            state_machine: RwLock::new(StateMachineData::default()),
            snapshot_idx: AtomicU64::new(0),
            current_snapshot: RwLock::new(None),
            node_ref,
        }
    }
    #[cfg(test)]
    pub(crate) unsafe fn new_uninitialized() -> Self {
        Self {
            state_machine: RwLock::new(StateMachineData::default()),
            snapshot_idx: AtomicU64::new(0),
            current_snapshot: RwLock::new(None),
            node_ref: NodeRef::default(),
        }
    }
}
impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachineStore> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        // Serialize the data of the state machine.
        let state_machine = self.state_machine.read().await;
        let snapshot = state_machine.node.clone();

        let last_applied_log = state_machine.last_applied_log;
        let last_membership = state_machine.last_membership.clone();

        // Lock the current snapshot before releasing the lock on the state machine, to avoid a race
        // condition on the written snapshot
        let mut current_snapshot = self.current_snapshot.write().await;
        drop(state_machine);

        let snapshot_idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{}", snapshot_idx)
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };
        let bytes = bincode::serde::encode_to_vec(&snapshot, BINCODE_CONFIG).unwrap();
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot,
        };
        *current_snapshot = Some(stored);
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for Arc<StateMachineStore> {
    type SnapshotBuilder = Arc<StateMachineStore>;
    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<<TypeConfig as RaftTypeConfig>::NodeId>>,
            StoredMembership<
                <TypeConfig as RaftTypeConfig>::NodeId,
                <TypeConfig as RaftTypeConfig>::Node,
            >,
        ),
        StorageError<<TypeConfig as RaftTypeConfig>::NodeId>,
    > {
        let state_machine = self.state_machine.read().await;
        Ok((
            state_machine.last_applied_log,
            state_machine.last_membership.clone(),
        ))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<
        Vec<<TypeConfig as RaftTypeConfig>::R>,
        StorageError<<TypeConfig as RaftTypeConfig>::NodeId>,
    >
    where
        I: IntoIterator<Item = <TypeConfig as RaftTypeConfig>::Entry> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let mut sm = self.state_machine.write().await;
        let mut res = Vec::new(); //No `with_capacity`; do not know `len` of iterator
        for entry in entries {
            sm.last_applied_log = Some(entry.log_id);
            match entry.payload {
                EntryPayload::Blank => res.push(RaftResponse { result: Ok(()) }),
                EntryPayload::Normal(ref proposal) => {
                    tracing::debug!(?proposal, "applying proposal to state machine");
                    let Some(node) = self.node_ref.upgrade() else {
                        res.push(RaftResponse { result: Err(()) });
                        continue;
                    };
                    let context = ProposalContext::new(node);
                    match proposal {
                        crate::protocol::node::raft::proposal::Proposal::DelegateMessage(
                            delegate_message,
                        ) => {
                            sm.node
                                .apply_delegate_message(delegate_message.clone(), context);
                            res.push(RaftResponse { result: Ok(()) })
                        }
                        crate::protocol::node::raft::proposal::Proposal::SetState(set_state) => {
                            sm.node.apply_set_state(set_state.clone(), context);
                            res.push(RaftResponse { result: Ok(()) })
                        }
                        crate::protocol::node::raft::proposal::Proposal::LoadTopic(load_topic) => {
                            sm.node.apply_load_topic(load_topic.clone(), context);
                            tracing::debug!(?load_topic, "topic loaded");
                            res.push(RaftResponse { result: Ok(()) })
                        }
                        crate::protocol::node::raft::proposal::Proposal::UnloadTopic(
                            unload_topic,
                        ) => {
                            sm.node.apply_unload_topic(unload_topic.clone());
                            res.push(RaftResponse { result: Ok(()) })
                        }
                        crate::protocol::node::raft::proposal::Proposal::EpOnline(ep_online) => {
                            sm.node.apply_ep_online(ep_online.clone(), context);
                            res.push(RaftResponse { result: Ok(()) })
                        }
                        crate::protocol::node::raft::proposal::Proposal::EpOffline(ep_offline) => {
                            sm.node.apply_ep_offline(ep_offline.clone(), context);
                            res.push(RaftResponse { result: Ok(()) })
                        }
                        crate::protocol::node::raft::proposal::Proposal::EpInterest(
                            ep_interest,
                        ) => {
                            sm.node.apply_ep_interest(ep_interest.clone(), context);
                            res.push(RaftResponse { result: Ok(()) })
                        }
                    }
                }
                EntryPayload::Membership(ref mem) => {
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    res.push(RaftResponse { result: Ok(()) })
                }
            };
        }
        Ok(res)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<
        Box<<TypeConfig as RaftTypeConfig>::SnapshotData>,
        StorageError<<TypeConfig as RaftTypeConfig>::NodeId>,
    > {
        // 1 Mb
        const SNAPSHOT_DEFAULT_CAPACITY: usize = 1 << 20;
        tracing::info!("begin receiving snapshot");
        Ok(Box::new(Cursor::new(Vec::with_capacity(
            SNAPSHOT_DEFAULT_CAPACITY,
        ))))
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<<TypeConfig as RaftTypeConfig>::NodeId>>
    {
        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let bytes = bincode::serde::encode_to_vec(&snapshot.data, BINCODE_CONFIG).unwrap();
                Ok(Some(Snapshot {
                    meta: snapshot.meta.clone(),
                    snapshot: Box::new(Cursor::new(bytes)),
                }))
            }
            None => Ok(None),
        }
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<
            <TypeConfig as RaftTypeConfig>::NodeId,
            <TypeConfig as RaftTypeConfig>::Node,
        >,
        mut snapshot: Box<<TypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<<TypeConfig as RaftTypeConfig>::NodeId>> {
        tracing::info!(
            { snapshot_size = snapshot.get_ref().len(), meta= ?meta },
            "decoding snapshot for installation"
        );
        let new_data: NodeData =
            bincode::serde::decode_from_std_read::<NodeData, _, _>(&mut snapshot, BINCODE_CONFIG)
                .map_err(|e| {
                StorageError::from_io_error(
                    openraft::ErrorSubject::Snapshot(None),
                    openraft::ErrorVerb::Read,
                    io::Error::other(e),
                )
            })?;
        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: new_data.clone(),
        };

        // Update the state machine.

        let mut state_machine = self.state_machine.write().await;
        state_machine.last_membership = new_snapshot.meta.last_membership.clone();
        state_machine.last_applied_log = new_snapshot.meta.last_log_id;
        state_machine.node = new_data;

        // Apply the side effects
        self.sync_node_from_snapshot(&state_machine.node);
        // Lock the current snapshot before releasing the lock on the state machine, to avoid a race
        // condition on the written snapshot
        let mut current_snapshot = self.current_snapshot.write().await;
        drop(state_machine);

        // Update current snapshot.
        *current_snapshot = Some(new_snapshot);
        Ok(())
    }
}

impl StateMachineStore {
    pub fn sync_node_from_snapshot(&self, data: &NodeData) {
        let Some(node) = self.node_ref.upgrade() else {
            return;
        };
        let mut topic_write_wg = node.topics.write().unwrap();
        for code in data.topics.keys() {
            topic_write_wg.insert(
                code.clone(),
                Topic {
                    inner: Arc::new(TopicInner {
                        code: code.clone(),
                        node: node.clone(),
                        ack_waiting_pool: Default::default(),
                        local_endpoints: Default::default(),
                    }),
                },
            );
        }
    }
}
