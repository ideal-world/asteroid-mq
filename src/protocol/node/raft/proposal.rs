use std::{collections::HashMap, sync::Arc};

use openraft::Raft;
use serde::{Deserialize, Serialize};
use tokio::sync::{oneshot, RwLock};

use crate::{
    prelude::{MessageId, NodeId, TopicCode},
    protocol::{
        endpoint::{
            DelegateMessage, EndpointAddr, EndpointInterest, EndpointOffline, EndpointOnline,
            Message, MessageAck, SetState,
        },
        node::NodeRef,
        topic::durable_message::{LoadTopic, UnloadTopic},
    },
};

use super::{
    state_machine::topic::wait_ack::{WaitAckError, WaitAckResult, WaitAckSuccess},
    MaybeLoadingRaft, TypeConfig,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[repr(u8)]
pub enum Proposal {
    /// Hold Message: edge node ask cluster node to hold a message.
    DelegateMessage(DelegateMessage),
    /// Set State: set ack state
    SetState(SetState),
    /// Load Queue: load messages into the topic queue.
    LoadTopic(LoadTopic),
    UnloadTopic(UnloadTopic),
    /// En Online: report endpoint online.
    EpOnline(EndpointOnline),
    /// En Offline: report endpoint offline.
    EpOffline(EndpointOffline),
    /// En Interest: set endpoint's interests.
    EpInterest(EndpointInterest),
}
#[derive(Debug, Clone)]
pub struct ProposalContext {
    pub node_ref: NodeRef,
    pub topic_code: Option<TopicCode>,
}

impl ProposalContext {
    pub fn set_topic_code(&mut self, code: TopicCode) {
        self.topic_code = Some(code);
    }
    pub fn resolve_ack(&self, id: MessageId, result: WaitAckResult) {
        let Some(node) = self.node_ref.upgrade() else {
            return;
        };
        let Some(ref code) = self.topic_code else {
            return;
        };
        let Some(topic) = node.get_topic(code) else {
            return;
        };
        tokio::spawn(async move {
            if let Some(tx) = topic.ack_waiting_pool.write().await.remove(&id) {
                let _ = tx.send(result);
            }
        });
    }
    pub fn dispatch_message(&self, message: &Message, endpoint: EndpointAddr) {}
}
