use std::collections::HashMap;

use asteroid_mq_model::EndpointAddr;
use serde::{Deserialize, Serialize};

use crate::{
    prelude::{MessageId, Node, TopicCode},
    protocol::{message::*, node::durable_message::DurableCommand},
};

use super::state_machine::topic::wait_ack::WaitAckResult;
pub(crate) mod ep_online;
pub use ep_online::EndpointOnline;
pub(crate) mod ep_offline;
pub use ep_offline::EndpointOffline;
pub(crate) mod ep_interest;
pub use ep_interest::EndpointInterest;
pub(crate) mod set_state;
pub use set_state::*;
pub(crate) mod load_topic;
pub use load_topic::LoadTopic;
pub(crate) mod unload_topic;
pub use unload_topic::UnloadTopic;
pub(crate) mod delegate_message;
pub use delegate_message::DelegateMessage;
pub(crate) mod ack_finished;
pub use ack_finished::AckFinished;

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
    /// Ep Online: report endpoint online.
    EpOnline(EndpointOnline),
    /// Ep Offline: report endpoint offline.
    EpOffline(EndpointOffline),
    /// Ep Interest: set endpoint's interests.
    EpInterest(EndpointInterest),
    /// Ack Finished: ack finished.
    AckFinished(AckFinished),
}
#[derive(Debug, Clone)]
pub(crate) struct ProposalContext {
    pub node: Node,
    pub topic_code: Option<TopicCode>,
    pub debug_ep_online: bool,
}

impl ProposalContext {
    pub(crate) fn new(node: Node) -> Self {
        Self {
            node,
            topic_code: None,
            debug_ep_online: false,
        }
    }
    pub(crate) fn push_durable_command(&mut self, command: DurableCommand) {
        if let Some(topic_code) = self.topic_code.clone() {
            self.node.push_durable_commands(Some((topic_code, command)));
        }
    }
    pub(crate) fn set_topic_code(&mut self, code: TopicCode) {
        self.topic_code = Some(code);
    }
    pub(crate) fn resolve_ack(&self, message_id: MessageId, result: WaitAckResult) {
        let node = self.node.clone();
        let node_id = self.node.id();
        let Some(topic) = self.topic_code.clone() else {
            tracing::warn!(?self.topic_code, "topic not found");
            return;
        };
        tokio::spawn(async move {
            let mut wg = node.ack_waiting_pool.write().await;
            if let Some(mut tx) = wg.remove(&message_id) {
                drop(wg);
                tx.send(result);
                let proposal_result = node
                    .propose(Proposal::AckFinished(AckFinished { message_id, topic }))
                    .await;
                if let Err(e) = proposal_result {
                    tracing::warn!(error=%e, "ack finished proposal failed");
                }
            } else {
                tracing::trace!(%node_id, %message_id, "ack wait handle not found");
            }
        });
    }
    #[tracing::instrument(skip(self))]
    // return true if the message is reachable
    pub(crate) fn dispatch_message(&self, message: &Message, endpoint: EndpointAddr) -> bool {
        let Some(ref code) = self.topic_code else {
            // topic code is not set
            tracing::warn!("topic code is not set");
            return false;
        };

        let message = message.clone();
        let code = code.clone();
        let node = self.node.clone();
        let message_id = message.id();
        let status: MessageStatusKind;
        if let Some(node_id) = node.get_edge_routing(&endpoint) {
            tracing::debug!(%message_id, %node_id, ?endpoint, "dispatch message to edge");
            if let Some(edge) = node.get_edge_connection(node_id) {
                let result = edge.push_message(&endpoint, message);
                status = if let Err(err) = result {
                    tracing::warn!(?err, "push message failed");
                    MessageStatusKind::Unreachable
                } else {
                    MessageStatusKind::Sent
                };
            } else {
                tracing::warn!(%message_id, %node_id, "edge connection not found when trying to dispatch message");
                status = MessageStatusKind::Unreachable;
            };
        } else {
            tracing::warn!(%message_id, "edge routing not found when trying to dispatch message");
            status = MessageStatusKind::Unreachable;
        }
        tokio::spawn(async move {
            let proposal_result = node
                .propose(Proposal::SetState(SetState {
                    topic: code,
                    update: MessageStateUpdate::new(
                        message_id,
                        HashMap::from([(endpoint, status)]),
                    ),
                }))
                .await;
            if let Err(err) = proposal_result {
                tracing::error!(?err, "set state failed");
            }
            Some(())
        });
        return MessageStatusKind::Sent == status;
    }
}

impl Node {
    pub(self) fn push_durable_commands(
        &self,
        commands: impl IntoIterator<Item = (TopicCode, DurableCommand)>,
    ) {
        if let Some(service) = &self.durable_commit_service {
            service
                .durable_commands_queue
                .write()
                .unwrap()
                .extend(commands);
        }
    }
}
