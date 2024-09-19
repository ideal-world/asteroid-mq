use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{
    prelude::{MessageId, Node, TopicCode},
    protocol::{
        endpoint::{
            DelegateMessage, EndpointAddr, EndpointInterest, EndpointOffline, EndpointOnline,
            Message, MessageStateUpdate, MessageStatusKind, SetState,
        },
        topic::durable_message::{LoadTopic, UnloadTopic},
    },
};

use super::state_machine::topic::wait_ack::WaitAckResult;

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
    pub node: Node,
    pub topic_code: Option<TopicCode>,
}

impl ProposalContext {
    pub fn set_topic_code(&mut self, code: TopicCode) {
        self.topic_code = Some(code);
    }
    pub fn resolve_ack(&self, id: MessageId, result: WaitAckResult) {
        let Some(ref code) = self.topic_code else {
            return;
        };
        let Some(topic) = self.node.get_topic(code) else {
            return;
        };
        tokio::spawn(async move {
            if let Some(tx) = topic.ack_waiting_pool.write().await.remove(&id) {
                let _ = tx.send(result);
            }
        });
    }
    #[tracing::instrument(skip(self))]
    pub fn dispatch_message(&self, message: &Message, endpoint: EndpointAddr) {
        let Some(ref code) = self.topic_code else {
            // topic code is not set
            tracing::warn!("topic code is not set");
            return;
        };
        let Some(topic) = self.node.get_topic(code) else {
            // TODO: recover snapshot here
            tracing::warn!(?code, "topic not found");
            return;
        };
        let message = message.clone();
        let code = code.clone();
        let node = self.node.clone();
        tokio::spawn(async move {
            let message_id = message.id();
            let status = topic
                .dispatch_message(message, &endpoint)
                .await
                .unwrap_or(MessageStatusKind::Unreachable);
            let proposal_result = node
                .proposal(Proposal::SetState(SetState {
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
        });
    }
}
