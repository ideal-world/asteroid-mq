use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use chrono::Utc;
use serde::{Deserialize, Serialize};
use tracing::Instrument;

use crate::{
    prelude::{DurableMessage, DurableService, MessageId, Node, TopicCode},
    protocol::{endpoint::EndpointAddr, message::*, topic::durable_message::DurableCommand},
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
pub(crate) struct ProposalContext {
    pub node: Node,
    pub topic_code: Option<TopicCode>,
}

impl ProposalContext {
    pub(crate) fn new(node: Node) -> Self {
        Self {
            node,
            topic_code: None,
        }
    }
    pub(crate) fn push_durable_command(&mut self, command: DurableCommand) {
        self.node.push_durable_commands(Some(command));
    }
    pub(crate) fn commit_durable_commands(&mut self) {
        if let Some(service) = self.durable_service() {
            let topic_code = self.topic_code.clone().expect("topic code not set");
            let node = self.node.clone();
            let Some(_topic) = node.get_topic(&topic_code) else {
                tracing::warn!(?topic_code, "topic not found");
                return;
            };
            tokio::spawn(
                async move {
                    // only one execute task at a time for each topic
                    let sync_lock = node.get_durable_lock(topic_code.clone()).await;
                    let _sync_guard = sync_lock.lock().await;
                    let commands = node.swap_out_durable_commands();
                    if node.raft().await.ensure_linearizable().await.is_err() {
                        tracing::trace!("raft not leader, skip durable commands");
                        return;
                    } else {
                        tracing::trace!(?commands, "raft is leader, commit durable commands");
                    };
                    for command in commands {
                        let topic = topic_code.clone();
                        let result = match command {
                            DurableCommand::Create(command) => {
                                service
                                    .save(
                                        topic,
                                        DurableMessage {
                                            message: command,
                                            status: Default::default(),
                                            time: Utc::now(),
                                        },
                                    )
                                    .await
                            }
                            DurableCommand::UpdateStatus(command) => {
                                service.update_status(topic, command).await
                            }
                            DurableCommand::Archive(command) => {
                                service.archive(topic, command).await
                            }
                        };
                        if let Err(err) = result {
                            tracing::error!(?err, "durable command failed");
                        }
                    }
                }
                .instrument(tracing::info_span!("flush durable commands")),
            );
        }
    }
    pub(crate) fn durable_service(&self) -> Option<DurableService> {
        self.node.config.durable.clone()
    }
    pub(crate) fn set_topic_code(&mut self, code: TopicCode) {
        self.topic_code = Some(code);
    }
    pub(crate) fn resolve_ack(&self, id: MessageId, result: WaitAckResult) {
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
    pub(crate) fn dispatch_message(&self, message: &Message, endpoint: EndpointAddr) {
        let Some(ref code) = self.topic_code else {
            // topic code is not set
            tracing::warn!("topic code is not set");
            return;
        };
        let Some(topic) = self.node.get_topic(code) else {
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
        });
    }
}

impl Node {
    pub(self) fn push_durable_commands(&self, commands: impl IntoIterator<Item = DurableCommand>) {
        self.durable_commands_queue
            .write()
            .unwrap()
            .extend(commands);
    }
    pub(self) fn swap_out_durable_commands(&self) -> VecDeque<DurableCommand> {
        let mut queue = self.durable_commands_queue.write().unwrap();
        std::mem::take(&mut *queue)
    }
    pub(self) async fn get_durable_lock(&self, topic: TopicCode) -> Arc<tokio::sync::Mutex<()>> {
        self.durable_syncs
            .lock()
            .await
            .entry(topic)
            .or_default()
            .clone()
    }
}
