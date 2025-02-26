use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::{
    prelude::TopicCode,
    protocol::node::raft::proposal::{
        AckFinished, DelegateMessage, EndpointInterest, EndpointOffline, EndpointOnline, LoadTopic,
        ProposalContext, SetState, UnloadTopic,
    },
};

use super::topic::TopicData;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeData {
    pub(crate) topics: HashMap<TopicCode, TopicData>,
}

impl NodeData {
    #[instrument(skip_all, fields(node_id=%ctx.node.id(), topic=%topic, message_id = %message.id()))]
    pub(crate) fn apply_delegate_message(
        &mut self,
        DelegateMessage {
            topic,
            message,
            source,
        }: DelegateMessage,
        mut ctx: ProposalContext,
    ) {
        ctx.set_topic_code(topic.clone());
        if let Some(topic) = self.topics.get_mut(&topic) {
            topic.hold_new_message(message, source, &mut ctx);
        } else {
            tracing::error!(?topic, "topic not found");
        }
    }

    pub(crate) fn apply_load_topic(
        &mut self,
        LoadTopic { config, mut queue }: LoadTopic,
        mut ctx: ProposalContext,
    ) {
        use std::collections::hash_map::Entry;
        let code = config.code.clone();
        let entry = self.topics.entry(code.clone());
        match entry {
            Entry::Vacant(entry) => {
                queue.sort_by_key(|m| m.time);
                ctx.set_topic_code(code.clone());
                let topic = TopicData::from_durable(config, queue);
                entry.insert(topic);
            }
            _ => {
                tracing::warn!(?code, "topic already loaded");
            }
        }
    }
    #[instrument(skip_all, fields(node_id=%ctx.node.id(), topic=%topic, message_id=%update.message_id))]
    pub(crate) fn apply_set_state(
        &mut self,
        SetState { topic, update }: SetState,
        mut ctx: ProposalContext,
    ) {
        ctx.set_topic_code(topic.clone());
        if let Some(topic) = self.topics.get_mut(&topic) {
            topic.update_and_flush(update.clone(), &mut ctx);
        } else {
            tracing::error!(?topic, "topic not found");
        }
    }
    pub(crate) fn apply_unload_topic(&mut self, UnloadTopic { code }: UnloadTopic) {
        self.topics.remove(&code);
    }
    pub(crate) fn apply_ep_online(
        &mut self,
        EndpointOnline {
            topic_code,
            endpoint,
            interests,
            host,
        }: EndpointOnline,
        mut ctx: ProposalContext,
    ) {
        let Some(topic) = self.topics.get_mut(&topic_code) else {
            return;
        };
        ctx.set_topic_code(topic_code);
        topic.ep_online(endpoint, interests, host, &mut ctx);
    }

    pub(crate) fn apply_ep_offline(
        &mut self,
        EndpointOffline {
            topic_code,
            endpoint,
            host,
        }: EndpointOffline,
        mut ctx: ProposalContext,
    ) {
        let Some(topic) = self.topics.get_mut(&topic_code) else {
            return;
        };
        ctx.set_topic_code(topic_code);
        topic.ep_offline(host, &endpoint, &mut ctx);
    }

    pub(crate) fn apply_ep_interest(
        &mut self,
        EndpointInterest {
            topic_code,
            endpoint,
            interests,
        }: EndpointInterest,
        mut ctx: ProposalContext,
    ) {
        let Some(topic) = self.topics.get_mut(&topic_code) else {
            return;
        };
        ctx.set_topic_code(topic_code);
        topic.update_ep_interest(&endpoint, interests, &mut ctx);
    }
    #[instrument]
    pub(crate) fn apply_ack_finished(
        &mut self,
        AckFinished { message_id, topic }: AckFinished,
        mut _ctx: ProposalContext,
    ) {
        if let Some(topic_data) = self.topics.get_mut(&topic) {
            topic_data.finish_ack(message_id);
        } else {
            tracing::warn!(%topic, "no such topic")
        }
    }
}
