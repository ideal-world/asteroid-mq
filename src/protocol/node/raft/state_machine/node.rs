use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::{
    prelude::{Topic, TopicCode},
    protocol::{
        node::raft::proposal::{
            DelegateMessage, EndpointInterest, EndpointOffline, EndpointOnline, LoadTopic,
            ProposalContext, SetState, UnloadTopic,
        },
        topic::TopicInner,
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
        DelegateMessage { topic, message }: DelegateMessage,
        mut ctx: ProposalContext,
    ) {
        ctx.set_topic_code(topic.clone());
        if let Some(topic) = self.topics.get_mut(&topic) {
            topic.hold_new_message(message.clone(), &mut ctx);
        } else {
            tracing::error!(?topic, "topic not found");
        }
        ctx.commit_durable_commands();
    }
    pub(crate) fn apply_load_topic(
        &mut self,
        LoadTopic { config, mut queue }: LoadTopic,
        mut ctx: ProposalContext,
    ) {
        queue.sort_by_key(|m| m.time);
        let code = config.code.clone();
        ctx.set_topic_code(code.clone());
        let topic = TopicData::from_durable(config, queue);
        self.topics.insert(code.clone(), topic);
        let node = ctx.node.clone();
        let topic = Topic {
            inner: Arc::new(TopicInner {
                code: code.clone(),
                node: node.clone(),
                ack_waiting_pool: Default::default(),
                local_endpoints: Default::default(),
            }),
        };
        node.topics.write().unwrap().insert(code.clone(), topic);
        ctx.commit_durable_commands();

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
        ctx.commit_durable_commands();
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
        ctx.commit_durable_commands();
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
        ctx.commit_durable_commands();
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
        ctx.commit_durable_commands();
    }
}
