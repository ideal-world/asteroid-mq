use std::collections::HashMap;

use crate::{
    prelude::{NodeId, TopicCode},
    protocol::{
        endpoint::{DelegateMessage, EndpointInterest, EndpointOffline, EndpointOnline, SetState},
        node::{N2nRoutingInfo, NodeSnapshot},
        topic::{
            durable_message::{LoadTopic, UnloadTopic},
            TopicSnapshot,
        },
    },
};

use super::topic::TopicData;

#[derive(Debug, Clone, Default)]
pub struct NodeData {
    pub(crate) topics: HashMap<TopicCode, TopicData>,
    routing: HashMap<NodeId, N2nRoutingInfo>,
}

impl NodeData {
    pub(crate) fn snapshot(&self) -> NodeSnapshot {
        let topics = self
            .topics
            .iter()
            .map(|(code, topic)| {
                let snapshot = topic.snapshot();
                (code.clone(), snapshot)
            })
            .collect();
        let routing = self.routing.clone();
        NodeSnapshot { topics, routing }
    }
    pub(crate) fn apply_delegate_message(
        &mut self,
        DelegateMessage { topic, message }: DelegateMessage,
    ) {
        if let Some(topic) = self.topics.get_mut(&topic) {
            topic.hold_new_message(message);
        } else {
            todo!()
        }
    }
    pub(crate) fn apply_snapshot(&mut self, snapshot: NodeSnapshot) {
        self.topics = snapshot
            .topics
            .into_iter()
            .map(|(key, value)| (key, TopicData::from_snapshot(value)))
            .collect();
        self.routing = snapshot.routing
    }
    pub(crate) fn apply_load_topic(&mut self, LoadTopic { config, mut queue }: LoadTopic) {
        queue.sort_by_key(|m| m.time);
        let topic = TopicData::new(config);
        self.topics.insert(topic.config.code.clone(), topic);
        let topic = self.topics.get_mut(&config.code).expect("just inserted");
        topic.apply_snapshot(TopicSnapshot {
            ep_routing_table: Default::default(),
            ep_interest_map: Default::default(),
            ep_latest_active: Default::default(),
            queue,
        });
    }
    pub(crate) fn apply_set_state(&mut self, SetState { topic, update }: SetState) {
        let topic = self.get_topic(topic);
        topic.update_and_flush(update);
    }
    pub(crate) fn apply_unload_topic(&self, UnloadTopic { code }: UnloadTopic) {
        self.remove_topic(&code);
    }
    pub(crate) fn apply_ep_online(
        &mut self,
        EndpointOnline {
            topic_code,
            endpoint,
            interests,
            host,
        }: EndpointOnline,
    ) {
        self.get_topic(topic_code)
            .ep_online(endpoint, interests, host);
    }
    pub(crate) fn apply_ep_offline(
        &mut self,
        EndpointOffline {
            topic_code,
            endpoint,
            host: _,
        }: EndpointOffline,
    ) {
        self.get_topic(topic_code).ep_offline(&endpoint);
    }
    pub(crate) fn apply_ep_interest(
        &mut self,
        EndpointInterest {
            topic_code,
            endpoint,
            interests,
        }: EndpointInterest,
    ) {
        self.get_topic(topic_code)
            .update_ep_interest(&endpoint, interests);
    }
}
