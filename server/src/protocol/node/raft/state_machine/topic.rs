pub mod config;
pub mod message_queue;
pub mod wait_ack;
use crate::{
    prelude::{DurableMessage, Interest, NodeId, Subject},
    protocol::{
        endpoint::EndpointAddr,
        interest::InterestMap,
        message::*,
        node::raft::proposal::{MessageStateUpdate, Proposal, ProposalContext},
        topic::durable_message::DurableCommand,
    },
};
use asteroid_mq_model::SetState;
use config::TopicConfig;
use message_queue::{HoldMessage, MessageQueue};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    task::Poll,
};
use tsuki_scheduler::{Task, TaskUid};
use wait_ack::{WaitAck, WaitAckError, WaitAckErrorException};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TopicData {
    pub(crate) config: TopicConfig,
    pub(crate) ep_routing_table: HashMap<NodeId, HashSet<EndpointAddr>>,
    pub(crate) ep_interest_map: InterestMap<EndpointAddr>,
    pub(crate) queue: MessageQueue,
}

impl TopicData {
    pub(crate) fn from_durable(config: TopicConfig, mut messages: Vec<DurableMessage>) -> Self {
        messages.sort_by_key(|f| f.time);
        let mut queue = MessageQueue::new(
            config.blocking,
            config
                .overflow_config
                .as_ref()
                .map(|x| x.size())
                .unwrap_or(MessageQueue::DEFAULT_CAPACITY),
        );
        for message in messages {
            queue.push_durable_message(message);
        }
        Self {
            config,
            ep_routing_table: HashMap::new(),
            ep_interest_map: InterestMap::new(),
            queue,
        }
    }
    pub(crate) fn collect_addr_by_subjects<'i>(
        &self,
        subjects: impl Iterator<Item = &'i Subject>,
    ) -> HashSet<EndpointAddr> {
        let mut ep_collect = HashSet::new();
        for subject in subjects {
            ep_collect.extend(self.ep_interest_map.find(subject));
        }
        ep_collect
    }
    pub fn hold_new_message(&mut self, message: Message, ctx: &mut ProposalContext) {
        let ep_collect = match message.header.target_kind {
            MessageTargetKind::Durable | MessageTargetKind::Online => {
                self.collect_addr_by_subjects(message.header.subjects.iter())
                // just accept all
            }
            MessageTargetKind::Available => {
                unimplemented!("available kind is not supported");
                // unsupported
            }
            MessageTargetKind::Push => {
                let message_hash = crate::util::hash64(&message.id());
                let ep_collect = self.collect_addr_by_subjects(message.header.subjects.iter());

                let mut hash_ring = ep_collect
                    .iter()
                    .map(|ep| (crate::util::hash64(ep), *ep))
                    .collect::<Vec<_>>();
                hash_ring.sort_by_key(|x| x.0);
                if hash_ring.is_empty() {
                    ctx.resolve_ack(
                        message.id(),
                        Err(WaitAckError::exception(
                            WaitAckErrorException::NoAvailableTarget,
                        )),
                    );
                    return;
                } else {
                    let ep = hash_ring[(message_hash as usize) % (hash_ring.len())].1;
                    tracing::debug!(?ep, "select ep");
                    HashSet::from([ep])
                }
            }
        };
        let hold_message = HoldMessage {
            message: message.clone(),
            wait_ack: WaitAck::new(message.ack_kind(), ep_collect.clone()),
        };
        {
            // put in queue
            if let Some(overflow_config) = &self.config.overflow_config {
                let size = u32::from(overflow_config.size) as usize;
                let waiting_size = self.queue.len();
                if waiting_size >= size {
                    match overflow_config.policy {
                        config::TopicOverflowPolicy::RejectNew => {
                            ctx.resolve_ack(
                                message.id(),
                                Err(WaitAckError::exception(WaitAckErrorException::Overflow)),
                            );
                            return;
                        }
                        config::TopicOverflowPolicy::DropOld => {
                            let old = self.queue.pop().expect("queue at least one element");
                            ctx.resolve_ack(
                                old.message.id(),
                                Err(WaitAckError::exception(WaitAckErrorException::Overflow)),
                            );
                        }
                    }
                }
            }
            self.queue.push(hold_message);
            'durable_task: {
                if let Some(durable_config) = &message.header.durability {
                    if message.header.target_kind != MessageTargetKind::Durable {
                        tracing::warn!("durable message should have durable target kind");
                        break 'durable_task;
                    };
                    let topic = ctx.topic_code.clone().expect("topic code not set");
                    let node = ctx.node.clone();
                    let message_id = message.id();
                    ctx.node.scheduler.add_task(
                        TaskUid::new(message.id().to_u128()),
                        Task::tokio(
                            tsuki_scheduler::schedule::Once::new(durable_config.expire),
                            move || {
                                let node = node.clone();
                                let topic = topic.clone();
                                async move {
                                    if node.raft().await.ensure_linearizable().await.is_err() {
                                        tracing::trace!("raft not leader, skip durable commands");
                                        return;
                                    }
                                    let result = node
                                        .propose(Proposal::SetState(SetState {
                                            topic,
                                            update: MessageStateUpdate::new_empty(message_id),
                                        }))
                                        .await;
                                    if let Err(e) = result {
                                        tracing::warn!(?e, "proposal expire message failed");
                                    }
                                }
                            },
                        ),
                    );
                    ctx.push_durable_command(DurableCommand::Create(message.clone()));
                }
            }
        }
        self.update_and_flush(MessageStateUpdate::new_empty(message.id()), ctx);
        tracing::debug!(?ep_collect, "hold new message");
    }
    pub(crate) fn reachable_eps(&self, node_id: &NodeId) -> HashSet<EndpointAddr> {
        self.ep_routing_table
            .get(node_id)
            .cloned()
            .unwrap_or_default()
    }
    pub(crate) fn update_and_flush(
        &mut self,
        update: MessageStateUpdate,
        ctx: &mut ProposalContext,
    ) {
        let reachable_eps = self.reachable_eps(&ctx.node.id());
        ctx.push_durable_command(DurableCommand::UpdateStatus(update.clone()));
        let poll_result = {
            for (from, status) in update.status {
                self.queue.update_ack(&update.message_id, from, status)
            }
            self.queue
                .poll_message(update.message_id, &reachable_eps, ctx)
        };
        if let Some(Poll::Ready(())) = poll_result {
            self.queue.flush(&reachable_eps, ctx);
        }
    }
    pub(crate) fn update_ep_interest(
        &mut self,
        ep: &EndpointAddr,
        interests: Vec<Interest>,
        ctx: &mut ProposalContext,
    ) {
        self.ep_interest_map.delete(ep);
        for interest in interests {
            self.ep_interest_map.insert(interest, *ep);
        }
        let mut message_need_poll = HashSet::new();
        for (id, message) in &mut self.queue.hold_messages {
            if message.message.header.target_kind == MessageTargetKind::Durable {
                for subject in message.message.header.subjects.iter() {
                    // if
                    if self.ep_interest_map.find(subject).contains(ep) {
                        if !message.wait_ack.status.contains_key(ep) {
                            message
                                .wait_ack
                                .status
                                .insert(*ep, MessageStatusKind::Unsent);
                        }
                        message_need_poll.insert(*id);
                    }
                }
            }
        }
        for id in message_need_poll {
            self.update_and_flush(MessageStateUpdate::new_empty(id), ctx);
        }
    }
    pub(crate) fn ep_online(
        &mut self,
        endpoint: EndpointAddr,
        interests: Vec<Interest>,
        host: NodeId,
        ctx: &mut ProposalContext,
    ) {
        let mut message_need_poll = HashSet::new();
        {
            self.ep_routing_table
                .entry(host)
                .or_default()
                .insert(endpoint);
            for interest in &interests {
                self.ep_interest_map.insert(interest.clone(), endpoint);
            }
            let queue = &mut self.queue;
            for (id, message) in &mut queue.hold_messages {
                if message.message.header.target_kind == MessageTargetKind::Durable {
                    let status = &mut message.wait_ack.status;
                    if !status.contains_key(&endpoint)
                        && message
                            .message
                            .header
                            .subjects
                            .iter()
                            .any(|s| self.ep_interest_map.find(s).contains(&endpoint))
                    {
                        status.insert(endpoint, MessageStatusKind::Unsent);
                        message_need_poll.insert(*id);
                    }
                }
            }
        }
        tracing::trace!(?message_need_poll, ?endpoint, "flush durable messages");
        for id in message_need_poll {
            self.update_and_flush(MessageStateUpdate::new_empty(id), ctx);
        }
    }

    pub(crate) fn ep_offline(
        &mut self,
        host: NodeId,
        endpoint: &EndpointAddr,
        ctx: &mut ProposalContext,
    ) {
        self.ep_routing_table
            .entry(host)
            .or_default()
            .remove(endpoint);
        self.ep_interest_map.delete(endpoint);
        let mut message_need_poll = HashSet::new();
        // update state
        for message in self.queue.hold_messages.values_mut() {
            if let Some(status) = message.wait_ack.status.get_mut(endpoint) {
                *status = MessageStatusKind::Unreachable;
                message_need_poll.insert(message.message.id());
            }
        }
        for id in message_need_poll {
            self.update_and_flush(MessageStateUpdate::new_empty(id), ctx);
        }
    }
}
