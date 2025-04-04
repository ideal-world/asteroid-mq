pub mod config;
pub mod message_queue;
pub mod wait_ack;
use crate::{
    prelude::{DurableMessage, Interest, NodeId, Subject},
    protocol::{
        interest::InterestMap,
        message::*,
        node::durable_message::DurableCommand,
        node::raft::proposal::{MessageStateUpdate, Proposal, ProposalContext},
    },
};
use asteroid_mq_model::{EndpointAddr, SetState};
use config::TopicConfig;
use message_queue::{HoldMessage, MessageQueue};
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
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
    // the earlier message should be in front
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
        for mut message in messages {
            // if message is unsent, mark it unreachable, since it's not possible to send it
            message.status.values_mut().for_each(|s| {
                if *s == MessageStatusKind::Unsent {
                    *s = MessageStatusKind::Unreachable;
                }
            });
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
    pub fn hold_new_message(
        &mut self,
        message: Message,
        source: NodeId,
        ctx: &mut ProposalContext,
    ) {
        /// 分布式一致的选出top n节点，如果节点数量小于等于top n则直接返回
        fn hash_ring<T: std::hash::Hash>(
            mut set: HashSet<EndpointAddr>,
            seed: &T,
            top_n: usize,
        ) -> HashSet<EndpointAddr> {
            if set.len() <= top_n {
                set
            } else {
                let seed_u64 = crate::util::hash64(&seed);
                let mut hash_ring = set
                    .iter()
                    .map(|ep| (crate::util::hash64(&(ep, seed_u64)), *ep))
                    .collect::<Vec<_>>();
                hash_ring.sort();
                set.clear();
                set.extend(hash_ring.into_iter().map(|(_hash, ep)| ep).take(top_n));
                tracing::debug!(?set, "hash ring ep selected");
                set
            }
        }

        let max_payload_size = self.config.max_payload_size as usize;
        if message.payload.0.len() >= max_payload_size {
            ctx.resolve_ack(
                message.id(),
                Err(WaitAckError::exception(
                    WaitAckErrorException::PayloadToLarge,
                )),
            );
            return;
        }

        let message_id = message.id();
        let ep_collect = match message.header.target_kind {
            MessageTargetKind::Online => {
                self.collect_addr_by_subjects(message.header.subjects.iter())
                // just accept all
            }
            MessageTargetKind::Durable => {
                let set = self.collect_addr_by_subjects(message.header.subjects.iter());
                if let Some(max_receiver) = &message
                    .header
                    .durability
                    .as_ref()
                    .and_then(|d| d.max_receiver)
                {
                    // select top n by hash
                    hash_ring(set, &message.id(), *max_receiver as usize)
                } else {
                    set
                }
                // just accept all
            }
            MessageTargetKind::Push => {
                let ep_collect = self.collect_addr_by_subjects(message.header.subjects.iter());
                if ep_collect.is_empty() {
                    ctx.resolve_ack(
                        message.id(),
                        Err(WaitAckError::exception(
                            WaitAckErrorException::NoAvailableTarget,
                        )),
                    );
                    return;
                }
                hash_ring(ep_collect, &message.id(), 1)
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
            self.queue.push(hold_message, source);
            'durable_task: {
                if message.header.target_kind == MessageTargetKind::Durable {
                    let Some(durable_config) = &message.header.durability else {
                        tracing::warn!("durable message should have durable target kind");
                        ctx.resolve_ack(
                            message_id,
                            Err(WaitAckError::exception(
                                WaitAckErrorException::DurableMessageWithoutConfig,
                            )),
                        );
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
                                    tracing::debug!(%message_id, "durable message expired");
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
                    if message.header.is_durable() {
                        ctx.push_durable_command(DurableCommand::Create(message.clone()));
                    }
                }
            }
        }
        self.update_and_flush(MessageStateUpdate::new_empty(message.id()), ctx);
        tracing::debug!(?ep_collect, "hold new message");
    }
    pub(crate) fn reachable_eps(&self, node_id: &NodeId) -> Cow<'_, HashSet<EndpointAddr>> {
        self.ep_routing_table
            .get(node_id)
            .map(Cow::Borrowed)
            // .cloned()
            .unwrap_or(Cow::Owned(HashSet::new()))
    }
    pub(crate) fn update_and_flush(
        &mut self,
        update: MessageStateUpdate,
        ctx: &mut ProposalContext,
    ) {
        // check if message is of durable kind
        if let Some(message) = self.queue.hold_messages.get(&update.message_id) {
            if message.message.header.is_durable() && !update.status.is_empty() {
                ctx.push_durable_command(DurableCommand::UpdateStatus(update.clone()));
            }
        }
        // hacky here to temporary take queue memory:
        // let mut queue: MessageQueue = unsafe {
        //     #[allow(
        //         clippy::uninit_assumed_init,
        //         invalid_value,
        //         reason = "we don't access it anyway"
        //     )]
        //     std::mem::MaybeUninit::zeroed().assume_init()
        // };
        let mut queue: MessageQueue = MessageQueue::default();
        {
            std::mem::swap(&mut queue, &mut self.queue);
            let reachable_eps = self.reachable_eps(&ctx.node.id());
            let poll_result = {
                for (from, status) in update.status {
                    queue.update_ack(&update.message_id, from, status)
                }
                queue.poll_message(update.message_id, &reachable_eps, ctx)
            };
            if let Some(Poll::Ready(_)) = poll_result {
                queue.flush(&reachable_eps, ctx);
            }
            std::mem::swap(&mut queue, &mut self.queue);
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
                if message.message.header.target_kind.is_durable()
                    || message.message.header.target_kind.is_push()
                {
                    let status = &mut message.wait_ack.status;
                    if !status.contains_key(&endpoint)
                        && message
                            .message
                            .header
                            .subjects
                            .iter()
                            .any(|s| self.ep_interest_map.find(s).contains(&endpoint))
                    {
                        if message.message.header.target_kind.is_push() {
                            if status
                                .values()
                                .any(|status| !status.is_failed_or_unreachable())
                            {
                                // already has a receiver
                                continue;
                            }
                        } else if let Some(config) = message.message.header.durability.as_ref() {
                            if let Some(max_receiver) = config.max_receiver {
                                if status
                                    .values()
                                    .filter(|status| status.is_fulfilled(message.wait_ack.expect))
                                    .count()
                                    >= max_receiver as usize
                                {
                                    // max receiver reached
                                    continue;
                                }
                            }
                            if config.expire < chrono::Utc::now() {
                                // expired
                                continue;
                            }
                        }
                        status.insert(endpoint, MessageStatusKind::Unsent);
                        message_need_poll.insert(*id);
                    }
                }
            }
        }
        tracing::trace!(?message_need_poll, ?endpoint, "flush durable messages");
        let message_count = message_need_poll.len();
        tracing::info!(
            ?endpoint,
            "endpoint online, {} messaged will be polled",
            message_count
        );
        for id in message_need_poll {
            // enable to debug online proposal
            ctx.debug_ep_online = false;
            self.update_and_flush(MessageStateUpdate::new_empty(id), ctx);
        }
        tracing::info!(
            ?endpoint,
            "endpoint online, {} messaged have been polled",
            message_count
        );
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

    pub(crate) fn finish_ack(&mut self, message_id: MessageId) {
        self.queue.finish_ack(message_id);
    }
}
