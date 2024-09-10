use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::RwLock,
    task::Poll,
};

use chrono::{DateTime, Utc};
use openraft::{raft::AppendEntriesRequest, Raft};
use serde::{Deserialize, Serialize};

use crate::{
    prelude::DurableMessage,
    protocol::{
        endpoint::{
            EndpointAddr, Message, MessageAck, MessageId, MessageStateUpdate, MessageStatusKind,
            SetState,
        },
        node::raft::{
            proposal::{Proposal, ProposalContext},
            state_machine::topic::wait_ack::{WaitAckError, WaitAckSuccess},
            TypeConfig,
        },
    },
    util::Timed,
};

use super::{
    wait_ack::{WaitAck, WaitAckResult},
    TopicData,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HoldMessage {
    pub message: Message,
    pub wait_ack: WaitAck,
}

impl HoldMessage {
    // pub(crate) fn as_durable(&self) -> DurableMessage {
    //     DurableMessage {
    //         message: self.message.clone(),
    //         status: self.wait_ack.status.read().unwrap().clone(),
    //         time: Utc::now(),
    //     }
    // }
    // pub(crate) fn from_durable(durable: DurableMessage) -> Self {
    //     let wait_ack = WaitAck {
    //         expect: durable.message.ack_kind(),
    //         status: durable.status.into(),
    //         timeout: None,
    //     };
    //     Self {
    //         message: durable.message,
    //         wait_ack,
    //     }
    // }
    pub(crate) fn send_unsent(&self, context: &ProposalContext) {
        let eps = self
            .wait_ack
            .status
            .iter()
            .filter_map(|(ep, status)| status.is_unsent().then_some(*ep));
        for ep in eps {
            context.dispatch_message(&self.message, ep)
        }
    }
    pub(crate) fn is_resolved(&self) -> bool {
        match self.message.header.target_kind {
            crate::protocol::endpoint::MessageTargetKind::Durable => {
                let Some(durability_config) = self.message.header.durability.as_ref() else {
                    return true;
                };
                let now = Utc::now();
                if now > durability_config.expire {
                    return true;
                }
                if let Some(max_receiver) = durability_config.max_receiver {
                    if self.wait_ack.status.len() >= max_receiver as usize {
                        return true;
                    }
                }
                false
            }
            crate::protocol::endpoint::MessageTargetKind::Online
            | crate::protocol::endpoint::MessageTargetKind::Available
            | crate::protocol::endpoint::MessageTargetKind::Push => self
                .wait_ack
                .status
                .values()
                .all(|status| status.is_resolved(self.wait_ack.expect)),
        }
    }
    pub(crate) fn resolve(self) -> WaitAckResult {
        tracing::trace!("resolved: {self:?}");
        let wait_ack = self.wait_ack;
        let status = wait_ack.status;
        if status.iter().any(|(_, ack)| ack.is_failed()) {
            Err(WaitAckError {
                status,
                exception: None,
            })
        } else {
            Ok(WaitAckSuccess { status })
        }
    }
}

pub enum MessageContainer {
    Durable(),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MessageQueue {
    pub(crate) blocking: bool,
    pub(crate) hold_messages: HashMap<MessageId, HoldMessage>,
    pub(crate) time_id: BTreeSet<Timed<MessageId>>,
    pub(crate) id_time: HashMap<MessageId, DateTime<Utc>>,
    pub(crate) resolved: HashSet<MessageId>,
    pub(crate) size: usize,
}

impl MessageQueue {
    pub(crate) const DEFAULT_CAPACITY: usize = 1024;
    pub(crate) fn clear(&mut self) {
        self.hold_messages.clear();
        self.time_id.clear();
        self.id_time.clear();
        self.resolved.clear();
        self.size = 0;
    }
    pub(crate) fn new(blocking: bool, capacity: usize) -> Self {
        Self {
            blocking,
            hold_messages: HashMap::with_capacity(capacity),
            time_id: BTreeSet::new(),
            resolved: HashSet::with_capacity(capacity),
            id_time: HashMap::with_capacity(capacity),
            size: 0,
        }
    }
    pub(crate) fn push(&mut self, message: HoldMessage) {
        let message_id = message.message.header.message_id;
        let time = Utc::now();
        self.hold_messages.insert(message_id, message);
        self.time_id.insert(Timed::new(time, message_id));
        self.id_time.insert(message_id, time);
        self.size += 1;
    }
    pub(crate) fn push_durable_message(
        &mut self,
        DurableMessage {
            message,
            status,
            time,
        }: DurableMessage,
    ) {
        let message_id = message.header.message_id;
        self.hold_messages.insert(
            message_id,
            HoldMessage {
                wait_ack: WaitAck {
                    expect: message.header.ack_kind,
                    status,
                },
                message,
            },
        );
        self.time_id.insert(Timed::new(time, message_id));
        self.id_time.insert(message_id, time);
        self.size += 1;
    }
    pub(crate) fn pop(&mut self) -> Option<HoldMessage> {
        if let Some(timed) = self.time_id.pop_first() {
            self.id_time.remove(&timed.data);
            self.resolved.remove(&timed.data);
            self.size -= 1;
            self.hold_messages.remove(&timed.data)
        } else {
            None
        }
    }
    pub(crate) fn get_front(&self) -> Option<&HoldMessage> {
        self.time_id
            .first()
            .and_then(|timed| self.hold_messages.get(&timed.data))
    }
    pub(crate) fn remove(&mut self, message_id: MessageId) -> Option<HoldMessage> {
        if let Some(hm) = self.hold_messages.remove(&message_id) {
            self.time_id
                .remove(&Timed::new(self.id_time[&message_id], message_id));
            self.id_time.remove(&message_id);
            self.size -= 1;
            Some(hm)
        } else {
            None
        }
    }
    pub(crate) fn update_ack(
        &mut self,
        ack_to: &MessageId,
        from: EndpointAddr,
        kind: MessageStatusKind,
    ) {
        if let Some(hm) = self.hold_messages.get_mut(ack_to) {
            if let Some(status) = hm.wait_ack.status.get_mut(&from) {
                // resolved message should not be updated
                if status.is_resolved(hm.wait_ack.expect) {
                    return;
                }
                match status {
                    MessageStatusKind::Processed => return,
                    MessageStatusKind::Received => {
                        if kind == MessageStatusKind::Processed || kind == MessageStatusKind::Failed
                        {
                            *status = kind;
                        }
                    }
                    MessageStatusKind::Sending => {
                        if kind != MessageStatusKind::Unsent {
                            *status = kind;
                        }
                    }
                    MessageStatusKind::Sent => {
                        if kind != MessageStatusKind::Unsent || kind != MessageStatusKind::Sending {
                            *status = kind;
                        }
                    }
                    _ => {
                        // otherwise, it must be resolved
                    }
                }
            }
            hm.wait_ack.status.insert(from, kind);
        }
    }
    pub(crate) fn poll_all(&mut self, ctx: &ProposalContext) {
        for id in self.hold_messages.keys().copied().collect::<Vec<_>>() {
            self.poll_message(id, ctx);
        }
    }
    // poll with resolved cache
    pub(crate) fn poll_message(
        &mut self,
        id: MessageId,
        ctx: &ProposalContext,
    ) -> Option<Poll<()>> {
        if self.resolved.contains(&id) {
            Some(Poll::Ready(()))
        } else {
            let resolved = self.poll_message_inner(id, ctx)?;
            if resolved.is_ready() {
                self.resolved.insert(id);
            }
            Some(resolved)
        }
    }
    pub(crate) fn poll_message_inner(
        &mut self,
        id: MessageId,
        ctx: &ProposalContext,
    ) -> Option<Poll<()>> {
        let message = self.hold_messages.get(&id)?;
        if self.blocking {
            let front = self.get_front()?;
            // if blocking, check if the message is the first one
            if front.message.id() != id {
                return Some(Poll::Pending);
            }
        }
        message.send_unsent(ctx);

        if message.is_resolved() {
            Some(Poll::Ready(()))
        } else {
            Some(Poll::Pending)
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.size
    }
    pub(crate) fn swap_out_resolved(&mut self) -> HashSet<MessageId> {
        let mut swap_out = HashSet::new();
        std::mem::swap(&mut swap_out, &mut self.resolved);
        swap_out
    }
    pub(crate) fn blocking_pop(&mut self, context: &ProposalContext) -> Option<HoldMessage> {
        let next = self.get_front()?;
        let poll = self.poll_message(next.message.id(), context)?;
        if poll.is_ready() {
            self.pop()
        } else {
            None
        }
    }
    pub(crate) fn flush(&mut self, context: &ProposalContext) {
        tracing::trace!(blocking = self.blocking, "flushing");
        if self.blocking {
            while let Some(m) = self.blocking_pop(context) {
                let id = m.message.id();
                let result = m.resolve();
                context.resolve_ack(id, result);
            }
        } else {
            for id in self.swap_out_resolved() {
                if let Some(m) = self.remove(id) {
                    let result = m.resolve();
                    context.resolve_ack(id, result);
                }
            }
        }
    }
}
