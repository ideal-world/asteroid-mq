use std::{
    collections::{BTreeSet, HashMap, HashSet},
    task::Poll,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    prelude::DurableMessage,
    protocol::{
        endpoint::EndpointAddr,
        message::*,
        node::raft::{
            proposal::ProposalContext,
            state_machine::topic::wait_ack::{WaitAckError, WaitAckSuccess},
        },
        topic::durable_message::DurableCommand,
    },
    util::Timed,
};

use super::wait_ack::{WaitAck, WaitAckResult};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HoldMessage {
    pub message: Message,
    pub wait_ack: WaitAck,
}

impl HoldMessage {
    pub(crate) fn send_unsent(
        &mut self,
        reachable_eps: &HashSet<EndpointAddr>,
        context: &ProposalContext,
    ) {
        for (ep, status) in self.wait_ack.status.iter_mut() {
            tracing::trace!(?ep, %status, ?reachable_eps, "send_unsent");
            if status.is_unsent() && reachable_eps.contains(ep) {
                *status = MessageStatusKind::Sending;
                context.dispatch_message(&self.message, *ep);
            }
        }
    }
    pub(crate) fn is_resolved(&self) -> bool {
        match self.message.header.target_kind {
            MessageTargetKind::Durable => {
                let Some(durability_config) = self.message.header.durability.as_ref() else {
                    return true;
                };
                let now = Utc::now();
                if now > durability_config.expire {
                    return true;
                }
                if let Some(max_receiver) = durability_config.max_receiver {
                    let resolved_count = self
                        .wait_ack
                        .status
                        .values()
                        .filter(|status| status.is_resolved(self.wait_ack.expect))
                        .count();
                    if resolved_count >= max_receiver as usize {
                        return true;
                    }
                }
                false
            }
            MessageTargetKind::Online | MessageTargetKind::Push => self
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
                    MessageStatusKind::Sent => {
                        if kind != MessageStatusKind::Unsent || kind != MessageStatusKind::Sending {
                            *status = kind;
                        }
                    }
                    MessageStatusKind::Sending => {
                        if kind != MessageStatusKind::Unsent {
                            *status = kind;
                        }
                    }
                    MessageStatusKind::Unsent => {
                        *status = kind;
                    }
                    _ => {
                        // otherwise, it must be resolved
                    }
                }
            }
            hm.wait_ack.status.insert(from, kind);
        }
    }
    // poll with resolved cache
    pub(crate) fn poll_message(
        &mut self,
        id: MessageId,
        reachable_eps: &HashSet<EndpointAddr>,
        ctx: &mut ProposalContext,
    ) -> Option<Poll<()>> {
        if self.resolved.contains(&id) {
            Some(Poll::Ready(()))
        } else {
            let resolved = self.poll_message_inner(id, reachable_eps, ctx)?;
            if resolved.is_ready() {
                self.resolved.insert(id);
            }
            Some(resolved)
        }
    }
    pub(crate) fn poll_message_inner(
        &mut self,
        id: MessageId,
        reachable_eps: &HashSet<EndpointAddr>,
        ctx: &mut ProposalContext,
    ) -> Option<Poll<()>> {
        if self.blocking {
            let front = self.get_front()?;
            // if blocking, check if the message is the first one
            if front.message.id() != id {
                return Some(Poll::Pending);
            }
        }
        let message = self.hold_messages.get_mut(&id)?;
        message.send_unsent(reachable_eps, ctx);

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
    pub(crate) fn blocking_pop(
        &mut self,
        reachable_eps: &HashSet<EndpointAddr>,
        context: &mut ProposalContext,
    ) -> Option<HoldMessage> {
        let next = self.get_front()?;
        let poll = self.poll_message(next.message.id(), reachable_eps, context)?;
        if poll.is_ready() {
            self.pop()
        } else {
            None
        }
    }
    pub(crate) fn flush(
        &mut self,
        reachable_eps: &HashSet<EndpointAddr>,
        context: &mut ProposalContext,
    ) {
        tracing::trace!(blocking = self.blocking, "flushing");
        if self.blocking {
            while let Some(m) = self.blocking_pop(reachable_eps, context) {
                let id = m.message.id();
                let is_durable = m.message.header.is_durable();
                let result = m.resolve();
                context.resolve_ack(id, result);
                if is_durable {
                    context.push_durable_command(DurableCommand::Archive(id));
                }
            }
        } else {
            for id in self.swap_out_resolved() {
                if let Some(m) = self.remove(id) {
                    let is_durable = m.message.header.is_durable();
                    let result = m.resolve();
                    context.resolve_ack(id, result);
                    if is_durable {
                        context.push_durable_command(DurableCommand::Archive(id));
                    }
                }
            }
        }
    }
}
