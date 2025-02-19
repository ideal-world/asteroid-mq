use std::{
    collections::{BTreeSet, HashMap, HashSet},
    task::Poll,
};

use asteroid_mq_model::{EndpointAddr, NodeId, WaitAckErrorException};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    prelude::DurableMessage,
    protocol::{
        message::*,
        node::durable_message::DurableCommand,
        node::raft::{
            proposal::ProposalContext,
            state_machine::topic::wait_ack::{WaitAckError, WaitAckSuccess},
        },
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
    pub(crate) fn poll_resolved(&self) -> Poll<Result<(), WaitAckErrorException>> {
        match self.message.header.target_kind {
            MessageTargetKind::Durable => {
                let Some(durability_config) = self.message.header.durability.as_ref() else {
                    return Poll::Ready(Err(WaitAckErrorException::DurableMessageWithoutConfig));
                };
                let now = Utc::now();
                if now > durability_config.expire {
                    // message expired
                    tracing::debug!(
                        id = %self.message.id(),
                        "message resolved for expired reason"
                    );
                    return Poll::Ready(Err(WaitAckErrorException::DurableMessageExpired));
                }
                if let Some(max_receiver) = durability_config.max_receiver {
                    let resolved_count = self
                        .wait_ack
                        .status
                        .values()
                        .filter(|status| status.is_fulfilled(self.wait_ack.expect))
                        .count();
                    if resolved_count >= max_receiver as usize {
                        return Poll::Ready(Ok(()));
                    }
                }
                Poll::Pending
            }
            MessageTargetKind::Push => {
                let ok = self
                    .wait_ack
                    .status
                    .values()
                    .any(|status| status.is_fulfilled(self.wait_ack.expect));
                if ok {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Pending
                }
            }
            MessageTargetKind::Online => {
                let ok = self
                    .wait_ack
                    .status
                    .values()
                    .all(|status| status.is_resolved(self.wait_ack.expect));
                if ok {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Pending
                }
            }
        }
    }
    pub(crate) fn resolve(self, exception: Option<WaitAckErrorException>) -> WaitAckResult {
        tracing::trace!("resolved: {self:?}");
        let wait_ack = self.wait_ack;
        let status = wait_ack.status;
        let expect = wait_ack.expect;
        if exception.is_some() {
            return Err(WaitAckError { status, exception });
        }
        match self.message.header.target_kind {
            MessageTargetKind::Push => {
                if status.iter().any(|(_, ack)| ack.is_fulfilled(expect)) {
                    Ok(WaitAckSuccess { status })
                } else {
                    Err(WaitAckError {
                        status,
                        exception: None,
                    })
                }
            }
            MessageTargetKind::Durable => Ok(WaitAckSuccess { status }),
            MessageTargetKind::Online => Ok(WaitAckSuccess { status }),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct MessageQueue {
    pub(crate) blocking: bool,
    pub(crate) hold_messages: HashMap<MessageId, HoldMessage>,
    pub(crate) time_id: BTreeSet<Timed<MessageId>>,
    pub(crate) id_time: HashMap<MessageId, DateTime<Utc>>,
    pub(crate) resolved: HashMap<MessageId, Result<(), WaitAckErrorException>>,
    pub(crate) pending_ack: HashMap<MessageId, WaitAckResult>,
    pub(crate) ack_handle_location: HashMap<NodeId, HashSet<MessageId>>,
    pub(crate) size: usize,
}

impl std::fmt::Debug for MessageQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageQueue")
            .field("blocking", &self.blocking)
            .field("size", &self.size)
            .field("hold_count", &self.hold_messages.len())
            .field("resolved_count", &self.resolved.len())
            .field("pending_ack_count", &self.pending_ack.len())
            .finish()
    }
}

impl MessageQueue {
    pub(crate) const DEFAULT_CAPACITY: usize = 1024;
    pub(crate) fn new(blocking: bool, capacity: usize) -> Self {
        Self {
            blocking,
            hold_messages: HashMap::with_capacity(capacity),
            time_id: BTreeSet::new(),
            resolved: HashMap::with_capacity(capacity),
            id_time: HashMap::with_capacity(capacity),
            pending_ack: HashMap::new(),
            ack_handle_location: HashMap::new(),
            size: 0,
        }
    }
    pub(crate) fn push(&mut self, message: HoldMessage, source: NodeId) {
        let message_id = message.message.header.message_id;
        let time = Utc::now();
        self.hold_messages.insert(message_id, message);
        self.time_id.insert(Timed::new(time, message_id));
        self.id_time.insert(message_id, time);
        self.ack_handle_location
            .entry(source)
            .or_default()
            .insert(message_id);
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
    ) -> Option<Poll<Result<(), WaitAckErrorException>>> {
        #[allow(clippy::map_entry)]
        if self.resolved.contains_key(&id) {
            Some(Poll::Ready(Ok(())))
        } else {
            let resolved = self.poll_message_inner(id, reachable_eps, ctx)?;
            if let Poll::Ready(result) = resolved {
                self.resolved.insert(id, result);
            }
            Some(resolved)
        }
    }
    pub(crate) fn poll_message_inner(
        &mut self,
        id: MessageId,
        reachable_eps: &HashSet<EndpointAddr>,
        ctx: &mut ProposalContext,
    ) -> Option<Poll<Result<(), WaitAckErrorException>>> {
        if self.blocking {
            let front = self.get_front()?;
            // if blocking, check if the message is the first one
            if front.message.id() != id {
                return Some(Poll::Pending);
            }
        }
        let message = self.hold_messages.get_mut(&id)?;
        message.send_unsent(reachable_eps, ctx);

        Some(message.poll_resolved())
    }

    pub(crate) fn len(&self) -> usize {
        self.size
    }
    pub(crate) fn swap_out_resolved(
        &mut self,
    ) -> HashMap<MessageId, Result<(), WaitAckErrorException>> {
        let mut swap_out = HashMap::new();
        std::mem::swap(&mut swap_out, &mut self.resolved);
        swap_out
    }
    pub(crate) fn blocking_pop(
        &mut self,
        reachable_eps: &HashSet<EndpointAddr>,
        context: &mut ProposalContext,
    ) -> Option<(HoldMessage, Result<(), WaitAckErrorException>)> {
        let next = self.get_front()?;
        let poll = self.poll_message(next.message.id(), reachable_eps, context)?;
        if let Poll::Ready(result) = poll {
            self.pop().map(|m| (m, result))
        } else {
            None
        }
    }
    pub(crate) fn finish_ack(&mut self, message_id: MessageId) {
        self.pending_ack.remove(&message_id);
        self.ack_handle_location.iter_mut().for_each(|(_node, s)| {
            s.remove(&message_id);
        });
    }
    pub(crate) fn flush_ack(&self, context: &mut ProposalContext) {
        let local_node_id = context.node.id();
        for (id, result) in &self.pending_ack {
            if let Some(message_id_set) = self.ack_handle_location.get(&local_node_id) {
                if message_id_set.contains(id) {
                    context.resolve_ack(*id, result.clone());
                }
            }
        }
    }
    pub(crate) fn flush(
        &mut self,
        reachable_eps: &HashSet<EndpointAddr>,
        context: &mut ProposalContext,
    ) {
        tracing::trace!(blocking = self.blocking, "flushing");
        if self.blocking {
            while let Some((m, result)) = self.blocking_pop(reachable_eps, context) {
                let id = m.message.id();
                let is_durable = m.message.header.is_durable();
                let result = m.resolve(result.err());
                self.pending_ack.insert(id, result);
                if is_durable {
                    context.push_durable_command(DurableCommand::Archive(id));
                }
            }
        } else {
            let resolved = self.swap_out_resolved();
            for (id, result) in resolved {
                if let Some(m) = self.remove(id) {
                    let is_durable = m.message.header.is_durable();
                    let result = m.resolve(result.err());
                    self.pending_ack.insert(id, result);
                    if is_durable {
                        context.push_durable_command(DurableCommand::Archive(id));
                    }
                } else {
                    tracing::warn!(id = %id, "message not found");
                }
            }
        }
        self.flush_ack(context);
    }
}
