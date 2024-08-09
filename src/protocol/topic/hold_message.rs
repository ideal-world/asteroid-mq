use std::{
    collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet},
    sync::{atomic::AtomicUsize, Mutex, RwLock},
    task::Poll,
};

use bytes::Bytes;
use chrono::{DateTime, Utc};

use crate::{
    protocol::{
        endpoint::{
            EndpointAddr, Message, MessageAck, MessageHeader, MessageId, MessageStatusKind,
        },
        node::event::N2nPacket,
        topic::wait_ack::{self, WaitAckSuccess},
    },
    util::Timed,
};

use super::{
    durable_message::DurableMessage,
    wait_ack::{WaitAck, WaitAckError},
    Topic,
};
#[derive(Debug)]
pub(crate) struct HoldMessage {
    pub message: Message,
    pub wait_ack: WaitAck,
}

impl HoldMessage {
    pub(crate) fn as_durable(&self) -> DurableMessage {
        DurableMessage {
            message: self.message.clone(),
            status: self.wait_ack.status.read().unwrap().clone(),
            time: Utc::now(),
        }
    }
    pub(crate) fn send_unsent(&self, context: &MessagePollContext) {
        let mut status = self.wait_ack.status.write().unwrap();
        let eps = status
            .iter()
            .filter_map(|(ep, status)| status.is_unsent().then_some(*ep));
        // if the message is the first one, call send
        for (ep, result) in context.topic.send_out_message(&self.message, eps) {
            if result.is_ok() {
                status.insert(ep, MessageStatusKind::Sent);
            } else {
                status.insert(ep, MessageStatusKind::Failed);
            }
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
                    if self.wait_ack.status.read().unwrap().len() >= max_receiver as usize {
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
                .read()
                .unwrap()
                .values()
                .all(|status| status.is_resolved(self.wait_ack.expect)),
        }
    }
    pub(crate) fn resolve(self) {
        tracing::trace!("resolved: {self:?}");
        let wait_ack = self.wait_ack;
        let status = wait_ack.status.into_inner().unwrap();
        if status.iter().any(|(_, ack)| ack.is_failed()) {
            let _ = wait_ack.reporter.send(Err(WaitAckError {
                status,
                exception: None,
            }));
        } else {
            let _ = wait_ack.reporter.send(Ok(WaitAckSuccess { status }));
        }
    }
}

pub(crate) struct MessagePollContext<'t> {
    pub(crate) topic: &'t Topic,
}

#[derive(Debug)]
pub(crate) struct MessageQueue {
    pub(crate) blocking: bool,
    pub(crate) hold_messages: HashMap<MessageId, HoldMessage>,
    pub(crate) time_id: BTreeSet<Timed<MessageId>>,
    pub(crate) id_time: HashMap<MessageId, DateTime<Utc>>,
    pub(crate) resolved: RwLock<HashSet<MessageId>>,
    pub(crate) size: usize,
}

impl MessageQueue {
    pub(crate) fn new(blocking: bool, capacity: usize) -> Self {
        Self {
            blocking,
            hold_messages: HashMap::with_capacity(capacity),
            time_id: BTreeSet::new(),
            resolved: RwLock::new(HashSet::with_capacity(capacity)),
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
    pub(crate) fn pop(&mut self) -> Option<HoldMessage> {
        if let Some(timed) = self.time_id.pop_first() {
            self.id_time.remove(&timed.data);
            self.resolved.write().unwrap().remove(&timed.data);
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
    pub(crate) fn set_ack(&self, ack: &MessageAck) {
        tracing::trace!("set ack: {ack:?}");
        if let Some(hm) = self.hold_messages.get(&ack.ack_to) {
            let mut wg = hm.wait_ack.status.write().unwrap();
            wg.insert(ack.from, ack.kind);
        }
    }
    // poll with resolved cache
    pub(crate) fn poll_message(
        &self,
        id: MessageId,
        context: &MessagePollContext<'_>,
    ) -> Option<Poll<()>> {
        if self.resolved.read().unwrap().contains(&id) {
            Some(Poll::Ready(()))
        } else {
            let resolved = self.poll_message_inner(id, context)?;
            if resolved.is_ready() {
                self.resolved.write().unwrap().insert(id);
            }
            Some(resolved)
        }
    }
    pub(crate) fn poll_message_inner(
        &self,
        id: MessageId,
        context: &MessagePollContext<'_>,
    ) -> Option<Poll<()>> {
        let message = self.hold_messages.get(&id)?;
        if self.blocking {
            let front = self.get_front()?;
            // if blocking, check if the message is the first one
            if front.message.id() != id {
                return Some(Poll::Pending);
            }
        }
        message.send_unsent(context);

        if message.is_resolved() {
            // durable: archive the message
            if let Some(durability_service) = context.topic.durability_service.clone() {
                let durable = message.as_durable();
                tokio::spawn(async move {
                    let result = durability_service.archive(durable).await;
                    if let Err(e) = result {
                        tracing::error!(?e, "failed to save durable message");
                    }
                });
            }
            Some(Poll::Ready(()))
        } else {
            // durable: save the message
            if let Some(durability_service) = context.topic.durability_service.clone() {
                let durable = message.as_durable();
                tokio::spawn(async move {
                    let result = durability_service.save(durable).await;
                    if let Err(e) = result {
                        tracing::error!(?e, "failed to save durable message");
                    }
                });
            }
            Some(Poll::Pending)
        }
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.size == 0
    }
    pub(crate) fn len(&self) -> usize {
        self.size
    }
    pub(crate) fn swap_out_resolved(&self) -> HashSet<MessageId> {
        let mut resolved = self.resolved.write().unwrap();
        let mut swap_out = HashSet::new();
        std::mem::swap(&mut swap_out, &mut resolved);
        swap_out
    }
    pub(crate) fn blocking_pop(&mut self, context: &MessagePollContext) -> Option<HoldMessage> {
        let next = self.get_front()?;
        let poll = self.poll_message(next.message.id(), context)?;
        if poll.is_ready() {
            self.pop()
        } else {
            None
        }
    }
    pub(crate) fn flush(&mut self, context: &MessagePollContext) {
        tracing::trace!(blocking = self.blocking, "flushing");
        if self.blocking {
            while let Some(m) = self.blocking_pop(context) {
                m.resolve();
            }
        } else {
            for id in self.swap_out_resolved() {
                if let Some(m) = self.remove(id) {
                    m.resolve()
                }
            }
        }
    }
}

impl Topic {
    pub(crate) fn local_ack(&self, ack: MessageAck) {
        let poll_result = {
            let rg = self.queue.read().unwrap();
            rg.set_ack(&ack);
            rg.poll_message(ack.ack_to, &MessagePollContext { topic: self })
        };
        if let Some(Poll::Ready(())) = poll_result {
            self.queue
                .write()
                .unwrap()
                .flush(&MessagePollContext { topic: self });
        }
    }
    pub(crate) fn handle_ack(&self, ack: MessageAck) {
        if self.node.is(ack.holder) {
            self.local_ack(ack);
        } else if let Some(next_jump) = self.node.get_next_jump(ack.holder) {
            if let Err(e) = self.node.send_packet(
                N2nPacket::event(self.node.new_ack(ack.holder, ack)),
                next_jump,
            ) {}
        } else {
            // handle unreachable
        }
    }
}
