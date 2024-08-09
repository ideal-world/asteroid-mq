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
    wait_ack::{WaitAck, WaitAckError},
    Topic,
};
#[derive(Debug)]
pub struct HoldMessage {
    pub message: Message,
    pub wait_ack: WaitAck,
}

impl HoldMessage {
    pub fn send_unsent(&self, context: &MessagePollContext) {
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
    pub fn is_resolved(&self) -> bool {
        match self.message.header.target_kind {
            crate::protocol::endpoint::MessageTargetKind::Durable => todo!(),
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
    pub fn resolve(mut self) {
        tracing::trace!("resolved: {self:?}");
        let mut wait_ack = self.wait_ack;
        let status: Vec<(EndpointAddr, MessageStatusKind)> = wait_ack
            .status
            .get_mut()
            .unwrap()
            .iter()
            .map(|(ep, ack)| (*ep, *ack))
            .collect();
        let mut fail_list: Vec<EndpointAddr> = Vec::new();
        let mut unreachable_list: Vec<EndpointAddr> = Vec::new();
        for (ep, ack) in status {
            match ack {
                MessageStatusKind::Failed => {
                    fail_list.push(ep);
                }
                MessageStatusKind::Unreachable => {
                    unreachable_list.push(ep);
                }
                _ => {}
            }
        }
        if fail_list.is_empty() && unreachable_list.is_empty() {
            let _ = wait_ack.reporter.send(Ok(WaitAckSuccess {
                ep_list: wait_ack.ep_list.iter().cloned().collect(),
            }));
        } else {
            let _ = wait_ack.reporter.send(Err(WaitAckError {
                ep_list: wait_ack.ep_list.iter().cloned().collect(),
                failed_list: fail_list,
                unreachable_list,
                timeout_list: Vec::new(),
                exception: None,
            }));
        }
    }
}

pub struct MessagePollContext<'t> {
    pub(crate) message_id: MessageId,
    pub(crate) topic: &'t Topic,
}

#[derive(Debug)]
pub struct MemoryMessageQueue {
    pub(crate) blocking: bool,
    pub(crate) hold_messages: HashMap<MessageId, HoldMessage>,
    pub(crate) time_id: BTreeSet<Timed<MessageId>>,
    pub(crate) id_time: HashMap<MessageId, DateTime<Utc>>,
    pub(crate) resolved: RwLock<HashSet<MessageId>>,
    pub(crate) size: usize,
}

impl MemoryMessageQueue {
    pub fn new(blocking: bool, capacity: usize) -> Self {
        Self {
            blocking,
            hold_messages: HashMap::with_capacity(capacity),
            time_id: BTreeSet::new(),
            resolved: RwLock::new(HashSet::with_capacity(capacity)),
            id_time: HashMap::with_capacity(capacity),
            size: 0,
        }
    }
    pub fn push(&mut self, message: HoldMessage) {
        let message_id = message.message.header.message_id;
        let time = Utc::now();
        self.hold_messages.insert(message_id, message);
        self.time_id.insert(Timed::new(time, message_id));
        self.id_time.insert(message_id, time);
        self.size += 1;
    }
    pub fn get_back(&self) -> Option<&HoldMessage> {
        self.time_id
            .last()
            .and_then(|timed| self.hold_messages.get(&timed.data))
    }
    pub fn pop(&mut self) -> Option<HoldMessage> {
        if let Some(timed) = self.time_id.pop_first() {
            self.id_time.remove(&timed.data);
            self.resolved.write().unwrap().remove(&timed.data);
            self.size -= 1;
            self.hold_messages.remove(&timed.data)
        } else {
            None
        }
    }
    pub fn get_front(&self) -> Option<&HoldMessage> {
        self.time_id
            .first()
            .and_then(|timed| self.hold_messages.get(&timed.data))
    }
    pub fn remove(&mut self, message_id: MessageId) -> Option<HoldMessage> {
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
    pub fn set_ack(&self, ack: &MessageAck) {
        tracing::trace!("set ack: {ack:?}");
        if let Some(hm) = self.hold_messages.get(&ack.ack_to) {
            let mut wg = hm.wait_ack.status.write().unwrap();
            wg.insert(ack.from, ack.kind);
        }
    }

    pub fn poll_message(&self, context: &MessagePollContext<'_>) -> Option<Poll<()>> {
        if self.resolved.read().unwrap().contains(&context.message_id) {
            Some(Poll::Ready(()))
        } else {
            let resolved = self.poll_message_inner(context)?;
            if resolved.is_ready() {
                self.resolved.write().unwrap().insert(context.message_id);
            }
            Some(resolved)
        }
    }
    pub fn poll_message_inner(&self, context: &MessagePollContext<'_>) -> Option<Poll<()>> {
        let message = self.hold_messages.get(&context.message_id)?;
        if self.blocking {
            let front = self.get_front()?;
            // if blocking, check if the message is the first one
            if front.message.id() != context.message_id {
                return Some(Poll::Pending);
            }
        }
        message.send_unsent(context);
        if message.is_resolved() {
            Some(Poll::Ready(()))
        } else {
            Some(Poll::Pending)
        }
    }
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }
    pub fn len(&self) -> usize {
        self.size
    }
    pub fn swap_out_resolved(&self) -> HashSet<MessageId> {
        let mut resolved = self.resolved.write().unwrap();
        let mut swap_out = HashSet::new();
        std::mem::swap(&mut swap_out, &mut resolved);
        swap_out
    }
    pub fn blocking_pop(&mut self, topic: &Topic) -> Option<HoldMessage> {
        let next = self.get_front()?;
        let poll = self.poll_message(&MessagePollContext {
            message_id: next.message.id(),
            topic,
        })?;
        if poll.is_ready() {
            self.pop()
        } else {
            None
        }
    }
    pub fn flush(&mut self, topic: &Topic) {
        tracing::trace!(blocking = self.blocking, "flushing");
        if self.blocking {
            while let Some(m) = self.blocking_pop(topic) {
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
            rg.poll_message(&MessagePollContext {
                message_id: ack.ack_to,
                topic: self,
            })
        };
        if let Some(Poll::Ready(())) = poll_result {
            self.queue.write().unwrap().flush(self);
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
