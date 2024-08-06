use std::{
    collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet},
    sync::{atomic::AtomicUsize, Mutex, RwLock},
    task::Poll,
};

use chrono::{DateTime, Utc};

use crate::{
    protocol::{
        endpoint::{EndpointAddr, Message, MessageAck, MessageAckKind, MessageHeader, MessageId},
        node::event::N2nPacket,
        topic::wait_ack,
    },
    util::Timed,
};

use super::{
    wait_ack::{WaitAck, WaitAckError},
    Topic,
};
#[derive(Debug)]
pub struct HoldMessage {
    pub header: MessageHeader,
    pub wait_ack: Option<WaitAck>,
}

impl HoldMessage {
    pub fn resolve(mut self) {
        if let Some(ref mut wait_ack) = self.wait_ack {
            let status: Vec<(EndpointAddr, MessageAckKind)> = wait_ack
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
                    MessageAckKind::Failed => {
                        fail_list.push(ep);
                    }
                    MessageAckKind::Unreachable => {
                        unreachable_list.push(ep);
                    }
                    _ => {}
                }
            }
            if fail_list.is_empty() && unreachable_list.is_empty() {
                let _ = wait_ack.reporter.send(Ok(()));
            } else {
                let _ = wait_ack.reporter.send(Err(WaitAckError {
                    ep_list: wait_ack.ep_list.iter().cloned().collect(),
                    failed_list: fail_list,
                    unreachable_list,
                    timeout_list: Vec::new(),
                    exception: None,
                }));
            }
        } else {
            // don't expect ack
        }
    }
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
        let message_id = message.header.message_id;
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
        if let Some(hm) = self.hold_messages.get(&ack.ack_to) {
            if let Some(wait_ack) = &hm.wait_ack {
                let mut wg = wait_ack.status.write().unwrap();
                wg.insert(ack.from, ack.kind);
            }
        }
    }
    pub fn poll_message(&self, id: MessageId) -> Option<Poll<()>> {
        if self.resolved.read().unwrap().contains(&id) {
            Some(Poll::Ready(()))
        } else {
            let resolved = self.poll_message_inner(id)?;
            if resolved.is_ready() {
                self.resolved.write().unwrap().insert(id);
            }
            Some(resolved)
        }
    }
    pub fn poll_message_inner(&self, id: MessageId) -> Option<Poll<()>> {
        let message = self.hold_messages.get(&id)?;
        if let Some(wait_ack) = &message.wait_ack {
            let rg = wait_ack.status.read().unwrap();
            if rg.values().all(|ack| ack.is_resolved(wait_ack.expect)) {
                if self.blocking
                    && self
                        .time_id
                        .last()
                        .map(|timed| timed.data != id)
                        .unwrap_or(false)
                {
                    Some(Poll::Pending)
                } else {
                    Some(Poll::Ready(()))
                }
            } else {
                Some(Poll::Pending)
            }
        } else {
            Some(Poll::Ready(()))
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
    pub fn blocking_pop(&mut self) -> Option<HoldMessage> {
        while let Some(next) = self.get_front() {
            if let Some(Poll::Ready(_)) = self.poll_message(next.header.message_id) {
                return self.pop();
            }
        }
        None
    }
    pub fn flush(&mut self) {
        if self.blocking {
            while let Some(m) = self.blocking_pop() {
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
        self.queue.read().unwrap().set_ack(&ack);
        self.queue.write().unwrap().flush();
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
