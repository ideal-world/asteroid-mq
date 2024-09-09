use std::sync::Arc;

use crate::{
    impl_codec,
    protocol::{
        codec::CodecType,
        interest::Subject,
        topic::{durable_message::MessageDurabilityConfig, TopicCode},
    },
};
use bytes::{BufMut, Bytes};
use serde::{Deserialize, Serialize};

use super::EndpointAddr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MessageStatusKind {
    Sending = 0xfe,
    Unsent = 0xff,
    Sent = 0x00,
    Received = 0x01,
    Processed = 0x02,
    Failed = 0x80,
    Unreachable = 0x81,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum MessageAckExpectKind {
    #[default]
    Sent = 0x00,
    Received = 0x01,
    Processed = 0x02,
}
impl_codec! {
    enum MessageAckExpectKind {
        Sent = 0x00,
        Received = 0x01,
        Processed = 0x02,
    }
}
impl_codec! {
    enum MessageStatusKind {
        Sending = 0xfe,
        Unsent = 0xff,
        Sent = 0x00,
        Received = 0x01,
        Processed = 0x02,
        Failed = 0x80,
        Unreachable = 0x81,
    }
}

impl MessageStatusKind {
    #[inline(always)]
    pub fn is_unsent(&self) -> bool {
        *self == MessageStatusKind::Unsent
    }
    pub fn is_reached(&self, condition: MessageAckExpectKind) -> bool {
        match condition {
            MessageAckExpectKind::Sent => {
                *self == MessageStatusKind::Sent
                    || *self == MessageStatusKind::Received
                    || *self == MessageStatusKind::Processed
            }
            MessageAckExpectKind::Received => {
                *self == MessageStatusKind::Received || *self == MessageStatusKind::Processed
            }
            MessageAckExpectKind::Processed => *self == MessageStatusKind::Processed,
        }
    }
    pub fn is_failed(&self) -> bool {
        *self == MessageStatusKind::Failed || *self == MessageStatusKind::Unreachable
    }
    pub fn is_resolved(&self, condition: MessageAckExpectKind) -> bool {
        self.is_failed() || self.is_reached(condition)
    }
}
#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(transparent)]
pub struct MessageId {
    pub bytes: [u8; 16],
}

impl CodecType for MessageId {
    fn decode(bytes: Bytes) -> Result<(Self, Bytes), crate::protocol::codec::DecodeError> {
        let mut buf = [0; 16];
        buf.copy_from_slice(&bytes[0..16]);
        Ok((Self { bytes: buf }, bytes.slice(16..)))
    }

    fn encode(&self, buf: &mut bytes::BytesMut) {
        buf.put_slice(&self.bytes);
    }
}

impl std::fmt::Debug for MessageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("MessageId")
            .field(&crate::util::dashed(&[
                crate::util::hex(&self.bytes[0..4]),
                crate::util::hex(&self.bytes[4..12]),
                crate::util::hex(&self.bytes[12..16]),
            ]))
            .finish()
    }
}

impl MessageId {
    pub fn new_snowflake() -> Self {
        thread_local! {
            static COUNTER: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
        }
        let timestamp = crate::util::timestamp_sec();
        let counter = COUNTER.with(|c| {
            let v = c.get();
            c.set(v.wrapping_add(1));
            v
        });
        let eid = crate::util::executor_digest() as u32;
        let mut bytes = [0; 16];
        bytes[0..4].copy_from_slice(&eid.to_be_bytes());
        bytes[4..12].copy_from_slice(&timestamp.to_be_bytes());
        bytes[12..16].copy_from_slice(&counter.to_be_bytes());
        Self { bytes }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub header: MessageHeader,
    pub payload: Bytes,
}

impl_codec!(
    struct Message {
        header: MessageHeader,
        payload: Bytes,
    }
);

impl Message {
    pub fn id(&self) -> MessageId {
        self.header.message_id
    }
    pub fn ack_kind(&self) -> MessageAckExpectKind {
        self.header.ack_kind
    }
    pub fn subjects(&self) -> &[Subject] {
        &self.header.subjects
    }
}

impl Message {
    pub fn new(header: MessageHeader, payload: impl Into<Bytes>) -> Self {
        Self {
            header,
            payload: payload.into(),
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageHeader {
    pub message_id: MessageId,
    pub ack_kind: MessageAckExpectKind,
    pub target_kind: MessageTargetKind,
    pub durability: Option<MessageDurabilityConfig>,
    pub subjects: Arc<[Subject]>,
}
pub struct MessageHeaderBuilder {
    pub ack_kind: MessageAckExpectKind,
    target_kind: MessageTargetKind,
    durability: Option<MessageDurabilityConfig>,
    pub subjects: Vec<Subject>,
}

impl MessageHeader {
    pub fn builder(subjects: impl IntoIterator<Item = Subject>) -> MessageHeaderBuilder {
        MessageHeaderBuilder::new(subjects)
    }
}

impl MessageHeaderBuilder {
    #[inline(always)]
    pub fn new(subjects: impl IntoIterator<Item = Subject>) -> Self {
        Self {
            ack_kind: MessageAckExpectKind::default(),
            target_kind: MessageTargetKind::default(),
            durability: None,
            subjects: subjects.into_iter().collect(),
        }
    }
    #[inline(always)]
    pub fn ack_kind(mut self, ack_kind: MessageAckExpectKind) -> Self {
        self.ack_kind = ack_kind;
        self
    }
    pub fn mode_online(mut self) -> Self {
        self.target_kind = MessageTargetKind::Online;
        self
    }
    pub fn mode_durable(mut self, config: MessageDurabilityConfig) -> Self {
        self.target_kind = MessageTargetKind::Durable;
        self.durability = Some(config);
        self
    }
    pub fn mode_push(mut self) -> Self {
        self.target_kind = MessageTargetKind::Push;
        self
    }
    pub fn build(self) -> MessageHeader {
        MessageHeader {
            message_id: MessageId::new_snowflake(),
            ack_kind: self.ack_kind,
            target_kind: self.target_kind,
            durability: self.durability,
            subjects: self.subjects.into(),
        }
    }
}

impl_codec! {
    struct MessageHeader {
        message_id: MessageId,
        ack_kind: MessageAckExpectKind,
        target_kind: MessageTargetKind,
        durability:  Option<MessageDurabilityConfig>,
        subjects: Arc<[Subject]>,
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]

pub struct MessageAck {
    pub ack_to: MessageId,
    pub topic_code: TopicCode,
    pub from: EndpointAddr,
    pub kind: MessageStatusKind,
}

impl_codec! {
    struct MessageAck {
        ack_to: MessageId,
        topic_code: TopicCode,
        from: EndpointAddr,
        kind: MessageStatusKind,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[repr(u8)]
pub enum MessageTargetKind {
    Durable = 0,
    Online = 1,
    Available = 2,
    #[default]
    Push = 3,
}

impl_codec!(
    enum MessageTargetKind {
        Durable = 0,
        Online = 1,
        Available = 2,
        Push = 3,
    }
);

impl MessageTargetKind {}
