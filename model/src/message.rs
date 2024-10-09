use std::sync::Arc;

use crate::{
    durable::MessageDurableConfig, interest::Subject, topic::TopicCode, util::MaybeBase64Bytes,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use typeshare::typeshare;

use super::endpoint::EndpointAddr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[typeshare]
#[repr(u8)]
pub enum MessageStatusKind {
    Sending = 0xfe,
    Unsent = 0xff,
    Sent = 0x00,
    Received = 0x01,
    Processed = 0x02,
    Failed = 0x80,
    Unreachable = 0x81,
}

impl std::fmt::Display for MessageStatusKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MessageStatusKind::Sending => write!(f, "Sending"),
            MessageStatusKind::Unsent => write!(f, "Unsent"),
            MessageStatusKind::Sent => write!(f, "Sent"),
            MessageStatusKind::Received => write!(f, "Received"),
            MessageStatusKind::Processed => write!(f, "Processed"),
            MessageStatusKind::Failed => write!(f, "Failed"),
            MessageStatusKind::Unreachable => write!(f, "Unreachable"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[typeshare]
pub enum MessageAckExpectKind {
    #[default]
    Sent = 0x00,
    Received = 0x01,
    Processed = 0x02,
}

impl MessageAckExpectKind {
    pub fn try_from_u8(v: u8) -> Option<Self> {
        match v {
            0x00 => Some(MessageAckExpectKind::Sent),
            0x01 => Some(MessageAckExpectKind::Received),
            0x02 => Some(MessageAckExpectKind::Processed),
            _ => None,
        }
    }
}

impl std::fmt::Display for MessageAckExpectKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MessageAckExpectKind::Sent => write!(f, "Sent"),
            MessageAckExpectKind::Received => write!(f, "Received"),
            MessageAckExpectKind::Processed => write!(f, "Processed"),
        }
    }
}

impl MessageStatusKind {
    pub fn try_from_u8(v: u8) -> Option<Self> {
        match v {
            0xfe => Some(MessageStatusKind::Sending),
            0xff => Some(MessageStatusKind::Unsent),
            0x00 => Some(MessageStatusKind::Sent),
            0x01 => Some(MessageStatusKind::Received),
            0x02 => Some(MessageStatusKind::Processed),
            0x80 => Some(MessageStatusKind::Failed),
            0x81 => Some(MessageStatusKind::Unreachable),
            _ => None,
        }
    }
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
#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[typeshare(serialized_as = "String")]
#[repr(transparent)]
pub struct MessageId {
    pub bytes: [u8; 16],
}

impl MessageId {
    pub fn to_base64(&self) -> String {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD.encode(self.bytes)
    }
    pub fn from_base64(s: &str) -> Result<Self, base64::DecodeError> {
        use base64::Engine;
        let bytes = base64::engine::general_purpose::STANDARD.decode(s.as_bytes())?;
        if bytes.len() != 16 {
            return Err(base64::DecodeError::InvalidLength(bytes.len()));
        }
        let mut addr = [0; 16];
        addr.copy_from_slice(&bytes);
        Ok(Self { bytes: addr })
    }
}

impl Serialize for MessageId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_base64())
        } else {
            <[u8; 16]>::serialize(&self.bytes, serializer)
        }
    }
}

impl<'de> Deserialize<'de> for MessageId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            use serde::de::Error;
            let s = <&'de str>::deserialize(deserializer)?;
            Self::from_base64(s).map_err(D::Error::custom)
        } else {
            Ok(Self {
                bytes: <[u8; 16]>::deserialize(deserializer)?,
            })
        }
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

impl std::fmt::Display for MessageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}-{}-{}",
            crate::util::hex(&self.bytes[0..4]),
            crate::util::hex(&self.bytes[4..12]),
            crate::util::hex(&self.bytes[12..16])
        )
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
#[typeshare]
pub struct Message {
    pub header: MessageHeader,
    pub payload: MaybeBase64Bytes,
}

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
            payload: MaybeBase64Bytes::new(payload.into()),
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[typeshare]
pub struct MessageHeader {
    pub message_id: MessageId,
    pub ack_kind: MessageAckExpectKind,
    pub target_kind: MessageTargetKind,
    pub durability: Option<MessageDurableConfig>,
    pub subjects: Arc<[Subject]>,
}

impl MessageHeader {
    #[inline(always)]
    pub fn ack(
        &self,
        topic_code: TopicCode,
        from: EndpointAddr,
        kind: MessageStatusKind,
    ) -> MessageAck {
        MessageAck {
            ack_to: self.message_id,
            kind,
            from,
            topic_code,
        }
    }
    #[inline(always)]
    pub fn ack_received(&self, topic_code: TopicCode, from: EndpointAddr) -> MessageAck {
        self.ack(topic_code, from, MessageStatusKind::Received)
    }
    #[inline(always)]
    pub fn ack_processed(&self, topic_code: TopicCode, from: EndpointAddr) -> MessageAck {
        self.ack(topic_code, from, MessageStatusKind::Processed)
    }
    #[inline(always)]
    pub fn ack_failed(&self, topic_code: TopicCode, from: EndpointAddr) -> MessageAck {
        self.ack(topic_code, from, MessageStatusKind::Failed)
    }
}

pub struct MessageHeaderBuilder {
    pub ack_kind: MessageAckExpectKind,
    target_kind: MessageTargetKind,
    durability: Option<MessageDurableConfig>,
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
    pub fn mode_durable(mut self, config: MessageDurableConfig) -> Self {
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

#[derive(Debug, Clone, Serialize, Deserialize)]

pub struct MessageAck {
    pub ack_to: MessageId,
    pub topic_code: TopicCode,
    pub from: EndpointAddr,
    pub kind: MessageStatusKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[repr(u8)]
#[typeshare]
pub enum MessageTargetKind {
    Durable = 0,
    Online = 1,
    Available = 2,
    #[default]
    Push = 3,
}

impl From<u8> for MessageTargetKind {
    fn from(kind: u8) -> MessageTargetKind {
        match kind {
            0 => MessageTargetKind::Durable,
            1 => MessageTargetKind::Online,
            2 => MessageTargetKind::Available,
            _ => MessageTargetKind::Push,
        }
    }
}

impl MessageTargetKind {}
