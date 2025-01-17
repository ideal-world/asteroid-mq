use std::borrow::Cow;
pub mod connection;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use typeshare::typeshare;

use crate::{
    durable::MessageDurableConfig,
    endpoint::EndpointAddr,
    interest::{Interest, Subject},
    message::{Message, MessageAckExpectKind, MessageHeader, MessageId, MessageTargetKind},
    proposal::{EndpointInterest, SetState},
    topic::{TopicCode, WaitAckError, WaitAckSuccess},
    util::MaybeBase64Bytes,
    NodeId,
};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "bincode", derive(bincode::Decode, bincode::Encode))]
#[repr(u8)]
#[typeshare]
#[serde(tag = "kind", content = "content")]
pub enum EdgeRequestEnum {
    SendMessage(EdgeMessage),
    EndpointOnline(EdgeEndpointOnline),
    EndpointOffline(EdgeEndpointOffline),
    EndpointInterest(EndpointInterest),
    SetState(SetState),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[typeshare]
#[cfg_attr(feature = "bincode", derive(bincode::Decode, bincode::Encode))]

pub struct EdgeEndpointOnline {
    pub topic_code: TopicCode,
    pub interests: Vec<Interest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[typeshare]
#[cfg_attr(feature = "bincode", derive(bincode::Decode, bincode::Encode))]
pub struct EdgeEndpointOffline {
    pub topic_code: TopicCode,
    pub endpoint: EndpointAddr,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "bincode", derive(bincode::Decode, bincode::Encode))]
#[typeshare]
#[serde(tag = "kind", content = "content")]
pub enum EdgePayload {
    Push(EdgePush),
    Response(EdgeResponse),
    Request(EdgeRequest),
    Error(EdgeError),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "bincode", derive(bincode::Decode, bincode::Encode))]
#[typeshare]
#[serde(tag = "kind", content = "content")]
pub enum EdgePush {
    Message {
        endpoints: Vec<EndpointAddr>,
        message: Message,
    },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "bincode", derive(bincode::Decode, bincode::Encode))]
#[typeshare]
pub struct EdgeRequest {
    pub seq_id: u32,
    pub request: EdgeRequestEnum,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "bincode", derive(bincode::Decode, bincode::Encode))]
#[typeshare]
pub struct EdgeResponse {
    pub seq_id: u32,
    pub result: EdgeResult<EdgeResponseEnum, EdgeError>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "bincode", derive(bincode::Decode, bincode::Encode))]
#[typeshare]
#[serde(tag = "kind", content = "content")]
pub enum EdgeResponseEnum {
    SendMessage(EdgeResult<WaitAckSuccess, WaitAckError>),
    EndpointOnline(EndpointAddr),
    EndpointOffline,
    EndpointInterest,
    SetState,
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "bincode", derive(bincode::Decode, bincode::Encode))]
#[typeshare]
#[serde(tag = "kind", content = "content")]
pub enum EdgeResult<T, E> {
    Ok(T),
    Err(E),
}

impl<T, E> EdgeResult<T, E> {
    pub fn from_std(result: Result<T, E>) -> Self {
        match result {
            Ok(t) => Self::Ok(t),
            Err(e) => Self::Err(e),
        }
    }
    pub fn into_std(self) -> Result<T, E> {
        match self {
            Self::Ok(t) => Ok(t),
            Self::Err(e) => Err(e),
        }
    }
}

impl EdgeResponse {
    pub fn from_result(id: u32, result: Result<EdgeResponseEnum, EdgeError>) -> Self {
        Self {
            seq_id: id,
            result: EdgeResult::from_std(result),
        }
    }
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[typeshare]
pub struct EdgeError {
    pub context: Cow<'static, str>,
    pub message: Option<Cow<'static, str>>,
    pub kind: EdgeErrorKind,
}

impl EdgeError {
    pub fn new(context: impl Into<Cow<'static, str>>, kind: EdgeErrorKind) -> Self {
        Self {
            context: context.into(),
            message: None,
            kind,
        }
    }
    pub fn with_message(
        context: impl Into<Cow<'static, str>>,
        message: impl Into<Cow<'static, str>>,
        kind: EdgeErrorKind,
    ) -> Self {
        Self {
            context: context.into(),
            message: Some(message.into()),
            kind,
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "bincode", derive(bincode::Decode, bincode::Encode))]
#[typeshare]
pub enum EdgeErrorKind {
    Decode = 0x00,
    TopicNotFound = 0x02,
    EndpointNotFound = 0x03,
    Unauthorized = 0x04,
    Internal = 0xf0,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "bincode", derive(bincode::Decode, bincode::Encode))]
#[typeshare]
pub struct EdgeMessageHeader {
    pub ack_kind: MessageAckExpectKind,
    pub target_kind: MessageTargetKind,
    pub durability: Option<MessageDurableConfig>,
    pub subjects: Vec<Subject>,
    pub topic: TopicCode,
}

impl EdgeMessageHeader {
    pub fn into_message_header(self) -> (MessageHeader, TopicCode) {
        (
            MessageHeader {
                message_id: MessageId::new_snowflake(),
                ack_kind: self.ack_kind,
                target_kind: self.target_kind,
                durability: self.durability,
                subjects: self.subjects.into(),
            },
            self.topic,
        )
    }
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "bincode", derive(bincode::Decode, bincode::Encode))]
#[typeshare]
pub struct EdgeMessage {
    pub header: EdgeMessageHeader,
    pub payload: MaybeBase64Bytes,
}

pub struct EdgeMessageBuilder {
    ack_kind: MessageAckExpectKind,
    target_kind: MessageTargetKind,
    durability: Option<MessageDurableConfig>,
    subjects: Vec<Subject>,
    topic: TopicCode,
    payload: Bytes,
}

impl EdgeMessage {
    pub fn builder<T, S, P>(topic_code: T, subjects: S, payload: P) -> EdgeMessageBuilder
    where
        T: Into<TopicCode>,
        S: IntoIterator<Item = Subject>,
        P: Into<Bytes>,
    {
        EdgeMessageBuilder {
            ack_kind: MessageAckExpectKind::Sent,
            target_kind: MessageTargetKind::Push,
            durability: None,
            subjects: subjects.into_iter().collect(),
            topic: topic_code.into(),
            payload: payload.into(),
        }
    }
    pub fn into_message(self) -> (Message, TopicCode) {
        let (header, topic) = self.header.into_message_header();
        (Message::new(header, self.payload.0), topic)
    }
}

impl EdgeMessageBuilder {
    pub fn ack_kind(mut self, ack_kind: MessageAckExpectKind) -> Self {
        self.ack_kind = ack_kind;
        self
    }
    pub fn mode_durable(mut self, durability: MessageDurableConfig) -> Self {
        self.durability = Some(durability);
        self.target_kind = MessageTargetKind::Durable;
        self
    }
    pub fn mode_online(mut self) -> Self {
        self.target_kind = MessageTargetKind::Online;
        self
    }
    pub fn mode_push(mut self) -> Self {
        self.target_kind = MessageTargetKind::Push;
        self
    }
    /// wrap a subject with this message
    pub fn with_subject(mut self, subject: Subject) -> Self {
        self.subjects.push(subject);
        self
    }
    pub fn build(self) -> EdgeMessage {
        EdgeMessage {
            header: EdgeMessageHeader {
                ack_kind: self.ack_kind,
                target_kind: self.target_kind,
                durability: self.durability,
                subjects: self.subjects,
                topic: self.topic,
            },
            payload: MaybeBase64Bytes(self.payload),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeConfig {
    pub peer_id: NodeId,
    pub peer_auth: EdgeAuth,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EdgeAuth {
    pub payload: MaybeBase64Bytes,
}
