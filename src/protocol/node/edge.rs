use std::{borrow::Cow, collections::HashSet};

use codec::CodecKind;
use packet::Auth;
use serde::{Deserialize, Serialize};
use typeshare::typeshare;
pub mod codec;
pub mod connection;
pub mod packet;
use crate::{
    prelude::{
        Interest, MessageAckExpectKind, MessageDurabilityConfig, MessageId, Subject, TopicCode,
    },
    protocol::endpoint::EndpointAddr,
    protocol::message::*,
    util::MaybeBase64Bytes,
};

use super::{
    raft::{
        proposal::{EndpointInterest, SetState},
        state_machine::topic::wait_ack::{WaitAckError, WaitAckSuccess},
    },
    NodeId,
};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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

pub struct EdgeEndpointOnline {
    pub topic_code: TopicCode,
    pub interests: Vec<Interest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[typeshare]

pub struct EdgeEndpointOffline {
    pub topic_code: TopicCode,
    pub endpoint: EndpointAddr,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[typeshare]
#[serde(tag = "kind", content = "content")]
pub enum EdgePayload {
    Push(EdgePush),
    Response(EdgeResponse),
    Request(EdgeRequest),
    Error(EdgeError),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[typeshare]
#[serde(tag = "kind", content = "content")]
pub enum EdgePush {
    Message {
        endpoints: Vec<EndpointAddr>,
        message: Message,
    },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[typeshare]
pub struct EdgeRequest {
    pub seq_id: u32,
    pub request: EdgeRequestEnum,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[typeshare]
pub struct EdgeResponse {
    pub seq_id: u32,
    pub result: EdgeResult<EdgeResponseEnum, EdgeError>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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
#[typeshare]
pub enum EdgeErrorKind {
    Decode = 0x00,
    TopicNotFound = 0x02,
    EndpointNotFound = 0x03,
    Unauthorized = 0x04,
    Internal = 0xf0,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[typeshare]
pub struct EdgeMessageHeader {
    pub ack_kind: MessageAckExpectKind,
    pub target_kind: MessageTargetKind,
    pub durability: Option<MessageDurabilityConfig>,
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
#[typeshare]
pub struct EdgeMessage {
    pub header: EdgeMessageHeader,
    pub payload: MaybeBase64Bytes,
}

impl EdgeMessage {
    pub fn into_message(self) -> (Message, TopicCode) {
        let (header, topic) = self.header.into_message_header();
        (Message::new(header, self.payload.0), topic)
    }
}

#[derive(Debug, Clone)]
pub struct EdgeConfig {
    pub supported_codec_kinds: HashSet<CodecKind>,
    pub peer_id: NodeId,
    pub peer_auth: Auth,
}
