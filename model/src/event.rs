pub mod json;
use std::future::Future;
use std::marker::PhantomData;

use crate::TopicCode;
use crate::{
    EdgeMessage, Message, MessageAckExpectKind, MessageDurableConfig, MessageHeader, Subject,
};
use bytes::Bytes;

pub trait EventCodec: Sized {
    fn from_bytes(bytes: Bytes) -> Option<Self>;
    fn to_bytes(&self) -> Bytes;
}

pub trait EventAttribute {
    const SUBJECT: Subject;
    const BROADCAST: bool = false;
    const EXPECT_ACK_KIND: MessageAckExpectKind = MessageAckExpectKind::Sent;
    fn durable_config() -> Option<MessageDurableConfig> {
        None
    }
}
pub trait Event: EventAttribute + EventCodec + Send {
    fn into_message(self) -> Message;
    fn into_edge_message(self, topic: TopicCode) -> EdgeMessage;
}

impl<E> Event for E
where
    E: EventAttribute + EventCodec + Send,
{
    fn into_message(self) -> Message {
        let bytes = self.to_bytes();
        let mut header_builder = MessageHeader::builder([E::SUBJECT]);
        if let Some(durable_config) = E::durable_config() {
            header_builder = header_builder.mode_durable(durable_config);
        } else if E::BROADCAST {
            header_builder = header_builder.mode_online();
        } else {
            header_builder = header_builder.mode_push();
        }
        header_builder = header_builder.ack_kind(E::EXPECT_ACK_KIND);
        Message::new(header_builder.build(), bytes)
    }
    fn into_edge_message(self, topic: TopicCode) -> EdgeMessage {
        let bytes = self.to_bytes();
        let mut builder = EdgeMessage::builder(topic, [E::SUBJECT], bytes);
        if let Some(durable_config) = E::durable_config() {
            builder = builder.mode_durable(durable_config);
        } else if E::BROADCAST {
            builder = builder.mode_online();
        } else {
            builder = builder.mode_push();
        }
        builder = builder.ack_kind(E::EXPECT_ACK_KIND);
        builder.build()
    }
}

pub trait Handler<A>: Clone + Sync + Send + 'static {
    type Error: std::error::Error + Send;
    type Event: Event;
    fn handle(self, event: Self::Event) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Basic function adapter for event handlers, this adapter accept a [`Message`] as argument.
#[derive(Debug, Clone)]
pub struct PlainFnAdapter<M, E>(PhantomData<*const fn(M) -> E>);
impl<M, F, Fut, E> Handler<PlainFnAdapter<M, E>> for F
where
    E: std::error::Error + Send,
    M: Event,
    F: (Fn(M) -> Fut) + Clone + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>> + Send,
{
    type Event = M;
    type Error = E;
    fn handle(self, message: Self::Event) -> impl Future<Output = Result<(), Self::Error>> + Send {
        (self)(message)
    }
}