/// This is a extension layer upon the mq. The user can create a loop to dispatch event to
/// corresponded handlers.
pub mod json;
use std::{collections::HashMap, future::Future, marker::PhantomData, pin::Pin};

use asteroid_mq_model::MessageDurableConfig;
use bytes::Bytes;
use tracing::Instrument;

use crate::{
    prelude::{Subject, Topic},
    protocol::{
        endpoint::LocalEndpoint, message::*,
        node::raft::state_machine::topic::wait_ack::WaitAckHandle,
    },
};

type InnerEventHandler =
    dyn Fn(Message) -> Pin<Box<dyn Future<Output = crate::Result<()>> + Send>> + Send;

pub struct HandleEventLoop {
    ep: LocalEndpoint,
    handlers: HashMap<Subject, Box<InnerEventHandler>>,
}
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
pub trait Event: EventAttribute + EventCodec + Send {}

impl<E> Event for E where E: EventAttribute + EventCodec + Send {}
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

impl HandleEventLoop {
    pub fn new(ep: LocalEndpoint) -> Self {
        Self {
            ep,
            handlers: Default::default(),
        }
    }
    pub fn with_handler<A>(mut self, handler: impl Handler<A>) -> Self {
        self.register_handler(handler);
        self
    }

    pub fn register_handler<H, A>(&mut self, handler: H)
    where
        H: Handler<A>,
    {
        let subject = H::Event::SUBJECT;
        let ep = self.ep.clone();
        tracing::debug!(?subject, "register event handler");
        let inner_handler = Box::new(move |message: Message| {
            let handler = handler.clone();
            let ep = ep.clone();
            Box::pin(async move {
                if H::Event::EXPECT_ACK_KIND == MessageAckExpectKind::Received
                    || H::Event::EXPECT_ACK_KIND == MessageAckExpectKind::Processed
                {
                    ep.ack_received(&message.header).await?;
                }
                let event = match H::Event::from_bytes(message.payload.into_inner()) {
                    Some(msg) => msg,
                    None => {
                        tracing::debug!("failed to decode event, this message will be ignored");
                        if H::Event::EXPECT_ACK_KIND == MessageAckExpectKind::Processed {
                            ep.ack_failed(&message.header).await?;
                        }
                        return Ok(());
                    }
                };
                let handle_result = handler.handle(event).await;
                if H::Event::EXPECT_ACK_KIND == MessageAckExpectKind::Processed {
                    if let Err(e) = handle_result {
                        tracing::warn!("failed to handle event: {:?}", e);
                        ep.ack_failed(&message.header).await?;
                    } else {
                        ep.ack_processed(&message.header).await?;
                    }
                }

                Ok(())
            }) as Pin<Box<dyn Future<Output = crate::Result<()>> + Send>>
        });
        self.handlers.insert(subject, inner_handler);
    }

    pub async fn run(self) {
        loop {
            let Some(message) = self.ep.next_message().await else {
                break;
            };
            tracing::trace!(?message, "handle message");
            let subjects = message.header.subjects.clone();
            for subject in subjects.iter() {
                let Some(handler) = self.handlers.get(subject) else {
                    tracing::debug!(?subject, "no handler found");
                    continue;
                };
                let fut = (handler)(message.clone());
                let subject = subject.clone();
                tokio::spawn(async move {
                    let result = fut
                        .instrument(tracing::info_span!("event_handler", %subject))
                        .await;
                    if let Err(e) = result {
                        tracing::warn!(error=%e, "handler process error")
                    }
                });
            }
        }
    }

    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(self.run())
    }
}
impl LocalEndpoint {
    pub fn create_event_loop(&self) -> HandleEventLoop {
        HandleEventLoop::new(self.clone())
    }
}

impl Topic {
    pub async fn send_event<E: Event>(&self, event: E) -> Result<WaitAckHandle, crate::Error> {
        let bytes = event.to_bytes();
        let mut header_builder = MessageHeader::builder([E::SUBJECT]);
        if let Some(durable_config) = E::durable_config() {
            header_builder = header_builder.mode_durable(durable_config);
        } else if E::BROADCAST {
            header_builder = header_builder.mode_online();
        } else {
            header_builder = header_builder.mode_push();
        }
        header_builder = header_builder.ack_kind(E::EXPECT_ACK_KIND);
        let message = Message::new(header_builder.build(), bytes);
        let handle = self.send_message(message).await?;
        Ok(handle)
    }
    pub async fn send_event_and_wait<E: Event>(&self, event: E) -> Result<(), crate::Error> {
        let handle = self.send_event(event).await?;
        let _ = handle
            .await
            .map_err(crate::Error::contextual("waiting event ack"))?;
        Ok(())
    }
}
