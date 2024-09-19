#[cfg(feature = "json")]
pub mod json;
use std::{collections::HashMap, future::Future, marker::PhantomData, pin::Pin};

use bytes::Bytes;

use crate::{
    prelude::{Subject, Topic},
    protocol::endpoint::{LocalEndpoint, Message, MessageAckExpectKind, MessageHeader},
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
}
pub trait Event: EventAttribute + EventCodec + Send {}

impl<E> Event for E where E: EventAttribute + EventCodec + Send {}
pub trait Handler<A>: Clone + Sync + Send + 'static {
    type Error: std::error::Error + Send;
    type Event: Event;
    fn handle(self, event: Self::Event) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// function Adapter for event handlers
#[derive(Debug, Clone)]
pub struct PlainFnAdapter<M, E>(PhantomData<*const fn(M) -> E>);
impl<M, F, Fut, E> Handler<PlainFnAdapter<M, E>> for F
where
    E: std::error::Error + Send,
    M: Event,
    F: Fn(M) -> Fut + Clone + Send + Sync + 'static,
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
                        ep.ack_received(&message.header).await?;
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
            let message = self.ep.next_message().await;
            let subjects = message.header.subjects.clone();
            for subject in subjects.iter() {
                let Some(handler) = self.handlers.get(subject) else {
                    continue;
                };
                let fut = (handler)(message.clone());
                tokio::spawn(fut);
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
    pub async fn send_event<E: Event>(&self, event: E) -> Result<(), crate::Error> {
        let bytes = event.to_bytes();
        let mut header_builder = MessageHeader::builder([E::SUBJECT]);
        if E::BROADCAST {
            header_builder = header_builder.mode_online();
        } else {
            header_builder = header_builder.mode_push();
        }
        header_builder = header_builder.ack_kind(E::EXPECT_ACK_KIND);
        let message = Message::new(header_builder.build(), bytes);
        let handle = self.send_message(message).await?;
        let _ = handle
            .await
            .map_err(crate::Error::contextual("waiting event ack"))?;
        Ok(())
    }
}
