use std::{collections::HashMap, future::Future, pin::Pin};

use crate::{ClientEndpoint, ClientNode, ClientNodeError, ClientReceivedMessage, MessageAckHandle};
use asteroid_mq_model::{
    event::{Event, EventAttribute, EventCodec, Handler},
    MessageAckExpectKind, Subject, TopicCode, WaitAckSuccess,
};
use tracing::Instrument;

impl ClientNode {
    pub async fn send_event<E: Event>(
        &self,
        topic: TopicCode,
        event: E,
    ) -> Result<MessageAckHandle, ClientNodeError> {
        let handle = self.send_message(event.into_edge_message(topic)).await?;
        Ok(handle)
    }
    pub async fn send_event_and_wait<E: Event>(
        &self,
        topic: TopicCode,
        event: E,
    ) -> Result<WaitAckSuccess, ClientNodeError> {
        let handle = self.send_event(topic, event).await?;
        let success = handle.wait().await?;
        Ok(success)
    }
}

type InnerEventHandler = dyn Fn(
        ClientReceivedMessage,
    ) -> Pin<Box<dyn Future<Output = Result<(), crate::error::ClientNodeError>> + Send>>
    + Send;

pub struct HandleEventLoop {
    ep: ClientEndpoint,
    handlers: HashMap<Subject, Box<InnerEventHandler>>,
}

impl HandleEventLoop {
    pub fn new(ep: ClientEndpoint) -> Self {
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
        tracing::debug!(?subject, "register event handler");
        let inner_handler = Box::new(move |message: ClientReceivedMessage| {
            let handler = handler.clone();
            Box::pin(async move {
                if H::Event::EXPECT_ACK_KIND == MessageAckExpectKind::Received
                    || H::Event::EXPECT_ACK_KIND == MessageAckExpectKind::Processed
                {
                    message.ack_received().await?;
                }
                let event = match H::Event::from_bytes(message.payload.clone().into_inner()) {
                    Some(msg) => msg,
                    None => {
                        tracing::debug!("failed to decode event, this message will be ignored");
                        if H::Event::EXPECT_ACK_KIND == MessageAckExpectKind::Processed {
                            message.ack_failed().await?;
                        }
                        return Ok(());
                    }
                };
                let handle_result = handler.handle(event).await;
                if H::Event::EXPECT_ACK_KIND == MessageAckExpectKind::Processed {
                    if let Err(e) = handle_result {
                        tracing::warn!("failed to handle event: {:?}", e);
                        message.ack_failed().await?;
                    } else {
                        message.ack_processed().await?;
                    }
                }

                Ok(())
            })
                as Pin<Box<dyn Future<Output = Result<(), crate::error::ClientNodeError>> + Send>>
        });
        self.handlers.insert(subject, inner_handler);
    }

    pub async fn run(mut self) {
        loop {
            let Ok(message) = self.ep.next_message_and_auto_respawn().await else {
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

impl ClientEndpoint {
    pub fn into_event_loop(self) -> HandleEventLoop {
        HandleEventLoop::new(self)
    }
}
