use std::{collections::HashMap, future::Future, marker::PhantomData, pin::Pin};

use bytes::Bytes;

use crate::{
    prelude::{Interest, Subject},
    protocol::{endpoint::LocalEndpoint, interest::InterestMap},
};

pub struct HandlerEp {
    ep: LocalEndpoint,
}

pub type MessageHandler = dyn Fn(Bytes) -> Pin<Box<dyn Future<Output = Result<(), ()>> + Send>>;

pub struct HandlerLoop {
    ep: HandlerEp,
    handlers: HashMap<Subject, Box<MessageHandler>>,
}

pub trait HandlerMessage: Sized {
    type Error: std::error::Error;
    const SUBJECT: Subject;
    fn from_bytes(bytes: Bytes) -> Result<Self, Self::Error>;
    fn to_bytes(&self) -> Bytes;
}

pub trait Handler<A>: Clone + Sync + Send + 'static {
    type Error: std::error::Error;
    type Message: HandlerMessage<Error = Self::Error>;
    fn handle(self, message: Self::Message)
        -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// function Adapter for event handlers
#[derive(Debug, Clone)]
pub struct FnHandler<M, E>(PhantomData<*const fn(M) -> E>);
impl<M, F, Fut, E> Handler<FnHandler<M, E>> for F
where
    E: std::error::Error,
    M: HandlerMessage<Error = E>,
    F: Fn(M) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>> + Send,
{
    type Message = M;
    type Error = E;
    fn handle(
        self,
        message: Self::Message,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        (self)(message)
    }
}

impl HandlerLoop {
    pub fn new(ep: HandlerEp) -> Self {
        Self {
            ep,
            handlers: Default::default(),
        }
    }

    pub fn with_handler<H, A>(&mut self, handler: H)
    where
        H: Handler<A>,
    {
        let subject = H::Message::SUBJECT;
        let inner_handler = Box::new(move |bytes: Bytes| {
            let handler = handler.clone();
            Box::pin(async move {
                let message = match H::Message::from_bytes(bytes) {
                    Ok(msg) => msg,
                    Err(e) => {
                        tracing::warn!("failed to parse message: {:?}", e);
                        return <Result<(), ()>>::Err(());
                    }
                };
                if let Err(e) = handler.handle(message).await {
                    tracing::warn!("failed to handle message: {:?}", e);
                    return <Result<(), ()>>::Err(());
                }
                <Result<(), ()>>::Ok(())
            }) as Pin<Box<dyn Future<Output = Result<(), ()>> + Send>>
        });
        self.handlers.insert(subject, inner_handler);
    }
}
