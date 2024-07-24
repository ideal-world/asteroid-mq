//! Pub/Sub system
//!
//! A event will be sended to certain target endpoints under a topic.

use std::future::Future;

use bytes::Bytes;

use crate::{
    codec::EncoderKind,
    error::EventHandleError,
    event::{Event, EventHandler},
    Endpoint, EndpointAddr,
};

pub struct Publish<E> {
    pub event: E,
}
impl Endpoint {
    fn subscribe<E: Event, A, H: EventHandler<A, E>>(&self, handler: H) {
        todo!("subscribe")
    }
    fn publish<E: Event>(&self, event: E) {
        todo!("publish")
    }
}
pub struct Subscribe<E> {
    pub topic: &'static [u8],
    pub handler: Box<dyn Fn(E) + Send>,
}
