use std::future::Future;

use bytes::Bytes;

use crate::{codec::EncoderKind, error::EventHandleError, EndpointAddr};

pub trait Event {
    const CODE: &'static [u8];
    fn target(&self) -> &[EndpointAddr];
    fn source(&self) -> &EndpointAddr;
    fn encoder(&self) -> EncoderKind;
    fn to_bytes(&self) -> Bytes;
    fn from_bytes(bytes: &Bytes) -> Self;
}
#[derive(Debug, Clone)]

pub struct TraceId {
    pub bytes: [u8; 16],
}

pub enum EndpointPolicy {
    /// For a endpoint addr, event will reach all endpoint instance claiming it.
    All,
    /// For a endpoint addr, event will reach only one endpoint instance claiming it.
    Single,
}
#[derive(Debug, Clone)]
pub struct EventPayload {
    pub source: EndpointAddr,
    pub ep_policy: bool,
    pub target: Vec<EndpointAddr>,
    pub encoder: EncoderKind,
    pub trace_id: TraceId,
    pub data: Bytes,
}
pub trait EventHandler<A, E>: Clone
where
    E: Event,
{
    fn handle(
        self,
        event: EventPayload,
    ) -> impl Future<Output = Result<(), EventHandleError>> + Send;
}
