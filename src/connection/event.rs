use crate::{event::EventPayload, EndpointAddr};

use super::{auth::ConnectionAuth, NodeId};

#[derive(Debug, Clone)]
pub enum ConnectionEvent {
    Hello {
        auth: ConnectionAuth,
    },
    Heartbeat {
        ts: u64,
    },
    EndpointOnline {
        endpoint: EndpointAddr,
    },
    EndpointOffline {
        endpoint: EndpointAddr,
    },
    Event {
        payload: EventPayload,
        node_trace: NodeTrace,
    },
}
#[derive(Debug, Clone)]

pub struct NodeTrace {
    pub source: NodeId,
    pub hops: Vec<NodeId>,
}
