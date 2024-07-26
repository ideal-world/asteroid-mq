use std::{collections::HashMap, sync};

use bytes::Bytes;

use crate::EndpointAddr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId {
    pub bytes: [u8; 16],
}
#[derive(Debug, Clone)]
pub struct NodeTrace {
    pub source: NodeId,
    pub hops: Vec<NodeId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeKind {
    Cluster = 0,
    Edge = 1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct N2NEventId {
    pub bytes: [u8; 16],
}

pub enum N2NEvent {
    Message {
        to: NodeId,
        payload: Bytes,
        trace: NodeTrace,
    },
    Unreachable {
        to: NodeId,
        trace: NodeTrace,
    },
    Routing {
        table: Vec<(NodeId, Vec<EndpointAddr>)>,
    },
}

pub struct NodeInfo {
    pub id: NodeId,
    pub kind: NodeKind,
}
pub struct Node {
    info: NodeInfo,
    ep_routing_table: HashMap<EndpointAddr, NodeId>,
    n2n_routing_table: HashMap<NodeId, N2nRoutingInfo>,
}

impl Node {
    pub fn routing_ep_next_jump(&self, addr: EndpointAddr) -> Option<NodeId> {
        let ep = self.ep_routing_table.get(&addr).copied()?;
        self.n2n_routing_table.get(&ep).map(|info| info.next_jump)
    }
}

pub struct N2nRoutingInfo {
    next_jump: NodeId,
    hops: u32,
}

pub struct Connection {
    pub attached_node: sync::Weak<Node>,
    pub peer_info: NodeInfo,
}
