use std::collections::HashMap;

use crate::{
    prelude::{NodeId, TopicConfig},
    protocol::{endpoint::EndpointAddr, interest::InterestMap, topic::hold_message::MessageQueue},
    TimestampSec,
};

#[derive(Debug, Clone)]
pub struct TopicData {
    pub(crate) config: TopicConfig,
    pub(crate) ep_routing_table: HashMap<EndpointAddr, NodeId>,
    pub(crate) ep_interest_map: InterestMap<EndpointAddr>,
    pub(crate) ep_latest_active: HashMap<EndpointAddr, TimestampSec>,
    pub(crate) queue: MessageQueue,
}

impl TopicData {
    
}