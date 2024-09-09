use serde::{Deserialize, Serialize};

use crate::protocol::{
    endpoint::{DelegateMessage, EndpointInterest, EndpointOffline, EndpointOnline, MessageAck, SetState},
    topic::durable_message::{LoadTopic, UnloadTopic},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[repr(u8)]
pub enum Proposal {
    /// Hold Message: edge node ask cluster node to hold a message.
    DelegateMessage(DelegateMessage),
    /// Set State: set ack state
    SetState(SetState),
    /// Load Queue: load messages into the topic queue.
    LoadTopic(LoadTopic),
    UnloadTopic(UnloadTopic),
    /// En Online: report endpoint online.
    EpOnline(EndpointOnline),
    /// En Offline: report endpoint offline.
    EpOffline(EndpointOffline),
    /// En Interest: set endpoint's interests.
    EpInterest(EndpointInterest),
}
