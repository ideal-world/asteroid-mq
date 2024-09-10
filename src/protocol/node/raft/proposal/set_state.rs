#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetState {
    pub topic: TopicCode,
    pub update: MessageStateUpdate,
}

impl_codec!(
    struct SetState {
        topic: TopicCode,
        update: MessageStateUpdate,
    }
);
