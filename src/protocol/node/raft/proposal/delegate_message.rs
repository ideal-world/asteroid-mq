#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DelegateMessage {
    pub topic: TopicCode,
    pub message: Message,
}