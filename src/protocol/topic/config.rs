use crate::protocol::endpoint::Message;

pub struct TopicConfig {
    blocking: bool,
    size: Option<u32>,
    durability: DurabilityConfig,
}

pub struct DurabilityConfig {

}

pub trait Durability {
    async fn save(&self, message: Message) -> Result<(), ()>;
}