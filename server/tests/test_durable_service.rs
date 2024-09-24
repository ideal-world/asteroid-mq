mod common;
use std::{collections::HashMap, process::id, str::FromStr};

use asteroid_mq::{
    prelude::{
        Durable, DurableMessage, DurableService, Interest, Message, MessageHeader, MessageId, Node,
        NodeConfig, NodeId, Subject, TopicCode, TopicConfig,
    },
    DEFAULT_TCP_SOCKET_ADDR,
};
use tokio::sync::RwLock;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};
#[derive(Debug, Default)]
pub struct MemoryDurable {
    pub messages: RwLock<HashMap<TopicCode, HashMap<MessageId, DurableMessage>>>,
    pub archived: RwLock<HashMap<TopicCode, HashMap<MessageId, DurableMessage>>>,
    pub topics: RwLock<HashMap<TopicCode, TopicConfig>>,
}

impl Durable for MemoryDurable {
    async fn archive(
        &self,
        topic: TopicCode,
        message_id: MessageId,
    ) -> Result<(), asteroid_mq::prelude::DurableError> {
        let mut messages = self.messages.write().await;
        let message = messages
            .get(&topic)
            .and_then(|m| m.get(&message_id))
            .cloned()
            .ok_or(asteroid_mq::prelude::DurableError::new_local(
                "message not found",
            ))?;
        let mut archived = self.archived.write().await;
        archived
            .entry(topic.clone())
            .or_insert_with(HashMap::new)
            .insert(message_id, message);
        messages.get_mut(&topic).unwrap().remove(&message_id);
        Ok(())
    }
    async fn update_status(
        &self,
        topic: TopicCode,
        update: asteroid_mq::protocol::node::raft::proposal::MessageStateUpdate,
    ) -> Result<(), asteroid_mq::prelude::DurableError> {
        let mut messages = self.messages.write().await;
        let message_id = update.message_id;
        let topic_messages = messages.entry(topic.clone()).or_insert_with(HashMap::new);
        if let Some(message) = topic_messages.get_mut(&message_id) {
            for (ep, status) in update.status {
                message.status.insert(ep, status);
            }
        }
        Ok(())
    }
    async fn save(
        &self,
        topic: TopicCode,
        message: DurableMessage,
    ) -> Result<(), asteroid_mq::prelude::DurableError> {
        self.messages
            .write()
            .await
            .entry(topic)
            .or_insert_with(HashMap::new)
            .insert(message.message.id(), message);
        Ok(())
    }
    async fn create_topic(
        &self,
        topic: TopicConfig,
    ) -> Result<(), asteroid_mq::prelude::DurableError> {
        self.topics.write().await.insert(topic.code.clone(), topic);
        Ok(())
    }
    async fn delete_topic(
        &self,
        topic: TopicCode,
    ) -> Result<(), asteroid_mq::prelude::DurableError> {
        self.topics.write().await.remove(&topic);
        Ok(())
    }
    async fn batch_retrieve(
        &self,
        topic: TopicCode,
        query: asteroid_mq::protocol::topic::durable_message::DurableMessageQuery,
    ) -> Result<Vec<DurableMessage>, asteroid_mq::prelude::DurableError> {
        unimplemented!("batch_retrieve")
    }
    async fn retrieve(
        &self,
        topic: TopicCode,
        message_id: MessageId,
    ) -> Result<DurableMessage, asteroid_mq::prelude::DurableError> {
        let messages = self.messages.read().await;
        let topic_messages =
            messages
                .get(&topic)
                .ok_or(asteroid_mq::prelude::DurableError::new_local(
                    "topic not found",
                ))?;
        topic_messages.get(&message_id).cloned().ok_or(
            asteroid_mq::prelude::DurableError::new_local("message not found"),
        )
    }
}

#[tokio::test]
async fn test_durable_service() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer().with_filter(
                tracing_subscriber::filter::EnvFilter::from_default_env()
                    .add_directive(tracing_subscriber::filter::Directive::from_str("info").unwrap())
                    .add_directive(
                        tracing_subscriber::filter::Directive::from_str("asteroid_mq=trace")
                            .unwrap(),
                    )
                    .add_directive(
                        tracing_subscriber::filter::Directive::from_str("openraft=info").unwrap(),
                    ),
            ),
        )
        .init();
    let durable = MemoryDurable::default();
    let service = DurableService::new(durable);
    let topic_config = TopicConfig {
        code: "test".into(),
        blocking: false,
        overflow_config: Some(asteroid_mq::prelude::TopicOverflowConfig {
            policy: asteroid_mq::prelude::TopicOverflowPolicy::RejectNew,
            size: std::num::NonZeroU32::new(500).unwrap(),
        }),
    };
    let cluster = common::TestClusterProvider::new(map!(
        NodeId::new_indexed(1) => DEFAULT_TCP_SOCKET_ADDR
    ));

    let node = Node::new(NodeConfig {
        id: NodeId::new_indexed(1),
        addr: DEFAULT_TCP_SOCKET_ADDR,
        durable: Some(service.clone()),
        ..Default::default()
    });

    node.init_raft(cluster.clone()).await?;

    if let Some(durable) = &node.config().durable {
        durable.create_topic(topic_config.clone()).await?;
    }
    let topic = node.create_new_topic(topic_config.clone()).await?;

    let endpoint = topic.create_endpoint([Interest::new("event/**")]).await?;
    tokio::spawn(async move {
        while let Some(message) = endpoint.next_message().await {
            tracing::info!(?message);
        }
    });
    let message = Message::new(
        MessageHeader::builder([Subject::new("event/all")]).build(),
        "hello",
    );
    let handle = topic.send_message(message).await?;
    let result = handle.await;
    assert!(result.is_ok());
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let inner = service.downcast_ref::<MemoryDurable>().unwrap();
    let messages = inner.archived.read().await;
    let messages = messages.get(&topic_config.code).unwrap();
    assert_eq!(messages.len(), 1);
    // check if message is archived

    Ok(())
}