use std::any::TypeId;
use std::{borrow::Cow, collections::HashMap, future::Future, sync::Arc};

use asteroid_mq_model::{EndpointAddr, MessageStateUpdate, TopicCode};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::protocol::message::*;
use crate::protocol::node::raft::state_machine::topic::config::TopicConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DurableMessage {
    pub message: Message,
    pub status: HashMap<EndpointAddr, MessageStatusKind>,
    pub time: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy)]
pub struct DurableMessageQuery {
    pub limit: u32,
    pub offset: u32,
}

impl DurableMessageQuery {
    pub fn new(limit: u32, offset: u32) -> Self {
        Self { limit, offset }
    }
    pub fn next_page(&self) -> Self {
        Self {
            limit: self.limit,
            offset: self.offset + self.limit,
        }
    }
}

pub use asteroid_mq_model::MessageDurableConfig;

#[derive(Debug)]
pub struct DurableError {
    pub context: Cow<'static, str>,
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl std::fmt::Display for DurableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DurableError: {}", self.context)?;
        if let Some(source) = &self.source {
            write!(f, "with source: {}", source)?;
        }
        Ok(())
    }
}

impl std::error::Error for DurableError {}

impl DurableError {
    pub fn new_local(context: &'static str) -> Self {
        Self {
            context: context.into(),
            source: None,
        }
    }
    pub fn with_source(
        context: &'static str,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            context: context.into(),
            source: Some(Box::new(source)),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum DurableCommand {
    Create(Message),
    UpdateStatus(MessageStateUpdate),
    Archive(MessageId),
}
#[derive(Clone)]
pub struct DurableService {
    provider: Cow<'static, str>,
    provider_type: TypeId,
    inner: Arc<dyn sealed::DurabilityObjectTrait>,
}

impl std::fmt::Debug for DurableService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DurableService")
            .field("provider", &self.provider)
            .finish()
    }
}
impl DurableService {
    pub fn new<T>(inner: T) -> Self
    where
        T: Durable + 'static,
    {
        Self {
            provider: std::any::type_name::<T>().into(),
            provider_type: TypeId::of::<T>(),
            inner: Arc::new(inner),
        }
    }
    pub fn downcast_ref<T: Durable>(&self) -> Option<&T> {
        if self.provider_type == TypeId::of::<T>() {
            unsafe {
                Some(
                    &*(self.inner.as_ref() as *const dyn sealed::DurabilityObjectTrait as *const T),
                )
            }
        } else {
            None
        }
    }
    #[inline(always)]
    pub async fn save(
        &self,
        topic: TopicCode,
        message: DurableMessage,
    ) -> Result<(), DurableError> {
        self.inner.save(topic, message).await
    }
    pub async fn update_status(
        &self,
        topic: TopicCode,
        update: MessageStateUpdate,
    ) -> Result<(), DurableError> {
        self.inner.update_status(topic, update).await
    }
    #[inline(always)]
    pub async fn archive(
        &self,
        topic: TopicCode,
        message_id: MessageId,
    ) -> Result<(), DurableError> {
        self.inner.archive(topic, message_id).await
    }
    #[inline(always)]
    pub async fn retrieve(
        &self,
        topic: TopicCode,
        message_id: MessageId,
    ) -> Result<DurableMessage, DurableError> {
        self.inner.retrieve(topic, message_id).await
    }
    #[inline(always)]
    pub async fn batch_retrieve(
        &self,
        topic: TopicCode,
        query: DurableMessageQuery,
    ) -> Result<Vec<DurableMessage>, DurableError> {
        self.inner.batch_retrieve(topic, query).await
    }
    #[inline(always)]
    pub async fn create_topic(&self, topic: TopicConfig) -> Result<(), DurableError> {
        self.inner.create_topic(topic).await
    }
    #[inline(always)]
    pub async fn delete_topic(&self, topic: TopicCode) -> Result<(), DurableError> {
        self.inner.delete_topic(topic).await
    }
    #[inline(always)]
    pub async fn topic_code_list(&self) -> Result<Vec<TopicCode>, DurableError> {
        self.inner.topic_code_list().await
    }
    pub async fn topic_list(&self) -> Result<Vec<TopicConfig>, DurableError> {
        self.inner.topic_list().await
    }
}

pub trait Durable: Send + Sync + 'static {
    fn save(
        &self,
        topic: TopicCode,
        message: DurableMessage,
    ) -> impl Future<Output = Result<(), DurableError>> + Send;
    fn update_status(
        &self,
        topic: TopicCode,
        update: MessageStateUpdate,
    ) -> impl Future<Output = Result<(), DurableError>> + Send;

    fn retrieve(
        &self,
        topic: TopicCode,
        message_id: MessageId,
    ) -> impl Future<Output = Result<DurableMessage, DurableError>> + Send;
    /// the earlier message should be in the front
    fn batch_retrieve(
        &self,
        topic: TopicCode,
        query: DurableMessageQuery,
    ) -> impl Future<Output = Result<Vec<DurableMessage>, DurableError>> + Send;
    fn archive(
        &self,
        topic: TopicCode,
        message_id: MessageId,
    ) -> impl Future<Output = Result<(), DurableError>> + Send;
    fn create_topic(
        &self,
        topic: TopicConfig,
    ) -> impl Future<Output = Result<(), DurableError>> + Send;
    fn delete_topic(
        &self,
        topic: TopicCode,
    ) -> impl Future<Output = Result<(), DurableError>> + Send;
    fn topic_code_list(&self) -> impl Future<Output = Result<Vec<TopicCode>, DurableError>> + Send;
    fn topic_list(&self) -> impl Future<Output = Result<Vec<TopicConfig>, DurableError>> + Send;
}

mod sealed {
    use std::{future::Future, pin::Pin};

    use asteroid_mq_model::MessageStateUpdate;

    use crate::{
        prelude::TopicCode,
        protocol::{message::*, node::raft::state_machine::topic::config::TopicConfig},
    };

    use super::{Durable, DurableError, DurableMessage, DurableMessageQuery};

    pub(super) trait DurabilityObjectTrait: Send + Sync + 'static {
        fn save(
            &self,
            topic: TopicCode,
            message: DurableMessage,
        ) -> Pin<Box<dyn Future<Output = Result<(), DurableError>> + Send + '_>>;
        fn retrieve(
            &self,
            topic: TopicCode,
            message_id: MessageId,
        ) -> Pin<Box<dyn Future<Output = Result<DurableMessage, DurableError>> + Send + '_>>;
        fn update_status(
            &self,
            topic: TopicCode,
            update: MessageStateUpdate,
        ) -> Pin<Box<dyn Future<Output = Result<(), DurableError>> + Send + '_>>;
        fn batch_retrieve(
            &self,
            topic: TopicCode,
            query: DurableMessageQuery,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<DurableMessage>, DurableError>> + Send + '_>>;
        fn archive(
            &self,
            topic: TopicCode,
            message_id: MessageId,
        ) -> Pin<Box<dyn Future<Output = Result<(), DurableError>> + Send + '_>>;
        fn create_topic(
            &self,
            topic: TopicConfig,
        ) -> Pin<Box<dyn Future<Output = Result<(), DurableError>> + Send + '_>>;
        fn delete_topic(
            &self,
            topic: TopicCode,
        ) -> Pin<Box<dyn Future<Output = Result<(), DurableError>> + Send + '_>>;
        fn topic_code_list(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<TopicCode>, DurableError>> + Send + '_>>;
        fn topic_list(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<TopicConfig>, DurableError>> + Send + '_>>;
    }

    impl<T> DurabilityObjectTrait for T
    where
        T: Durable,
    {
        #[inline(always)]
        fn save(
            &self,
            topic: TopicCode,
            message: DurableMessage,
        ) -> Pin<Box<dyn Future<Output = Result<(), DurableError>> + Send + '_>> {
            Box::pin(self.save(topic, message))
        }

        #[inline(always)]
        fn retrieve(
            &self,
            topic: TopicCode,
            message_id: MessageId,
        ) -> Pin<Box<dyn Future<Output = Result<DurableMessage, DurableError>> + Send + '_>>
        {
            Box::pin(self.retrieve(topic, message_id))
        }
        #[inline(always)]
        fn batch_retrieve(
            &self,
            topic: TopicCode,
            query: DurableMessageQuery,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<DurableMessage>, DurableError>> + Send + '_>>
        {
            Box::pin(self.batch_retrieve(topic, query))
        }
        #[inline(always)]
        fn archive(
            &self,
            topic: TopicCode,
            message_id: MessageId,
        ) -> Pin<Box<dyn Future<Output = Result<(), DurableError>> + Send + '_>> {
            Box::pin(self.archive(topic, message_id))
        }

        #[inline(always)]
        fn update_status(
            &self,
            topic: TopicCode,
            update: MessageStateUpdate,
        ) -> Pin<Box<dyn Future<Output = Result<(), DurableError>> + Send + '_>> {
            Box::pin(self.update_status(topic, update))
        }
        #[inline(always)]
        fn create_topic(
            &self,
            topic: TopicConfig,
        ) -> Pin<Box<dyn Future<Output = Result<(), DurableError>> + Send + '_>> {
            Box::pin(self.create_topic(topic))
        }
        #[inline(always)]
        fn delete_topic(
            &self,
            topic: TopicCode,
        ) -> Pin<Box<dyn Future<Output = Result<(), DurableError>> + Send + '_>> {
            Box::pin(self.delete_topic(topic))
        }
        #[inline(always)]
        fn topic_code_list(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<TopicCode>, DurableError>> + Send + '_>>
        {
            Box::pin(self.topic_code_list())
        }
        #[inline(always)]
        fn topic_list(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<TopicConfig>, DurableError>> + Send + '_>>
        {
            Box::pin(self.topic_list())
        }
    }
}
