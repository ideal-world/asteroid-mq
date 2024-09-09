use std::{borrow::Cow, collections::HashMap, future::Future, sync::Arc};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    impl_codec,
    protocol::endpoint::{EndpointAddr, Message, MessageId, MessageStatusKind},
};

use super::{config::TopicConfig, TopicCode};
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DurableMessage {
    pub message: Message,
    pub status: HashMap<EndpointAddr, MessageStatusKind>,
    pub time: DateTime<Utc>,
}

impl_codec!(
    struct DurableMessage {
        message: Message,
        status: HashMap<EndpointAddr, MessageStatusKind>,
        time: DateTime<Utc>,
    }
);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadTopic {
    pub config: TopicConfig,
    pub queue: Vec<DurableMessage>,
}

impl LoadTopic {
    pub fn from_config<C: Into<TopicConfig>>(config: C) -> Self {
        Self {
            config: config.into(),
            queue: Vec::new(),
        }
    }
}

impl_codec!(
    struct LoadTopic {
        config: TopicConfig,
        queue: Vec<DurableMessage>,
    }
);
#[derive(Debug, Clone, Serialize, Deserialize)]

pub struct UnloadTopic {
    pub code: TopicCode,
}

impl UnloadTopic {
    pub fn new(code: TopicCode) -> Self {
        Self { code }
    }
}

impl_codec!(
    struct UnloadTopic {
        code: TopicCode,
    }
);

#[derive(Debug, Clone)]
pub struct DurableMessageQuery {
    limit: u32,
    offset: u32,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageDurabilityConfig {
    // we should have a expire time
    pub expire: DateTime<Utc>,
    // once it reached the max_receiver, it will be removed
    pub max_receiver: Option<u32>,
}

impl_codec!(
    struct MessageDurabilityConfig {
        expire: DateTime<Utc>,
        max_receiver: Option<u32>,
    }
);
#[derive(Debug)]
pub struct DurabilityError {
    pub context: Cow<'static, str>,
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

#[derive(Clone)]
pub struct DurabilityService {
    provider: Cow<'static, str>,
    inner: Arc<dyn sealed::DurabilityObjectTrait>,
}

impl std::fmt::Debug for DurabilityService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DurabilityObject")
            .field("provider", &self.provider)
            .finish()
    }
}
impl DurabilityService {
    pub fn new<T>(inner: T) -> Self
    where
        T: Durability + 'static,
    {
        Self {
            provider: std::any::type_name::<T>().into(),
            inner: Arc::new(inner),
        }
    }
    #[inline(always)]
    pub async fn save(&self, message: DurableMessage) -> Result<(), DurabilityError> {
        self.inner.save(message).await
    }

    #[inline(always)]
    pub async fn retrieve(&self, message_id: MessageId) -> Result<DurableMessage, DurabilityError> {
        self.inner.retrieve(message_id).await
    }
    #[inline(always)]
    pub async fn batch_retrieve(
        &self,
        query: DurableMessageQuery,
    ) -> Result<Vec<DurableMessage>, DurabilityError> {
        self.inner.batch_retrieve(query).await
    }
    #[inline(always)]
    pub async fn archive(&self, message: DurableMessage) -> Result<(), DurabilityError> {
        self.inner.archive(message).await
    }
}

pub trait Durability: Send + Sync + 'static {
    fn save(
        &self,
        message: DurableMessage,
    ) -> impl Future<Output = Result<(), DurabilityError>> + Send;
    fn update_status(
        &self,
        message_id: MessageId,
        endpoint: EndpointAddr,
        status: MessageStatusKind,
    ) -> impl Future<Output = Result<(), DurabilityError>> + Send;

    fn retrieve(
        &self,
        message_id: MessageId,
    ) -> impl Future<Output = Result<DurableMessage, DurabilityError>> + Send;
    fn batch_retrieve(
        &self,
        query: DurableMessageQuery,
    ) -> impl Future<Output = Result<Vec<DurableMessage>, DurabilityError>> + Send;
    fn archive(
        &self,
        message: DurableMessage,
    ) -> impl Future<Output = Result<(), DurabilityError>> + Send;
}

mod sealed {
    use std::{future::Future, pin::Pin};

    use crate::protocol::endpoint::MessageId;

    use super::{Durability, DurabilityError, DurableMessage, DurableMessageQuery};

    pub(super) trait DurabilityObjectTrait: Send + Sync + 'static {
        fn save(
            &self,
            message: DurableMessage,
        ) -> Pin<Box<dyn Future<Output = Result<(), DurabilityError>> + Send + '_>>;
        fn retrieve(
            &self,
            message_id: MessageId,
        ) -> Pin<Box<dyn Future<Output = Result<DurableMessage, DurabilityError>> + Send + '_>>;
        fn batch_retrieve(
            &self,
            query: DurableMessageQuery,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<DurableMessage>, DurabilityError>> + Send + '_>>;
        fn archive(
            &self,
            message: DurableMessage,
        ) -> Pin<Box<dyn Future<Output = Result<(), DurabilityError>> + Send + '_>>;
    }

    impl<T> DurabilityObjectTrait for T
    where
        T: Durability,
    {
        #[inline(always)]

        fn save(
            &self,
            message: DurableMessage,
        ) -> Pin<Box<dyn Future<Output = Result<(), DurabilityError>> + Send + '_>> {
            Box::pin(self.save(message))
        }

        #[inline(always)]

        fn retrieve(
            &self,
            message_id: MessageId,
        ) -> Pin<Box<dyn Future<Output = Result<DurableMessage, DurabilityError>> + Send + '_>>
        {
            Box::pin(self.retrieve(message_id))
        }
        #[inline(always)]

        fn batch_retrieve(
            &self,
            query: DurableMessageQuery,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<DurableMessage>, DurabilityError>> + Send + '_>>
        {
            Box::pin(self.batch_retrieve(query))
        }
        #[inline(always)]

        fn archive(
            &self,
            message: DurableMessage,
        ) -> Pin<Box<dyn Future<Output = Result<(), DurabilityError>> + Send + '_>> {
            Box::pin(self.archive(message))
        }
    }
}
