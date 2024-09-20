use std::{borrow::Cow, collections::HashMap, future::Future, sync::Arc};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typeshare::typeshare;

use crate::protocol::endpoint::EndpointAddr;
use crate::protocol::message::*;

use super::MessageStateUpdate;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DurableMessage {
    pub message: Message,
    pub status: HashMap<EndpointAddr, MessageStatusKind>,
    pub time: DateTime<Utc>,
}

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
#[typeshare]
pub struct MessageDurableConfig {
    // we should have a expire time
    pub expire: DateTime<Utc>,
    // once it reached the max_receiver, it will be removed
    pub max_receiver: Option<u32>,
}

#[derive(Debug)]
pub struct DurableError {
    pub context: Cow<'static, str>,
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

#[derive(Clone)]
pub struct DurableService {
    provider: Cow<'static, str>,
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
            inner: Arc::new(inner),
        }
    }
    #[inline(always)]
    pub async fn save(&self, message: DurableMessage) -> Result<(), DurableError> {
        self.inner.save(message).await
    }
    pub async fn update_status(&self, update: MessageStateUpdate) -> Result<(), DurableError> {
        self.inner.update_status(update).await
    }
    #[inline(always)]
    pub async fn retrieve(&self, message_id: MessageId) -> Result<DurableMessage, DurableError> {
        self.inner.retrieve(message_id).await
    }
    #[inline(always)]
    pub async fn batch_retrieve(
        &self,
        query: DurableMessageQuery,
    ) -> Result<Vec<DurableMessage>, DurableError> {
        self.inner.batch_retrieve(query).await
    }
    #[inline(always)]
    pub async fn archive(&self, message: DurableMessage) -> Result<(), DurableError> {
        self.inner.archive(message).await
    }
}

pub trait Durable: Send + Sync + 'static {
    fn save(
        &self,
        message: DurableMessage,
    ) -> impl Future<Output = Result<(), DurableError>> + Send;
    fn update_status(
        &self,
        update: MessageStateUpdate,
    ) -> impl Future<Output = Result<(), DurableError>> + Send;

    fn retrieve(
        &self,
        message_id: MessageId,
    ) -> impl Future<Output = Result<DurableMessage, DurableError>> + Send;
    fn batch_retrieve(
        &self,
        query: DurableMessageQuery,
    ) -> impl Future<Output = Result<Vec<DurableMessage>, DurableError>> + Send;
    fn archive(
        &self,
        message: DurableMessage,
    ) -> impl Future<Output = Result<(), DurableError>> + Send;
}

mod sealed {
    use std::{future::Future, pin::Pin};

    use crate::{
        prelude::EndpointAddr,
        protocol::{message::*, topic::MessageStateUpdate},
    };

    use super::{Durable, DurableError, DurableMessage, DurableMessageQuery};

    pub(super) trait DurabilityObjectTrait: Send + Sync + 'static {
        fn save(
            &self,
            message: DurableMessage,
        ) -> Pin<Box<dyn Future<Output = Result<(), DurableError>> + Send + '_>>;
        fn retrieve(
            &self,
            message_id: MessageId,
        ) -> Pin<Box<dyn Future<Output = Result<DurableMessage, DurableError>> + Send + '_>>;
        fn update_status(
            &self,
            update: MessageStateUpdate,
        ) -> Pin<Box<dyn Future<Output = Result<(), DurableError>> + Send + '_>>;
        fn batch_retrieve(
            &self,
            query: DurableMessageQuery,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<DurableMessage>, DurableError>> + Send + '_>>;
        fn archive(
            &self,
            message: DurableMessage,
        ) -> Pin<Box<dyn Future<Output = Result<(), DurableError>> + Send + '_>>;
    }

    impl<T> DurabilityObjectTrait for T
    where
        T: Durable,
    {
        #[inline(always)]

        fn save(
            &self,
            message: DurableMessage,
        ) -> Pin<Box<dyn Future<Output = Result<(), DurableError>> + Send + '_>> {
            Box::pin(self.save(message))
        }

        #[inline(always)]

        fn retrieve(
            &self,
            message_id: MessageId,
        ) -> Pin<Box<dyn Future<Output = Result<DurableMessage, DurableError>> + Send + '_>>
        {
            Box::pin(self.retrieve(message_id))
        }
        #[inline(always)]

        fn batch_retrieve(
            &self,
            query: DurableMessageQuery,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<DurableMessage>, DurableError>> + Send + '_>>
        {
            Box::pin(self.batch_retrieve(query))
        }
        #[inline(always)]

        fn archive(
            &self,
            message: DurableMessage,
        ) -> Pin<Box<dyn Future<Output = Result<(), DurableError>> + Send + '_>> {
            Box::pin(self.archive(message))
        }

        #[inline(always)]
        fn update_status(
            &self,
            update: MessageStateUpdate,
        ) -> Pin<Box<dyn Future<Output = Result<(), DurableError>> + Send + '_>> {
            Box::pin(self.update_status(update))
        }
    }
}
