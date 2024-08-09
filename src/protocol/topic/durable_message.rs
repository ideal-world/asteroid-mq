use std::{borrow::Cow, collections::HashMap, future::Future, ops::RangeToInclusive, sync::Arc};

use chrono::{DateTime, Utc};

use crate::{
    impl_codec,
    protocol::endpoint::{EndpointAddr, Message, MessageStatusKind, MessageId},
};

pub struct DurableMessage {
    pub message: Message,
    pub status: HashMap<EndpointAddr, MessageStatusKind>,
}

pub struct ArchiveMessage {
    pub message: Message,
    pub status: HashMap<EndpointAddr, MessageStatusKind>,
}

#[derive(Debug, Clone)]
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
    pub async fn update_status(
        &self,
        message_id: MessageId,
        endpoint: EndpointAddr,
        status: MessageStatusKind,
    ) -> Result<(), DurabilityError> {
        self.inner.update_status(message_id, endpoint, status).await
    }
    #[inline(always)]
    pub async fn remove(&self, message_id: MessageId) -> Result<(), DurabilityError> {
        self.inner.remove(message_id).await
    }
    #[inline(always)]
    pub async fn retrieve(&self, message_id: MessageId) -> Result<DurableMessage, DurabilityError> {
        self.inner.retrieve(message_id).await
    }
    #[inline(always)]
    pub async fn batch_retrieve(
        &self,
        time_range: RangeToInclusive<DateTime<Utc>>,
    ) -> Result<Vec<DurableMessage>, DurabilityError> {
        self.inner.batch_retrieve(time_range).await
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
    fn remove(
        &self,
        message_id: MessageId,
    ) -> impl Future<Output = Result<(), DurabilityError>> + Send;
    fn retrieve(
        &self,
        message_id: MessageId,
    ) -> impl Future<Output = Result<DurableMessage, DurabilityError>> + Send;
    fn batch_retrieve(
        &self,
        time_range: RangeToInclusive<DateTime<Utc>>,
    ) -> impl Future<Output = Result<Vec<DurableMessage>, DurabilityError>> + Send;
    fn archive(
        &self,
        message: DurableMessage,
    ) -> impl Future<Output = Result<(), DurabilityError>> + Send;
}

mod sealed {
    use std::{future::Future, ops::RangeToInclusive, pin::Pin};

    use chrono::{DateTime, Utc};

    use crate::protocol::endpoint::{EndpointAddr, MessageStatusKind, MessageId};

    use super::{Durability, DurabilityError, DurableMessage};

    pub(super) trait DurabilityObjectTrait: Send + Sync + 'static {
        fn save(
            &self,
            message: DurableMessage,
        ) -> Pin<Box<dyn Future<Output = Result<(), DurabilityError>> + Send + '_>>;
        fn update_status(
            &self,
            message_id: MessageId,
            endpoint: EndpointAddr,
            status: MessageStatusKind,
        ) -> Pin<Box<dyn Future<Output = Result<(), DurabilityError>> + Send + '_>>;
        fn remove(
            &self,
            message_id: MessageId,
        ) -> Pin<Box<dyn Future<Output = Result<(), DurabilityError>> + Send + '_>>;
        fn retrieve(
            &self,
            message_id: MessageId,
        ) -> Pin<Box<dyn Future<Output = Result<DurableMessage, DurabilityError>> + Send + '_>>;
        fn batch_retrieve(
            &self,
            time_range: RangeToInclusive<DateTime<Utc>>,
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

        fn update_status(
            &self,
            message_id: MessageId,
            endpoint: EndpointAddr,
            status: MessageStatusKind,
        ) -> Pin<Box<dyn Future<Output = Result<(), DurabilityError>> + Send + '_>> {
            Box::pin(self.update_status(message_id, endpoint, status))
        }
        #[inline(always)]

        fn remove(
            &self,
            message_id: MessageId,
        ) -> Pin<Box<dyn Future<Output = Result<(), DurabilityError>> + Send + '_>> {
            Box::pin(self.remove(message_id))
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
            time_range: RangeToInclusive<DateTime<Utc>>,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<DurableMessage>, DurabilityError>> + Send + '_>>
        {
            Box::pin(self.batch_retrieve(time_range))
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
