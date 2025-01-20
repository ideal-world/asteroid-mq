use super::{EventAttribute, EventCodec};
use bytes::Bytes;
use serde::{de::DeserializeOwned, Serialize};

#[derive(Debug)]
pub struct Json<T: Serialize + DeserializeOwned>(pub T);

impl<T: Serialize + DeserializeOwned> EventCodec for Json<T> {
    fn to_bytes(&self) -> Bytes {
        Bytes::from(serde_json::to_vec(&self.0).expect("the type cannot be converted to json"))
    }
    fn from_bytes(bytes: Bytes) -> Option<Self> {
        serde_json::from_slice(&bytes).ok().map(Json)
    }
}

impl<T> EventAttribute for Json<T>
where
    T: Serialize + DeserializeOwned + EventAttribute,
{
    const SUBJECT: crate::Subject = T::SUBJECT;
    const BROADCAST: bool = T::BROADCAST;
    const EXPECT_ACK_KIND: crate::MessageAckExpectKind = T::EXPECT_ACK_KIND;
}
