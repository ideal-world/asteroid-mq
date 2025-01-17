use asteroid_mq_model::EdgeAuth;
use bytes::Bytes;



#[derive(Debug, Clone, Default)]
pub struct Auth {
    pub payload: Bytes,
}

impl Auth {
    pub fn new(payload: impl Into<Bytes>) -> Self {
        Self {
            payload: payload.into(),
        }
    }
}


impl From<EdgeAuth> for Auth {
    fn from(value: EdgeAuth) -> Self {
        Auth { payload: value.payload.0 }
    }
}
