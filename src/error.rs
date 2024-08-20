use std::{
    borrow::Cow,
    fmt::{Display, Formatter},
};

use crate::protocol::topic::durable_message::DurabilityError;

#[derive(Debug)]
pub struct Error {
    pub context: Cow<'static, str>,
    pub kind: ErrorKind,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {:?}", self.context, self.kind)
    }
}

impl Error {
    pub const fn contextual<T: Into<ErrorKind>>(
        context: impl Into<Cow<'static, str>>,
    ) -> impl FnOnce(T) -> Self {
        move |kind| Self {
            context: context.into(),
            kind: kind.into(),
        }
    }
    pub fn new(context: impl Into<Cow<'static, str>>, kind: impl Into<ErrorKind>) -> Self {
        Self {
            context: context.into(),
            kind: kind.into(),
        }
    }
    pub fn unknown(context: impl Into<Cow<'static, str>>) -> Self {
        Self {
            context: context.into(),
            kind: ErrorKind::Custom("unknown error".into()),
        }
    }
}

macro_rules! error_kind {
    (
        pub enum $ErrorKind: ident {
            $($Kind: ident$(: $InnerType: ty)?),*
        }
    ) => {
        #[derive(Debug)]
        pub enum ErrorKind {
            $($Kind$(($InnerType))?,)*
        }
        $(
            $(
                impl From<$InnerType> for ErrorKind {
                    fn from(e: $InnerType) -> Self {
                        ErrorKind::$Kind(e)
                    }
                }
            )?
        )*
    };
}
error_kind! {
    pub enum ErrorKind {
        Durability: DurabilityError,
        Raft: crate::protocol::node::raft::RaftCommitError,
        Offline,
        Custom: Box<dyn std::error::Error + Send + Sync>,
        Io: std::io::Error
    }
}
