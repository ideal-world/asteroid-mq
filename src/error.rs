use std::borrow::Cow;

use crate::protocol::topic::durable_message::DurabilityError;

#[derive(Debug)]
pub struct Error {
    pub context: Cow<'static, str>,
    pub kind: ErrorKind,
}

impl Error {
    pub const fn contextual<T: Into<ErrorKind>>(context: impl Into<Cow<'static, str>>) -> impl FnOnce(T) -> Self {
        move |kind| {
            Self {
                context: context.into(),
                kind: kind.into(),
            }
        }
    }
    pub fn new(context: impl Into<Cow<'static, str>>, kind: impl Into<ErrorKind>) -> Self {
        Self {
            context: context.into(),
            kind: kind.into(),
        }
    }
}

macro_rules! error_kind {
    (
        pub enum $ErrorKind: ident {
            $($Kind: ident: $InnerType: ty)*
        }
    ) => {
        #[derive(Debug)]
        pub enum ErrorKind {
            $($Kind($InnerType),)*
        }
        $(
            impl From<$InnerType> for ErrorKind {
                fn from(e: $InnerType) -> Self {
                    ErrorKind::$Kind(e)
                }
            }
        )*
    };
}
error_kind! {
    pub enum ErrorKind {
        Durability: DurabilityError
    }
}
