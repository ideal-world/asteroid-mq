use futures_util::{Sink, Stream};
use std::borrow::Cow;

use crate::codec::CodecError;

use super::EdgePayload;

#[derive(Debug)]
pub struct EdgeConnectionError {
    pub kind: EdgeConnectionErrorKind,
    pub context: Cow<'static, str>,
}

impl std::fmt::Display for EdgeConnectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.context, self.kind)
    }
}

impl EdgeConnectionError {
    pub fn new(kind: EdgeConnectionErrorKind, context: impl Into<Cow<'static, str>>) -> Self {
        Self {
            kind,
            context: context.into(),
        }
    }
    pub const fn underlying<E: std::error::Error + Send + 'static>(
        context: impl Into<Cow<'static, str>>,
    ) -> impl FnOnce(E) -> Self {
        move |e| Self::new(EdgeConnectionErrorKind::Underlying(Box::new(e)), context)
    }
    pub const fn codec(context: impl Into<Cow<'static, str>>) -> impl FnOnce(CodecError) -> Self {
        move |e| Self::new(EdgeConnectionErrorKind::Codec(e), context)
    }
}
#[derive(Debug)]
pub enum EdgeConnectionErrorKind {
    Io(std::io::Error),
    Underlying(Box<dyn std::error::Error + Send>),
    Codec(CodecError),
    Closed,
    Timeout,
    Protocol,
    Existed,
    Reconnect,
}

impl std::fmt::Display for EdgeConnectionErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EdgeConnectionErrorKind::Io(e) => write!(f, "io error: {}", e),
            EdgeConnectionErrorKind::Underlying(e) => write!(f, "underlying error: {}", e),
            EdgeConnectionErrorKind::Codec(e) => write!(f, "codec error: {}", e),
            EdgeConnectionErrorKind::Closed => write!(f, "connection closed"),
            EdgeConnectionErrorKind::Timeout => write!(f, "timeout"),
            EdgeConnectionErrorKind::Protocol => write!(f, "protocol error"),
            EdgeConnectionErrorKind::Existed => write!(f, "existed"),
            EdgeConnectionErrorKind::Reconnect => write!(f, "reconnect"),
        }
    }
}

impl std::error::Error for EdgeConnectionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.kind {
            EdgeConnectionErrorKind::Io(e) => Some(e),
            EdgeConnectionErrorKind::Underlying(e) => Some(&**e),
            EdgeConnectionErrorKind::Codec(e) => Some(e),
            _ => None,
        }
    }

    fn description(&self) -> &str {
        match &self.kind {
            EdgeConnectionErrorKind::Io(_) => "io error",
            EdgeConnectionErrorKind::Underlying(_) => "underlying error",
            EdgeConnectionErrorKind::Codec(_) => "codec error",
            EdgeConnectionErrorKind::Closed => "connection closed",
            EdgeConnectionErrorKind::Timeout => "timeout",
            EdgeConnectionErrorKind::Protocol => "protocol error",
            EdgeConnectionErrorKind::Existed => "existed",
            EdgeConnectionErrorKind::Reconnect => "reconnect",
        }
    }
}

pub trait EdgeNodeConnection:
    Send
    + 'static
    + Stream<Item = Result<EdgePayload, EdgeConnectionError>>
    + Sink<EdgePayload, Error = EdgeConnectionError>
{
}
