use asteroid_mq_model::{connection::EdgeConnectionError, EdgeError, EdgeRequestEnum, EdgeResponseEnum, WaitAckError};
#[derive(Debug)]
pub struct ClientNodeError {
    pub kind: ClientErrorKind,
}

impl std::fmt::Display for ClientNodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.kind)
    }
}

impl std::error::Error for ClientNodeError {}

impl ClientNodeError {
    pub fn unexpected_response(response: EdgeResponseEnum) -> Self {
        ClientNodeError {
            kind: ClientErrorKind::UnexpectedResponse(response),
        }
    }
    pub fn disconnected() -> Self {
        ClientNodeError {
            kind: ClientErrorKind::Disconnected,
        }
    }
}

impl From<WaitAckError> for ClientNodeError {
    fn from(e: WaitAckError) -> Self {
        Self {
            kind: ClientErrorKind::WaitAck(e),
        }
    }
}
impl From<EdgeConnectionError> for ClientNodeError {
    fn from(e: EdgeConnectionError) -> Self {
        Self {
            kind: ClientErrorKind::Connection(e),
        }
    }
}

impl From<EdgeError> for ClientNodeError {
    fn from(value: EdgeError) -> Self {
        Self {
            kind: ClientErrorKind::Edge(value),
        }
    }
}
#[derive(Debug)]
pub enum ClientErrorKind {
    UnexpectedResponse(EdgeResponseEnum),
    Edge(EdgeError),
    Connection(EdgeConnectionError),
    NoConnection(EdgeRequestEnum),
    Disconnected,
    Io(std::io::Error),
    WaitAck(WaitAckError),
}

impl std::fmt::Display for ClientErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientErrorKind::UnexpectedResponse(response) => {
                write!(f, "Unexpected response: {:?}", response)
            }
            ClientErrorKind::Edge(e) => write!(f, "Edge error: {:?}", e),
            ClientErrorKind::Connection(e) => write!(f, "Edge connection error: {:?}", e),
            ClientErrorKind::NoConnection(req) => write!(f, "No connection for request: {:?}", req),
            ClientErrorKind::Disconnected => write!(f, "Disconnected"),
            ClientErrorKind::Io(e) => write!(f, "IO error: {:?}", e),
            ClientErrorKind::WaitAck(e) => write!(f, "WaitAck error: {:?}", e),
        }
    }
}
