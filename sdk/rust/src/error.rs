use asteroid_mq_model::{EdgeError, EdgeRequestEnum, EdgeResponseEnum, WaitAckError};
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
impl From<tokio_tungstenite::tungstenite::Error> for ClientNodeError {
    fn from(e: tokio_tungstenite::tungstenite::Error) -> Self {
        Self {
            kind: ClientErrorKind::Ws(e),
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
    Ws(tokio_tungstenite::tungstenite::Error),
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
            ClientErrorKind::Ws(e) => write!(f, "WebSocket error: {:?}", e),
            ClientErrorKind::NoConnection(req) => write!(f, "No connection for request: {:?}", req),
            ClientErrorKind::Disconnected => write!(f, "Disconnected"),
            ClientErrorKind::Io(e) => write!(f, "IO error: {:?}", e),
            ClientErrorKind::WaitAck(e) => write!(f, "WaitAck error: {:?}", e),
        }
    }
}
