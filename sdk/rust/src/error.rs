pub struct ClientNodeError {
    kind: ClientErrorKind,
}

impl From<tokio_tungstenite::tungstenite::Error> for ClientNodeError {
    fn from(e: tokio_tungstenite::tungstenite::Error) -> Self {
        Self {
            kind: ClientErrorKind::Ws(e),
        }
    }
}


pub enum ClientErrorKind {
    Ws(tokio_tungstenite::tungstenite::Error),
    Io(std::io::Error),
}