use std::{
    borrow::Cow,
    collections::HashMap,
    error::Error,
    future::poll_fn,
    sync::{Arc, Weak},
    task::{Context, Poll},
};

use auth::{ConnectionAuth, ConnectionAuthenticator};
use event::ConnectionEvent;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionId(u64);

pub struct Node {
    pub id: NodeId,
    pub connections: HashMap<ConnectionId, ConnectionRef>,
}


#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId {
    pub bytes: [u8; 16],
}

mod auth;
mod event;
mod routing;

#[derive(Debug, Clone)]
pub struct ConnectionRef {
    pub inner: Arc<Connection>,
}

#[derive(Debug)]
pub struct Connection {
    pub peer: NodeId,
    pub attached_node: Weak<Node>,
    pub auth: ConnectionAuth,
    pub authenticator: Box<dyn ConnectionAuthenticator>,
}

pub struct ConnectionError {
    pub error: Option<Box<dyn Error + Send>>,
    pub context: Cow<'static, str>,
}

impl ConnectionError {
    pub fn new(
        error: impl Into<Box<dyn Error + Send>>,
        context: impl Into<Cow<'static, str>>,
    ) -> Self {
        Self {
            error: Some(error.into()),
            context: context.into(),
        }
    }
    pub fn context(&self) -> &str {
        &self.context
    }
    pub fn from_context(context: impl Into<Cow<'static, str>>) -> Self {
        Self {
            error: None,
            context: context.into(),
        }
    }
}

impl Connection {
    pub fn close(&self) {
        unimplemented!("close")
    }
    pub fn send_event(&self, event: ConnectionEvent) {
        unimplemented!("send_event")
    }
    pub fn poll_next_event(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<ConnectionEvent, Box<dyn Error + Send>>>> {
        unimplemented!("poll_next_event")
    }

    pub async fn start(&self) -> Result<(), ConnectionError> {
        self.send_event(ConnectionEvent::Hello {
            auth: self.auth.clone(),
        });
        let peer_auth = match poll_fn(|cx| self.poll_next_event(cx)).await {
            Some(Ok(ConnectionEvent::Hello { auth })) => auth,
            Some(Ok(event)) => {
                return Err(ConnectionError::from_context("unexpected event"));
            }
            Some(Err(err)) => {
                return Err(ConnectionError::new(err, "poll first event"));
            }
            None => {
                return Err(ConnectionError::from_context("connection closed"));
            }
        };
        self.authenticator.auth_connect(peer_auth)?;
        // loop {
        //     match poll_fn(|cx| self.poll_next_event(cx)).await {
        //         Some(Ok(event)) => match event {
        //             ConnectionEvent::Hello { auth } => {}
        //             ConnectionEvent::Heartbeat { ts } => {}
        //             ConnectionEvent::EndpointOnline { endpoint } => {
        //                 self.authenticator.auth_endpoint_online(endpoint)?;
        //             }
        //             ConnectionEvent::EndpointOffline { endpoint } => {
        //                 self.authenticator.auth_endpoint_offline(endpoint)?;
        //             }
        //             ConnectionEvent::Event {
        //                 payload,
        //                 node_trace,
        //             } => {
        //                 self.authenticator.auth_event(payload)?;
        //             }
        //             ConnectionEvent::Close { reason } => {
        //                 return Err(ConnectionError::from_context("connection closed"));
        //             }
        //         },
        //         Some(Err(err)) => {
        //             return Err(ConnectionError::new(err, "poll next event"));
        //         }
        //         None => {
        //             return Err(ConnectionError::from_context("connection closed"));
        //         }
        //     }
        // }
        unimplemented!("start")
    }
}
