use std::fmt::Debug;

use crate::{event::EventPayload, EndpointAddr};

use super::ConnectionError;

#[derive(Debug, Clone)]
pub struct ConnectionAuth {}

pub trait ConnectionAuthenticator: Debug {
    fn auth_connect(&self, auth: ConnectionAuth) -> Result<(), ConnectionError>;
    fn auth_endpoint_online(&self, addr: EndpointAddr) -> Result<(), ConnectionError>;
    fn auth_endpoint_offline(&self, addr: EndpointAddr) -> Result<(), ConnectionError>;
    fn auth_event(&self, event: EventPayload) -> Result<(), ConnectionError>;
}
