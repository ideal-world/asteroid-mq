use std::error::Error;

pub struct EventHandleError {}

pub enum EventHandleErrorKind {
    Decode(Box<dyn Error + Send>),
    Process(Box<dyn Error + Send>),
}
