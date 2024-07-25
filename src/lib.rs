use std::time::Duration;

use async_nats::{
    jetstream::{self, consumer},
    HeaderMap,
};
use bytes::Bytes;
use connection::ConnectionRef;
use event::{Event, EventHandler};
use futures_util::StreamExt;
use tokio::io::AsyncWriteExt;
use tracing::Level;

pub mod codec;
pub mod connection;
pub mod error;
pub mod event;
pub mod protocol;
pub struct Endpoint {
    pub connection: ConnectionRef,
    pub address: EndpointAddr,
}

impl Endpoint {
    pub async fn send() {
        unimplemented!()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct EndpointAddr {
    pub bytes: [u8; 16],
}
#[tokio::test]
pub async fn nats() {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();
    let client = async_nats::connect("localhost:4222").await.unwrap();
    let jetstream_context = async_nats::jetstream::new(client);
    // if let Err(e) = jetstream_context.delete_stream("EVENTS").await {
    //     println!("{:?}", e);
    // }
    let stream = jetstream_context
        .get_or_create_stream(jetstream::stream::Config {
            name: "EVENTS".to_string(),
            retention: jetstream::stream::RetentionPolicy::Limits,
            max_age: Duration::from_secs(18000),
            subjects: vec!["events.>".to_string()],
            ..Default::default()
        })
        .await
        .unwrap();
    let consumer = stream
        .get_or_create_consumer(
            "processor-1",
            consumer::pull::Config {
                durable_name: Some("processor-1".to_string()),
                filter_subjects: vec!["events.*.user-1".to_string()],
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let mut messages = consumer.messages().await.unwrap();
    while let Ok(m) = messages.next().await.unwrap() {
        let _ = m.double_ack().await;
        tracing::info!("pld: {:?}", m.payload);
    }
}

#[tokio::test]
pub async fn nats_cli() {
    let client = async_nats::connect("localhost:4222").await.unwrap();
    let jetstream_context = async_nats::jetstream::new(client);
    let ack = jetstream_context
        .publish("events.hello.user-1", "data2".into())
        .await
        .unwrap();
    let ack = ack.await.unwrap();
    println!("{:?}", ack);
}
