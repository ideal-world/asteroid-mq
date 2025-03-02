use std::str::FromStr;

use asteroid_mq::model::event::{json::Json, EventAttribute};
use asteroid_mq::prelude::{Interest, MessageAckExpectKind, Node, NodeConfig, Subject, TopicCode};
use asteroid_mq::protocol::node::raft::cluster::StaticClusterProvider;
use serde::{Deserialize, Serialize};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloWorld {
    pub number: u32,
    pub text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ByeWorld {
    pub texts: Vec<String>,
}

impl EventAttribute for HelloWorld {
    const SUBJECT: Subject = Subject::const_new("test/hello-world");
    const EXPECT_ACK_KIND: MessageAckExpectKind = MessageAckExpectKind::Processed;
}

impl EventAttribute for ByeWorld {
    const SUBJECT: Subject = Subject::const_new("test/bye-world");
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtherEvent {
    pub texts: Vec<String>,
}
impl EventAttribute for OtherEvent {
    const SUBJECT: Subject = Subject::const_new("other-test/bye-world");
    const BROADCAST: bool = true;
}
async fn hello_world_handler(Json(hello_world): Json<HelloWorld>) -> asteroid_mq::Result<()> {
    println!("Received hello world: {:?}", hello_world);
    Ok(())
}

#[tokio::test]
async fn test_create_handler_loop() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer().with_filter(
                tracing_subscriber::filter::EnvFilter::from_default_env()
                    .add_directive(tracing_subscriber::filter::Directive::from_str("info").unwrap())
                    .add_directive(
                        tracing_subscriber::filter::Directive::from_str("asteroid_mq=trace")
                            .unwrap(),
                    )
                    .add_directive(
                        tracing_subscriber::filter::Directive::from_str("openraft=info").unwrap(),
                    ),
            ),
        )
        .init();
    let node = Node::new(NodeConfig::default());
    let cluster_provider =
        StaticClusterProvider::singleton(node.id(), node.config().addr.to_string());
    node.start(cluster_provider).await?;
    const TOPIC_CODE: TopicCode = TopicCode::const_new("test");
    node.create_new_topic(TOPIC_CODE).await?;
    let edge_sender = asteroid_mq_sdk::ClientNode::connect_local_without_auth(node.clone())
        .await
        .unwrap();
    edge_sender
        .create_endpoint(TOPIC_CODE, [Interest::new("other-test/*")])
        .await?
        .into_event_loop()
        .with_handler(|event: Json<OtherEvent>| async move {
            println!("Received other event {:?}", event);
            asteroid_mq::Result::Ok(())
        })
        .spawn();
    let _evt_loop_handle = edge_sender
        .create_endpoint(TOPIC_CODE, [Interest::new("test/*")])
        .await?
        .into_event_loop()
        .with_handler(hello_world_handler)
        .with_handler(|Json(bye_world): Json<ByeWorld>| async move {
            println!("Received bye world: {:?}", bye_world);
            asteroid_mq::Result::Ok(())
        })
        .spawn();
    edge_sender
        .send_event(
            TOPIC_CODE,
            Json(HelloWorld {
                number: 42,
                text: "Hello, world!".to_string(),
            }),
        )
        .await?;
    edge_sender
        .send_event(
            TOPIC_CODE,
            Json(ByeWorld {
                texts: vec!["Goodbye, world!".to_string()],
            }),
        )
        .await?;
    edge_sender
        .send_event(
            TOPIC_CODE,
            Json(OtherEvent {
                texts: vec!["Other event".to_string()],
            }),
        )
        .await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    Ok(())
}
