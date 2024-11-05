use asteroid_mq::event_handler::json::Json;
use asteroid_mq::event_handler::EventAttribute;
use asteroid_mq::prelude::{Interest, MessageAckExpectKind, Node, NodeConfig, Subject, TopicCode};
use asteroid_mq::protocol::node::raft::cluster::StaticClusterProvider;
use serde::{Deserialize, Serialize};
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
    const SUBJECT: Subject = Subject::const_new("hello-world");
    const EXPECT_ACK_KIND: MessageAckExpectKind = MessageAckExpectKind::Processed;
}

impl EventAttribute for ByeWorld {
    const SUBJECT: Subject = Subject::const_new("bye-world");
}

async fn hello_world_handler(Json(hello_world): Json<HelloWorld>) -> asteroid_mq::Result<()> {
    println!("Received hello world: {:?}", hello_world);
    Ok(())
}

#[tokio::test]
async fn test_create_handler_loop() -> asteroid_mq::Result<()> {
    let node = Node::new(NodeConfig::default());
    let cluster_provider = StaticClusterProvider::singleton(node.id(), node.config().addr);
    node.init_raft(cluster_provider).await?;
    let topic = node.create_new_topic(TopicCode::const_new("test")).await?;
    let _evt_loop_handle = topic
        .create_endpoint([Interest::new("*")])
        .await?
        .create_event_loop()
        .with_handler(hello_world_handler)
        .with_handler(|Json(bye_world): Json<ByeWorld>| async move {
            println!("Received bye world: {:?}", bye_world);
            asteroid_mq::Result::Ok(())
        })
        .spawn();
    topic
        .send_event(Json(HelloWorld {
            number: 42,
            text: "Hello, world!".to_string(),
        }))
        .await?;
    topic
        .send_event(Json(ByeWorld {
            texts: vec!["Goodbye, world!".to_string()],
        }))
        .await?;

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    Ok(())
}
