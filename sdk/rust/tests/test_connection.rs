use asteroid_mq_model::{EdgeMessage, Interest, Subject, TopicCode};
use asteroid_mq_sdk::ClientNode;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct HelloMessage {
    pub message: String,
}

async fn get_ws_url() -> Result<String, Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    const NODE_ID_API: &str = "http://localhost:8080/node_id";
    let node_id = client.put(NODE_ID_API).send().await?.text().await?;
    let url = format!("ws://localhost:8080/connect?node_id={node_id}");
    Ok(url)
}
#[tokio::test]
async fn test_connection() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();
    let url_a = get_ws_url().await?;
    let url_b = get_ws_url().await?;
    let node_a = ClientNode::connect(url_a).await?;
    let node_b = ClientNode::connect(url_b).await?;
    const TOPIC_CODE: TopicCode = TopicCode::const_new("test");
    fn message(message: &'static str) -> EdgeMessage {
        EdgeMessage::builder(
            TOPIC_CODE,
            [
                Subject::new("event/hello"),
                Subject::new("event/hello/avatar/b2"),
            ],
            message,
        )
        .build()
    }

    let mut endpoint_b1 = node_b
        .create_endpoint(TOPIC_CODE, [Interest::new("event/*")])
        .await?;
    let mut endpoint_b2 = node_b
        .create_endpoint(TOPIC_CODE, [Interest::new("event/**/b2")])
        .await?;

    endpoint_b1
        .update_interests([Interest::new("event/hello")])
        .await?;

    let task_b1 = tokio::spawn(async move {
        while let Some(message) = endpoint_b1.next_message().await {
            tracing::info!("Received message in b1: {:?}", &message.payload.0);
            let _result = message.ack_processed().await;
        }
    });
    let task_b2 = tokio::spawn(async move {
        while let Some(message) = endpoint_b2.next_message().await {
            tracing::info!("Received message in b2: {:?}", &message.payload.0);
            let _result = message.ack_processed().await;
        }
    });
    node_a.send_message(message("world")).await?;
    node_a.send_message(message("alice")).await?;
    node_a.send_message(message("bob")).await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    drop(node_a);
    drop(node_b);
    task_b1.await?;
    task_b2.await?;
    Ok(())
}
