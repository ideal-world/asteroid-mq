use asteroid_mq_model::{
    codec::{CodecKind, DynCodec},
    EdgeMessage, Interest, MessageAckExpectKind, MessageDurableConfig, Subject, TopicCode,
};
use asteroid_mq_sdk::ClientNode;
use chrono::{TimeDelta, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct HelloMessage {
    pub message: String,
}
const CODEC: &str = "bincode";

fn get_codec() -> DynCodec {
    let codec_kind = CODEC.parse::<CodecKind>().unwrap();
    DynCodec::form_kind(codec_kind).unwrap()
}

async fn get_ws_url() -> Result<String, Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    const NODE_ID_API: &str = "http://localhost:8080/node_id";
    let node_id = client.put(NODE_ID_API).send().await?.text().await?;
    let url = format!("ws://localhost:8080/connect?node_id={node_id}&codec={CODEC}");
    Ok(url)
}

#[tokio::test]
async fn test_connection() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();
    if std::env::var("K8S_PROVIDER").is_err() {
        let build_serve_result = tokio::process::Command::new("cargo")
            .arg("build")
            .arg("-p")
            .arg("asteroid-mq")
            .arg("--example")
            .arg("axum-server")
            .arg("--features")
            .arg("cluster-k8s")
            .spawn()?
            .wait()
            .await?;
        if !build_serve_result.success() {
            return Err("Failed to build axum-server example".into());
        }
        let _server_process = tokio::process::Command::new("cargo")
            .arg("run")
            .arg("-p")
            .arg("asteroid-mq")
            .arg("--example")
            .arg("axum-server")
            .arg("--features")
            .arg("cluster-k8s")
            .spawn()?;
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
    let url_a = get_ws_url().await?;
    let url_b = get_ws_url().await?;
    let node_a = ClientNode::connect_ws2(url_a, get_codec()).await?;
    let node_b = ClientNode::connect_ws2(url_b, get_codec()).await?;
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
    let send_task = tokio::spawn(async move {
        let send_result = node_a
            .send_message_and_wait(
                EdgeMessage::builder(
                    TOPIC_CODE,
                    [Subject::new("event/hello/durable")],
                    "durable message",
                )
                .mode_durable(
                    MessageDurableConfig::new(Utc::now() + TimeDelta::minutes(10))
                        .with_max_receiver(1),
                )
                .ack_kind(MessageAckExpectKind::Received)
                .build(),
            )
            .await;
        tracing::info!("Send result: {:?}", send_result);
        send_result
    });
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    let mut ep_durable_recv = node_b
        .create_endpoint(TOPIC_CODE, [Interest::new("event/hello/durable")])
        .await?;
    let message = ep_durable_recv
        .next_message()
        .await
        .expect("No message received");
    message.ack_received().await?;
    tracing::info!("Received durable message: {:?}", message);
    send_task.await??;
    drop(node_b);
    task_b1.await?;
    task_b2.await?;
    Ok(())
}
